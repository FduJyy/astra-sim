#include "astra-sim/node/DmaEngine.hh"

namespace AstraSim::NodeSim {
DmaEngine::DmaEngine(const NodeTopology& topology,
                     NodeEventScheduler& clock,
                     MemorySystem& memory,
                     NodeStatistics& statistics)
    : topology_(topology),
      clock_(clock),
      memory_(memory),
      statistics_(statistics) {
    for (const auto& config : topology.configuration()["dma_engines"]) {
        engines_.emplace(config["id"].get<std::string>(), Engine{config, {}});
    }
}
bool DmaEngine::copy(BufferRef source,
                     BufferRef destination,
                     const std::string& requester,
                     const std::string& binding,
                     Done completed,
                     RequestContext context) {
    check(source.bytes == destination.bytes,
          "DMA source/destination length mismatch");
    check(!engines_.empty() && (binding.empty() || engines_.count(binding)),
          "DMA binding not available");
    check(topology_.allowed(source.storage, requester) &&
              topology_.allowed(destination.storage, requester),
          "DMA StoragePermission requester=" + requester);
    if (source.storage == destination.storage && source.id == destination.id) {
        check(add(source.offset, source.bytes) <= destination.offset ||
                  add(destination.offset, destination.bytes) <= source.offset,
              "DMA overlapping source and destination");
    }
    memory_.validate(source, true);
    memory_.validate(destination, false);
    uint64_t used = waiting_.size(), capacity = 0;
    for (const auto& [id, engine] : engines_) {
        used = add(used, engine.queue.size() + (engine.active ? 1 : 0));
        capacity = add(capacity, number(engine.config, "queue_entries", id));
    }
    if (used >= capacity) {
        return false;
    }
    if (!binding.empty()) {
        const auto& engine = engines_.at(binding);
        uint64_t reserved = engine.queue.size() + (engine.active ? 1 : 0);
        for (const auto& job : waiting_) {
            if (job->binding.empty() || job->binding == binding) {
                ++reserved;
            }
        }
        if (reserved >= number(engine.config, "queue_entries", binding)) {
            return false;
        }
    }
    memory_.retain(source, true);
    memory_.retain(destination, false);
    auto job = std::make_shared<Job>();
    job->source = source;
    job->destination = destination;
    job->requester = requester;
    job->binding = binding;
    job->completed = std::move(completed);
    job->context = statistics_.child(context);
    job->enqueue = clock_.now();
    waiting_.push_back(job);
    clock_.after(0, [this] { dispatch(); });
    return true;
}
void DmaEngine::dispatch() {
    for (auto it = waiting_.begin(); it != waiting_.end();) {
        auto best = engines_.end();
        uint64_t least = UINT64_MAX;
        for (auto& [id, engine] : engines_) {
            if (!(*it)->binding.empty() && (*it)->binding != id) {
                continue;
            }
            const auto used = engine.queue.size() + (engine.active ? 1 : 0);
            if (used >= number(engine.config, "queue_entries", id)) {
                continue;
            }
            if (used < least) {
                least = used;
                best = engines_.find(id);
            }
        }
        if (best == engines_.end()) {
            ++it;
        } else {
            best->second.queue.push_back(*it);
            best->second.peak = std::max(best->second.peak, least + 1);
            it = waiting_.erase(it);
        }
    }
    for (auto& [id, engine] : engines_) {
        if (engine.active || engine.queue.empty()) {
            continue;
        }
        engine.active = engine.queue.front();
        engine.queue.pop_front();
        statistics_.add_time(engine.active->context.operation, "dma_wait_ns",
                             clock_.now() - engine.active->enqueue);
        statistics_.record(
            engine.active->context, "dma_start", id,
            {{"bytes", engine.active->source.bytes},
             {"source_storage", engine.active->source.storage},
             {"destination_storage", engine.active->destination.storage}});
        // The endpoint routes are checked before issuing any source traffic.
        topology_.route(id, engine.active->source.storage);
        topology_.route(id, engine.active->destination.storage);
        clock_.after(number(engine.config, "startup_ns", id, true),
                     [this, id] { issue(id); });
    }
}
void DmaEngine::issue(const std::string& id) {
    auto& engine = engines_.at(id);
    auto job = engine.active;
    while (job->inflight < number(engine.config, "max_outstanding", id) &&
           job->next < job->source.bytes) {
        const auto offset = job->next,
                   bytes = std::min(number(engine.config, "burst_bytes", id),
                                    job->source.bytes - offset);
        job->next += bytes;
        ++job->inflight;
        auto source = job->source, destination = job->destination;
        source.offset = add(source.offset, offset);
        destination.offset = add(destination.offset, offset);
        source.bytes = destination.bytes = bytes;
        memory_.access(
            id, job->requester, source, false,
            [this, id, job, destination, bytes] {
                auto& engine = engines_.at(id);
                const auto begin = std::max(clock_.now(), engine.port);
                engine.port = add(
                    begin,
                    service(bytes, rate(engine.config, "bytes_per_ns", id)));
                clock_.after(engine.port - clock_.now(), [this, id, job,
                                                          destination, bytes] {
                    memory_.access(
                        id, job->requester, destination, true,
                        [this, id, job, bytes] {
                            --job->inflight;
                            job->finished += bytes;
                            engines_.at(id).bytes =
                                add(engines_.at(id).bytes, bytes);
                            if (job->finished == job->source.bytes) {
                                memory_.mark_valid(job->destination);
                                memory_.unretain(job->source);
                                memory_.unretain(job->destination);
                                engines_.at(id).active.reset();
                                statistics_.record(
                                    job->context, "dma_complete", id,
                                    {{"bytes", job->source.bytes}});
                                if (capacity_) {
                                    clock_.after(0, capacity_, 0);
                                }
                                clock_.after(0, job->completed, 1);
                                clock_.after(0, [this] { dispatch(); });
                            } else {
                                issue(id);
                            }
                        },
                        false, job->context);
                });
            },
            true, job->context);
    }
}
bool DmaEngine::idle() const {
    if (!waiting_.empty()) {
        return false;
    }
    for (const auto& [id, engine] : engines_) {
        if (engine.active || !engine.queue.empty()) {
            return false;
        }
    }
    return true;
}
json DmaEngine::statistics() const {
    json result = json::object();
    for (const auto& [id, engine] : engines_) {
        result[id] = {{"bytes", engine.bytes},
                      {"descriptor_queue_peak", engine.peak}};
    }
    return result;
}
}  // namespace AstraSim::NodeSim
