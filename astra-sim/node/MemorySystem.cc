#include "astra-sim/node/MemorySystem.hh"

namespace AstraSim::NodeSim {
MemorySystem::MemorySystem(const NodeTopology& topology,
                           NodeEventScheduler& clock,
                           NodeStatistics& statistics)
    : topology_(topology),
      clock_(clock),
      statistics_(statistics),
      network_(topology, clock, statistics) {
    for (const auto& configuration : topology.configuration()["storages"]) {
        const auto id = configuration["id"].get<std::string>();
        storages_[id] =
            std::make_unique<StorageModule>(configuration, clock, statistics);
        storages_[id]->on_capacity([this] { network_.wake(); });
    }
    network_.on_capacity([this] {
        for (auto& [id, storage] : storages_) {
            storage->drain_completions();
        }
        retry_requests();
    });
}
void MemorySystem::transmit(std::string source,
                            std::string destination,
                            uint64_t bytes,
                            bool data,
                            MeshNoC::Receiver receiver,
                            Done done,
                            RequestContext context) {
    auto request = [this, source, destination, bytes, data,
                    receiver = std::move(receiver), done = std::move(done),
                    context] {
        return network_.send(source, destination, bytes, data, receiver, done,
                             context);
    };
    if (!request()) {
        pending_requests_.push_back(std::move(request));
    }
}
void MemorySystem::retry_requests() {
    const auto count = pending_requests_.size();
    for (size_t i = 0; i < count; ++i) {
        auto request = std::move(pending_requests_.front());
        pending_requests_.pop_front();
        if (!request()) {
            pending_requests_.push_back(std::move(request));
        }
    }
}
void MemorySystem::payload(const std::string& source,
                           const std::string& destination,
                           uint64_t bytes,
                           Done done,
                           RequestContext context) {
    transmit(source, destination, bytes, true, {}, std::move(done), context);
}
BufferRef MemorySystem::resolve(const json& value,
                                const std::string& local) const {
    fields(value, {"storage", "buffer_id", "generation", "offset", "bytes"},
           "BufferRef");
    auto storage = text(value, "storage", "BufferRef");
    if (storage == "$cu.local_storage") {
        check(!local.empty(), "CU has no local_storage");
        storage = local;
    }
    check(storages_.count(storage),
          "BufferRef.storage: unknown storage " + storage);
    return {storage, text(value, "buffer_id", "BufferRef"),
            number(value, "generation", "BufferRef", true),
            number(value, "offset", "BufferRef", true),
            number(value, "bytes", "BufferRef")};
}
void MemorySystem::allocate(json request, RequestContext context) {
    const auto storage = text(request, "storage", "allocate"),
               id = text(request, "buffer_id", "allocate");
    check(storages_.count(storage),
          "allocate.storage: unknown storage " + storage);
    check(!locations_.count(id), "DuplicateAllocation buffer=" + id);
    const auto generation = number(request, "generation", "allocate", true);
    if (generations_.count(id)) {
        check(generation == add(generations_.at(id), 1),
              "StaleGeneration buffer=" + id);
    }
    storages_.at(storage)->allocate(request, context);
    locations_[id] = {storage, generation};
    generations_[id] = generation;
}
void MemorySystem::release(const json& request, RequestContext context) {
    fields(request, {"storage", "buffer_id", "generation"}, "free");
    const auto storage = text(request, "storage", "free"),
               id = text(request, "buffer_id", "free");
    check(storages_.count(storage), "free.storage: unknown storage");
    storages_.at(storage)->release(
        id, number(request, "generation", "free", true), context);
    locations_.erase(id);
}
void MemorySystem::validate(const BufferRef& ref, bool read) const {
    storages_.at(ref.storage)->validate(ref, read);
}
void MemorySystem::retain(const BufferRef& ref, bool read) {
    storages_.at(ref.storage)->retain(ref, read);
}
void MemorySystem::unretain(const BufferRef& ref) {
    storages_.at(ref.storage)->unretain(ref);
}
void MemorySystem::mark_valid(const BufferRef& ref) {
    storages_.at(ref.storage)->mark_valid(ref);
}
void MemorySystem::access(const std::string& endpoint,
                          const std::string& requester,
                          BufferRef ref,
                          bool write,
                          Done completed,
                          bool publish,
                          RequestContext context) {
    check(topology_.allowed(ref.storage, requester),
          "StoragePermission requester=" + requester +
              " storage=" + ref.storage);
    topology_.route(endpoint, ref.storage);
    topology_.route(ref.storage, endpoint);
    retain(ref, !write);
    ++active_;
    const auto started = clock_.now();
    context = statistics_.child(context);
    statistics_.record(context, "memory_begin", endpoint,
                       {{"storage_id", ref.storage},
                        {"buffer_id", ref.id},
                        {"generation", ref.generation},
                        {"offset", ref.offset},
                        {"bytes", ref.bytes}});
    struct Transfer {
        uint64_t next = 0, inflight = 0, finished = 0;
        Done issue;
    };
    auto state = std::make_shared<Transfer>();
    std::weak_ptr<Transfer> weak = state;
    state->issue = [this, weak, endpoint, ref, write, publish, context, started,
                    completed = std::move(completed)] {
        auto state = weak.lock();
        check(bool(state), "memory transfer lost");
        while (state->inflight < 8 && state->next < ref.bytes) {
            BufferRef part = ref;
            part.offset = add(ref.offset, state->next);
            part.bytes = std::min(uint64_t{256}, ref.bytes - state->next);
            state->next += part.bytes;
            ++state->inflight;
            auto transaction = statistics_.child(context);
            auto finish = [this, state, ref, part, write, publish, completed,
                           transaction, started] {
                --state->inflight;
                state->finished += part.bytes;
                if (state->finished == ref.bytes) {
                    if (write && publish) {
                        mark_valid(ref);
                    }
                    unretain(ref);
                    --active_;
                    statistics_.add_time(transaction.operation,
                                         "memory_stall_ns",
                                         clock_.now() - started);
                    statistics_.record(
                        transaction, "memory_complete", ref.storage,
                        {{"buffer_id", ref.id}, {"bytes", ref.bytes}});
                    clock_.after(0, completed, 1);
                } else {
                    clock_.after(0, [state] { state->issue(); });
                }
            };
            auto service_done = [this, endpoint, part, write, finish,
                                 transaction] {
                // A full injection port retains the finite storage service
                // slot.
                return network_.send(part.storage, endpoint,
                                     write ? 0 : part.bytes, !write, {}, finish,
                                     transaction);
            };
            auto receive = [this, part, write, service_done, transaction] {
                return storages_.at(part.storage)
                    ->try_service(part, write, service_done, transaction);
            };
            transmit(endpoint, ref.storage, write ? part.bytes : 0, write,
                     receive, {}, transaction);
        }
    };
    clock_.after(0, [state] { state->issue(); });
}
json MemorySystem::statistics() const {
    json result = {
        {"storages", json::object()},
        {"links", network_.statistics()},
        {"memory_pending_initiator_requests", pending_requests_.size()},
        {"memory_active_requests", active_},
        {"noc_idle", network_.idle()}};
    for (const auto& [id, storage] : storages_) {
        result["storages"][id] = storage->statistics();
    }
    return result;
}
}  // namespace AstraSim::NodeSim
