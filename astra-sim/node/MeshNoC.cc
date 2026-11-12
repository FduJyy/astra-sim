#include "astra-sim/node/MeshNoC.hh"
#include <algorithm>

namespace AstraSim::NodeSim {
MeshNoC::MeshNoC(const NodeTopology& topology,
                 NodeEventScheduler& clock,
                 NodeStatistics& statistics)
    : topology_(topology),
      clock_(clock),
      statistics_(statistics) {
    for (const auto& [id, description] : topology.links()) {
        links_.emplace(id, Link{});
    }
}
bool MeshNoC::send(const std::string& src,
                   const std::string& dst,
                   uint64_t bytes,
                   bool data,
                   Receiver receiver,
                   Done delivered,
                   RequestContext context) {
    const unsigned traffic = data ? 1 : 0;
    const auto limit = number(topology_.configuration()["noc"],
                              "input_buffer_packets_per_class", "noc");
    auto& slots = source_slots_[{src, traffic}];
    if (slots >= limit) {
        return false;
    }
    auto transfer = std::make_shared<Transfer>();
    transfer->source = src;
    transfer->destination = dst;
    transfer->route = topology_.route(src, dst);
    transfer->remaining = bytes;
    transfer->data = data;
    transfer->receiver = std::move(receiver);
    transfer->delivered = std::move(delivered);
    transfer->context = context;
    ++slots;
    ++active_;
    inject(transfer);
    return true;
}
void MeshNoC::inject(const std::shared_ptr<Transfer>& transfer) {
    auto packet = std::make_shared<Packet>();
    packet->transfer = transfer;
    packet->id = statistics_.next_id();
    packet->payload = std::min(transfer->remaining, topology_.packet_bytes());
    packet->wire = add(packet->payload, topology_.header_bytes());
    packet->traffic = transfer->data ? 1 : 0;
    const auto delay =
        transfer->route.empty()
            ? 0
            : topology_.links().at(transfer->route.front()).router_latency;
    packet->ready = add(clock_.now(), delay);
    injection_[transfer->source + ":" + std::to_string(packet->traffic)]
        .push_back(packet);
    clock_.after(delay, [this] { wake(); });
}
void MeshNoC::wake() {
    if (pending_) {
        return;
    }
    pending_ = true;
    clock_.after(0, [this] {
        pending_ = false;
        pump();
    });
}
void MeshNoC::pump() {
    using Queue = std::deque<std::shared_ptr<Packet>>;
    std::vector<std::pair<std::string, Queue*>> inputs;
    for (auto& [id, queue] : injection_) {
        inputs.push_back({"inject:" + id, &queue});
    }
    for (auto& [id, link] : links_) {
        for (unsigned c = 0; c < 2; ++c) {
            inputs.push_back({id + ":" + std::to_string(c), &link.arrived[c]});
        }
    }
    // Endpoint consumption releases the last input credit only when the
    // consumer accepts.
    for (auto& [name, queue] : inputs) {
        if (queue->empty()) {
            continue;
        }
        auto packet = queue->front();
        const auto& t = packet->transfer;
        if (packet->hop != t->route.size()) {
            continue;
        }
        const bool final = packet->payload == t->remaining;
        if (final && t->receiver && !t->receiver()) {
            continue;
        }
        statistics_.add_time(t->context.operation, "noc_wait_ns",
                             clock_.now() - packet->ready);
        statistics_.record(
            t->context, "packet_delivered", t->destination,
            {{"packet", packet->id}, {"bytes", packet->payload}});
        queue->pop_front();
        if (!packet->input.empty()) {
            --links_.at(packet->input).occupied[packet->traffic];
        }
        t->remaining -= packet->payload;
        if (final) {
            --active_;
            --source_slots_.at({t->source, t->data ? 1u : 0u});
            if (t->delivered) {
                clock_.after(0, t->delivered, 1);
            }
            if (capacity_) {
                clock_.after(0, capacity_, 0);
            }
        } else {
            inject(t);
        }
        wake();
    }
    for (auto& [id, output] : links_) {
        if (output.busy || inputs.empty()) {
            continue;
        }
        const auto& description = topology_.links().at(id);
        for (size_t offset = 0; offset < inputs.size(); ++offset) {
            const auto index = (output.cursor + offset) % inputs.size();
            auto* queue = inputs[index].second;
            if (queue->empty()) {
                continue;
            }
            auto packet = queue->front();
            if (packet->hop >= packet->transfer->route.size() ||
                packet->transfer->route[packet->hop] != id) {
                continue;
            }
            if (packet->ready > clock_.now()) {
                continue;
            }
            if (output.occupied[packet->traffic] >= description.buffers) {
                continue;
            }
            queue->pop_front();
            if (!packet->input.empty()) {
                --links_.at(packet->input).occupied[packet->traffic];
            }
            ++output.occupied[packet->traffic];
            output.peak =
                std::max(output.peak, output.occupied[0] + output.occupied[1]);
            output.cursor = (index + 1) % inputs.size();
            output.busy = true;
            statistics_.add_time(packet->transfer->context.operation,
                                 "noc_wait_ns", clock_.now() - packet->ready);
            statistics_.record(packet->transfer->context, "link_transmit", id,
                               {{"packet", packet->id},
                                {"bytes", packet->wire},
                                {"port", description.destination}});
            const auto serialization =
                service(packet->wire, description.bandwidth);
            output.busy_ns = add(output.busy_ns, serialization);
            output.bytes = add(output.bytes, packet->wire);
            ++output.packets;
            // Routing happens before wire service; propagation does not hold
            // the wire.
            clock_.after(
                serialization,
                [this, id] {
                    links_.at(id).busy = false;
                    wake();
                },
                0);
            const auto arrival = add(serialization, description.latency);
            clock_.after(
                arrival,
                [this, id, packet] {
                    ++packet->hop;
                    packet->input = id;
                    const auto delay =
                        packet->hop < packet->transfer->route.size()
                            ? topology_.links()
                                  .at(packet->transfer->route[packet->hop])
                                  .router_latency
                            : 0;
                    packet->ready = add(clock_.now(), delay);
                    links_.at(id).arrived[packet->traffic].push_back(packet);
                    clock_.after(delay, [this] { wake(); });
                },
                1);
            wake();  // upstream credit became available
            break;
        }
    }
}
bool MeshNoC::idle() const {
    return active_ == 0;
}
json MeshNoC::statistics() const {
    json result = json::object();
    for (const auto& [id, link] : links_) {
        const auto& config = topology_.links().at(id);
        result[id] = {{"wire_bytes", link.bytes},
                      {"packets", link.packets},
                      {"busy_ns", link.busy_ns},
                      {"buffer_peak_packets", link.peak},
                      {"occupied_control_packets", link.occupied[0]},
                      {"occupied_data_packets", link.occupied[1]},
                      {"bytes_per_ns", config.bandwidth},
                      {"latency_ns", config.latency},
                      {"router_latency_ns", config.router_latency},
                      {"buffer_packets_per_class", config.buffers}};
        json waiting = json::array();
        for (const auto& queue : link.arrived) {
            if (!queue.empty()) {
                const auto& packet = queue.front();
                const auto& c = packet->transfer->context;
                waiting.push_back(
                    {{"op", c.operation},
                     {"txn", c.transaction},
                     {"stage", c.stage},
                     {"destination", packet->transfer->destination},
                     {"hop", packet->hop}});
            }
        }
        result[id]["waiting_heads"] = waiting;
    }
    return result;
}
}  // namespace AstraSim::NodeSim
