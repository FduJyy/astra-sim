#include "astra-sim/node/NodeSystem.hh"
#include "astra-sim/system/IntData.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/astraccl/native_collectives/collective_algorithm/Ring.hh"

namespace AstraSim {
NodeSystem::NodeSystem(Sys& sys, const std::string& filename)
    : NodeSystem(sys, NodeSim::NodeModel::load(filename)) {}
NodeSystem::NodeSystem(Sys& sys,
                       std::pair<NodeSim::json, NodeSim::json> configuration)
    : scheduler(
          [&sys] { return uint64_t(sys.comm_NI->sim_get_time().time_val); },
          [this, &sys](uint64_t delay, NodeSim::Done done) {
              if (delay == 0) {
                  immediate_.push_back(std::move(done));
                  return;
              }
              auto* wake = new Wake;
              wake->callback = std::move(done);
              sys.register_event(this, EventType::General, wake, delay);
          }),
      model(std::move(configuration.first),
            std::move(configuration.second),
            scheduler,
            sys.id),
      sys_(sys) {
    NodeSim::check(
        sys.comm_NI->supports_node_model(),
        "detailed mode currently requires analytical congestion-aware backend");
    NodeSim::NodeModel::Transport transport;
    transport.send = [this](int peer, int tag, uint64_t bytes,
                            NodeSim::Done done) {
        NodeSim::check(peer >= 0 && peer < sys_.total_nodes,
                       "send peer outside rank space");
        auto* callback = new NetworkCallback{this, std::move(done), {}};
        callback->request.srcRank = sys_.id;
        callback->request.dstRank = peer;
        callback->request.reqCount = bytes;
        callback->request.tag = tag;
        callback->request.reqType = UINT8;
        sys_.sim_send(0, Sys::dummy_data, bytes, UINT8, peer, tag,
                      &callback->request, &NodeSystem::network_completed,
                      callback);
    };
    transport.recv = [this](int peer, int tag, uint64_t bytes,
                            NodeSim::Done done) {
        NodeSim::check(peer >= 0 && peer < sys_.total_nodes,
                       "receive peer outside rank space");
        auto* callback = new NetworkCallback{this, std::move(done), {}};
        callback->request.srcRank = peer;
        callback->request.dstRank = sys_.id;
        callback->request.reqCount = bytes;
        callback->request.tag = tag;
        callback->request.reqType = UINT8;
        sys_.sim_recv(0, Sys::dummy_data, bytes, UINT8, peer, tag,
                      &callback->request, &NodeSystem::network_completed,
                      callback);
    };
    transport.collective = [this](uint64_t id, const NodeSim::json& profile,
                                  NodeSim::Done done) {
        start_collective(id, profile, std::move(done));
    };
    transport.remote = [this](uint64_t bytes, NodeSim::Done done) {
        sys_.workload->issue_node_remote(bytes, std::move(done));
    };
    model.transport(std::move(transport));
}
void NodeSystem::call(EventType event, CallData* data) {
    if (failed()) {
        return;
    }
    if (event == EventType::CollectiveCommunicationFinished) {
        const auto id = static_cast<IntData*>(data)->data;
        auto completion = datasets_.at(id);
        datasets_.erase(id);
        scheduler.after(
            0,
            [completion] {
                delete completion.dataset;
                completion.callback();
            },
            3);
        return;
    }
    std::unique_ptr<Wake> wake(static_cast<Wake*>(data));
    auto callback = std::move(wake->callback);
    wake.reset();
    callback();
    drain();
}
void NodeSystem::drain() {
    if (draining_ || failed()) {
        return;
    }
    draining_ = true;
    try {
        do {
            while (!immediate_.empty()) {
                auto callback = std::move(immediate_.front());
                immediate_.pop_front();
                callback();
            }
            // Transport/resource callbacks can finish after the last ET
            // completion observer. Recheck termination at the adapter's actual
            // quiescent point.
            if (sys_.workload && model.idle() && !sys_.workload->is_finished) {
                sys_.workload->issue_detailed_nodes();
            }
        } while (!immediate_.empty());
    } catch (...) {
        fail(std::current_exception());
    }
    draining_ = false;
}
void NodeSystem::fail(std::exception_ptr error) {
    if (!failure_) {
        failure_ = std::move(error);
    }
}
bool NodeSystem::failed() const {
    return bool(failure_);
}
void NodeSystem::rethrow_if_failed() const {
    if (failure_) {
        std::rethrow_exception(failure_);
    }
}
void NodeSystem::after_current_batch(NodeSim::Done callback) {
    // The remote backend may have a sibling callback sharing the same data.
    // Append disposal after that sibling in Sys's existing current-time bucket.
    auto* wake = new Wake;
    wake->callback = std::move(callback);
    sys_.register_event(this, EventType::General, wake, 0);
}
void NodeSystem::network_completed(void* argument) {
    std::unique_ptr<NetworkCallback> callback(
        static_cast<NetworkCallback*>(argument));
    auto* owner = callback->owner;
    if (!owner->failed()) {
        try {
            owner->scheduler.after(0, std::move(callback->callback), 1);
            owner->drain();
        } catch (...) {
            owner->fail(std::current_exception());
        }
    }
}
void NodeSystem::start_collective(uint64_t id,
                                  const NodeSim::json& profile,
                                  NodeSim::Done done) {
    const auto& participants = profile.at("participants");
    auto node = sys_.workload->et_feeder->lookupNode(id);
    CommunicatorGroup* group = nullptr;
    const auto pg = node->pg_name<std::string>("");
    if (!pg.empty() && pg != "0") {
        group = sys_.workload->comm_groups.at(std::stoi(pg));
    }
    std::vector<int> expected;
    if (group) {
        expected = group->involved_NPUs;
    } else {
        for (int i = 0; i < sys_.total_nodes; ++i) {
            expected.push_back(i);
        }
    }
    NodeSim::check(participants == expected,
                   "collective participants differ from ET communicator");
    const auto kind = profile.at("operation").get<std::string>();
    const std::map<std::string, std::pair<ComType, int>> kinds = {
        {"all_reduce", {ComType::All_Reduce, 0}},
        {"all_gather", {ComType::All_Gather, 2}},
        {"all_to_all", {ComType::All_to_All, 6}},
        {"reduce_scatter", {ComType::Reduce_Scatter, 7}}};
    NodeSim::check(kinds.count(kind) &&
                       int(node->comm_type()) == kinds.at(kind).second,
                   "ET/profile collective operation mismatch");
    const auto input = profile.at("input").at("bytes").get<uint64_t>();
    NodeSim::check(input == node->comm_size(),
                   "ET/profile collective comm_size mismatch");
    const auto key = participants.dump() + "@" + (pg.empty() ? "0" : pg);
    auto& registry = collective_registry_[key];
    const auto ordinal = registry.size();
    NodeSim::json signature = {
        {"instance_id", profile.at("instance_id")},
        {"operation", kind},
        {"input_bytes", input},
        {"output_bytes", profile.at("output").at("bytes")},
        {"elem_bytes", profile.at("elem_bytes")}};
    for (auto rank : expected) {
        auto* peer = Sys::all_sys.at(rank);
        NodeSim::check(peer && peer->node_system,
                       "collective participant has no detailed node");
        auto it = peer->node_system->collective_registry_.find(key);
        if (it != peer->node_system->collective_registry_.end() &&
            it->second.size() > ordinal) {
            NodeSim::check(it->second[ordinal] == signature,
                           "CollectiveMismatch communicator=" + key +
                               " ordinal=" + std::to_string(ordinal));
        }
    }
    registry.push_back(signature);
    const auto implementations =
        sys_.collective_impl_lookup->get_collective_impl(kinds.at(kind).first,
                                                         id);
    for (auto* implementation : implementations) {
        NodeSim::check(implementation->type == CollectiveImplType::Ring ||
                           implementation->type == CollectiveImplType::OneRing,
                       "detailed collective supports Ring only");
    }
    if (participants.size() == 1) {
        scheduler.after(0, std::move(done), 3);
        return;
    }
    NodeSim::check(sys_.preferred_dataset_splits > 0,
                   "preferred-dataset-splits must be positive");
    const auto splits = uint64_t(sys_.preferred_dataset_splits),
               members = participants.size();
    const auto stream_start = group ? group->num_streams : sys_.num_streams;
    const auto base = group ? uint64_t(std::stoi(pg)) * 1000000 : 0;
    NodeSim::check(stream_start >= 0 && uint64_t(stream_start) >= base &&
                       NodeSim::add(uint64_t(stream_start), splits) <=
                           base + 1000000,
                   "detailed communicator stream/tag namespace exhausted");
    NodeSim::check(input % splits == 0 && (kind == "all_gather" ||
                                           (input / splits) % members == 0),
                   "collective requires equal, nonempty Ring chunks");
    const auto message_bytes =
        kind == "all_gather" ? input / splits : input / splits / members;
    NodeSim::check(
        message_bytes > 0 &&
            message_bytes % profile.at("elem_bytes").get<uint64_t>() == 0,
        "collective Ring message is not element-aligned");
    const auto& nic = model.topology().configuration().at("network_endpoint");
    NodeSim::check(
        message_bytes <= nic.at("tx_buffer_bytes").get<uint64_t>() &&
            message_bytes <= nic.at("rx_buffer_bytes").get<uint64_t>(),
        "CapacityExceeded NIC collective message exceeds staging capacity");
    // Detailed data offsets use a flat logical Ring over this communicator.
    // The external backend still routes every transfer through its full
    // physical topology.
    auto logical = std::make_shared<RingTopology>(
        RingTopology::Dimension::Local, sys_.id, expected);
    DataSet* dataset = sys_.generate_collective(
        input, logical.get(), {implementations.front()}, {true},
        kinds.at(kind).first, 0, group, id);
    NodeSim::check(dataset->active,
                   "collective produced no active Ring streams");
    datasets_[dataset->my_id] = {dataset, std::move(done), logical};
    dataset->set_notifier(this, EventType::CollectiveCommunicationFinished);
}
void NodeSystem::ring_transfer(Ring& ring, NodeSim::Done completed) {
    using namespace NodeSim;
    const auto id = ring.stream->dataset->node_operation;
    const auto& profile = model.operation_profile(id);
    const auto count = int(profile.at("participants").size());
    check(ring.nodes_in_ring == count,
          "Ring phase/context membership mismatch");
    const auto index =
        static_cast<RingTopology*>(ring.logical_topo)->get_index_in_ring();
    const int sign =
        ring.direction == RingTopology::Direction::Clockwise ? 1 : -1;
    auto mod = [count](int value) {
        value %= count;
        return value < 0 ? value + count : value;
    };
    auto ref = [](const json& b) {
        return BufferRef{b.at("storage"), b.at("buffer_id"), b.at("generation"),
                         b.at("offset"), b.at("bytes")};
    };
    const auto input = ref(profile.at("input")),
               output = ref(profile.at("output"));
    auto source = model.operation_workspace(id), destination = source;
    const auto offset = ring.stream->node_chunk_offset, bytes = ring.msg_size;
    const auto original = profile.at("operation").get<std::string>();
    const auto round = int(ring.total_packets_sent);
    bool reduce = false;
    if (ring.comType == ComType::All_to_All) {
        int distance = 1, hop = round;
        while (hop >= distance) {
            hop -= distance;
            ++distance;
        }
        check(distance < count, "invalid all-to-all Ring round");
        if (hop == 0) {
            source = input;
            source.offset =
                add(input.offset, add(uint64_t(mod(index + sign * distance)) *
                                          (input.bytes / count),
                                      offset / count));
        } else {
            source.offset = add(source.offset, offset / count);
        }
        if (hop == distance - 1) {
            destination = output;
            destination.offset =
                add(output.offset, add(uint64_t(mod(index - sign * (hop + 1))) *
                                           (output.bytes / count),
                                       offset / count));
        } else {
            destination.offset = add(destination.offset, offset / count);
        }
    } else if (ring.comType == ComType::Reduce_Scatter ||
               (ring.comType == ComType::All_Reduce && round < count - 1)) {
        source.offset =
            add(source.offset,
                add(offset, uint64_t(mod(index - sign * (round + 1))) * bytes));
        destination.offset =
            add(destination.offset,
                add(offset, uint64_t(mod(index - sign * (round + 2))) * bytes));
        reduce = true;
    } else {
        const int step =
            ring.comType == ComType::All_Reduce ? round - (count - 1) : round;
        const uint64_t stride = original == "all_gather" ? input.bytes : bytes;
        const uint64_t base = offset;
        source.offset =
            add(source.offset,
                add(base, uint64_t(mod(index - sign * step)) * stride));
        destination.offset =
            add(destination.offset,
                add(base, uint64_t(mod(index - sign * (step + 1))) * stride));
    }
    source.bytes = destination.bytes = bytes;
    check(ring.stream->stream_id >= 0 && ring.stream->stream_id < 500000000,
          "Ring tag overflow");
    const auto tag = ring.stream->stream_id + 500000000;
    const auto send_peer = ring.curr_receiver, recv_peer = ring.curr_sender;
    if (original == "reduce_scatter" && round == count - 2) {
        auto result = output;
        result.offset = add(output.offset, offset / count);
        result.bytes = bytes;
        auto callback = [this, id, destination, result, completed] {
            model.local_copy(id, destination, result, completed);
        };
        model.collective_step(id, source, destination, reduce, send_peer,
                              recv_peer, tag, callback,
                              {{"round", round},
                               {"stream", ring.stream->stream_id},
                               {"chunk_offset", offset},
                               {"instance_id", profile["instance_id"]}});
    } else {
        model.collective_step(id, source, destination, reduce, send_peer,
                              recv_peer, tag, std::move(completed),
                              {{"round", round},
                               {"stream", ring.stream->stream_id},
                               {"chunk_offset", offset},
                               {"instance_id", profile["instance_id"]}});
    }
}
}  // namespace AstraSim
