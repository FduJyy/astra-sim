/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/Workload.hh"

#include "astra-sim/common/Logging.hh"
#include "astra-sim/node/NodeSystem.hh"
#include "astra-sim/system/IntData.hh"
#include "astra-sim/system/MemEventHandlerData.hh"
#include "astra-sim/system/RecvPacketEventHandlerData.hh"
#include "astra-sim/system/SendPacketEventHandlerData.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include <filesystem>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <json/json.hpp>

#include <iostream>
#include <stdlib.h>
#include <unistd.h>

using namespace std;
using namespace AstraSim;
using namespace Chakra::FeederV3;
using json = nlohmann::json;

typedef ChakraProtoMsg::NodeType ChakraNodeType;
typedef ChakraProtoMsg::CollectiveCommType ChakraCollectiveCommType;

Workload::Workload(Sys* sys, string et_filename, string comm_group_filename) {
    string workload_filename = et_filename + "." + to_string(sys->id) + ".et";
    // Check if workload filename exists
    if (access(workload_filename.c_str(), R_OK) < 0) {
        string error_msg;
        if (errno == ENOENT) {
            error_msg =
                "workload file: " + workload_filename + " does not exist";
        } else if (errno == EACCES) {
            error_msg = "workload file: " + workload_filename +
                        " exists but is not readable";
        } else {
            error_msg =
                "Unknown workload file: " + workload_filename + " access error";
        }
        LoggerFactory::get_logger("workload")->critical(error_msg);
        exit(EXIT_FAILURE);
    }
    bool empty_trace = false;
    if (sys->node_system) {
        std::ifstream input(workload_filename, std::ios::binary);
        google::protobuf::io::IstreamInputStream raw(&input);
        google::protobuf::io::CodedInputStream coded(&raw);
        uint32_t length = 0;
        NodeSim::check(coded.ReadVarint32(&length) && length <= INT_MAX,
                       "workload invalid Chakra metadata envelope");
        auto limit = coded.PushLimit(int(length));
        ChakraProtoMsg::GlobalMetadata metadata;
        NodeSim::check(metadata.ParseFromCodedStream(&coded) &&
                           coded.BytesUntilLimit() == 0,
                       "workload missing Chakra global metadata");
        coded.PopLimit(limit);
        empty_trace = uint64_t(coded.CurrentPosition()) ==
                      std::filesystem::file_size(workload_filename);
    }
    this->et_feeder = empty_trace ? nullptr : new ETFeeder(workload_filename);
    this->comm_groups.clear();
    // TODO: parametrize the number of available hardware resources
    this->hw_resource = new HardwareResource(1, sys->id);
    this->local_mem_usage_tracker =
        std::make_unique<LocalMemUsageTracker>(sys->id);
    this->sys = sys;
    initialize_comm_groups(comm_group_filename);
    this->stats = new Statistics(this);
    this->is_finished = false;
    if (sys->node_system) {
        sys->node_system->model.on_capacity([this] { issue_detailed_nodes(); });
    }
}

Workload::~Workload() {
    for (auto comm_group : comm_groups) {
        delete comm_group.second;
    }
    comm_groups.clear();

    if (this->et_feeder != nullptr) {
        delete this->et_feeder;
    }
    if (this->hw_resource != nullptr) {
        delete this->hw_resource;
    }
    if (this->stats != nullptr) {
        delete this->stats;
    }
}

void Workload::initialize_comm_groups(string comm_group_filename) {
    // communicator group input file is not given
    if (comm_group_filename.find("empty") != std::string::npos) {
        comm_groups.clear();
        return;
    }

    ifstream inFile;
    json j;
    inFile.open(comm_group_filename);
    inFile >> j;

    for (json::iterator it = j.begin(); it != j.end(); ++it) {
        std::string comm_group_name = it.key();
        int comm_group_id = std::stoi(comm_group_name);
        if (sys->node_system) {
            NodeSim::check(comm_group_id > 0 && comm_group_id < 500,
                           "detailed communicator ID must be in [1,499] for "
                           "the existing tag namespace");
        }

        std::vector<int> involved_NPUs;
        for (auto id : it.value()) {
            involved_NPUs.push_back(id);
        }

        comm_groups[comm_group_id] =
            new CommunicatorGroup(comm_group_id, involved_NPUs, sys);
    }
}

void Workload::issue_pytorch_pg_metadata(
    std::shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    // For read comm groups from torch, might overwrite previous.
    std::string pg_info = node->get_inputs_values();
    if (pg_info.empty()) {
        return;
    }
    pg_info = pg_info.substr(2, pg_info.size() - 4);

    try {
        json valuesRoot = json::parse(pg_info);

        for (const auto& item : valuesRoot) {
            std::string pgName = item.at("pg_name").get<std::string>();
            std::vector<int> involved_NPUs =
                item.at("ranks").get<std::vector<int>>();

            if (involved_NPUs.empty()) {
                for (int i = 0; i < sys->total_nodes; i++) {
                    involved_NPUs.push_back(i);
                }
            }

            int32_t pgNameInt = std::stoi(pgName);
            // To ensure pgName > 0
            CommunicatorGroup* cg =
                new CommunicatorGroup(pgNameInt + 1, involved_NPUs, sys);
            this->comm_groups[pgNameInt] = cg;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error parsing or processing JSON: " << e.what()
                  << std::endl;
    }
}

void Workload::issue_dep_free_nodes() {
    if (sys->node_system) {
        issue_detailed_nodes();
        return;
    }
    auto& dependancy_resolver = this->et_feeder->getDependancyResolver();
    auto dependancy_free_nodes =
        dependancy_resolver.get_dependancy_free_nodes();
    std::set<uint64_t> dependancy_free_nodes_set;
    for (const auto node_id : dependancy_free_nodes) {
        dependancy_free_nodes_set.insert(node_id);
    }
    for (const auto node_id : dependancy_free_nodes_set) {
        std::shared_ptr<ETFeederNode> node = et_feeder->lookupNode(node_id);
        if (hw_resource->is_available(node)) {
            issue(node);
        }
    }
}

void Workload::issue_detailed_nodes() {
    if (is_finished) {
        return;
    }
    if (!et_feeder) {
        report();
        is_finished = true;
        sys->comm_NI->sim_notify_finished();
        return;
    }
    auto& resolver = et_feeder->getDependancyResolver();
    auto ready = resolver.get_dependancy_free_nodes();
    std::set<uint64_t> ordered(ready.begin(), ready.end());
    for (auto id : ordered) {
        auto node = et_feeder->lookupNode(id);
        if (node->type() == ChakraNodeType::INVALID_NODE ||
            node->type() == ChakraNodeType::METADATA_NODE) {
            resolver.take_node(id);
            if (node->type() == ChakraNodeType::METADATA_NODE) {
                issue_pytorch_pg_metadata(node);
            }
            resolver.finish_node(id);
            sys->node_system->scheduler.after(
                0, [this] { issue_detailed_nodes(); });
            continue;
        }
        const bool cpu = node->is_cpu_op<bool>(false);
        const auto profile =
            cpu ? std::string{}
                : node->get_attr<std::string>("node_model_profile", "");
        json metadata = {
            {"cpu", cpu},
            {"cpu_runtime_ns",
             node->runtime() ? NodeSim::service(node->runtime(), 0.001) : 1},
            {"et_type", int(node->type())}};
        if (node->type() == ChakraNodeType::COMM_COLL_NODE) {
            auto* group = extract_comm_group(node);
            std::vector<int> participants;
            if (group) {
                participants = group->involved_NPUs;
            } else {
                for (int i = 0; i < sys->total_nodes; ++i) {
                    participants.push_back(i);
                }
            }
            metadata["participants"] = participants;
            metadata["pg_name"] = node->pg_name<std::string>("");
            metadata["comm_type"] = int(node->comm_type());
            metadata["comm_size"] = node->comm_size();
        } else if (node->type() == ChakraNodeType::COMM_SEND_NODE ||
                   node->type() == ChakraNodeType::COMM_RECV_NODE) {
            metadata["peer"] = node->type() == ChakraNodeType::COMM_SEND_NODE
                                   ? node->comm_dst()
                                   : node->comm_src();
            metadata["tag"] = node->comm_tag();
            metadata["comm_size"] = node->comm_size();
        } else if (node->type() == ChakraNodeType::MEM_LOAD_NODE ||
                   node->type() == ChakraNodeType::MEM_STORE_NODE) {
            metadata["tensor_size"] = node->tensor_size();
        }
        if (sys->node_system->model.try_submit(
                id, profile, metadata,
                [this, node](auto, auto tick) {
                    stats->record_start(node, tick);
                    if (sys->track_local_mem) {
                        local_mem_usage_tracker->recordStart(node, tick);
                    }
                },
                [this](auto id, auto) { complete_detailed(id); })) {
            resolver.take_node(id);
        }
    }
    if (resolver.get_dependancy_free_nodes().empty() &&
        resolver.get_ongoing_nodes().empty() &&
        sys->node_system->model.idle()) {
        report();
        is_finished = true;
        sys->comm_NI->sim_notify_finished();
    }
}

void Workload::complete_detailed(uint64_t id) {
    auto node = et_feeder->lookupNode(id);
    stats->record_end(node, Sys::boostedTick());
    if (sys->track_local_mem) {
        local_mem_usage_tracker->recordEnd(node, Sys::boostedTick());
    }
    et_feeder->getDependancyResolver().finish_node(id);
    issue_detailed_nodes();
}

void Workload::issue(shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    auto logger = LoggerFactory::get_logger("workload");
    if (sys->trace_enabled) {
        logger->debug("issue,sys->id={}, tick={}, node->id={}, "
                      "node->name={}, node->type={}",
                      sys->id, Sys::boostedTick(), node->id(), node->name(),
                      static_cast<uint64_t>(node->type()));
    }

    this->et_feeder->getDependancyResolver().take_node(node->id());
    this->hw_resource->occupy(node);
    // stats->record_end will be called in Workload::call
    stats->record_start(node, Sys::boostedTick());
    if (this->sys->track_local_mem) {
        this->local_mem_usage_tracker->recordStart(node, Sys::boostedTick());
    }
    if (sys->replay_only) {
        issue_replay(node);
    } else {
        if ((node->type() == ChakraNodeType::MEM_LOAD_NODE) ||
            (node->type() == ChakraNodeType::MEM_STORE_NODE)) {
            issue_remote_mem(node);
        } else if (node->type() == ChakraNodeType::COMP_NODE) {
            if (!this->sys->roofline_enabled) {
                issue_replay(node);
            } else {
                if (node->is_cpu_op<bool>(false)) {
                    // comp node on cpu
                    // should only appears in real system trace and should run
                    // with replay.
                    issue_replay(node);
                } else {
                    // comp node on gpu
                    issue_comp(node);
                }
            }
        } else if (node->type() == ChakraNodeType::COMM_COLL_NODE ||
                   node->type() == ChakraNodeType::COMM_SEND_NODE ||
                   node->type() == ChakraNodeType::COMM_RECV_NODE) {
            issue_comm(node);
        } else if (node->type() == ChakraNodeType::INVALID_NODE) {
            skip_invalid(node);
        } else if (node->type() == ChakraNodeType::METADATA_NODE) {
            issue_metadata(node);
        } else {
            logger->critical("Unknown node type");
            exit(EXIT_FAILURE);
        }
    }
}

void Workload::issue_metadata(shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    // TODO: someway to identify this metadata node is a pytorch pg node
    if (true) {
        issue_pytorch_pg_metadata(node);
    } else {
        throw std::runtime_error("Unknown metadata node type");
    }
    this->skip_invalid(node);  // for proper dependancy resolving
}

void Workload::issue_replay(shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    WorkloadLayerHandlerData* wlhd = new WorkloadLayerHandlerData;
    wlhd->node_id = node->id();
    uint64_t runtime = 1ul;
    if (node->runtime() != 0ul) {
        // chakra runtimes are in microseconds and we should convert it into
        // nanoseconds
        runtime = node->runtime() * 1000;
    }
    if (node->is_cpu_op()) {
        hw_resource->tics_cpu_ops += runtime;
    } else {
        hw_resource->tics_gpu_ops += runtime;
    }
    sys->register_event(this, EventType::General, wlhd, runtime);
}

void Workload::issue_remote_mem(
    shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    WorkloadLayerHandlerData* wlhd = new WorkloadLayerHandlerData;
    wlhd->sys_id = sys->id;
    wlhd->workload = this;
    wlhd->node_id = node->id();
    sys->remote_mem->issue(node->tensor_size(), wlhd);
}

void Workload::issue_comp(shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    if (!this->sys->roofline_enabled) {
        throw std::runtime_error(
            "Roofline model is not enabled for non-replay comp");
    }

    if (node->is_cpu_op()) {
        throw std::runtime_error("Roofline is only available for GPU nodes");
        return;
    }

    WorkloadLayerHandlerData* wlhd = new WorkloadLayerHandlerData;
    wlhd->node_id = node->id();

    double num_ops = static_cast<double>(node->num_ops<uint64_t>());
    double tensor_size = static_cast<double>(node->tensor_size<uint64_t>());

    // if tensor_size is 0 during roofline mode, this is an invalid node
    if (tensor_size == 0) {
        skip_invalid(node);
        return;
    }

    double operational_intensity = num_ops / tensor_size;
    double perf = sys->roofline->get_perf(operational_intensity);
    double elapsed_time = static_cast<double>(node->num_ops()) / perf;  // sec
    uint64_t runtime = static_cast<uint64_t>(elapsed_time * 1e9);  // sec -> ns
    if (node->is_cpu_op()) {
        hw_resource->tics_cpu_ops += runtime;
    } else {
        hw_resource->tics_gpu_ops += runtime;
    }
    sys->register_event(this, EventType::General, wlhd, runtime);

    auto& op_stat = this->stats->get_operator_statistics(node->id());
    op_stat.operation_intensity = operational_intensity;
    op_stat.compute_utilization = perf / sys->peak_perf;
    op_stat.memory_utilization =
        (perf / operational_intensity) / sys->local_mem_bw;
    op_stat.is_memory_bound = perf < sys->peak_perf;
    LoggerFactory::get_logger("workload")
        ->debug("operation_intensity={}, perf={}, elapsed_time={} "
                "compute_utilization={} memory_utilization={} tensor_size={} "
                "num_ops={}",
                operational_intensity, perf, elapsed_time,
                op_stat.compute_utilization.value(),
                op_stat.memory_utilization.value(), tensor_size, num_ops);
}

void Workload::issue_comm(shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    if (node->is_cpu_op<bool>(false)) {
        throw std::runtime_error("Comm node should not be on CPU");
    }
    const auto node_type = node->type();
    if (node_type == ChakraNodeType::COMM_COLL_NODE) {
        this->issue_coll_comm(node);
    } else if (node_type == ChakraNodeType::COMM_SEND_NODE) {
        this->issue_send_comm(node);
    } else if (node_type == ChakraNodeType::COMM_RECV_NODE) {
        this->issue_recv_comm(node);
    } else {
        throw std::runtime_error("Unknown comm node type");
    }
}

void Workload::issue_coll_comm(
    shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    const bool has_involve_dims = node->has_attr("involve_dims");
    std::vector<bool> involved_dims;
    if (node->has_attr("involved_dim")) {
        const ChakraProtoMsg::AttributeProto& attr =
            node->get_attr_msg("involved_dim");

        // Ensure the attribute is of type bool_list before accessing
        if (attr.has_bool_list()) {
            const ChakraProtoMsg::BoolList& bool_list = attr.bool_list();

            // Traverse bool_list and add values to involved_dim
            for (int i = 0; i < bool_list.values_size(); ++i) {
                involved_dims.push_back(bool_list.values(i));
            }
        } else {
            cerr << "Expected bool_list in involved_dim but found another type."
                 << endl;
            exit(EXIT_FAILURE);
        }
    } else {
        // involved_dims does not exist in ETFeeder.
        // Assume involved_dims = [1,1,1,1,1] which we could simulate
        // 5-Dimension. Could use Process Group to build involved_dims later.
        // Once process group is implemented, you should get
        // that with node->pg_name()

        for (int i = 0; i < 4; i++) {
            involved_dims.push_back(true);
        }
    }

    CommunicatorGroup* comm_group = extract_comm_group(node);
    const auto comm_type =
        static_cast<ChakraCollectiveCommType>(node->comm_type<uint64_t>());
    const auto comm_size = node->comm_size<uint64_t>();
    // Record communication size for bandwidth calculation
    stats->get_operator_statistics(node->id()).comm_size = comm_size;
    // TODO: comm_tag? which is used to distinguish two different collective in
    // same pg
    const auto comm_priority = node->comm_priority<uint32_t>();  // default 0u

    if (comm_type == ChakraCollectiveCommType::ALL_REDUCE) {
        DataSet* fp = sys->generate_all_reduce(comm_size, involved_dims,
                                               comm_group, comm_priority, node->id());
        collective_comm_node_id_map[fp->my_id] = node->id();
        collective_comm_wrapper_map[fp->my_id] = fp;
        fp->set_notifier(this, EventType::CollectiveCommunicationFinished);
    } else if (comm_type == ChakraCollectiveCommType::ALL_TO_ALL) {
        DataSet* fp = sys->generate_all_to_all(comm_size, involved_dims,
                                               comm_group, comm_priority, node->id());
        collective_comm_node_id_map[fp->my_id] = node->id();
        collective_comm_wrapper_map[fp->my_id] = fp;
        fp->set_notifier(this, EventType::CollectiveCommunicationFinished);
    } else if (comm_type == ChakraCollectiveCommType::ALL_GATHER) {
        DataSet* fp = sys->generate_all_gather(comm_size, involved_dims,
                                               comm_group, comm_priority, node->id());
        collective_comm_node_id_map[fp->my_id] = node->id();
        collective_comm_wrapper_map[fp->my_id] = fp;
        fp->set_notifier(this, EventType::CollectiveCommunicationFinished);
    } else if (comm_type == ChakraCollectiveCommType::REDUCE_SCATTER) {
        DataSet* fp = sys->generate_reduce_scatter(comm_size, involved_dims,
                                                   comm_group, comm_priority, node->id());
        collective_comm_node_id_map[fp->my_id] = node->id();
        collective_comm_wrapper_map[fp->my_id] = fp;
        fp->set_notifier(this, EventType::CollectiveCommunicationFinished);
    } else if (comm_type == ChakraCollectiveCommType::BROADCAST) {
        // TODO: implement broadcast, for now just replay
        uint64_t runtime = 1ul;
        if (node->runtime() != 0ul) {
            // chakra runtimes are in microseconds and we should convert it into
            // nanoseconds
            runtime = node->runtime() * 1000;
        }
        DataSet* fp = new DataSet(1);
        fp->set_notifier(this, EventType::CollectiveCommunicationFinished);
        collective_comm_node_id_map[fp->my_id] = node->id();
        collective_comm_wrapper_map[fp->my_id] = fp;
        sys->register_event(fp, EventType::General, nullptr,
                            // chakra runtimes are in microseconds and we
                            // should convert it into nanoseconds
                            runtime);
        fp->set_notifier(this, EventType::CollectiveCommunicationFinished);
    } else {
        throw std::runtime_error("Unsupported collective comm type");
    }
}

void Workload::issue_send_comm(
    shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    const auto src = node->comm_src<uint32_t>(this->sys->id);
    if (src != this->sys->id) {
        throw std::runtime_error("Send node should be issued by the sender");
    }
    const auto dst = node->comm_dst<uint32_t>();
    const auto size = node->comm_size<uint64_t>();
    // Record communication size for bandwidth calculation
    stats->get_operator_statistics(node->id()).comm_size = size;
    const auto tag = node->comm_tag<uint32_t>();

    sim_request snd_req;
    snd_req.srcRank = src;
    snd_req.dstRank = dst;
    snd_req.reqType = UINT8;
    SendPacketEventHandlerData* sehd = new SendPacketEventHandlerData;
    sehd->callable = this;
    sehd->wlhd = new WorkloadLayerHandlerData;
    sehd->wlhd->node_id = node->id();
    sehd->event = EventType::PacketSent;
    sys->front_end_sim_send(0, Sys::dummy_data, size, UINT8, dst, tag, &snd_req,
                            Sys::FrontEndSendRecvType::NATIVE,
                            &Sys::handleEvent, sehd);
}

void Workload::issue_recv_comm(
    shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    const auto src = node->comm_src<uint32_t>();
    const auto dst = node->comm_dst<uint32_t>(this->sys->id);
    if (dst != this->sys->id) {
        throw std::runtime_error("Recv node should be issued by the receiver");
    }
    const auto size = node->comm_size<uint64_t>();
    // Record communication size for bandwidth calculation
    stats->get_operator_statistics(node->id()).comm_size = size;
    const auto tag = node->comm_tag<uint32_t>();

    sim_request rcv_req;
    RecvPacketEventHandlerData* rcehd = new RecvPacketEventHandlerData;
    rcehd->wlhd = new WorkloadLayerHandlerData;
    rcehd->wlhd->node_id = node->id();
    rcehd->workload = this;
    rcehd->event = EventType::PacketReceived;
    sys->front_end_sim_recv(0, Sys::dummy_data, size, UINT8, src, tag, &rcv_req,
                            Sys::FrontEndSendRecvType::NATIVE,
                            &Sys::handleEvent, rcehd);
}

void Workload::skip_invalid(shared_ptr<Chakra::FeederV3::ETFeederNode> node) {
    const auto node_id = node->id();
    auto& dependancy_resolver = this->et_feeder->getDependancyResolver();
    dependancy_resolver.finish_node(node_id);
    auto logger = LoggerFactory::get_logger("workload");
    logger->debug("callback,sys->id={}, tick={}, node->id={}, "
                  "node->name={}, node->type={}",
                  sys->id, Sys::boostedTick(), node->id(), node->name(),
                  static_cast<uint64_t>(node->type()));
    hw_resource->release(node);
    stats->record_end(node, Sys::boostedTick());
    if (this->sys->track_local_mem) {
        this->local_mem_usage_tracker->recordEnd(node, Sys::boostedTick());
    }
}

void Workload::call(EventType event, CallData* data) {
    if (is_finished) {
        return;
    }
    if (sys->node_system && data && event == EventType::General) {
        auto* callback = static_cast<WorkloadLayerHandlerData*>(data);
        auto it = remote_callbacks_.find(callback->node_id);
        NodeSim::check(it != remote_callbacks_.end(),
                       "unknown detailed remote completion");
        auto done = std::move(it->second);
        remote_callbacks_.erase(it);
        sys->node_system->after_current_batch([callback] { delete callback; });
        sys->node_system->scheduler.after(0, std::move(done), 1);
        sys->node_system->drain();
        return;
    }
    if (sys->node_system) {
        NodeSim::check(data == nullptr,
                       "unexpected detailed workload callback");
        issue_detailed_nodes();
        return;
    }

    if (event == EventType::CollectiveCommunicationFinished) {
        IntData* int_data = (IntData*)data;
        uint64_t coll_comm_id = int_data->data;

        hw_resource->tics_gpu_comms += int_data->execution_time;
        uint64_t node_id = collective_comm_node_id_map[coll_comm_id];
        shared_ptr<Chakra::FeederV3::ETFeederNode> node =
            et_feeder->lookupNode(node_id);

        if (sys->trace_enabled) {
            LoggerFactory::get_logger("workload")
                ->debug("callback,sys->id={}, tick={}, node->id={}, "
                        "node->name={}, node->type={}",
                        sys->id, Sys::boostedTick(), node->id(), node->name(),
                        static_cast<uint64_t>(node->type()));
        }

        hw_resource->release(node);
        stats->record_end(node, Sys::boostedTick());

        // Calculate network bandwidth
        auto& op_stat = stats->get_operator_statistics(node_id);
        Tick execution_time = int_data->execution_time;
        if (execution_time > 0 && op_stat.comm_size.has_value()) {
            double bandwidth =
                static_cast<double>(op_stat.comm_size.value()) / execution_time;
            op_stat.network_bandwidth = bandwidth;
        }

        if (this->sys->track_local_mem) {
            this->local_mem_usage_tracker->recordEnd(node, Sys::boostedTick());
        }

        this->et_feeder->getDependancyResolver().finish_node(node_id);

        issue_dep_free_nodes();

        // The Dataset class provides statistics that should be used later to
        // dump more statistics in the workload layer
        delete collective_comm_wrapper_map[coll_comm_id];
        collective_comm_wrapper_map.erase(coll_comm_id);

    } else {
        if (data == nullptr) {
            issue_dep_free_nodes();
        } else {
            WorkloadLayerHandlerData* wlhd = (WorkloadLayerHandlerData*)data;
            shared_ptr<Chakra::FeederV3::ETFeederNode> node =
                et_feeder->lookupNode(wlhd->node_id);

            if (sys->trace_enabled) {
                LoggerFactory::get_logger("workload")
                    ->debug("callback,sys->id={}, tick={}, node->id={}, "
                            "node->name={}, node->type={}",
                            sys->id, Sys::boostedTick(), node->id(),
                            node->name(), static_cast<uint64_t>(node->type()));
            }

            hw_resource->release(node);
            stats->record_end(node, Sys::boostedTick());

            // Calculate network bandwidth for point-to-point communications
            if (event == EventType::PacketSent ||
                event == EventType::PacketReceived) {
                auto& op_stat = stats->get_operator_statistics(wlhd->node_id);
                Tick execution_time =
                    stats->get_operator_statistics(wlhd->node_id).end_time -
                    stats->get_operator_statistics(wlhd->node_id).start_time;
                if (execution_time > 0 && op_stat.comm_size.has_value()) {
                    double bandwidth =
                        static_cast<double>(op_stat.comm_size.value()) /
                        execution_time;
                    op_stat.network_bandwidth = bandwidth;
                }
            }

            if (this->sys->track_local_mem) {
                this->local_mem_usage_tracker->recordEnd(node,
                                                         Sys::boostedTick());
            }

            this->et_feeder->getDependancyResolver().finish_node(wlhd->node_id);

            issue_dep_free_nodes();

            delete wlhd;
        }
    }

    const auto& dep_resolver = this->et_feeder->getDependancyResolver();
    if ((dep_resolver.get_dependancy_free_nodes().empty()) &&
        (dep_resolver.get_ongoing_nodes().empty()) &&
        (hw_resource->num_in_flight_cpu_ops == 0) &&
        (hw_resource->num_in_flight_gpu_comp_ops == 0) &&
        (hw_resource->num_in_flight_gpu_comm_ops == 0)) {
        report();
        sys->comm_NI->sim_notify_finished();
        is_finished = true;
    }
}

void Workload::fire() {
    call(EventType::General, NULL);
    if (sys->node_system) {
        sys->node_system->drain();
    }
}

void Workload::issue_node_remote(uint64_t bytes, std::function<void()> done) {
    auto* data = new WorkloadLayerHandlerData;
    data->sys_id = sys->id;
    data->workload = this;
    data->node_id = remote_callback_id_--;
    remote_callbacks_[data->node_id] = std::move(done);
    sys->remote_mem->issue(bytes, data);
}

void Workload::report() {
    Tick curr_tick = Sys::boostedTick();
    if (sys->node_system) {
        LoggerFactory::get_logger("workload")
            ->info("sys[{}] detailed finished, {} ns; node-model-summary {}",
                   sys->id, curr_tick,
                   sys->node_system->model.statistics().dump());
    } else {
        LoggerFactory::get_logger("workload")
            ->info(
                "sys[{}] finished, {} cycles, exposed communication {} cycles.",
                sys->id, curr_tick, curr_tick - hw_resource->tics_gpu_ops);
    }
    stats->post_processing();
    stats->report();
    if (this->sys->track_local_mem) {
        this->local_mem_usage_tracker->buildMemoryTrace();
        this->local_mem_usage_tracker->buildMemoryTimeline();
        this->local_mem_usage_tracker->dumpMemoryTrace(
            this->sys->local_mem_trace_filename);
        auto [peak_mem_usage, unit] =
            this->local_mem_usage_tracker->getPeakMemUsageFormatted();
        auto logger = LoggerFactory::get_logger("workload");
        logger->info("sys[{}] peak memory usage: {:.2f} {}", sys->id,
                     peak_mem_usage, unit);
        this->local_mem_usage_tracker.reset();
    }
}

CommunicatorGroup* Workload::extract_comm_group(
    std::shared_ptr<Chakra::ETFeederNode> node) {
    std::string comm_group_name = node->pg_name<std::string>("");
    // [default communication group]
    // We assume that an empty comm group, or comm group '0' both correspond
    // to the default communicator group that includes all ranks.
    // If, in the future, we want to support a user-defined comm group '0',
    // revisit this logic.
    if (comm_group_name == "" || comm_group_name == "0") {
        // No communicator group is specified for this communication ET node.
        return nullptr;
    }

    int comm_group_id = std::stoi(comm_group_name);
    if (comm_groups.find(comm_group_id) == comm_groups.end()) {
        LoggerFactory::get_logger("workload")
            ->critical(
                "For rank {} ET node {}, communicator group {} not found",
                sys->id, node->id(), comm_group_id);
        exit(EXIT_FAILURE);
    }
    return comm_groups[comm_group_id];
}
