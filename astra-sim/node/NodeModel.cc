#include "astra-sim/node/NodeModel.hh"
#include "astra-sim/node/DmaEngine.hh"
#include "astra-sim/node/MemorySystem.hh"
#include "astra-sim/node/NetworkEndpoint.hh"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>

namespace AstraSim::NodeSim {
namespace {
json read_json(const std::filesystem::path& path) {
    std::ifstream input(path);
    check(bool(input), path.string() + ": cannot open configuration");
    try {
        json result;
        input >> result;
        return result;
    } catch (const json::exception& error) {
        throw Error(path.string() + ": " + error.what());
    }
}
}  // namespace
struct NodeModel::Impl {
    struct Operation {
        uint64_t id;
        json profile, metadata;
        Observer started, completed;
        std::string cu, profile_name;
        std::vector<unsigned> state;
        Tick enqueue, start = 0;
        bool running = false;
        std::optional<BufferRef> input, output, workspace;
        std::string communicator;
    };
    NodeTopology topology;
    NodeEventScheduler& clock;
    NodeStatistics telemetry;
    MemorySystem memory;
    DmaEngine dma;
    NetworkEndpoint endpoint;
    int rank;
    json profiles, stats;
    Transport transport;
    Done capacity;
    std::map<uint64_t, std::shared_ptr<Operation>> operations;
    std::vector<uint64_t> order;
    std::set<uint64_t> used_ids;
    std::set<std::string> busy;
    bool dispatch_pending = false;
    uint64_t active_collectives = 0;
    std::set<std::string> communicator_busy;
    std::set<std::pair<std::string, uint64_t>> instances;
    std::deque<std::function<bool()>> dma_waiters;
    std::map<uint64_t, Tick> first_attempt;
    std::map<std::string, std::set<std::string>> eligible;

    Impl(json configuration,
         json profile_table,
         NodeEventScheduler& scheduler,
         int rank_id)
        : topology(std::move(configuration)),
          clock(scheduler),
          telemetry(topology.configuration(), clock, rank_id),
          memory(topology, clock, telemetry),
          dma(topology, clock, memory, telemetry),
          endpoint(topology, clock, memory, telemetry),
          rank(rank_id),
          profiles(std::move(profile_table)) {
        fields(profiles, {"schema_version", "initial_buffers", "profiles"},
               "profiles");
        check(number(profiles, "schema_version", "profiles") == 1,
              "profiles.schema_version: unsupported");
        check(profiles.contains("profiles") && profiles["profiles"].is_object(),
              "profiles.profiles: expected object");
        for (auto it = profiles["profiles"].begin();
             it != profiles["profiles"].end(); ++it) {
            validate_profile(it.key(), it.value());
        }
        if (profiles.contains("initial_buffers")) {
            check(profiles["initial_buffers"].is_array(),
                  "initial_buffers: expected array");
            for (const auto& buffer : profiles["initial_buffers"]) {
                memory.allocate(buffer);
            }
        }
        stats = {{"schema_version", 1},
                 {"rank", rank},
                 {"fidelity", "transaction-store-and-forward"},
                 {"effective_configuration", topology.configuration()},
                 {"operations", json::object()},
                 {"compute_units", json::object()}};
        dma.on_capacity([this] {
            const auto count = dma_waiters.size();
            for (size_t i = 0; i < count; ++i) {
                auto submit = std::move(dma_waiters.front());
                dma_waiters.pop_front();
                if (!submit()) {
                    dma_waiters.push_back(std::move(submit));
                }
            }
        });
    }
    RequestContext context(uint64_t op, const std::string& stage) {
        return {op, telemetry.next_id(), 0, stage};
    }
    void copy(BufferRef source,
              BufferRef destination,
              const std::string& requester,
              const std::string& binding,
              Done done,
              RequestContext ctx = {}) {
        const auto ready = clock.now();
        // Waiting for a hardware descriptor is still an in-use software stage.
        memory.retain(source, true);
        memory.retain(destination, false);
        auto complete = [this, source, destination, done = std::move(done)] {
            memory.unretain(source);
            memory.unretain(destination);
            done();
        };
        auto submit = [this, source, destination, requester, binding,
                       done = std::move(complete), ctx, ready] {
            const bool accepted =
                dma.copy(source, destination, requester, binding, done, ctx);
            if (accepted) {
                telemetry.add_time(ctx.operation, "dma_wait_ns",
                                   clock.now() - ready);
            }
            return accepted;
        };
        if (!submit()) {
            dma_waiters.push_back(std::move(submit));
        }
    }
    void validate_profile(const std::string& name, const json& profile) {
        const auto path = "profiles." + name;
        fields(profile,
               {"kind", "cu_kind", "cu_id", "stages", "source", "destination",
                "requester", "dma_id", "remote_bytes", "local_buffer", "buffer",
                "instance_id", "input", "output", "elem_bytes", "operation",
                "participants", "workspace"},
               path);
        const auto kind = text(profile, "kind", path);
        check(std::set<std::string>{"compute", "local_dma", "remote_memory",
                                    "send", "recv", "collective"}
                  .count(kind),
              path + ".kind: unsupported");
        auto validate_ref = [&](const json& value) {
            auto ref = memory.resolve(value);
            const auto capacity = number(topology.modules().at(ref.storage),
                                         "capacity_bytes", path);
            check(ref.offset <= capacity && ref.bytes <= capacity - ref.offset,
                  path + ": BufferBounds exceeds storage capacity");
            return ref;
        };
        auto require_dma = [&] {
            check(!topology.configuration()["dma_engines"].empty(),
                  path + ": DMA engine required");
            if (profile.contains("dma_id")) {
                const auto id = text(profile, "dma_id", path);
                bool found = false;
                for (const auto& dma :
                     topology.configuration()["dma_engines"]) {
                    if (dma["id"] == id) {
                        found = true;
                    }
                }
                check(found, path + ": unknown dma_id");
            }
        };
        if (kind == "local_dma") {
            require_dma();
            const auto source = validate_ref(profile.at("source")),
                       destination = validate_ref(profile.at("destination"));
            check(source.bytes == destination.bytes,
                  path + ": DMA size mismatch");
            const auto requester = text(profile, "requester", path);
            check(topology.modules().count(requester),
                  path + ": unknown requester");
            check(topology.allowed(source.storage, requester) &&
                      topology.allowed(destination.storage, requester),
                  path + ": StoragePermission");
            return;
        }
        if (kind == "remote_memory") {
            require_dma();
            auto ref = validate_ref(profile.at("local_buffer"));
            check(ref.bytes == number(profile, "remote_bytes", path),
                  path + ": remote buffer size mismatch");
            return;
        }
        if (kind == "send" || kind == "recv" || kind == "collective") {
            check(topology.configuration().contains("network_endpoint"),
                  path + ": network_endpoint required");
            const auto& nic = topology.configuration()["network_endpoint"];
            const auto id = nic["id"].get<std::string>();
            number(profile, "instance_id", path, true);
            if (kind != "collective") {
                auto ref = validate_ref(profile.at("buffer"));
                check(topology.modules().at(ref.storage)["scope"] ==
                          "node_shared",
                      path + ": network buffer must be node_shared");
                check(ref.bytes <= number(nic,
                                          kind == "send" ? "tx_buffer_bytes"
                                                         : "rx_buffer_bytes",
                                          path),
                      path + ": CapacityExceeded NIC message");
                topology.route(id, ref.storage);
                topology.route(ref.storage, id);
                return;
            }
            require_dma();
            check(topology.configuration().contains("collective_engine"),
                  path + ": collective_engine required");
            const auto input = validate_ref(profile.at("input")),
                       output = validate_ref(profile.at("output"));
            for (const auto& ref : {input, output}) {
                check(topology.modules().at(ref.storage)["scope"] ==
                          "node_shared",
                      path + ": collective buffer must be node_shared");
                topology.route(id, ref.storage);
                topology.route(ref.storage, id);
            }
            check(profile.contains("participants") &&
                      profile["participants"].is_array() &&
                      !profile["participants"].empty(),
                  path + ": participants required");
            std::set<int> ranks;
            for (const auto& r : profile["participants"]) {
                check(r.is_number_integer() && r.get<int>() >= 0,
                      path + ": invalid participant");
                check(ranks.insert(r.get<int>()).second,
                      path + ": duplicate participant");
            }
            const auto operation = text(profile, "operation", path);
            check(std::set<std::string>{"all_reduce", "reduce_scatter",
                                        "all_gather", "all_to_all"}
                      .count(operation),
                  path + ": unsupported collective operation");
            number(profile, "elem_bytes", path);
            if (operation == "reduce_scatter" || operation == "all_to_all") {
                check(profile.contains("workspace"),
                      path + ": explicit workspace required");
            }
            if (profile.contains("workspace")) {
                validate_ref(profile["workspace"]);
            }
            return;
        }
        const auto cu_kind = text(profile, "cu_kind", path);
        bool found = false;
        for (const auto& cu : topology.configuration()["compute_units"]) {
            if (cu["kind"] == cu_kind &&
                (!profile.contains("cu_id") || cu["id"] == profile["cu_id"])) {
                found = true;
            }
        }
        check(found, path + ": unknown or incompatible CU");
        check(profile.contains("stages") && profile["stages"].is_array() &&
                  !profile["stages"].empty(),
              path + ".stages: required");
        std::map<std::string, size_t> indices;
        for (size_t i = 0; i < profile["stages"].size(); ++i) {
            const auto& stage = profile["stages"][i];
            fields(stage,
                   {"id", "kind", "deps", "ops", "storage", "buffer_id",
                    "generation", "bytes", "alignment", "storage_offset",
                    "buffer", "source", "destination", "requester", "dma_id"},
                   path + ".stages");
            check(indices.emplace(text(stage, "id", path), i).second,
                  path + ": duplicate stage ID");
            const auto type = text(stage, "kind", path);
            check(std::set<std::string>{"allocate", "free", "read", "write",
                                        "compute", "dma_copy"}
                      .count(type),
                  path + ": unknown stage kind");
            check(stage.contains("deps") && stage["deps"].is_array(),
                  path + ": deps must be an array");
            if (type == "compute") {
                number(stage, "ops", path, true);
            } else if (type == "read" || type == "write") {
                auto ref = stage.at("buffer");
                if (ref.value("storage", "") != "$cu.local_storage") {
                    validate_ref(ref);
                } else {
                    bool found_local = false;
                    for (const auto& cu :
                         topology.configuration()["compute_units"]) {
                        if (cu["kind"] == cu_kind &&
                            (!profile.contains("cu_id") ||
                             cu["id"] == profile["cu_id"]) &&
                            cu.contains("local_storage")) {
                            ref["storage"] = cu["local_storage"];
                            memory.resolve(ref);
                            found_local = true;
                        }
                    }
                    check(found_local, path + ": local storage required");
                }
            } else if (type == "allocate" || type == "free") {
                text(stage, "storage", path);
                text(stage, "buffer_id", path);
                number(stage, "generation", path, true);
                if (type == "allocate") {
                    number(stage, "bytes", path);
                    if (stage.contains("alignment")) {
                        number(stage, "alignment", path);
                    }
                }
            } else if (type == "dma_copy") {
                require_dma();
                for (const auto& key : {"source", "destination"}) {
                    auto ref = stage.at(key);
                    if (ref.value("storage", "") != "$cu.local_storage") {
                        validate_ref(ref);
                    } else {
                        bool found = false;
                        for (const auto& cu :
                             topology.configuration()["compute_units"]) {
                            if (cu["kind"] == cu_kind &&
                                (!profile.contains("cu_id") ||
                                 cu["id"] == profile["cu_id"]) &&
                                cu.contains("local_storage")) {
                                ref["storage"] = cu["local_storage"];
                                memory.resolve(ref);
                                found = true;
                            }
                        }
                        check(found, path + ": DMA local storage required");
                    }
                }
            }
        }
        std::vector<int> state(indices.size());
        std::function<void(size_t)> visit = [&](size_t i) {
            check(state[i] != 1, path + ": cycle in stage dependencies");
            if (state[i] == 2) {
                return;
            }
            state[i] = 1;
            std::set<std::string> deps;
            for (const auto& dep : profile["stages"][i]["deps"]) {
                check(dep.is_string() && indices.count(dep.get<std::string>()),
                      path + ": missing dependency");
                check(deps.insert(dep.get<std::string>()).second,
                      path + ": duplicate dependency");
                visit(indices.at(dep.get<std::string>()));
            }
            state[i] = 2;
        };
        for (size_t i = 0; i < state.size(); ++i) {
            visit(i);
        }
        for (const auto& cu : topology.configuration()["compute_units"]) {
            if (cu["kind"] != cu_kind ||
                (profile.contains("cu_id") && cu["id"] != profile["cu_id"])) {
                continue;
            }
            const auto id = cu["id"].get<std::string>(),
                       local = cu.value("local_storage", "");
            bool compatible = true;
            try {
                for (const auto& stage : profile["stages"]) {
                    const auto type = stage["kind"].get<std::string>();
                    if (type == "read" || type == "write") {
                        auto ref = memory.resolve(stage.at("buffer"), local);
                        const auto capacity =
                            number(topology.modules().at(ref.storage),
                                   "capacity_bytes", path);
                        check(ref.offset <= capacity &&
                                  ref.bytes <= capacity - ref.offset,
                              "CU buffer exceeds storage capacity");
                        check(topology.allowed(ref.storage, id),
                              "CU storage access");
                        topology.route(id, ref.storage);
                        topology.route(ref.storage, id);
                    } else if (type == "dma_copy") {
                        const auto requester = stage.value("requester", id);
                        for (const auto* key : {"source", "destination"}) {
                            auto ref = memory.resolve(stage.at(key), local);
                            const auto capacity =
                                number(topology.modules().at(ref.storage),
                                       "capacity_bytes", path);
                            check(ref.offset <= capacity &&
                                      ref.bytes <= capacity - ref.offset,
                                  "DMA buffer exceeds storage capacity");
                            check(topology.allowed(ref.storage, requester),
                                  "DMA storage access");
                        }
                    } else if (type == "allocate" || type == "free") {
                        auto storage = stage["storage"].get<std::string>();
                        if (storage == "$cu.local_storage") {
                            check(!local.empty(), "local storage required");
                            storage = local;
                        }
                        auto ref = memory.resolve({{"storage", storage},
                                                   {"buffer_id", "$validate"},
                                                   {"generation", 0},
                                                   {"offset", 0},
                                                   {"bytes", 1}});
                        check(topology.allowed(ref.storage, id),
                              "allocation storage access");
                        if (type == "allocate") {
                            check(number(stage, "bytes", path) <=
                                      number(topology.modules().at(ref.storage),
                                             "capacity_bytes", path),
                                  "allocation exceeds CU storage capacity");
                        }
                    }
                }
            } catch (const Error&) {
                compatible = false;
            }
            if (compatible) {
                eligible[name].insert(id);
            }
        }
        check(!eligible[name].empty(),
              path + ": no eligible CU for storage bindings and permissions");
    }
    void request_dispatch() {
        if (dispatch_pending) {
            return;
        }
        dispatch_pending = true;
        clock.after(0, [this] {
            dispatch_pending = false;
            dispatch();
        });
    }
    void dispatch() {
        const auto pending = order;
        for (auto id : pending) {
            auto op = operations.at(id);
            if (op->running) {
                continue;
            }
            if (op->metadata.value("cpu", false)) {
                if (busy.count("$cpu")) {
                    continue;
                }
                op->cu = "$cpu";
            } else if (op->profile["kind"] == "compute") {
                for (const auto& name : eligible.at(op->profile_name)) {
                    const auto& cu = topology.modules().at(name);
                    if (cu["kind"] != op->profile["cu_kind"] ||
                        busy.count(name)) {
                        continue;
                    }
                    if (op->profile.contains("cu_id") &&
                        op->profile["cu_id"] != name) {
                        continue;
                    }
                    op->cu = name;
                    break;
                }
                if (op->cu.empty()) {
                    continue;
                }
            } else if (op->profile["kind"] == "collective") {
                check(topology.configuration().contains("collective_engine"),
                      "collective_engine required");
                if (active_collectives >=
                        number(topology.configuration()["collective_engine"],
                               "slots", "collective_engine") ||
                    communicator_busy.count(op->communicator)) {
                    continue;
                }
                ++active_collectives;
                communicator_busy.insert(op->communicator);
            }
            op->running = true;
            op->start = clock.now();
            if (!op->cu.empty()) {
                busy.insert(op->cu);
            }
            stats["operations"][std::to_string(id)]["start_ns"] = op->start;
            stats["operations"][std::to_string(id)]["cu_wait_ns"] =
                op->start - op->enqueue;
            telemetry.record(context(id, "$dispatch"), "operation_started",
                             op->cu.empty() ? "node" : op->cu);
            if (op->started) {
                op->started(id, op->start);
            }
            if (op->metadata.value("cpu", false)) {
                clock.after(op->metadata.at("cpu_runtime_ns"),
                            [this, op] { finish(op); });
            } else if (op->profile["kind"] == "compute") {
                advance(op);
            } else {
                execute_operation(op);
            }
        }
    }
    void execute_operation(const std::shared_ptr<Operation>& op) {
        if (op->profile["kind"] == "local_dma") {
            copy(
                memory.resolve(op->profile.at("source")),
                memory.resolve(op->profile.at("destination")),
                text(op->profile, "requester", "local_dma"),
                op->profile.value("dma_id", ""), [this, op] { finish(op); },
                context(op->id, "$dma"));
        } else if (op->profile["kind"] == "send" ||
                   op->profile["kind"] == "recv") {
            auto ref = memory.resolve(op->profile.at("buffer"));
            const auto peer = op->metadata.at("peer").get<int>(),
                       tag = op->metadata.at("tag").get<int>();
            check(tag >= 0 && tag < 500000000,
                  "point-to-point tag outside native namespace");
            check(ref.bytes == op->metadata.at("comm_size").get<uint64_t>(),
                  "point-to-point comm_size mismatch");
            if (op->profile["kind"] == "send") {
                endpoint.send(
                    ref, peer, tag, [this, op] { finish(op); },
                    context(op->id, "$send"));
            } else {
                endpoint.recv(
                    ref, peer, tag, false, [this, op] { finish(op); },
                    context(op->id, "$recv"));
            }
        } else if (op->profile["kind"] == "collective") {
            prepare_collective(op);
        } else if (op->profile["kind"] == "remote_memory") {
            check(bool(transport.remote),
                  "remote memory transport unavailable");
            const auto bytes =
                number(op->profile, "remote_bytes", "remote_memory");
            check(bytes == op->metadata.at("tensor_size").get<uint64_t>(),
                  "remote_memory tensor_size mismatch");
            auto ref = memory.resolve(op->profile.at("local_buffer"));
            check(ref.bytes == bytes,
                  "remote_memory local buffer size mismatch");
            const bool load = op->metadata.at("et_type").get<int>() == 2;
            memory.retain(ref, !load);
            const auto unit = topology.configuration()
                                  .at("dma_engines")
                                  .at(0)
                                  .at("id")
                                  .get<std::string>();
            auto finish_remote = [this, op, ref] {
                memory.unretain(ref);
                finish(op);
            };
            const auto requester = op->profile.value("requester", unit);
            auto ctx = context(op->id, "$remote");
            if (load) {
                transport.remote(
                    bytes, [this, unit, requester, ref, finish_remote, ctx] {
                        memory.access(unit, requester, ref, true, finish_remote,
                                      true, ctx);
                    });
            } else {
                memory.access(
                    unit, requester, ref, false,
                    [this, bytes, finish_remote] {
                        transport.remote(bytes, finish_remote);
                    },
                    true, ctx);
            }
        } else {
            throw Error("operation kind not implemented: " +
                        op->profile["kind"].get<std::string>());
        }
    }
    void prepare_collective(const std::shared_ptr<Operation>& op) {
        check(bool(transport.collective), "collective transport unavailable");
        auto input = memory.resolve(op->profile.at("input")),
             output = memory.resolve(op->profile.at("output"));
        memory.validate(input, true);
        memory.validate(output, false);
        check(topology.modules().at(input.storage)["scope"] == "node_shared" &&
                  topology.modules().at(output.storage)["scope"] ==
                      "node_shared",
              "collective buffers require node_shared storage");
        check(input.storage != output.storage || input.id != output.id ||
                  add(input.offset, input.bytes) <= output.offset ||
                  add(output.offset, output.bytes) <= input.offset,
              "collective input/output overlap");
        const auto size = op->profile.at("participants").size();
        const auto kind = text(op->profile, "operation", "collective");
        check(kind != "all_gather" || input.bytes <= UINT64_MAX / size,
              "collective size overflow");
        const auto expected = kind == "all_gather"       ? input.bytes * size
                              : kind == "reduce_scatter" ? input.bytes / size
                                                         : input.bytes;
        check(output.bytes == expected &&
                  (kind != "reduce_scatter" || input.bytes % size == 0),
              "collective input/output size mismatch");
        check(number(op->profile, "elem_bytes", "collective") <= input.bytes &&
                  input.bytes %
                          number(op->profile, "elem_bytes", "collective") ==
                      0,
              "collective element alignment");
        auto workspace = output;
        if (op->profile.contains("workspace")) {
            workspace = memory.resolve(op->profile["workspace"]);
        }
        if (kind == "reduce_scatter" || kind == "all_to_all") {
            check(op->profile.contains("workspace"),
                  "collective requires explicit workspace");
        }
        const auto needed = kind == "all_to_all"   ? input.bytes / size
                            : kind == "all_gather" ? output.bytes
                                                   : input.bytes;
        check(workspace.bytes >= needed, "collective workspace too small");
        memory.validate(workspace, false);
        auto overlaps = [](const BufferRef& a, const BufferRef& b) {
            return a.storage == b.storage && a.id == b.id &&
                   a.offset < add(b.offset, b.bytes) &&
                   b.offset < add(a.offset, a.bytes);
        };
        check(!overlaps(workspace, input),
              "collective workspace overlaps input");
        if (kind == "all_to_all" || (kind == "reduce_scatter" && size > 1)) {
            check(!overlaps(workspace, output),
                  "collective workspace overlaps output");
        } else if (overlaps(workspace, output)) {
            check(workspace.offset == output.offset,
                  "collective workspace/output partial overlap");
        }
        check(topology.modules().at(workspace.storage)["scope"] ==
                  "node_shared",
              "collective workspace requires node_shared");
        memory.retain(input, true);
        memory.retain(output, false);
        if (workspace.storage != output.storage || workspace.id != output.id) {
            memory.retain(workspace, false);
        }
        op->input = input;
        op->output = output;
        op->workspace = workspace;
        const auto unit = topology.configuration()["collective_engine"]["id"]
                              .get<std::string>();
        auto source = input, dest = workspace;
        auto it = std::find(op->profile["participants"].begin(),
                            op->profile["participants"].end(), json(rank));
        const auto position =
            uint64_t(std::distance(op->profile["participants"].begin(), it));
        if (kind == "all_gather") {
            dest = workspace;
            dest.offset = add(dest.offset, position * input.bytes);
            dest.bytes = input.bytes;
        } else if (kind == "all_to_all") {
            source.bytes = dest.bytes = input.bytes / size;
            source.offset = add(input.offset, position * source.bytes);
            dest = output;
            dest.bytes = source.bytes;
            dest.offset = add(output.offset, position * dest.bytes);
        } else {
            dest.bytes = input.bytes;
        }
        clock.after(
            number(topology.configuration()["collective_engine"], "startup_ns",
                   "collective_engine", true),
            [this, op, source, dest, unit] {
                copy(
                    source, dest, unit, op->profile.value("dma_id", ""),
                    [this, op] {
                        transport.collective(op->id, op->profile, [this, op] {
                            const auto kind =
                                op->profile["operation"].get<std::string>();
                            const auto& w = *op->workspace;
                            const auto& o = *op->output;
                            if ((kind == "all_reduce" ||
                                 kind == "all_gather") &&
                                (w.storage != o.storage || w.id != o.id ||
                                 w.offset != o.offset)) {
                                auto source = w;
                                source.bytes = o.bytes;
                                const auto unit =
                                    topology
                                        .configuration()["collective_engine"]
                                                        ["id"]
                                        .get<std::string>();
                                copy(
                                    source, o, unit, "",
                                    [this, op] { finish(op); },
                                    context(op->id, "$collective_result"));
                            } else if (kind == "reduce_scatter" &&
                                       op->profile["participants"].size() ==
                                           1 &&
                                       (w.storage != o.storage ||
                                        w.id != o.id || w.offset != o.offset)) {
                                auto source = w;
                                source.bytes = o.bytes;
                                const auto unit =
                                    topology
                                        .configuration()["collective_engine"]
                                                        ["id"]
                                        .get<std::string>();
                                copy(
                                    source, o, unit, "",
                                    [this, op] { finish(op); },
                                    context(op->id, "$collective_result"));
                            } else {
                                clock.after(0, [this, op] { finish(op); }, 3);
                            }
                        });
                    },
                    context(op->id, "$collective_init"));
            });
    }
    void advance(const std::shared_ptr<Operation>& op) {
        const auto& stages = op->profile["stages"];
        bool complete = true;
        size_t inflight = std::count(op->state.begin(), op->state.end(), 1u);
        for (size_t i = 0; i < stages.size(); ++i) {
            if (op->state[i] != 2) {
                complete = false;
            }
            if (op->state[i] != 0 || inflight >= 8) {
                continue;
            }
            bool ready = true;
            for (const auto& dep : stages[i]["deps"]) {
                for (size_t j = 0; j < stages.size(); ++j) {
                    if (stages[j]["id"] == dep && op->state[j] != 2) {
                        ready = false;
                    }
                }
            }
            if (!ready) {
                continue;
            }
            op->state[i] = 1;
            ++inflight;
            try {
                execute_stage(op, stages[i], [this, op, i] {
                    op->state[i] = 2;
                    advance(op);
                });
            } catch (const Error& error) {
                throw Error("rank=" + std::to_string(rank) +
                            " op=" + std::to_string(op->id) +
                            " stage=" + stages[i]["id"].get<std::string>() +
                            " " + error.what());
            }
        }
        if (complete) {
            finish(op);
        }
    }
    void execute_stage(const std::shared_ptr<Operation>& op,
                       const json& stage,
                       Done done) {
        const auto& cu = topology.modules().at(op->cu);
        const auto local = cu.value("local_storage", "");
        auto ctx = context(op->id, stage["id"].get<std::string>());
        telemetry.record(ctx, "stage_started", op->cu,
                         {{"kind", stage["kind"]}});
        if (stage["kind"] == "compute") {
            const auto& cu = topology.modules().at(op->cu);
            const auto elapsed =
                add(number(cu, "startup_ns", op->cu, true),
                    service(number(stage, "ops", "stage", true),
                            rate(cu, "ops_per_ns", op->cu)));
            auto& result = stats["compute_units"][op->cu];
            if (result.is_null()) {
                result = json::object();
            }
            const auto begin = std::max(
                clock.now(), result.value("available_ns", uint64_t{0}));
            result["available_ns"] = add(begin, elapsed);
            result["busy_ns"] =
                add(result.value("busy_ns", uint64_t{0}), elapsed);
            telemetry.add_time(op->id, "compute_busy_ns", elapsed);
            clock.after(add(begin - clock.now(), elapsed), std::move(done), 0);
        } else if (stage["kind"] == "allocate" || stage["kind"] == "free") {
            auto request = stage;
            request.erase("id");
            request.erase("kind");
            request.erase("deps");
            if (request["storage"] == "$cu.local_storage") {
                request["storage"] = local;
            }
            if (stage["kind"] == "allocate") {
                memory.allocate(request, ctx);
            } else {
                memory.release(request, ctx);
            }
            clock.after(0, std::move(done));
        } else if (stage["kind"] == "read" || stage["kind"] == "write") {
            memory.access(op->cu, op->cu,
                          memory.resolve(stage.at("buffer"), local),
                          stage["kind"] == "write", std::move(done), true, ctx);
        } else if (stage["kind"] == "dma_copy") {
            copy(memory.resolve(stage.at("source"), local),
                 memory.resolve(stage.at("destination"), local),
                 stage.value("requester", op->cu), stage.value("dma_id", ""),
                 std::move(done), ctx);
        } else {
            throw Error("stage kind not implemented: " +
                        stage["kind"].get<std::string>());
        }
    }
    void finish(const std::shared_ptr<Operation>& op) {
        check(operations.count(op->id), "duplicate operation completion");
        if (!op->cu.empty()) {
            busy.erase(op->cu);
        }
        if (op->profile["kind"] == "collective") {
            --active_collectives;
            communicator_busy.erase(op->communicator);
            memory.unretain(*op->input);
            memory.unretain(*op->output);
            if (op->workspace->storage != op->output->storage ||
                op->workspace->id != op->output->id) {
                memory.unretain(*op->workspace);
            }
        }
        stats["operations"][std::to_string(op->id)]["end_ns"] = clock.now();
        telemetry.record(context(op->id, "$complete"), "operation_completed",
                         "node");
        operations.erase(op->id);
        order.erase(std::find(order.begin(), order.end(), op->id));
        request_dispatch();
        clock.after(
            0,
            [this, op] {
                if (op->completed) {
                    op->completed(op->id, clock.now());
                }
                if (capacity) {
                    capacity();
                }
            },
            3);
    }
};
NodeModel::NodeModel(json configuration,
                     json profiles,
                     NodeEventScheduler& clock,
                     int rank)
    : impl_(std::make_unique<Impl>(
          std::move(configuration), std::move(profiles), clock, rank)) {}
NodeModel::~NodeModel() = default;
std::pair<json, json> NodeModel::load(const std::string& filename) {
    const std::filesystem::path path(filename);
    auto configuration = read_json(path);
    const auto profile = text(configuration, "operation_profiles", filename);
    return {configuration, read_json(path.parent_path() / profile)};
}
bool NodeModel::try_submit(uint64_t id,
                           const std::string& name,
                           json metadata,
                           Observer started,
                           Observer completed) {
    auto& m = *impl_;
    if (metadata.is_null()) {
        metadata = json::object();
    }
    check(!m.used_ids.count(id),
          "duplicate operation ID " + std::to_string(id));
    json profile;
    if (metadata.value("cpu", false)) {
        profile = {{"kind", "cpu"}};
    } else {
        check(m.profiles["profiles"].contains(name), "missing profile " + name);
        profile = m.profiles["profiles"][name];
        if (metadata.contains("et_type")) {
            const auto type = metadata["et_type"].get<int>();
            const auto kind = profile["kind"].get<std::string>();
            check((type == 4 && kind == "compute") ||
                      (type == 5 && kind == "send") ||
                      (type == 6 && kind == "recv") ||
                      (type == 7 && kind == "collective") ||
                      ((type == 2 || type == 3) &&
                       (kind == "local_dma" || kind == "remote_memory")),
                  "ET node type incompatible with profile kind");
        }
    }
    if (!m.first_attempt.count(id)) {
        m.first_attempt[id] = m.clock.now();
    }
    if (m.operations.size() >=
        number(m.topology.configuration(), "admission_capacity", "node")) {
        return false;
    }
    if (profile["kind"] == "collective") {
        size_t queued = 0;
        for (const auto& [key, operation] : m.operations) {
            if (operation->profile["kind"] == "collective") {
                ++queued;
            }
        }
        if (queued >= number(m.topology.configuration()["collective_engine"],
                             "queue_entries", "collective_engine")) {
            return false;
        }
    }
    auto op = std::make_shared<Impl::Operation>();
    op->id = id;
    op->profile_name = name;
    op->profile = std::move(profile);
    op->metadata = std::move(metadata);
    op->started = std::move(started);
    op->completed = std::move(completed);
    op->enqueue = m.clock.now();
    if (op->profile.contains("stages")) {
        op->state.resize(op->profile["stages"].size());
    }
    if (op->profile["kind"] == "collective") {
        const auto& participants = op->profile.at("participants");
        check(participants.is_array() && !participants.empty(),
              "collective participants required");
        std::set<int> ranks;
        for (const auto& r : participants) {
            check(r.is_number_integer() && r.get<int>() >= 0,
                  "invalid participant rank");
            check(ranks.insert(r.get<int>()).second,
                  "duplicate participant rank");
        }
        check(ranks.count(m.rank), "rank not in collective participants");
        const auto kind = text(op->profile, "operation", "collective");
        check(std::set<std::string>{"all_reduce", "all_gather",
                                    "reduce_scatter", "all_to_all"}
                  .count(kind),
              "unsupported collective operation");
        const auto pg = op->metadata.value("pg_name", std::string("0"));
        op->communicator = participants.dump() + "@" + (pg.empty() ? "0" : pg);
        const auto instance =
            number(op->profile, "instance_id", "collective", true);
        check(m.instances.insert({op->communicator, instance}).second,
              "duplicate collective instance_id");
    }
    m.operations[id] = op;
    m.order.push_back(id);
    m.used_ids.insert(id);
    m.stats["operations"][std::to_string(id)] = {
        {"enqueue_ns", op->enqueue},
        {"profile", name},
        {"admission_wait_ns", op->enqueue - m.first_attempt.at(id)}};
    m.request_dispatch();
    return true;
}
void NodeModel::on_capacity(Done done) {
    impl_->capacity = std::move(done);
}
void NodeModel::transport(Transport t) {
    impl_->endpoint.transport(t.send, t.recv);
    impl_->transport = std::move(t);
}
bool NodeModel::idle() const {
    return impl_->operations.empty() && impl_->memory.idle() &&
           impl_->dma.idle() && impl_->endpoint.idle();
}
json NodeModel::statistics() const {
    auto result = impl_->stats;
    result.update(impl_->memory.statistics());
    result["dma_engines"] = impl_->dma.statistics();
    result["network_endpoint"] = impl_->endpoint.statistics();
    result["trace"] = impl_->telemetry.trace();
    result["active_waits"] = json::array();
    for (const auto& [id, op] : impl_->operations) {
        json stages = json::array();
        for (size_t i = 0; i < op->state.size(); ++i) {
            if (op->state[i] != 2) {
                stages.push_back({{"id", op->profile["stages"][i]["id"]},
                                  {"state", op->state[i] == 1
                                                ? "waiting_resource_or_response"
                                                : "waiting_dependencies"}});
            }
        }
        result["active_waits"].push_back({{"op", id},
                                          {"cu", op->cu},
                                          {"running", op->running},
                                          {"stages", stages}});
    }
    for (auto it = result["operations"].begin();
         it != result["operations"].end(); ++it) {
        it.value().update(impl_->telemetry.metrics(std::stoull(it.key())));
    }
    result["timing_categories"] = "Accumulated overlapping request/packet "
                                  "waits; not additive wall-time components";
    result["effective_runtime_limits"] = {
        {"max_inflight_stages_per_operation", 8},
        {"memory_transactions_per_access", 8},
        {"direct_link_buffer_packets_per_class", 8}};
    return result;
}
const NodeTopology& NodeModel::topology() const {
    return impl_->topology;
}
const json& NodeModel::operation_profile(uint64_t id) const {
    return impl_->operations.at(id)->profile;
}
BufferRef NodeModel::operation_workspace(uint64_t id) const {
    return *impl_->operations.at(id)->workspace;
}
void NodeModel::local_copy(uint64_t operation,
                           BufferRef source,
                           BufferRef destination,
                           Done done) {
    const auto unit = impl_->topology.configuration()
                          .at("collective_engine")
                          .at("id")
                          .get<std::string>();
    impl_->copy(source, destination, unit, "", std::move(done),
                impl_->context(operation, "$collective_result"));
}
void NodeModel::collective_step(uint64_t id,
                                BufferRef source,
                                BufferRef destination,
                                bool reduce,
                                int send_peer,
                                int recv_peer,
                                int tag,
                                Done done,
                                json metadata) {
    check(impl_->operations.count(id), "missing collective transfer operation");
    auto ctx = impl_->context(id, "$ring_step");
    metadata.update({{"send_peer", send_peer},
                     {"recv_peer", recv_peer},
                     {"tag", tag},
                     {"bytes", source.bytes},
                     {"source_storage", source.storage},
                     {"source_offset", source.offset},
                     {"destination_storage", destination.storage},
                     {"destination_offset", destination.offset},
                     {"reduce", reduce}});
    impl_->telemetry.record(ctx, "collective_step", "collective_engine",
                            metadata);
    if (source.storage == destination.storage && source.id == destination.id &&
        source.offset < add(destination.offset, destination.bytes) &&
        destination.offset < add(source.offset, source.bytes)) {
        // Ring all-to-all forwarding may reuse one scratch range. Consume the
        // old payload before allowing the next incoming payload to overwrite
        // it.
        impl_->endpoint.send(
            source, send_peer, tag,
            [this, destination, recv_peer, tag, reduce, done, ctx] {
                impl_->endpoint.recv(destination, recv_peer, tag, reduce, done,
                                     ctx);
            },
            ctx);
        return;
    }
    auto count = std::make_shared<unsigned>(0);
    auto finish = [count, done] {
        if (++*count == 2) {
            done();
        }
    };
    impl_->endpoint.recv(destination, recv_peer, tag, reduce, finish, ctx);
    impl_->endpoint.send(source, send_peer, tag, finish, ctx);
}
}  // namespace AstraSim::NodeSim
