#include "astra-sim/node/NodeModel.hh"
#include <iostream>
#include <queue>
#include <stdexcept>

using namespace AstraSim::NodeSim;
namespace {
void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}
class Clock {
  public:
    using Entry =
        std::pair<std::pair<uint64_t, uint64_t>, std::function<void()>>;
    struct Later {
        bool operator()(const Entry& a, const Entry& b) const {
            return a.first > b.first;
        }
    };
    uint64_t tick = 0, sequence = 0;
    std::priority_queue<Entry, std::vector<Entry>, Later> events;
    NodeEventScheduler scheduler{
        [this] { return tick; },
        [this](uint64_t delay, auto callback) {
            events.push({{tick + delay, sequence++}, std::move(callback)});
        }};
    void run() {
        size_t count = 0;
        while (!events.empty()) {
            require(++count < 1000000, "event loop did not converge");
            auto entry = events.top();
            events.pop();
            tick = entry.first.first;
            entry.second();
        }
    }
};
json configuration() {
    return {{"schema_version", 1},
            {"admission_capacity", 8},
            {"compute_units", json::array({{{"id", "cu0"},
                                            {"kind", "tensor"},
                                            {"ops_per_ns", 10},
                                            {"startup_ns", 2}},
                                           {{"id", "cu1"},
                                            {"kind", "tensor"},
                                            {"ops_per_ns", 10},
                                            {"startup_ns", 2}}})},
            {"storages", json::array()},
            {"dma_engines", json::array()},
            {"noc",
             {{"rows", 1},
              {"columns", 1},
              {"routing", "xy"},
              {"packet_payload_bytes", 64},
              {"header_bytes", 16},
              {"link_bytes_per_ns", 32},
              {"link_latency_ns", 1},
              {"router_latency_ns", 1},
              {"input_buffer_packets_per_class", 2},
              {"attachments", json::array()}}},
            {"direct_links", json::array()}};
}
void parallel_compute() {
    Clock clock;
    auto profiles = json::parse(R"({"schema_version":1,"profiles":{"compute":{
        "kind":"compute","cu_kind":"tensor","stages":[
            {"id":"compute","kind":"compute","ops":100,"deps":[]}
        ]}}})");
    NodeModel node(configuration(), profiles, clock.scheduler, 0);
    std::vector<uint64_t> completed;
    require(
        node.try_submit(1, "compute", {}, {},
                        [&](auto, auto tick) { completed.push_back(tick); }),
        "first accepted");
    require(
        node.try_submit(2, "compute", {}, {},
                        [&](auto, auto tick) { completed.push_back(tick); }),
        "second accepted");
    require(completed.empty(), "completion must be asynchronous");
    clock.run();
    require(completed == std::vector<uint64_t>({12, 12}),
            "two CUs must overlap");
    require(node.idle(), "node must drain");
}

json compute_profile(json stages) {
    return {
        {"schema_version", 1},
        {"profiles",
         {{"op",
           {{"kind", "compute"}, {"cu_kind", "tensor"}, {"stages", stages}}}}}};
}
json memory_configuration(uint64_t capacity = 65536) {
    auto c = configuration();
    c["noc"]["columns"] = 2;
    c["storages"] = json::array({{{"id", "shared"},
                                  {"kind", "sram"},
                                  {"scope", "node_shared"},
                                  {"clients", json::array({"*"})},
                                  {"capacity_bytes", capacity},
                                  {"bytes_per_ns", 32},
                                  {"access_latency_ns", 5},
                                  {"queue_entries", 2}}});
    c["dma_engines"] = json::array({{{"id", "dma0"},
                                     {"queue_entries", 2},
                                     {"max_outstanding", 2},
                                     {"burst_bytes", 256},
                                     {"bytes_per_ns", 32},
                                     {"startup_ns", 4}}});
    c["noc"]["attachments"] = json::array(
        {{{"port", "cu0.fabric"}, {"router", json::array({0, 0})}},
         {{"port", "cu1.fabric"}, {"router", json::array({0, 0})}},
         {{"port", "shared.fabric"}, {"router", json::array({1, 0})}},
         {{"port", "dma0.fabric"}, {"router", json::array({0, 0})}}});
    return c;
}
json buffer(const std::string& id, uint64_t bytes, uint64_t offset = 0) {
    return {{"storage", "shared"},
            {"buffer_id", id},
            {"generation", 0},
            {"offset", offset},
            {"bytes", bytes}};
}
template <class F> void expect_error(const std::string& expected, F action) {
    bool raised = false;
    try {
        action();
    } catch (const Error& error) {
        raised = true;
        require(std::string(error.what()).find(expected) != std::string::npos,
                error.what());
    }
    require(raised, "expected diagnosed error");
}
void storage_capacity() {
    Clock clock;
    auto profiles = compute_profile(json::array({{{"id", "alloc"},
                                                  {"kind", "allocate"},
                                                  {"storage", "shared"},
                                                  {"buffer_id", "new"},
                                                  {"generation", 0},
                                                  {"bytes", 32768},
                                                  {"deps", json::array()}}}));
    profiles["initial_buffers"] = json::array({{{"storage", "shared"},
                                                {"buffer_id", "old"},
                                                {"generation", 0},
                                                {"bytes", 49152},
                                                {"initialized", true}}});
    NodeModel node(memory_configuration(), profiles, clock.scheduler, 0);
    node.try_submit(1, "op", {}, {}, {});
    expect_error("CapacityExceeded", [&] { clock.run(); });
    require(
        node.statistics()["storages"]["shared"]["occupied_or_reserved_bytes"] ==
            49152,
        "failed allocation must be atomic");
}
void read_round_trip() {
    Clock clock;
    auto profiles =
        compute_profile(json::array({{{"id", "read"},
                                      {"kind", "read"},
                                      {"buffer", buffer("input", 64)},
                                      {"deps", json::array()}}}));
    profiles["initial_buffers"] = json::array({{{"storage", "shared"},
                                                {"buffer_id", "input"},
                                                {"generation", 0},
                                                {"bytes", 64},
                                                {"initialized", true}}});
    NodeModel node(memory_configuration(), profiles, clock.scheduler, 0);
    Tick end = 0;
    node.try_submit(1, "op", {}, {}, [&](auto, auto tick) { end = tick; });
    clock.run();
    // Header: 2+3+3 ns; storage: 5+2 ns; data response: 4+5+5 ns.
    require(end == 29,
            "read must include requests, source service and response path");
    require(node.statistics()["storages"]["shared"]["read_bytes"] == 64,
            "read bytes conserved");
    require(node.idle(), "read drains");
}
void unavailable_data() {
    Clock clock;
    auto profiles =
        compute_profile(json::array({{{"id", "read"},
                                      {"kind", "read"},
                                      {"buffer", buffer("input", 64)},
                                      {"deps", json::array()}}}));
    profiles["initial_buffers"] = json::array({{{"storage", "shared"},
                                                {"buffer_id", "input"},
                                                {"generation", 0},
                                                {"bytes", 64},
                                                {"initialized", false}}});
    NodeModel node(memory_configuration(), profiles, clock.scheduler, 0);
    node.try_submit(1, "op", {}, {}, {});
    expect_error("DataUnavailable", [&] { clock.run(); });
}
void dma_copy() {
    Clock clock;
    auto profiles = json::parse(R"({"schema_version":1,"initial_buffers":[
       {"storage":"shared","buffer_id":"source","generation":0,"bytes":4096,"initialized":true},
       {"storage":"shared","buffer_id":"destination","generation":0,"bytes":4096,"initialized":false}
    ],"profiles":{}})");
    profiles["profiles"]["copy"] = {
        {"kind", "local_dma"},
        {"source", buffer("source", 4096)},
        {"destination", buffer("destination", 4096)},
        {"requester", "cu0"}};
    profiles["profiles"]["read"] = {
        {"kind", "compute"},
        {"cu_kind", "tensor"},
        {"stages", json::array({{{"id", "read"},
                                 {"kind", "read"},
                                 {"buffer", buffer("destination", 4096)},
                                 {"deps", json::array()}}})}};
    NodeModel node(memory_configuration(), profiles, clock.scheduler, 0);
    unsigned completed = 0;
    node.try_submit(1, "copy", {}, {}, [&](auto, auto) {
        ++completed;
        node.try_submit(2, "read", {}, {}, [&](auto, auto) { ++completed; });
    });
    clock.run();
    require(completed == 2 && node.idle(),
            "DMA publishes all data then completes");
    auto stats = node.statistics();
    require(stats["dma_engines"]["dma0"]["bytes"] == 4096,
            "DMA byte conservation");
    require(stats["storages"]["shared"]["write_bytes"] == 4096,
            "DMA destination service counted once");
    require(stats["storages"]["shared"]["read_bytes"] == 8192,
            "source and later consumer reads counted");
}
void single_cu() {
    Clock clock;
    auto config = configuration();
    config["compute_units"].erase(1);
    auto profiles = compute_profile(json::array({{{"id", "c"},
                                                  {"kind", "compute"},
                                                  {"ops", 100},
                                                  {"deps", json::array()}}}));
    NodeModel node(config, profiles, clock.scheduler, 0);
    std::vector<Tick> ends;
    node.try_submit(1, "op", {}, {},
                    [&](auto, auto tick) { ends.push_back(tick); });
    node.try_submit(2, "op", {}, {},
                    [&](auto, auto tick) { ends.push_back(tick); });
    clock.run();
    require(ends == std::vector<Tick>({12, 24}),
            "one CU serializes independent operations");
}
void admission_recovery() {
    Clock clock;
    auto config = configuration();
    config["admission_capacity"] = 1;
    auto profiles = compute_profile(json::array({{{"id", "c"},
                                                  {"kind", "compute"},
                                                  {"ops", 100},
                                                  {"deps", json::array()}}}));
    NodeModel node(config, profiles, clock.scheduler, 0);
    unsigned completed = 0;
    bool accepted = false;
    auto done = [&](auto, auto) { ++completed; };
    node.try_submit(1, "op", {}, {}, done);
    require(!node.try_submit(2, "op", {}, {}, done),
            "full admission must not accept");
    node.on_capacity([&] {
        if (!accepted) {
            accepted = node.try_submit(2, "op", {}, {}, done);
        }
    });
    clock.run();
    require(accepted && completed == 2 && node.idle(),
            "capacity notification resumes a rejected submission exactly once");
}
void concurrent_reservations() {
    Clock clock;
    auto config = memory_configuration();
    auto profiles = compute_profile(json::array({{{"id", "a"},
                                                  {"kind", "allocate"},
                                                  {"storage", "shared"},
                                                  {"buffer_id", "one"},
                                                  {"generation", 0},
                                                  {"bytes", 40960},
                                                  {"deps", json::array()}}}));
    profiles["profiles"]["two"] = profiles["profiles"]["op"];
    profiles["profiles"]["two"]["stages"][0]["buffer_id"] = "two";
    NodeModel node(config, profiles, clock.scheduler, 0);
    node.try_submit(1, "op", {}, {}, {});
    node.try_submit(2, "two", {}, {}, {});
    expect_error("CapacityExceeded", [&] { clock.run(); });
    require(
        node.statistics()["storages"]["shared"]["occupied_or_reserved_bytes"] ==
            40960,
        "concurrent reservations must be atomic");
}
void release_and_generation() {
    Clock clock;
    auto stages = json::parse(R"([
      {"id":"a","kind":"allocate","storage":"shared","buffer_id":"temp","generation":0,"bytes":65536,"deps":[]},
      {"id":"b","kind":"free","storage":"shared","buffer_id":"temp","generation":0,"deps":["a"]},
      {"id":"c","kind":"allocate","storage":"shared","buffer_id":"temp","generation":1,"bytes":65536,"deps":["b"]},
      {"id":"d","kind":"free","storage":"shared","buffer_id":"temp","generation":1,"deps":["c"]}
    ])");
    NodeModel node(memory_configuration(), compute_profile(stages),
                   clock.scheduler, 0);
    node.try_submit(1, "op", {}, {}, {});
    clock.run();
    auto stats = node.statistics()["storages"]["shared"];
    require(stats["occupied_or_reserved_bytes"] == 0 &&
                stats["peak_bytes"] == 65536,
            "explicit free reclaims capacity");
    require(stats["allocate_count"] == 2 && stats["free_count"] == 2,
            "generation reuse is explicit");
}
void free_while_in_use() {
    Clock clock;
    auto profiles =
        compute_profile(json::array({{{"id", "r"},
                                      {"kind", "read"},
                                      {"buffer", buffer("input", 64)},
                                      {"deps", json::array()}},
                                     {{"id", "f"},
                                      {"kind", "free"},
                                      {"storage", "shared"},
                                      {"buffer_id", "input"},
                                      {"generation", 0},
                                      {"deps", json::array()}}}));
    profiles["initial_buffers"] = json::array({{{"storage", "shared"},
                                                {"buffer_id", "input"},
                                                {"generation", 0},
                                                {"bytes", 64},
                                                {"initialized", true}}});
    NodeModel node(memory_configuration(), profiles, clock.scheduler, 0);
    node.try_submit(1, "op", {}, {}, {});
    expect_error("BufferInUse", [&] { clock.run(); });
}
void stage_cycle() {
    Clock clock;
    auto stages = json::parse(R"([
      {"id":"a","kind":"compute","ops":1,"deps":["b"]},
      {"id":"b","kind":"compute","ops":1,"deps":["a"]}
    ])");
    expect_error("cycle", [&] {
        NodeModel node(configuration(), compute_profile(stages),
                       clock.scheduler, 0);
    });
}
void mesh_paths(bool reverse) {
    Clock clock;
    auto config = memory_configuration();
    config["noc"]["rows"] = 2;
    auto second = config["storages"][0];
    second["id"] = "other";
    config["storages"].push_back(second);
    config["noc"]["attachments"] = json::parse(R"([
       {"port":"cu0.fabric","router":[0,0]}, {"port":"cu1.fabric","router":[1,0]},
       {"port":"shared.fabric","router":[0,1]}, {"port":"other.fabric","router":[1,1]},
       {"port":"dma0.fabric","router":[0,0]}])");
    if (reverse) {
        config["noc"]["attachments"][1]["router"] = json::array({1, 0});
        config["noc"]["attachments"][2]["router"] = json::array({1, 0});
        config["noc"]["attachments"][3]["router"] = json::array({0, 0});
    }
    auto profiles = compute_profile(json::array({{{"id", "r"},
                                                  {"kind", "read"},
                                                  {"buffer", buffer("a", 64)},
                                                  {"deps", json::array()}}}));
    profiles["profiles"]["op"]["cu_id"] = "cu0";
    profiles["profiles"]["other"] = profiles["profiles"]["op"];
    profiles["profiles"]["other"]["cu_id"] = "cu1";
    profiles["profiles"]["other"]["stages"][0]["buffer"]["storage"] = "other";
    profiles["profiles"]["other"]["stages"][0]["buffer"]["buffer_id"] = "b";
    profiles["initial_buffers"] = json::array({{{"storage", "shared"},
                                                {"buffer_id", "a"},
                                                {"generation", 0},
                                                {"bytes", 64},
                                                {"initialized", true}},
                                               {{"storage", "other"},
                                                {"buffer_id", "b"},
                                                {"generation", 0},
                                                {"bytes", 64},
                                                {"initialized", true}}});
    NodeModel node(config, profiles, clock.scheduler, 0);
    std::vector<Tick> ends;
    node.try_submit(1, "op", {}, {}, [&](auto, auto t) { ends.push_back(t); });
    node.try_submit(2, "other", {}, {},
                    [&](auto, auto t) { ends.push_back(t); });
    clock.run();
    require(ends == std::vector<Tick>({29, 29}),
            "independent mesh paths must not use one global bus lock");
}
void disjoint_mesh_paths() {
    mesh_paths(false);
}
void full_duplex_paths() {
    mesh_paths(true);
}
void two_dma_engines() {
    Clock clock;
    auto config = memory_configuration();
    auto second = config["dma_engines"][0];
    second["id"] = "dma1";
    config["dma_engines"].push_back(second);
    config["noc"]["attachments"].push_back(
        {{"port", "dma1.fabric"}, {"router", json::array({0, 0})}});
    json profiles = {
        {"schema_version", 1},
        {"profiles", json::object()},
        {"initial_buffers", json::array({{{"storage", "shared"},
                                          {"buffer_id", "a"},
                                          {"generation", 0},
                                          {"bytes", 128},
                                          {"initialized", true}},
                                         {{"storage", "shared"},
                                          {"buffer_id", "b"},
                                          {"generation", 0},
                                          {"bytes", 128},
                                          {"initialized", false}}})}};
    profiles["profiles"]["one"] = {{"kind", "local_dma"},
                                   {"source", buffer("a", 64)},
                                   {"destination", buffer("b", 64)},
                                   {"requester", "cu0"}};
    profiles["profiles"]["two"] = {{"kind", "local_dma"},
                                   {"source", buffer("a", 64, 64)},
                                   {"destination", buffer("b", 64, 64)},
                                   {"requester", "cu0"}};
    NodeModel node(config, profiles, clock.scheduler, 0);
    node.try_submit(1, "one", {}, {}, {});
    node.try_submit(2, "two", {}, {}, {});
    clock.run();
    auto stats = node.statistics()["dma_engines"];
    require(stats["dma0"]["bytes"] == 64 && stats["dma1"]["bytes"] == 64,
            "unbound copies must use both idle engines");
}
void storage_queue_backpressure() {
    Clock clock;
    auto config = memory_configuration();
    config["storages"][0]["queue_entries"] = 1;
    config["noc"]["input_buffer_packets_per_class"] = 1;
    auto profiles = compute_profile(json::array({{{"id", "r"},
                                                  {"kind", "read"},
                                                  {"buffer", buffer("a", 4096)},
                                                  {"deps", json::array()}}}));
    profiles["initial_buffers"] = json::array({{{"storage", "shared"},
                                                {"buffer_id", "a"},
                                                {"generation", 0},
                                                {"bytes", 4096},
                                                {"initialized", true}}});
    NodeModel node(config, profiles, clock.scheduler, 0);
    node.try_submit(1, "op", {}, {}, {});
    clock.run();
    require(node.idle(), "finite buffers must recover without losing requests");
    require(node.statistics()["storages"]["shared"]["queue_peak"] == 1,
            "storage queue bound must be respected");
}
void pending_dma_pins_buffers() {
    Clock clock;
    auto config = memory_configuration();
    config["dma_engines"][0]["queue_entries"] = 1;
    auto profiles =
        compute_profile(json::array({{{"id", "one"},
                                      {"kind", "dma_copy"},
                                      {"source", buffer("a", 64)},
                                      {"destination", buffer("b", 64)},
                                      {"deps", json::array()}},
                                     {{"id", "two"},
                                      {"kind", "dma_copy"},
                                      {"source", buffer("a", 64)},
                                      {"destination", buffer("c", 64)},
                                      {"deps", json::array()}},
                                     {{"id", "free"},
                                      {"kind", "free"},
                                      {"storage", "shared"},
                                      {"buffer_id", "c"},
                                      {"generation", 0},
                                      {"deps", json::array()}}}));
    profiles["initial_buffers"] = json::array();
    for (auto id : {"a", "b", "c"}) {
        profiles["initial_buffers"].push_back(
            {{"storage", "shared"},
             {"buffer_id", id},
             {"generation", 0},
             {"bytes", 64},
             {"initialized", std::string(id) == "a"}});
    }
    NodeModel node(config, profiles, clock.scheduler, 0);
    node.try_submit(1, "op", {}, {}, {});
    expect_error("BufferInUse", [&] { clock.run(); });
}
void eligible_local_storage_cu() {
    Clock clock;
    auto config = memory_configuration();
    config["compute_units"][0]["local_storage"] = "shared";
    config["storages"][0]["scope"] = "cu_private";
    config["storages"][0]["clients"] = json::array({"cu0"});
    config["direct_links"].push_back({{"a", "cu0.mem"},
                                      {"b", "shared.local"},
                                      {"bytes_per_ns", 32},
                                      {"latency_ns", 1}});
    auto stages = json::parse(R"([
        {"id":"a","kind":"allocate","storage":"$cu.local_storage","buffer_id":"one","generation":0,"bytes":64,"deps":[]},
        {"id":"c","kind":"compute","ops":100,"deps":["a"]},
        {"id":"f","kind":"free","storage":"$cu.local_storage","buffer_id":"one","generation":0,"deps":["c"]}
    ])");
    auto profiles = compute_profile(stages);
    profiles["profiles"]["two"] = profiles["profiles"]["op"];
    profiles["profiles"]["two"]["stages"][0]["buffer_id"] = "two";
    profiles["profiles"]["two"]["stages"][2]["buffer_id"] = "two";
    NodeModel node(config, profiles, clock.scheduler, 0);
    std::vector<Tick> ends;
    node.try_submit(1, "op", {}, {}, [&](auto, auto t) { ends.push_back(t); });
    node.try_submit(2, "two", {}, {}, [&](auto, auto t) { ends.push_back(t); });
    clock.run();
    require(ends == std::vector<Tick>({12, 24}),
            "profile must not dispatch to a CU without required local storage");
}
void fragmented_storage() {
    Clock clock;
    auto stages = json::parse(R"([
        {"id":"a","kind":"allocate","storage":"shared","buffer_id":"a","generation":0,"bytes":16384,"deps":[]},
        {"id":"b","kind":"allocate","storage":"shared","buffer_id":"b","generation":0,"bytes":16384,"deps":["a"]},
        {"id":"c","kind":"allocate","storage":"shared","buffer_id":"c","generation":0,"bytes":16384,"deps":["b"]},
        {"id":"f","kind":"free","storage":"shared","buffer_id":"b","generation":0,"deps":["c"]},
        {"id":"d","kind":"allocate","storage":"shared","buffer_id":"d","generation":0,"bytes":32768,"deps":["f"]}
    ])");
    NodeModel node(memory_configuration(), compute_profile(stages),
                   clock.scheduler, 0);
    node.try_submit(1, "op", {}, {}, {});
    expect_error("reason=fragmented", [&] { clock.run(); });
}
}  // namespace
int main() {
    try {
        for (const auto& test : std::vector<std::pair<const char*, void (*)()>>{
                 {"parallel_compute", parallel_compute},
                 {"storage_capacity", storage_capacity},
                 {"read_round_trip", read_round_trip},
                 {"unavailable_data", unavailable_data},
                 {"dma_copy", dma_copy},
                 {"single_cu", single_cu},
                 {"admission_recovery", admission_recovery},
                 {"concurrent_reservations", concurrent_reservations},
                 {"release_and_generation", release_and_generation},
                 {"free_while_in_use", free_while_in_use},
                 {"stage_cycle", stage_cycle},
                 {"disjoint_mesh_paths", disjoint_mesh_paths},
                 {"full_duplex_paths", full_duplex_paths},
                 {"two_dma_engines", two_dma_engines},
                 {"storage_queue_backpressure", storage_queue_backpressure},
                 {"pending_dma_pins_buffers", pending_dma_pins_buffers},
                 {"eligible_local_storage_cu", eligible_local_storage_cu},
                 {"fragmented_storage", fragmented_storage}}) {
            test.second();
            std::cout << "PASS " << test.first << '\n';
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
