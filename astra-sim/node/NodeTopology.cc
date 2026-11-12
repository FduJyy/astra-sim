#include "astra-sim/node/NodeTopology.hh"
#include <cctype>

namespace AstraSim::NodeSim {
namespace {
std::string router(uint64_t x, uint64_t y) {
    return "router:" + std::to_string(x) + ":" + std::to_string(y);
}
}  // namespace
void NodeTopology::module(const json& entry,
                          const std::string& type,
                          const std::string& path) {
    const auto id = text(entry, "id", path);
    check(std::all_of(id.begin(), id.end(),
                      [](unsigned char c) {
                          return std::isalnum(c) || c == '_' || c == '-';
                      }),
          path + ": invalid module ID");
    check(!modules_.count(id), path + ": duplicate module " + id);
    modules_[id] = entry;
    types_[id] = type;
}
void NodeTopology::bind(const std::string& port) {
    const auto dot = port.find('.');
    check(dot != std::string::npos, "port: expected module.port: " + port);
    const auto id = port.substr(0, dot), name = port.substr(dot + 1);
    check(types_.count(id), "port: unknown module " + port);
    const auto& type = types_.at(id);
    check(name == "fabric" || (type == "cu" && name == "mem") ||
              (type == "storage" && name == "local"),
          "unknown port " + port);
    check(bound_.insert(port).second, "duplicate port connection " + port);
}
void NodeTopology::link(const std::string& src,
                        const std::string& dst,
                        double bandwidth,
                        Tick latency,
                        Tick router_latency,
                        uint64_t buffers) {
    const auto id = src + "->" + dst;
    check(!links_.count(id), "duplicate link " + id);
    links_.emplace(id, LinkDescription{id, dst, bandwidth, latency,
                                       router_latency, buffers});
}
NodeTopology::NodeTopology(json configuration)
    : config_(std::move(configuration)) {
    fields(config_,
           {"schema_version", "admission_capacity", "cpu", "compute_units",
            "storages", "dma_engines", "collective_engine", "network_endpoint",
            "noc", "direct_links", "operation_profiles", "trace"},
           "node");
    check(number(config_, "schema_version", "node") == 1,
          "node.schema_version: unsupported");
    number(config_, "admission_capacity", "node");
    if (config_.contains("cpu")) {
        fields(config_["cpu"], {"slots"}, "cpu");
        check(number(config_["cpu"], "slots", "cpu") == 1,
              "cpu.slots: only one supported");
    }
    for (const auto* array : {"compute_units", "storages", "dma_engines"}) {
        check(config_.contains(array) && config_[array].is_array(),
              std::string("node.") + array + ": expected array");
    }
    check(!config_["compute_units"].empty(),
          "compute_units: at least one CU required");
    for (const auto& cu : config_["compute_units"]) {
        fields(cu, {"id", "kind", "ops_per_ns", "startup_ns", "local_storage"},
               "compute_units");
        module(cu, "cu", "compute_units");
        text(cu, "kind", "compute_units");
        rate(cu, "ops_per_ns", "compute_units");
        number(cu, "startup_ns", "compute_units", true);
    }
    for (const auto& storage : config_["storages"]) {
        fields(storage,
               {"id", "kind", "scope", "clients", "capacity_bytes",
                "bytes_per_ns", "access_latency_ns", "queue_entries"},
               "storages");
        module(storage, "storage", "storages");
        const auto kind = text(storage, "kind", "storages"),
                   scope = text(storage, "scope", "storages");
        check(kind == "sram" || kind == "hbm" || kind == "ddr",
              "storages.kind: unsupported");
        check(scope == "cu_private" || scope == "group_local" ||
                  scope == "node_shared",
              "storages.scope: unsupported");
        check(storage.contains("clients") && storage["clients"].is_array() &&
                  !storage["clients"].empty(),
              "storages.clients: expected nonempty array");
        std::set<std::string> clients;
        for (const auto& client : storage["clients"]) {
            check(client.is_string(), "storages.clients: expected CU ID");
            const auto id = client.get<std::string>();
            check(clients.insert(id).second, "storages.clients: duplicate CU");
            check(id == "*" || (types_.count(id) && types_.at(id) == "cu"),
                  "storages.clients: unknown CU " + id);
        }
        check(scope == "node_shared" ? clients == std::set<std::string>{"*"}
                                     : !clients.count("*"),
              "storages.clients: invalid sharing scope");
        if (scope == "cu_private") {
            check(clients.size() == 1, "cu_private must belong to one CU");
        }
        number(storage, "capacity_bytes", "storages");
        rate(storage, "bytes_per_ns", "storages");
        number(storage, "access_latency_ns", "storages", true);
        number(storage, "queue_entries", "storages");
    }
    for (const auto& cu : config_["compute_units"]) {
        if (cu.contains("local_storage")) {
            const auto id = text(cu, "local_storage", "compute_units");
            check(types_.count(id) && types_.at(id) == "storage",
                  "compute_units.local_storage: unknown storage");
            check(modules_.at(id)["scope"] == "cu_private" &&
                      allowed(id, cu["id"]),
                  "compute_units.local_storage: invalid owner");
        }
    }
    for (const auto& dma : config_["dma_engines"]) {
        fields(dma,
               {"id", "queue_entries", "max_outstanding", "burst_bytes",
                "bytes_per_ns", "startup_ns"},
               "dma_engines");
        module(dma, "dma", "dma_engines");
        for (auto key : {"queue_entries", "max_outstanding", "burst_bytes"}) {
            number(dma, key, "dma_engines");
        }
        rate(dma, "bytes_per_ns", "dma_engines");
        number(dma, "startup_ns", "dma_engines", true);
        check(dma["burst_bytes"].get<uint64_t>() <=
                  UINT64_MAX / dma["max_outstanding"].get<uint64_t>(),
              "DMA staging capacity overflow");
    }
    if (config_.contains("collective_engine")) {
        const auto& c = config_["collective_engine"];
        fields(c,
               {"id", "slots", "queue_entries", "startup_ns",
                "reduce_bytes_per_ns"},
               "collective_engine");
        module(c, "collective", "collective_engine");
        number(c, "slots", "collective_engine");
        number(c, "queue_entries", "collective_engine");
        number(c, "startup_ns", "collective_engine", true);
        rate(c, "reduce_bytes_per_ns", "collective_engine");
    }
    if (config_.contains("network_endpoint")) {
        const auto& n = config_["network_endpoint"];
        fields(n,
               {"id", "tx_buffer_bytes", "rx_buffer_bytes", "bytes_per_ns",
                "startup_ns"},
               "network_endpoint");
        module(n, "nic", "network_endpoint");
        number(n, "tx_buffer_bytes", "network_endpoint");
        number(n, "rx_buffer_bytes", "network_endpoint");
        number(n, "startup_ns", "network_endpoint", true);
        rate(n, "bytes_per_ns", "network_endpoint");
    }
    check(config_.contains("noc"), "node.noc: required");
    const auto& noc = config_["noc"];
    fields(noc,
           {"rows", "columns", "routing", "packet_payload_bytes",
            "header_bytes", "link_bytes_per_ns", "link_latency_ns",
            "router_latency_ns", "input_buffer_packets_per_class",
            "attachments"},
           "noc");
    const auto rows = number(noc, "rows", "noc"),
               columns = number(noc, "columns", "noc");
    check(rows <= UINT64_MAX / columns, "noc dimensions overflow");
    check(text(noc, "routing", "noc") == "xy",
          "noc.routing: only xy is supported");
    packet_bytes_ = number(noc, "packet_payload_bytes", "noc");
    header_bytes_ = number(noc, "header_bytes", "noc");
    const auto bandwidth = rate(noc, "link_bytes_per_ns", "noc");
    const auto latency = number(noc, "link_latency_ns", "noc", true),
               delay = number(noc, "router_latency_ns", "noc", true),
               buffers = number(noc, "input_buffer_packets_per_class", "noc");
    for (uint64_t y = 0; y < rows; ++y) {
        for (uint64_t x = 0; x < columns; ++x) {
            for (const auto& neighbor :
                 {std::make_pair(x + 1, y), std::make_pair(x, y + 1)}) {
                if (neighbor.first >= columns || neighbor.second >= rows) {
                    continue;
                }
                const auto a = router(x, y),
                           b = router(neighbor.first, neighbor.second);
                link(a, b, bandwidth, latency, delay, buffers);
                link(b, a, bandwidth, latency, delay, buffers);
            }
        }
    }
    check(noc.contains("attachments") && noc["attachments"].is_array(),
          "noc.attachments: expected array");
    for (const auto& attachment : noc["attachments"]) {
        fields(attachment, {"port", "router"}, "noc.attachments");
        const auto port = text(attachment, "port", "noc.attachments");
        bind(port);
        check(port.substr(port.find('.') + 1) == "fabric",
              "noc.attachments: requires fabric port");
        check(attachment.contains("router") &&
                  attachment["router"].is_array() &&
                  attachment["router"].size() == 2,
              "noc.attachments.router: expected [x,y]");
        json xy = {{"x", attachment["router"][0]},
                   {"y", attachment["router"][1]}};
        auto x = number(xy, "x", "router", true),
             y = number(xy, "y", "router", true);
        check(x < columns && y < rows,
              "noc.attachments.router: coordinate outside mesh");
        const auto id = port.substr(0, port.find('.'));
        attachments_[id] = {x, y};
        link(port, router(x, y), bandwidth, latency, 0, buffers);
        link(router(x, y), port, bandwidth, latency, delay, buffers);
    }
    check(config_.contains("direct_links") &&
              config_["direct_links"].is_array(),
          "direct_links: expected array");
    for (const auto& direct : config_["direct_links"]) {
        fields(direct, {"a", "b", "bytes_per_ns", "latency_ns"},
               "direct_links");
        const auto a = text(direct, "a", "direct_links"),
                   b = text(direct, "b", "direct_links");
        bind(a);
        bind(b);
        const auto am = a.substr(0, a.find('.')), bm = b.substr(0, b.find('.'));
        check(types_.at(am) == "cu" && types_.at(bm) == "storage" &&
                  a == am + ".mem" && b == bm + ".local",
              "direct_links: expected CU.mem -> storage.local");
        check(modules_.at(am).value("local_storage", "") == bm,
              "direct_links: storage is not CU local_storage");
        const auto bw = rate(direct, "bytes_per_ns", "direct_links");
        const auto lat = number(direct, "latency_ns", "direct_links", true);
        link(a, b, bw, lat, 0, 8);
        link(b, a, bw, lat, 0, 8);
        direct_[{am, bm}] = a + "->" + b;
        direct_[{bm, am}] = b + "->" + a;
    }
    for (const auto& n : config_["compute_units"]) {
        if (n.contains("local_storage")) {
            route(n["id"], n["local_storage"]);
        }
    }
    if (config_.contains("network_endpoint")) {
        const auto& n = config_["network_endpoint"];
        check(n["tx_buffer_bytes"].get<uint64_t>() >= packet_bytes_ &&
                  n["rx_buffer_bytes"].get<uint64_t>() >= packet_bytes_,
              "network_endpoint: buffer smaller than one payload packet");
    }
}
bool NodeTopology::allowed(const std::string& storage,
                           const std::string& requester) const {
    const auto& s = modules_.at(storage);
    if (s["scope"] == "node_shared") {
        return true;
    }
    for (const auto& client : s["clients"]) {
        if (client == requester) {
            return true;
        }
    }
    return false;
}
std::vector<std::string> NodeTopology::route(
    const std::string& source, const std::string& destination) const {
    check(modules_.count(source) && modules_.count(destination),
          "route: unknown module " + source + " -> " + destination);
    if (source == destination) {
        return {};
    }
    if (direct_.count({source, destination})) {
        return {direct_.at({source, destination})};
    }
    check(attachments_.count(source) && attachments_.count(destination),
          "route: unreachable " + source + " -> " + destination);
    auto [x, y] = attachments_.at(source);
    auto [dx, dy] = attachments_.at(destination);
    std::vector<std::string> result{source + ".fabric->" + router(x, y)};
    while (x != dx) {
        const auto before = router(x, y);
        x = x < dx ? x + 1 : x - 1;
        result.push_back(before + "->" + router(x, y));
    }
    while (y != dy) {
        const auto before = router(x, y);
        y = y < dy ? y + 1 : y - 1;
        result.push_back(before + "->" + router(x, y));
    }
    result.push_back(router(x, y) + "->" + destination + ".fabric");
    return result;
}
}  // namespace AstraSim::NodeSim
