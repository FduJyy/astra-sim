#ifndef ASTRASIM_NODE_TOPOLOGY_HH
#define ASTRASIM_NODE_TOPOLOGY_HH

#include "astra-sim/node/NodeTypes.hh"
#include <map>
#include <vector>

namespace AstraSim::NodeSim {
struct LinkDescription {
    std::string id, destination;
    double bandwidth;
    Tick latency, router_latency;
    uint64_t buffers;
};
class NodeTopology {
  public:
    explicit NodeTopology(json configuration);
    const json& configuration() const {
        return config_;
    }
    const std::map<std::string, json>& modules() const {
        return modules_;
    }
    const std::map<std::string, LinkDescription>& links() const {
        return links_;
    }
    std::vector<std::string> route(const std::string& source,
                                   const std::string& destination) const;
    bool allowed(const std::string& storage,
                 const std::string& requester) const;
    uint64_t packet_bytes() const {
        return packet_bytes_;
    }
    uint64_t header_bytes() const {
        return header_bytes_;
    }

  private:
    void module(const json&, const std::string&, const std::string&);
    void bind(const std::string& port);
    void link(
        const std::string&, const std::string&, double, Tick, Tick, uint64_t);
    json config_;
    std::map<std::string, json> modules_;
    std::map<std::string, std::string> types_;
    std::map<std::string, LinkDescription> links_;
    std::map<std::string, std::pair<uint64_t, uint64_t>> attachments_;
    std::map<std::pair<std::string, std::string>, std::string> direct_;
    std::set<std::string> bound_;
    uint64_t packet_bytes_, header_bytes_;
};
}  // namespace AstraSim::NodeSim
#endif
