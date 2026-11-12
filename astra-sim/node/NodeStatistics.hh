#ifndef ASTRASIM_NODE_STATISTICS_HH
#define ASTRASIM_NODE_STATISTICS_HH

#include "astra-sim/node/NodeEventScheduler.hh"
#include <map>

namespace AstraSim::NodeSim {
class NodeStatistics {
  public:
    NodeStatistics(const json& config, NodeEventScheduler& clock, int rank);
    uint64_t next_id();
    RequestContext child(RequestContext context);
    void add_time(uint64_t operation,
                  const std::string& category,
                  Tick duration);
    void record(const RequestContext&,
                const std::string& event,
                const std::string& module,
                json details = json::object());
    json metrics(uint64_t operation) const;
    json trace() const;

  private:
    NodeEventScheduler& clock_;
    int rank_;
    uint64_t sequence_ = 0, limit_ = 10000;
    bool enabled_ = false, truncated_ = false;
    json events_ = json::array();
    std::map<uint64_t, json> metrics_;
};
}  // namespace AstraSim::NodeSim
#endif
