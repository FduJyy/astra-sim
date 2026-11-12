#ifndef ASTRASIM_NODE_SYSTEM_HH
#define ASTRASIM_NODE_SYSTEM_HH

#include "astra-sim/node/NodeModel.hh"
#include "astra-sim/system/Callable.hh"
#include <deque>
#include <exception>

namespace AstraSim {
class Sys;
class Ring;
class DataSet;
class RingTopology;
// Adapter to ASTRA-sim's existing event and workload interfaces.
class NodeSystem : public Callable {
  public:
    NodeSystem(Sys& sys, const std::string& filename);
    void call(EventType, CallData*) override;
    void drain();
    void fail(std::exception_ptr error);
    bool failed() const;
    void rethrow_if_failed() const;
    void after_current_batch(NodeSim::Done callback);
    void ring_transfer(Ring& ring, NodeSim::Done completed);
    NodeSim::NodeEventScheduler scheduler;
    NodeSim::NodeModel model;

  private:
    NodeSystem(Sys& sys, std::pair<NodeSim::json, NodeSim::json> configuration);
    struct Wake : CallData {
        NodeSim::Done callback;
    };
    std::deque<NodeSim::Done> immediate_;
    bool draining_ = false;
    std::exception_ptr failure_;
    Sys& sys_;
    struct DatasetCompletion {
        DataSet* dataset;
        NodeSim::Done callback;
        std::shared_ptr<RingTopology> topology;
    };
    std::map<int, DatasetCompletion> datasets_;
    std::map<std::string, std::vector<NodeSim::json>> collective_registry_;
    void start_collective(uint64_t id,
                          const NodeSim::json& profile,
                          NodeSim::Done done);
    struct NetworkCallback {
        NodeSystem* owner;
        NodeSim::Done callback;
        sim_request request;
    };
    static void network_completed(void*);
};
}  // namespace AstraSim
#endif
