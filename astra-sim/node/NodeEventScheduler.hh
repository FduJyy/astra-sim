#ifndef ASTRASIM_NODE_EVENT_SCHEDULER_HH
#define ASTRASIM_NODE_EVENT_SCHEDULER_HH

#include "astra-sim/node/NodeTypes.hh"
#include <map>
#include <queue>
#include <vector>

namespace AstraSim::NodeSim {
// Host scheduling is the existing simulator's clock, never a private time loop.
class NodeEventScheduler {
  public:
    using HostSchedule = std::function<void(Tick, Done)>;
    NodeEventScheduler(std::function<Tick()> now, HostSchedule schedule);
    Tick now() const;
    void after(Tick delay, Done callback, unsigned phase = 2);
    bool idle() const;

  private:
    struct Event {
        uint64_t delta, sequence;
        unsigned phase;
        Done callback;
    };
    void pump(Tick tick);
    std::function<Tick()> now_;
    HostSchedule host_;
    std::map<Tick, std::vector<Event>> events_;
    uint64_t sequence_ = 0, delta_ = 0;
    bool pumping_ = false;
};
}  // namespace AstraSim::NodeSim
#endif
