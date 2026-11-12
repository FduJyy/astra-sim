#ifndef ASTRASIM_NODE_DMA_ENGINE_HH
#define ASTRASIM_NODE_DMA_ENGINE_HH

#include "astra-sim/node/MemorySystem.hh"
#include <deque>

namespace AstraSim::NodeSim {
class DmaEngine {
  public:
    DmaEngine(const NodeTopology&,
              NodeEventScheduler&,
              MemorySystem&,
              NodeStatistics&);
    bool copy(BufferRef source,
              BufferRef destination,
              const std::string& requester,
              const std::string& binding,
              Done completed,
              RequestContext context = {});
    void on_capacity(Done callback) {
        capacity_ = std::move(callback);
    }
    bool idle() const;
    json statistics() const;

  private:
    struct Job {
        BufferRef source, destination;
        std::string requester, binding;
        Done completed;
        uint64_t next = 0, inflight = 0, finished = 0;
        RequestContext context;
        Tick enqueue = 0;
    };
    struct Engine {
        json config;
        std::shared_ptr<Job> active;
        uint64_t bytes = 0;
        Tick port = 0;
        std::deque<std::shared_ptr<Job>> queue;
        uint64_t peak = 0;
    };
    void dispatch();
    void issue(const std::string& id);
    const NodeTopology& topology_;
    NodeEventScheduler& clock_;
    MemorySystem& memory_;
    NodeStatistics& statistics_;
    std::map<std::string, Engine> engines_;
    std::deque<std::shared_ptr<Job>> waiting_;
    Done capacity_;
};
}  // namespace AstraSim::NodeSim
#endif
