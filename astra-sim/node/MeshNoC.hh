#ifndef ASTRASIM_NODE_MESH_NOC_HH
#define ASTRASIM_NODE_MESH_NOC_HH

#include "astra-sim/node/NodeEventScheduler.hh"
#include "astra-sim/node/NodeStatistics.hh"
#include "astra-sim/node/NodeTopology.hh"
#include <array>
#include <deque>
#include <memory>

namespace AstraSim::NodeSim {
class MeshNoC {
  public:
    using Receiver = std::function<bool()>;
    MeshNoC(const NodeTopology&, NodeEventScheduler&, NodeStatistics&);
    // payload=0 is a header-only control packet. Receiver can exert endpoint
    // backpressure.
    bool send(const std::string& source,
              const std::string& destination,
              uint64_t payload,
              bool data,
              Receiver receiver,
              Done delivered,
              RequestContext context = {});
    void wake();
    void on_capacity(Done callback) {
        capacity_ = std::move(callback);
    }
    bool idle() const;
    json statistics() const;

  private:
    struct Transfer;
    struct Packet {
        std::shared_ptr<Transfer> transfer;
        size_t hop = 0;
        uint64_t payload, wire;
        unsigned traffic;
        std::string input;
        Tick ready = 0;
        uint64_t id = 0;
    };
    struct Transfer {
        std::string source, destination;
        std::vector<std::string> route;
        uint64_t remaining;
        bool data;
        Receiver receiver;
        Done delivered;
        RequestContext context;
    };
    struct Link {
        std::array<std::deque<std::shared_ptr<Packet>>, 2> arrived;
        std::array<uint64_t, 2> occupied{};
        bool busy = false;
        uint64_t bytes = 0, packets = 0, peak = 0;
        Tick busy_ns = 0;
        size_t cursor = 0;
    };
    void inject(const std::shared_ptr<Transfer>&);
    void pump();
    const NodeTopology& topology_;
    NodeEventScheduler& clock_;
    NodeStatistics& statistics_;
    std::map<std::string, Link> links_;
    std::map<std::string, std::deque<std::shared_ptr<Packet>>> injection_;
    uint64_t active_ = 0;
    bool pending_ = false;
    std::map<std::pair<std::string, unsigned>, uint64_t> source_slots_;
    Done capacity_;
};
}  // namespace AstraSim::NodeSim
#endif
