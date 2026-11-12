#ifndef ASTRASIM_NODE_NETWORK_ENDPOINT_HH
#define ASTRASIM_NODE_NETWORK_ENDPOINT_HH

#include "astra-sim/node/MemorySystem.hh"
#include <deque>

namespace AstraSim::NodeSim {
class NetworkEndpoint {
  public:
    using NetworkCall = std::function<void(int, int, uint64_t, Done)>;
    NetworkEndpoint(const NodeTopology&,
                    NodeEventScheduler&,
                    MemorySystem&,
                    NodeStatistics&);
    void transport(NetworkCall send, NetworkCall recv);
    void send(BufferRef source,
              int peer,
              int tag,
              Done done,
              RequestContext context = {});
    void recv(BufferRef destination,
              int peer,
              int tag,
              bool reduce,
              Done done,
              RequestContext context = {});
    bool idle() const;
    json statistics() const;

  private:
    void retry();
    void finish_receive(BufferRef, bool, Done, RequestContext);
    const NodeTopology& topology_;
    NodeEventScheduler& clock_;
    MemorySystem& memory_;
    NodeStatistics& statistics_;
    json config_;
    std::string id_;
    NetworkCall send_, recv_;
    std::deque<Done> tx_wait_, rx_wait_;
    uint64_t tx_used_ = 0, rx_used_ = 0, tx_peak_ = 0, rx_peak_ = 0,
             tx_bytes_ = 0, rx_bytes_ = 0;
    Tick tx_port_ = 0, rx_port_ = 0, reduce_port_ = 0, network_wait_ = 0;
    bool retry_pending_ = false;
    json transfers_ = json::array();
};
}  // namespace AstraSim::NodeSim
#endif
