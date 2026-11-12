#ifndef ASTRASIM_NODE_MEMORY_SYSTEM_HH
#define ASTRASIM_NODE_MEMORY_SYSTEM_HH

#include "astra-sim/node/MeshNoC.hh"
#include "astra-sim/node/StorageModule.hh"
#include <map>

namespace AstraSim::NodeSim {
class MemorySystem {
  public:
    MemorySystem(const NodeTopology&, NodeEventScheduler&, NodeStatistics&);
    BufferRef resolve(const json&, const std::string& local_storage = "") const;
    void allocate(json allocation, RequestContext context = {});
    void release(const json& allocation, RequestContext context = {});
    void validate(const BufferRef&, bool read) const;
    void retain(const BufferRef&, bool read);
    void unretain(const BufferRef&);
    void mark_valid(const BufferRef&);
    void access(const std::string& endpoint,
                const std::string& requester,
                BufferRef buffer,
                bool write,
                Done completed,
                bool publish = true,
                RequestContext context = {});
    void payload(const std::string& source,
                 const std::string& destination,
                 uint64_t bytes,
                 Done done,
                 RequestContext context = {});
    bool idle() const {
        return active_ == 0 && network_.idle();
    }
    json statistics() const;

  private:
    const NodeTopology& topology_;
    NodeEventScheduler& clock_;
    NodeStatistics& statistics_;
    MeshNoC network_;
    std::map<std::string, std::unique_ptr<StorageModule>> storages_;
    std::map<std::string, std::pair<std::string, uint64_t>> locations_;
    std::map<std::string, uint64_t> generations_;
    uint64_t active_ = 0;
    std::deque<std::function<bool()>> pending_requests_;
    void transmit(std::string source,
                  std::string destination,
                  uint64_t bytes,
                  bool data,
                  MeshNoC::Receiver receiver,
                  Done done,
                  RequestContext context);
    void retry_requests();
};
}  // namespace AstraSim::NodeSim
#endif
