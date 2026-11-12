#ifndef ASTRASIM_NODE_STORAGE_MODULE_HH
#define ASTRASIM_NODE_STORAGE_MODULE_HH

#include "astra-sim/node/NodeEventScheduler.hh"
#include "astra-sim/node/NodeStatistics.hh"
#include <map>
#include <memory>
#include <vector>

namespace AstraSim::NodeSim {
struct BufferRef {
    std::string storage, id;
    uint64_t generation, offset, bytes;
};
class StorageModule {
  public:
    StorageModule(json configuration, NodeEventScheduler&, NodeStatistics&);
    void allocate(const json& allocation, RequestContext context = {});
    void release(const std::string& id,
                 uint64_t generation,
                 RequestContext context = {});
    void validate(const BufferRef&, bool read) const;
    void retain(const BufferRef&, bool read);
    void unretain(const BufferRef&);
    void mark_valid(const BufferRef&);
    bool try_service(const BufferRef&,
                     bool write,
                     std::function<bool()> completed,
                     RequestContext context = {});
    void drain_completions();
    void on_capacity(Done callback) {
        capacity_ = std::move(callback);
    }
    json statistics() const;

  private:
    struct Allocation {
        uint64_t generation, address, bytes, reserved, references = 0;
        std::vector<std::pair<uint64_t, uint64_t>> valid;
    };
    Allocation& allocation(const BufferRef&);
    const Allocation& allocation(const BufferRef&) const;
    json config_;
    NodeEventScheduler& clock_;
    NodeStatistics& statistics_;
    std::map<std::string, Allocation> allocations_;
    std::map<std::string, uint64_t> generations_;
    uint64_t occupied_ = 0, peak_ = 0, queued_ = 0, max_queue_ = 0;
    Tick port_ = 0, busy_ = 0;
    uint64_t read_bytes_ = 0, write_bytes_ = 0, allocated_ = 0, freed_ = 0;
    Done capacity_;
    std::deque<std::function<bool()>> retired_;
};
}  // namespace AstraSim::NodeSim
#endif
