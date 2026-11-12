#ifndef ASTRASIM_NODE_MODEL_HH
#define ASTRASIM_NODE_MODEL_HH

#include "astra-sim/node/NodeEventScheduler.hh"
#include "astra-sim/node/NodeTopology.hh"
#include "astra-sim/node/StorageModule.hh"
#include <memory>

namespace AstraSim::NodeSim {
// One instance and one mutable resource graph per simulated accelerator/rank.
class NodeModel {
  public:
    using Observer = std::function<void(uint64_t, Tick)>;
    struct Transport {
        std::function<void(uint64_t, const json&, Done)> collective;
        std::function<void(int, int, uint64_t, Done)> send, recv;
        std::function<void(uint64_t, Done)> remote;
    };
    NodeModel(json configuration, json profiles, NodeEventScheduler&, int rank);
    ~NodeModel();
    NodeModel(const NodeModel&) = delete;
    NodeModel& operator=(const NodeModel&) = delete;
    static std::pair<json, json> load(const std::string& filename);
    bool try_submit(uint64_t id,
                    const std::string& profile,
                    json metadata,
                    Observer started,
                    Observer completed);
    void on_capacity(Done callback);
    void transport(Transport transport);
    void collective_step(uint64_t operation,
                         BufferRef source,
                         BufferRef destination,
                         bool reduce,
                         int send_peer,
                         int recv_peer,
                         int tag,
                         Done completed,
                         json metadata = json::object());
    const json& operation_profile(uint64_t id) const;
    BufferRef operation_workspace(uint64_t id) const;
    void local_copy(uint64_t operation,
                    BufferRef source,
                    BufferRef destination,
                    Done completed);
    bool idle() const;
    json statistics() const;
    const NodeTopology& topology() const;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}  // namespace AstraSim::NodeSim
#endif
