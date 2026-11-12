#include "astra-sim/node/NodeStatistics.hh"

namespace AstraSim::NodeSim {
NodeStatistics::NodeStatistics(const json& config,
                               NodeEventScheduler& clock,
                               int rank)
    : clock_(clock),
      rank_(rank) {
    if (config.contains("trace")) {
        const auto& trace = config["trace"];
        fields(trace, {"enabled", "max_events"}, "trace");
        check(trace.contains("enabled") && trace["enabled"].is_boolean(),
              "trace.enabled: expected boolean");
        enabled_ = trace["enabled"];
        if (trace.contains("max_events")) {
            limit_ = number(trace, "max_events", "trace");
        }
    }
}
uint64_t NodeStatistics::next_id() {
    sequence_ = add(sequence_, 1);
    return sequence_;
}
RequestContext NodeStatistics::child(RequestContext context) {
    context.parent = context.transaction;
    context.transaction = next_id();
    return context;
}
void NodeStatistics::add_time(uint64_t operation,
                              const std::string& category,
                              Tick duration) {
    if (operation == UINT64_MAX) {
        return;
    }
    auto& metrics = metrics_[operation];
    if (metrics.is_null()) {
        metrics = json::object();
    }
    metrics[category] = add(metrics.value(category, uint64_t{0}), duration);
}
void NodeStatistics::record(const RequestContext& context,
                            const std::string& event,
                            const std::string& module,
                            json details) {
    if (!enabled_) {
        return;
    }
    if (events_.size() >= limit_) {
        truncated_ = true;
        return;
    }
    details.update({{"rank", rank_},
                    {"op", context.operation},
                    {"txn", context.transaction},
                    {"parent", context.parent},
                    {"stage", context.stage},
                    {"event", event},
                    {"module", module},
                    {"tick_ns", clock_.now()}});
    events_.push_back(std::move(details));
}
json NodeStatistics::metrics(uint64_t operation) const {
    json result = {{"memory_stall_ns", 0}, {"dma_wait_ns", 0},
                   {"noc_wait_ns", 0},     {"external_network_wait_ns", 0},
                   {"compute_busy_ns", 0}, {"storage_queue_wait_ns", 0}};
    if (metrics_.count(operation)) {
        result.update(metrics_.at(operation));
    }
    return result;
}
json NodeStatistics::trace() const {
    return {{"enabled", enabled_},
            {"truncated", truncated_},
            {"max_events", limit_},
            {"events", events_}};
}
}  // namespace AstraSim::NodeSim
