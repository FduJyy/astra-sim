#include "astra-sim/node/NodeEventScheduler.hh"
#include <algorithm>
#include <tuple>

namespace AstraSim::NodeSim {
NodeEventScheduler::NodeEventScheduler(std::function<Tick()> now,
                                       HostSchedule schedule)
    : now_(std::move(now)),
      host_(std::move(schedule)) {}
Tick NodeEventScheduler::now() const {
    return now_();
}
bool NodeEventScheduler::idle() const {
    return events_.empty() && !pumping_;
}
void NodeEventScheduler::after(Tick delay, Done callback, unsigned phase) {
    check(bool(callback), "empty event callback");
    const auto tick = add(now(), delay);
    const bool wake = !events_.count(tick);
    events_[tick].push_back({pumping_ && delay == 0 ? add(delta_, 1) : 0,
                             sequence_++, phase, std::move(callback)});
    if (wake && !(pumping_ && delay == 0)) {
        host_(delay, [this, tick] { pump(tick); });
    }
}
void NodeEventScheduler::pump(Tick tick) {
    check(now() == tick && !pumping_, "invalid/reentrant event pump");
    pumping_ = true;
    size_t iterations = 0;
    auto& events = events_.at(tick);
    while (!events.empty()) {
        check(++iterations < 1000000, "zero-time event cycle");
        auto batch = std::move(events);
        events.clear();
        std::stable_sort(batch.begin(), batch.end(),
                         [](const Event& a, const Event& b) {
                             return std::tie(a.delta, a.phase, a.sequence) <
                                    std::tie(b.delta, b.phase, b.sequence);
                         });
        for (auto& event : batch) {
            delta_ = event.delta;
            event.callback();
        }
    }
    events_.erase(tick);
    pumping_ = false;
}
}  // namespace AstraSim::NodeSim
