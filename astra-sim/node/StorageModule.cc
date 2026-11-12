#include "astra-sim/node/StorageModule.hh"
#include <algorithm>
#include <sstream>

namespace AstraSim::NodeSim {
StorageModule::StorageModule(json configuration,
                             NodeEventScheduler& clock,
                             NodeStatistics& statistics)
    : config_(std::move(configuration)),
      clock_(clock),
      statistics_(statistics) {}
const StorageModule::Allocation& StorageModule::allocation(
    const BufferRef& ref) const {
    const auto name = config_["id"].get<std::string>();
    check(allocations_.count(ref.id),
          "AllocationMissing storage=" + name + " buffer=" + ref.id);
    const auto& a = allocations_.at(ref.id);
    check(a.generation == ref.generation,
          "StaleGeneration storage=" + name + " buffer=" + ref.id);
    check(ref.offset <= a.bytes && ref.bytes <= a.bytes - ref.offset,
          "BufferBounds storage=" + name + " buffer=" + ref.id);
    return a;
}
StorageModule::Allocation& StorageModule::allocation(const BufferRef& ref) {
    return const_cast<Allocation&>(
        static_cast<const StorageModule&>(*this).allocation(ref));
}
void StorageModule::allocate(const json& request, RequestContext context) {
    fields(request,
           {"storage", "buffer_id", "generation", "bytes", "alignment",
            "storage_offset", "initialized"},
           "allocate");
    const auto id = text(request, "buffer_id", "allocate");
    const auto generation = number(request, "generation", "allocate", true),
               bytes = number(request, "bytes", "allocate");
    check(!allocations_.count(id), "DuplicateAllocation buffer=" + id);
    if (generations_.count(id)) {
        check(generation == add(generations_.at(id), 1),
              "StaleGeneration buffer=" + id);
    }
    const uint64_t limit = number(config_, "capacity_bytes", "storage"),
                   alignment = request.contains("alignment")
                                   ? number(request, "alignment", "allocate")
                                   : 1;
    const auto remainder = bytes % alignment;
    const auto reserved = remainder ? add(bytes, alignment - remainder) : bytes;
    auto overflow = [&](const std::string& reason = "insufficient_capacity") {
        throw Error(
            "CapacityExceeded storage=" + config_["id"].get<std::string>() +
            " capacity=" + std::to_string(limit) + " occupied_or_reserved=" +
            std::to_string(occupied_) + " requested=" + std::to_string(bytes) +
            " available=" + std::to_string(limit - occupied_) + " reason=" +
            reason + " allocations=" + statistics()["allocations"].dump());
    };
    if (reserved > limit - occupied_) {
        overflow();
    }
    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    for (const auto& [key, a] : allocations_) {
        ranges.push_back({a.address, add(a.address, a.reserved)});
    }
    std::sort(ranges.begin(), ranges.end());
    auto align = [&](uint64_t value) {
        auto rest = value % alignment;
        return rest ? add(value, alignment - rest) : value;
    };
    uint64_t address = 0;
    if (request.contains("storage_offset")) {
        address = number(request, "storage_offset", "allocate", true);
        check(address % alignment == 0, "allocate.storage_offset: misaligned");
        if (address > limit || reserved > limit - address) {
            overflow("address_out_of_range");
        }
        for (const auto& r : ranges) {
            check(address >= r.second || add(address, reserved) <= r.first,
                  "AllocationOverlap buffer=" + id);
        }
    } else {
        bool found = false;
        for (const auto& r : ranges) {
            address = align(address);
            if (address <= r.first && reserved <= r.first - address) {
                found = true;
                break;
            }
            address = std::max(address, r.second);
        }
        if (!found) {
            address = align(address);
        }
        if (address > limit || reserved > limit - address) {
            overflow("fragmented");
        }
    }
    allocations_[id] = {generation, address, bytes, reserved, 0, {}};
    generations_[id] = generation;
    occupied_ = add(occupied_, reserved);
    peak_ = std::max(peak_, occupied_);
    ++allocated_;
    if (request.value("initialized", false)) {
        allocations_.at(id).valid.push_back({0, bytes});
    }
    statistics_.record(context, "allocate", config_["id"],
                       {{"buffer_id", id},
                        {"generation", generation},
                        {"bytes", bytes},
                        {"offset", address}});
}
void StorageModule::release(const std::string& id,
                            uint64_t generation,
                            RequestContext context) {
    BufferRef ref{config_["id"], id, generation, 0, 0};
    auto& a = allocation(ref);
    check(a.references == 0,
          "BufferInUse storage=" + ref.storage + " buffer=" + id);
    occupied_ -= a.reserved;
    allocations_.erase(id);
    ++freed_;
    statistics_.record(context, "free", config_["id"],
                       {{"buffer_id", id}, {"generation", generation}});
}
void StorageModule::validate(const BufferRef& ref, bool read) const {
    const auto& a = allocation(ref);
    if (!read) {
        return;
    }
    if (ref.bytes == 0) {
        return;
    }
    bool valid = false;
    for (auto range : a.valid) {
        if (range.first <= ref.offset &&
            range.second >= add(ref.offset, ref.bytes)) {
            valid = true;
        }
    }
    check(valid,
          "DataUnavailable storage=" + ref.storage + " buffer=" + ref.id);
}
void StorageModule::retain(const BufferRef& ref, bool read) {
    validate(ref, read);
    ++allocation(ref).references;
}
void StorageModule::unretain(const BufferRef& ref) {
    auto& a = allocation(ref);
    check(a.references > 0, "buffer reference underflow");
    --a.references;
}
void StorageModule::mark_valid(const BufferRef& ref) {
    auto& a = allocation(ref);
    a.valid.push_back({ref.offset, add(ref.offset, ref.bytes)});
    std::sort(a.valid.begin(), a.valid.end());
    std::vector<std::pair<uint64_t, uint64_t>> merged;
    for (auto range : a.valid) {
        if (!merged.empty() && range.first <= merged.back().second) {
            merged.back().second = std::max(merged.back().second, range.second);
        } else {
            merged.push_back(range);
        }
    }
    a.valid = std::move(merged);
}
bool StorageModule::try_service(const BufferRef& ref,
                                bool write,
                                std::function<bool()> completed,
                                RequestContext context) {
    validate(ref, !write);
    if (queued_ >= number(config_, "queue_entries", "storage")) {
        return false;
    }
    ++queued_;
    max_queue_ = std::max(max_queue_, queued_);
    const auto begin =
        std::max(add(clock_.now(),
                     number(config_, "access_latency_ns", "storage", true)),
                 port_);
    const auto duration =
        service(ref.bytes, rate(config_, "bytes_per_ns", "storage"));
    statistics_.add_time(
        context.operation, "storage_queue_wait_ns",
        begin - add(clock_.now(),
                    number(config_, "access_latency_ns", "storage", true)));
    statistics_.record(context, write ? "storage_write" : "storage_read",
                       config_["id"],
                       {{"storage_id", ref.storage},
                        {"buffer_id", ref.id},
                        {"generation", ref.generation},
                        {"offset", ref.offset},
                        {"bytes", ref.bytes},
                        {"service_start_ns", begin}});
    port_ = add(begin, duration);
    busy_ = add(busy_, duration);
    if (write) {
        write_bytes_ = add(write_bytes_, ref.bytes);
    } else {
        read_bytes_ = add(read_bytes_, ref.bytes);
    }
    clock_.after(
        port_ - clock_.now(),
        [this, completed = std::move(completed)] {
            retired_.push_back(completed);
            drain_completions();
        },
        0);
    return true;
}
void StorageModule::drain_completions() {
    const auto count = retired_.size();
    for (size_t i = 0; i < count; ++i) {
        auto completed = std::move(retired_.front());
        retired_.pop_front();
        if (completed()) {
            --queued_;
            if (capacity_) {
                capacity_();
            }
        } else {
            retired_.push_back(std::move(completed));
        }
    }
}
json StorageModule::statistics() const {
    json allocations = json::array();
    for (const auto& [id, a] : allocations_) {
        allocations.push_back({{"buffer_id", id},
                               {"generation", a.generation},
                               {"storage_offset", a.address},
                               {"bytes", a.bytes},
                               {"reserved_bytes", a.reserved},
                               {"references", a.references}});
    }
    return {{"capacity_bytes", config_["capacity_bytes"]},
            {"occupied_or_reserved_bytes", occupied_},
            {"peak_bytes", peak_},
            {"allocate_count", allocated_},
            {"free_count", freed_},
            {"read_bytes", read_bytes_},
            {"write_bytes", write_bytes_},
            {"port_busy_ns", busy_},
            {"queue_peak", max_queue_},
            {"allocations", allocations}};
}
}  // namespace AstraSim::NodeSim
