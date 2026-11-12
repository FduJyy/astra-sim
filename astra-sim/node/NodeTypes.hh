#ifndef ASTRASIM_NODE_TYPES_HH
#define ASTRASIM_NODE_TYPES_HH

#include <cmath>
#include <cstdint>
#include <functional>
#include <json/json.hpp>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>

namespace AstraSim::NodeSim {
using json = nlohmann::json;
using Tick = uint64_t;
using Done = std::function<void()>;
struct RequestContext {
    uint64_t operation = UINT64_MAX, transaction = 0, parent = 0;
    std::string stage;
};

class Error : public std::runtime_error {
  public:
    explicit Error(const std::string& message)
        : std::runtime_error("NodeModel: " + message) {}
};
inline void check(bool condition, const std::string& message) {
    if (!condition) {
        throw Error(message);
    }
}
inline uint64_t add(uint64_t a, uint64_t b) {
    check(b <= UINT64_MAX - a, "integer overflow");
    return a + b;
}
inline void fields(const json& value,
                   std::initializer_list<const char*> allowed,
                   const std::string& path) {
    check(value.is_object(), path + ": expected object");
    std::set<std::string> names(allowed.begin(), allowed.end());
    for (auto it = value.begin(); it != value.end(); ++it) {
        check(names.count(it.key()), path + "." + it.key() + ": unknown field");
    }
}
inline uint64_t number(const json& value,
                       const std::string& key,
                       const std::string& path,
                       bool zero = false) {
    check(value.contains(key), path + "." + key + ": required");
    const auto& n = value.at(key);
    check(n.is_number_unsigned() ||
              (n.is_number_integer() && n.get<int64_t>() >= 0),
          path + "." + key + ": expected nonnegative integer");
    const auto result = n.get<uint64_t>();
    check(zero || result > 0, path + "." + key + ": must be positive");
    return result;
}
inline double rate(const json& value,
                   const std::string& key,
                   const std::string& path) {
    check(value.contains(key) && value.at(key).is_number(),
          path + "." + key + ": expected rate");
    double n = value.at(key).get<double>();
    check(std::isfinite(n) && n > 0,
          path + "." + key + ": rate must be finite and positive");
    return n;
}
inline Tick service(uint64_t bytes, double bandwidth) {
    const long double delay =
        std::ceil(static_cast<long double>(bytes) / bandwidth);
    check(delay >= 0 && delay < static_cast<long double>(UINT64_MAX),
          "service time overflow");
    return static_cast<Tick>(delay);
}
inline std::string text(const json& value,
                        const std::string& key,
                        const std::string& path) {
    check(value.contains(key) && value.at(key).is_string(),
          path + "." + key + ": expected string");
    auto result = value.at(key).get<std::string>();
    check(!result.empty(), path + "." + key + ": must not be empty");
    return result;
}
}  // namespace AstraSim::NodeSim
#endif
