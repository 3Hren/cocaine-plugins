#pragma once

#include <chrono>
#include <string>

#include <boost/optional/optional.hpp>

namespace cocaine {
namespace service {
namespace node {
namespace slave {

class stats_t {
public:
    /// Current state name.
    std::string state;

    std::uint64_t tx;
    std::uint64_t rx;
    std::uint64_t load;
    std::uint64_t total;

    boost::optional<std::chrono::high_resolution_clock::time_point> age;

    stats_t();
};

}  // namespace slave
}  // namespace node
}  // namespace service
}  // namespace cocaine
