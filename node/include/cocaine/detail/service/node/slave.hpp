#pragma once

#include <chrono>
#include <functional>
#include <string>
#include <system_error>

#include <cocaine/forwards.hpp>

#include "cocaine/idl/rpc.hpp"

namespace cocaine {

class control_t;
class machine_t;
class overseer_t;
class session_t;

struct profile_t;
struct manifest_t;

}  // namespace cocaine

namespace cocaine {
namespace app {

class event_t;

}
}

namespace cocaine {
namespace service {
namespace node {
namespace slave {

class id_t;
class stats_t;

}  // namespace slave
}  // namespace node
}  // namespace service
}  // namespace cocaine

namespace cocaine {
namespace slave {

using service::node::slave::id_t;
using service::node::slave::stats_t;

struct channel_t;

}  // namespace slave
}  // namespace cocaine

namespace cocaine {
namespace detail {
namespace service {
namespace node {

// TODO: Rename to `comrade`, because in Soviet Russia slave owns you!
class slave_t {
public:
    // TODO: Replace with something like `engine->rebalance_events()`.
    typedef std::function<void(std::uint64_t)> channel_handler;

    // TODO: Replace with `engine->erase(id, reason, std::nothrow_t)`.
    typedef std::function<void(const std::error_code&)> cleanup_handler;

private:
    /// Construction time point.
    const std::chrono::high_resolution_clock::time_point birthstamp;
    /// Termination reason.
    std::error_code reason;
    /// The slave state machine implementation.
    std::shared_ptr<machine_t> machine;

public:
    slave_t(context_t& context, slave::id_t id, profile_t profile, manifest_t manifest,
        asio::io_service& loop, cleanup_handler fn);

    slave_t(const slave_t& other) = delete;
    slave_t(slave_t&& other) = default;

    ~slave_t();

    /// Observers.

    /// Returns slave id.
    auto id() const noexcept -> const std::string&;

    /// Returns true is this slave is in active state, i.e. ready to serve requests.
    auto active() const noexcept -> bool;

    // TODO: Return duration instead.
    long long
    uptime() const;

    std::uint64_t
    load() const;

    auto stats() const -> slave::stats_t;

    /// Returns the profile attached.
    profile_t
    profile() const;

    // Modifiers.

    std::shared_ptr<control_t>
    activate(std::shared_ptr<session_t> session, upstream<io::worker::control_tag> stream);

    std::uint64_t
    inject(slave::channel_t& channel, channel_handler handler);

    void
    seal();

    /// Marks the slave for termination using the given error code.
    ///
    /// It will be terminated later in destructor.
    auto terminate(std::error_code ec) -> void;
};

}  // namespace node
}  // namespace service
}  // namespace detail
}  // namespace cocaine
