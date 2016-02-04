#pragma once

#include <functional>
#include <string>
#include <system_error>

#include <boost/circular_buffer.hpp>
#include <boost/variant/variant.hpp>

#include <asio/io_service.hpp>
#include <asio/deadline_timer.hpp>
#include <asio/posix/stream_descriptor.hpp>

#include <cocaine/logging.hpp>
#include <cocaine/unique_id.hpp>

#include "cocaine/api/isolate.hpp"
#include "cocaine/api/stream.hpp"
#include "cocaine/idl/rpc.hpp"
#include "cocaine/idl/node.hpp"

#include "cocaine/service/node/event.hpp"
#include "cocaine/service/node/manifest.hpp"
#include "cocaine/service/node/profile.hpp"
#include "cocaine/service/node/slave/error.hpp"
#include "cocaine/service/node/slot.hpp"
#include "cocaine/service/node/slave/channel.hpp"

namespace cocaine {

class machine_t;
class overseer_t;

}  // namespace cocaine

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

class client_rpc_dispatch_t;

class control_t;

namespace slave {

using service::node::slave::id_t;
using service::node::slave::stats_t;

} // namespace slave

struct slave_context {
    context_t&  context;
    manifest_t  manifest;
    profile_t   profile;
    std::string id;

    slave_context(context_t& context, manifest_t manifest, profile_t profile) :
        context(context),
        manifest(manifest),
        profile(profile),
        id(unique_id_t().string())
    {}
};

// TODO: Rename to `comrade`, because in Soviet Russia slave owns you!
class slave_t {
public:
    // TODO: Replace with something like `engine->rebalance_events()`.
    typedef std::function<void(std::uint64_t)> channel_handler;

    // TODO: Replace with `engine->erase(id, reason, std::nothrow_t)`.
    typedef std::function<void(const std::error_code&)> cleanup_handler;

private:
    /// Termination reason.
    std::error_code ec;

    struct {
        std::string id;
        std::chrono::high_resolution_clock::time_point birthstamp;
    } data;

    /// The slave state machine implementation.
    std::shared_ptr<machine_t> machine;

public:
    slave_t(slave_context context, asio::io_service& loop, cleanup_handler fn);
    slave_t(const slave_t& other) = delete;
    slave_t(slave_t&&) = default;

    ~slave_t();

    slave_t& operator=(const slave_t& other) = delete;
    slave_t& operator=(slave_t&&) = default;

    // Observers.

    const std::string&
    id() const noexcept;

    long long
    uptime() const;

    std::uint64_t
    load() const;

    auto stats() const -> slave::stats_t;

    bool
    active() const noexcept;

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
    void
    terminate(std::error_code ec);
};

} // namespace cocaine
