#pragma once

#include <deque>
#include <string>

#include <cocaine/rpc/dispatch.hpp>

#include "cocaine/idl/node.hpp"
#include "cocaine/idl/rpc.hpp"

#include "cocaine/service/node/app/stats.hpp"
#include "cocaine/service/node/event.hpp"
#include "cocaine/service/node/slave/channel.hpp"
#include "cocaine/service/node/slot.hpp"
#include "cocaine/service/node/manifest.hpp"
#include "cocaine/service/node/profile.hpp"
#include "cocaine/service/node/overseer.hpp"

namespace cocaine {
namespace detail {
namespace service {
namespace node {

class slave_t;

}  // namespace node
}  // namespace service
}  // namespace detail
}  // namespace cocaine


namespace cocaine {
// namespace service {
// namespace node {

class control_t;

using detail::service::node::slave_t;

// TODO: Drop flags. Use method overloading instead.
enum class despawn_policy_t {
    graceful,
    force
};

class engine_t : public std::enable_shared_from_this<engine_t> {
public:
    typedef std::unordered_map<
        std::string,
        slave_t
    > pool_type;

    struct channel_wrapper_t {
        slave::channel_t channel;
        trace_t trace;

        auto operator*() -> slave::channel_t& { return channel; }
        auto operator*() const -> const slave::channel_t& { return channel; }
        auto operator->() -> slave::channel_t* { return &channel; }
        auto operator->() const -> const slave::channel_t* { return &channel; }
    };

    typedef std::deque<channel_wrapper_t> queue_type;

    const std::unique_ptr<logging::logger_t> log;

    context_t& context;

    /// Time point, when the overseer was created.
    const std::chrono::system_clock::time_point birthstamp;

    /// The application manifest.
    const manifest_t manifest;

    /// The application profile.
    synchronized<profile_t> profile;

    /// IO loop for timers and standard output fetchers.
    std::shared_ptr<asio::io_service> loop;

    /// Slave pool.
    synchronized<pool_type> pool;
    std::atomic<int> pool_target;

    /// Pending queue.
    synchronized<queue_type> queue;

    /// Statistics.
    stats_t stats;

    ////////////////////////////////////////////////////////////////////////////////////////////////

    engine_t(context_t& context, profile_t profile, manifest_t manifest,
             std::shared_ptr<asio::io_service> loop);

    ~engine_t();

    auto uptime() const -> std::chrono::seconds;

    auto info(io::node::info::flags_t flags) const -> dynamic_t::object_t;

    // Modifiers.

    auto enqueue(io::streaming_slot<io::app::enqueue>::upstream_type downstream, app::event_t event,
                 boost::optional<slave::id_t> id) -> std::shared_ptr<client_rpc_dispatch_t>;

    auto enqueue(std::shared_ptr<api::stream_t> rx, app::event_t event,
                 boost::optional<slave::id_t> id) -> std::shared_ptr<api::stream_t>;

    auto failover(int count) -> void;

    io::dispatch_ptr_t
    prototype();

    /// Spawns a new slave using current manifest and profile.
    void
    spawn(pool_type& pool);

    /// \warning must be called under the pool lock.
    void
    assign(slave_t& slave, slave::channel_t& payload);

    /// Seals the worker, preventing it from new requests.
    ///
    /// Then forces the slave to send terminate event. Starts the timer. On timeout or on response
    /// erases slave.
    void
    despawn(const std::string& id, despawn_policy_t policy);

    std::shared_ptr<control_t>
    on_handshake(const std::string& id,
                 std::shared_ptr<session_t> session,
                 upstream<io::worker::control_tag>&& stream);

    void
    on_slave_death(const std::error_code& ec, std::string uuid);

    void
    rebalance_events();

    void
    rebalance_slaves();
};

// }  // namespace node
// }  // namespace service
}  // namespace cocaine
