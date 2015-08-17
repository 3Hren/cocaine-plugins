#pragma once

#include <deque>
#include <string>

#include <cocaine/logging.hpp>

#include <cocaine/rpc/dispatch.hpp>

#include "cocaine/idl/rpc.hpp"
#include "cocaine/idl/node.hpp"

#include "cocaine/detail/service/node/app/stats.hpp"
#include "cocaine/detail/service/node/event.hpp"
#include "cocaine/detail/service/node/slave.hpp"
#include "cocaine/detail/service/node/slot.hpp"

namespace cocaine {

class balancer_t;
class unix_actor_t;
class slave_t;
class control_t;
class client_rpc_dispatch_t;

} // namespace cocaine

namespace cocaine {

class overseer_t:
    public std::enable_shared_from_this<overseer_t>
{
public:
    enum class despawn_policy_t {
        graceful,
        force
    };

    typedef std::unordered_map<
        std::string,
        slave_t
    > pool_type;

    typedef std::deque<
        slave::channel_t
    > queue_type;

private:
    const std::unique_ptr<logging::log_t> log;

    context_t& context;

    /// Time point, when the overseer was created.
    const std::chrono::system_clock::time_point birthstamp;

    /// The application manifest.
    const manifest_t manifest_;

    /// The application profile.
    synchronized<profile_t> profile_;

    /// IO loop for timers and standard output fetchers.
    std::shared_ptr<asio::io_service> loop;

    /// Slave pool.
    std::size_t pool_target = 0;
    synchronized<pool_type> pool;

    /// Pending queue.
    synchronized<queue_type> queue;

    /// The application balancing policy.
    std::shared_ptr<balancer_t> balancer;

    /// Statistics.
    stats_t stats;

public:
    overseer_t(context_t& context,
               manifest_t manifest,
               profile_t profile,
               std::shared_ptr<asio::io_service> loop);

    ~overseer_t();

    /// Returns a const reference to the application's manifest.
    ///
    /// Application's manifest is considered constant during all app's lifetime and can be
    /// changed only through restarting.
    const manifest_t&
    manifest() const {
        return manifest_;
    }

    /// Returns copy of the current profile, which is used to spawn new slaves.
    ///
    /// \note the current profile may change in any moment. Moveover some slaves can be in some kind
    /// of transition state, i.e. migrating from one profile to another.
    profile_t
    profile() const;

    /// Returns the complete info about how the application works.
    dynamic_t::object_t
    info(io::node::info::flags_t flags) const;

    /// Returns application total uptime in seconds.
    std::chrono::seconds
    uptime() const;

    // Modifiers.

    void
    set_balancer(std::shared_ptr<balancer_t> balancer);

    /// Tries to keep alive at least `count` workers no matter what.
    void
    keep_alive(std::size_t count);

    /// Enqueues the new event into the most appropriate slave.
    ///
    /// The event will be put into the queue if there are no slaves available at this moment or all
    /// of them are busy.
    ///
    /// \return the dispatch object, which is ready for processing the appropriate protocol
    /// messages.
    ///
    /// \param downstream represents the [Client <- Worker] stream.
    /// \param event an invocation event.
    /// \param id represents slave id to be enqueued (may be none, which means any slave).
    ///
    /// \todo consult with E. guys about deadline policy.
    std::shared_ptr<client_rpc_dispatch_t>
    enqueue(io::streaming_slot<io::app::enqueue>::upstream_type downstream,
            app::event_t event,
            boost::optional<service::node::slave::id_t> id);

    //std::shared_ptr<stream_t>
    //enqueue(std::shared_ptr<stream_t>&& downstream, app::event_t event, boost::optional<service::node::slave::id_t> id);

    /// Creates a new handshake dispatch, which will be consumed after a new incoming connection
    /// attached.
    ///
    /// This method is called when an unix-socket client (probably, a worker) has been accepted.
    /// The first message from it should be a handshake to be sure, that the remote peer is the
    /// worker we are waiting for.
    ///
    /// The handshake message should contain its peer id (likely uuid) by comparing that we either
    /// accept the session or drop it.
    ///
    /// \note after successful accepting the balancer will be notified about pool's changes.
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

    void
    terminate();

private:
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

}
