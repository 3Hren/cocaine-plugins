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

#include "forwards.hpp"

namespace cocaine {
namespace slave {
struct channel_t;

using cocaine::service::node::slave::id_t;

}
}

namespace cocaine {

class client_rpc_dispatch_t;

}  // namespace cocaine

namespace cocaine {

class engine_t;

class overseer_t {
    std::shared_ptr<engine_t> engine;

public:
    overseer_t(context_t& context, manifest_t manifest, profile_t profile,
        std::shared_ptr<asio::io_service> loop);

    /// TODO: Docs.
    ~overseer_t();

    /// Returns application total uptime in seconds.
    auto uptime() const -> std::chrono::seconds;

    /// Returns the copy of current profile, which is used to spawn new slaves.
    ///
    /// \note the current profile may change in any moment. Moveover some slaves can be in some kind
    /// of transition state, i.e. migrating from one profile to another.
    auto profile() const -> profile_t;

    /// Returns the copy of current manifest.
    ///
    /// Application's manifest is considered constant during all app's lifetime and can be
    /// changed only through restarting.
    auto manifest() const -> manifest_t;

    /// Returns the complete info about how the application works using json-like object.
    auto info(io::node::info::flags_t flags) const -> dynamic_t::object_t;

    // Modifiers.

    /// Enqueues the new event into the most appropriate slave.
    ///
    /// The event will be put into the queue if there are no slaves available at this moment or all
    /// of them are busy.
    ///
    /// \param downstream represents the [Client <- Worker] stream.
    /// \param event an invocation event.
    /// \param id represents slave id to be enqueued (may be none, which means any slave).
    ///
    /// \return the dispatch object, which is ready for processing the appropriate protocol
    ///     messages.
    ///
    /// \todo consult with E. guys about deadline policy.
    auto enqueue(io::streaming_slot<io::app::enqueue>::upstream_type downstream, app::event_t event,
                 boost::optional<slave::id_t> id) -> std::shared_ptr<client_rpc_dispatch_t>;

    /// Enqueues the new event into the most appropriate slave.
    ///
    /// The event will be put into the queue if there are no slaves available at this moment or all
    /// of them are busy.
    ///
    /// \param rx a receiver stream which methods will be called when the appropriate messages
    ///     received.
    /// \param event an invocation event.
    /// \param id represents slave id to be enqueued (may be none, which means any slave).
    ///
    /// \return a tx stream.
    auto enqueue(std::shared_ptr<api::stream_t> rx, app::event_t event,
                 boost::optional<slave::id_t> id) -> std::shared_ptr<api::stream_t>;

    /// Tries to keep alive at least `count` workers no matter what.
    ///
    /// Zero value is allowed and means not to spawn workers at all.
    auto failover(int count) -> void;

    /// Creates a new handshake dispatch, which will be consumed after a new incoming connection
    /// attached.
    ///
    /// This method is called when an unix-socket client (probably, a worker) has been accepted.
    /// The first message from it should be a handshake to be sure, that the remote peer is the
    /// worker we are waiting for.
    ///
    /// The handshake message should contain its peer id (likely uuid) by comparing that we either
    /// accept the session or drop it.
    auto prototype() -> io::dispatch_ptr_t;
};

}  // namespace cocaine
