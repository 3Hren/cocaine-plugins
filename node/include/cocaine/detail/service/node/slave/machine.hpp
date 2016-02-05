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
#include "cocaine/detail/service/node/slave.hpp"

#include "cocaine/service/node/event.hpp"
#include "cocaine/service/node/manifest.hpp"
#include "cocaine/service/node/profile.hpp"
#include "cocaine/service/node/slave/id.hpp"
#include "cocaine/service/node/slave/error.hpp"
#include "cocaine/service/node/slot.hpp"
#include "cocaine/service/node/splitter.hpp"

namespace cocaine {

typedef std::shared_ptr<
    const dispatch<io::event_traits<io::worker::rpc::invoke>::dispatch_type>
> inject_dispatch_ptr_t;

class overseer_t;

class client_rpc_dispatch_t;

// TODO: Detail namespace.
class active_t;
class stopped_t;
class channel_t;
class handshaking_t;
class spawning_t;
class state_t;
class terminating_t;

namespace service { namespace node { namespace slave { namespace state {
class sealing_t;
}}}}

class fetcher_t;

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

using slave::id_t;
// namespace detail {
// namespace service {
// namespace node {
// namespace slave {

/// Actual slave implementation.
class machine_t:
    public std::enable_shared_from_this<machine_t>
{
    friend class active_t;
    friend class stopped_t;
    friend class handshaking_t;
    friend class spawning_t;
    friend class terminating_t;
    friend class service::node::slave::state::sealing_t;

    friend class control_t;
    friend class fetcher_t;

    friend class client_rpc_dispatch_t;

    friend class channel_t;

    class lock_t {};

public:
    typedef std::function<void(std::uint64_t)> channel_handler;
    typedef std::function<void(const std::error_code&)> cleanup_handler;

private:
    const std::unique_ptr<logging::logger_t> log;

public:
    context_t& context;

    const id_t id;
    const profile_t profile;
    const manifest_t manifest;

private:
    // TODO: In current implementation this can be invalid, when engine is stopped.
    asio::io_service& loop;

    /// The flag means that the overseer has been destroyed and we shouldn't call the callback.
    std::atomic<bool> closed;
    cleanup_handler cleanup;

    splitter_t splitter;
    synchronized<std::shared_ptr<fetcher_t>> fetcher;
    boost::circular_buffer<std::string> lines;

    std::atomic<bool> shutdowned;

    synchronized<std::shared_ptr<state_t>> state;

    std::atomic<std::uint64_t> counter;

    typedef std::unordered_map<std::uint64_t, std::shared_ptr<channel_t>> channels_map_t;

    struct {
        synchronized<channels_map_t> channels;
    } data;

public:
    /// Creates the state machine instance and immediately starts it.
    static auto create(context_t& context, id_t id, profile_t profile, manifest_t manifest,
        asio::io_service& loop, cleanup_handler cleanup) -> std::shared_ptr<machine_t>;

    machine_t(lock_t, context_t& context, id_t id, profile_t profile, manifest_t manifest,
        asio::io_service& loop, cleanup_handler cleanup);

    ~machine_t();

    // Observers.

    /// Returns true is the slave is in active state.
    bool
    active() const noexcept;

    std::uint64_t
    load() const;

    auto stats() const -> slave::stats_t;

    // Modifiers.

    std::shared_ptr<control_t>
    activate(std::shared_ptr<session_t> session, upstream<io::worker::control_tag> stream);

    std::uint64_t
    inject(slave::channel_t& channel, channel_handler handler);

    void
    seal();

    /// Terminates the slave by sending terminate message to the worker instance.
    ///
    /// The cleanup callback won't be called after this call.
    void
    terminate(std::error_code ec);

private:
    /// Spawns a slave.
    ///
    /// \pre state == nullptr.
    /// \post state != nullptr.
    void
    start();

    void
    output(const char* data, size_t size);

    void
    migrate(std::shared_ptr<state_t> desired);

    /// Internal termination.
    ///
    /// Can be called multiple times, but only the first one takes an effect.
    void
    shutdown(std::error_code ec);

    void
    revoke(std::uint64_t id, channel_handler handler);

    void
    dump();
};

// }  // namespace slave
// }  // namespace node
// }  // namespace service
// }  // namespace detail
}  // namespace cocaine
