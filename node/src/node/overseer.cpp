#include "cocaine/service/node/overseer.hpp"

#include <blackhole/logger.hpp>

#include <cocaine/context.hpp>
#include <cocaine/logging.hpp>

#include "cocaine/service/node/slave/id.hpp"

#include "cocaine/detail/service/node/engine.hpp"
#include "cocaine/detail/service/node/slave.hpp"

namespace cocaine {

overseer_t::overseer_t(context_t& context, manifest_t manifest, profile_t profile,
                       std::shared_ptr<asio::io_service> loop)
    : engine(std::make_shared<engine_t>(context, profile, manifest, loop)) {
}

overseer_t::~overseer_t() {
    COCAINE_LOG_DEBUG(engine->log, "overseer is processing terminate request");

    engine->keep(0);
    engine->pool->clear();
}

auto overseer_t::manifest() const -> manifest_t {
    return engine->manifest;
}

auto overseer_t::profile() const -> profile_t {
    return *engine->profile.synchronize();
}

auto overseer_t::info(io::node::info::flags_t flags) const -> dynamic_t::object_t {
    return engine->info(flags);
}

auto overseer_t::uptime() const -> std::chrono::seconds {
    return engine->uptime();
}

auto overseer_t::keep_alive(int count) -> void {
    return engine->keep(count);
}

auto overseer_t::enqueue(io::streaming_slot<io::app::enqueue>::upstream_type downstream,
                         app::event_t event, boost::optional<slave::id_t> id)
    -> std::shared_ptr<client_rpc_dispatch_t>
{
    return engine->enqueue(downstream, event, id);
}

auto overseer_t::enqueue(std::shared_ptr<api::stream_t> rx, app::event_t event,
                         boost::optional<slave::id_t> id) -> std::shared_ptr<api::stream_t>
 {
    return engine->enqueue(rx, event, id);
}

auto overseer_t::prototype() -> io::dispatch_ptr_t {
    return engine->prototype();
}

}  // namespace cocaine
