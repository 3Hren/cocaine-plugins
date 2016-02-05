#include "cocaine/detail/service/node/slave.hpp"

#include "cocaine/service/node/slave/channel.hpp"
#include "cocaine/service/node/slave/stats.hpp"

#include "cocaine/detail/service/node/slave/machine.hpp"

namespace cocaine {
namespace detail {
namespace service {
namespace node {

using blackhole::attribute_list;

slave_t::slave_t(context_t& context, id_t id, profile_t profile, manifest_t manifest,
    asio::io_service& loop, cleanup_handler fn)
    :
    birthstamp(std::chrono::high_resolution_clock::now()),
    reason(error::overseer_shutdowning),
    machine(machine_t::create(context, id, profile, manifest, loop, fn))
{}

slave_t::~slave_t() {
    // This condition is required, because the class itself is movable.
    if (machine) {
        machine->terminate(std::move(reason));
    }
}

auto slave_t::id() const noexcept -> const std::string& {
    return machine->id.get();
}

auto slave_t::active() const noexcept -> bool {
    return machine->active();
}

long long
slave_t::uptime() const {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::high_resolution_clock::now() - birthstamp
    ).count();
}

std::uint64_t
slave_t::load() const {
    return machine->load();
}

auto slave_t::stats() const -> slave::stats_t{
    return machine->stats();
}

auto slave_t::profile() const -> profile_t {
    return machine->profile;
}

std::shared_ptr<control_t>
slave_t::activate(std::shared_ptr<session_t> session, upstream<io::worker::control_tag> stream) {
    return machine->activate(std::move(session), std::move(stream));
}

std::uint64_t
slave_t::inject(slave::channel_t& channel, channel_handler handler) {
    return machine->inject(channel, handler);
}

void
slave_t::seal() {
    return machine->seal();
}

auto slave_t::terminate(std::error_code ec) -> void {
    reason = std::move(ec);
}

}  // namespace node
}  // namespace service
}  // namespace detail
}  // namespace cocaine
