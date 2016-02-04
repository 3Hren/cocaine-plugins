#include "cocaine/service/node/slave.hpp"

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/min_element.hpp>

#include <cocaine/context.hpp>
#include <cocaine/rpc/actor.hpp>

#include "cocaine/api/isolate.hpp"

#include "cocaine/service/node/manifest.hpp"
#include "cocaine/service/node/profile.hpp"
#include "cocaine/service/node/slave/stats.hpp"

#include "cocaine/detail/service/node/slave/machine.hpp"
#include "cocaine/detail/service/node/slave/channel.hpp"
#include "cocaine/detail/service/node/slave/control.hpp"
#include "cocaine/detail/service/node/slave/fetcher.hpp"
#include "cocaine/detail/service/node/slave/state/active.hpp"
#include "cocaine/detail/service/node/slave/state/stopped.hpp"
#include "cocaine/detail/service/node/slave/state/handshaking.hpp"
#include "cocaine/detail/service/node/slave/state/spawning.hpp"
#include "cocaine/detail/service/node/slave/state/state.hpp"
#include "cocaine/detail/service/node/slave/state/terminating.hpp"
#include "cocaine/detail/service/node/dispatch/client.hpp"
#include "cocaine/detail/service/node/dispatch/worker.hpp"
#include "cocaine/detail/service/node/util.hpp"

#include <blackhole/logger.hpp>

namespace ph = std::placeholders;

using namespace cocaine;

using blackhole::attribute_list;

slave_t::slave_t(slave_context context, asio::io_service& loop, cleanup_handler fn):
    ec(error::overseer_shutdowning),
    machine(machine_t::create(context, loop, fn))
{
    data.id = context.id;
    data.birthstamp = std::chrono::high_resolution_clock::now();
}

slave_t::~slave_t() {
    // This condition is required, because the class itself is movable.
    if (machine) {
        machine->terminate(std::move(ec));
    }
}

const std::string&
slave_t::id() const noexcept {
    return data.id;
}

long long
slave_t::uptime() const {
    return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::high_resolution_clock::now() - data.birthstamp
    ).count();
}

std::uint64_t
slave_t::load() const {
    BOOST_ASSERT(machine);

    return machine->load();
}

auto slave_t::stats() const -> slave::stats_t{
    return machine->stats();
}

bool
slave_t::active() const noexcept {
    BOOST_ASSERT(machine);

    return machine->active();
}

profile_t
slave_t::profile() const {
    return machine->profile();
}

std::shared_ptr<control_t>
slave_t::activate(std::shared_ptr<session_t> session, upstream<io::worker::control_tag> stream) {
    BOOST_ASSERT(machine);

    return machine->activate(std::move(session), std::move(stream));
}

std::uint64_t
slave_t::inject(slave::channel_t& channel, channel_handler handler) {
    BOOST_ASSERT(machine);

    return machine->inject(channel, handler);
}

void
slave_t::seal() {
    return machine->seal();
}

void
slave_t::terminate(std::error_code ec) {
    this->ec = std::move(ec);
}
