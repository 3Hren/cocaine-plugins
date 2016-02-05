/*
    Copyright (c) 2011-2014 Andrey Sibiryov <me@kobology.ru>
    Copyright (c) 2011-2014 Other contributors as noted in the AUTHORS file.

    This file is part of Cocaine.

    Cocaine is free software; you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    Cocaine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#include "cocaine/service/node.hpp"

#include "cocaine/api/storage.hpp"

#include "cocaine/context.hpp"
#include "cocaine/logging.hpp"

#include "cocaine/traits/dynamic.hpp"
#include "cocaine/traits/endpoint.hpp"
#include "cocaine/traits/graph.hpp"
#include "cocaine/traits/tuple.hpp"
#include "cocaine/traits/vector.hpp"

#include "cocaine/service/node/app.hpp"

#include <blackhole/logger.hpp>
#include <blackhole/scope/holder.hpp>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/copy.hpp>

namespace cocaine {
namespace service {

namespace ph = std::placeholders;

typedef std::map<std::string, std::shared_ptr<node::app_t>> apps_t;

node_t::node_t(context_t& context, asio::io_service& asio, const std::string& name,
               const dynamic_t& args)
    : category_type(context, asio, name, args),
      dispatch<io::node_tag>(name),
      context(context),
      log(context.log(name))
{
    on<io::node::start_app>(std::bind(&node_t::start_app, this, ph::_1, ph::_2));
    on<io::node::pause_app>(std::bind(&node_t::pause_app, this, ph::_1));
    on<io::node::list>(std::bind(&node_t::list, this));
    on<io::node::info>(std::bind(&node_t::info, this, ph::_1, ph::_2));

    // Context signal/slot.
    signal = std::make_shared<dispatch<io::context_tag>>(name);
    signal->on<io::context::shutdown>(std::bind(&node_t::on_context_shutdown, this));

    const auto runname = args.as_object().at("runlist", "").as_string();

    if (runname.empty()) {
        context.listen(signal, asio);
        return;
    }

    COCAINE_LOG_INFO(log, "reading '{}' runlist", runname);

    typedef std::map<std::string, std::string> runlist_t;
    runlist_t runlist;

    const auto storage = api::storage(context, "core");

    try {
        // TODO (@antmat): Perform request to a special service, like "storage->runlist(runname)".
        runlist = storage->get<runlist_t>("runlists", runname);
    } catch (const std::system_error& err) {
        COCAINE_LOG_WARNING(log, "unable to read '{}' runlist: {}", runname, error::to_string(err));
    }

    if (runlist.empty()) {
        context.listen(signal, asio);
        return;
    }

    COCAINE_LOG_INFO(log, "starting {} app(s)", runlist.size());

    std::vector<std::string> errored;

    // TODO (@esafronov): parallelize.
    for (const auto& target : runlist) {
        const blackhole::scope::holder_t scoped(*log, {{ "app", target.first }});

        try {
            start_app(target.first, target.second);
        } catch(const std::exception& err) {
            COCAINE_LOG_WARNING(log, "unable to initialize app: {}", err.what());
            errored.push_back(target.first);
        }
    }

    if (!errored.empty()) {
        COCAINE_LOG_WARNING(log, "couldn't start {} app(s): {}", errored.size(),
                            boost::join(errored, ", "));
    }

    context.listen(signal, asio);
}

node_t::~node_t() = default;

auto node_t::prototype() const -> const io::basic_dispatch_t& {
    return *this;
}

auto node_t::start_app(const std::string& name, const std::string& profile) -> deferred<void> {
    COCAINE_LOG_DEBUG(log, "processing `start_app` request, app: '{}'", name);

    cocaine::deferred<void> deferred;

    apps.apply([&](apps_t& apps) {
        auto it = apps.find(name);

        if (it != apps.end()) {
            throw std::system_error(error::already_started);
        }

        apps.insert({name, std::make_shared<node::app_t>(context, name, profile, deferred)});
    });

    return deferred;
}

auto node_t::pause_app(const std::string& name) -> void {
    COCAINE_LOG_DEBUG(log, "processing `pause_app` request, app: '{}'", name);

    apps.apply([&](apps_t& apps) {
        auto it = apps.find(name);

        if (it == apps.end()) {
            throw std::system_error(error::not_running,
                                    cocaine::format("app '%s' is not running", name));
        }

        apps.erase(it);
    });
}

auto node_t::list() const -> dynamic_t {
    dynamic_t::array_t result;

    apps.apply([&](const apps_t& apps) {
        boost::copy(apps | boost::adaptors::map_keys, std::back_inserter(result));
    });

    return result;
}

auto node_t::info(const std::string& name, io::node::info::flags_t flags) const -> dynamic_t {
    auto app = apps.apply([&](const apps_t& apps) -> std::shared_ptr<node::app_t> {
        auto it = apps.find(name);

        if (it != apps.end()) {
            return it->second;
        }

        return nullptr;
    });

    if (!app) {
        throw cocaine::error_t("app '%s' is not running", name);
    }

    return app->info(flags);
}

auto node_t::overseer(const std::string& name) const -> std::shared_ptr<overseer_t> {
    auto app = apps.apply([&](const apps_t& apps) -> std::shared_ptr<node::app_t> {
        auto it = apps.find(name);

        if (it != apps.end()) {
            return it->second;
        }

        return nullptr;
    });

    if (!app) {
        throw cocaine::error_t("app '%s' is not running", name);
    }

    return app->overseer();
}

auto node_t::on_context_shutdown() -> void {
    // TODO: In fact this method may not be invoked during context shutdown - race - node service
    // can be terminated earlier than this completion handler be invoked.

    apps.apply([&](apps_t& apps) {
        COCAINE_LOG_INFO(log, "shutting down {} apps", apps.size());
        apps.clear();
    });

    signal = nullptr;
}

}  // namespace service
}  // namespace cocaine
