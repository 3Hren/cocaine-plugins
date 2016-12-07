#include "cocaine/service/vicodyn.hpp"

#include "cocaine/vicodyn/debug.hpp"
#include "cocaine/vicodyn/proxy.hpp"

#include <cocaine/context.hpp>
#include <cocaine/context/signal.hpp>
#include <cocaine/dynamic.hpp>
#include <cocaine/idl/context.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/memory.hpp>
#include <cocaine/rpc/actor.hpp>
#include <cocaine/traits/endpoint.hpp>
#include <cocaine/traits/vector.hpp>

#include <blackhole/logger.hpp>


namespace cocaine {
namespace service {

namespace ph = std::placeholders;

struct vicodyn_t::proxy_description_t: public api::gateway_t::service_description_t {
    proxy_description_t(std::unique_ptr<actor_t> _actor, vicodyn::proxy_t& _proxy) :
            actor(std::move(_actor)),
            proxy(_proxy)
    {}

    auto endpoints() -> std::vector<asio::ip::tcp::endpoint> {
        return actor->endpoints();
    }

    auto protocol() -> io::graph_root_t {
        return proxy.root();
    }

    auto version() -> unsigned int {
        return proxy.version();
    }

    std::unique_ptr<actor_t> actor;
    vicodyn::proxy_t& proxy;
};

vicodyn_t::vicodyn_t(context_t& _context,
                     const std::string& name,
                     const dynamic_t& args) :
    gateway_t(_context, name, args),
    context(_context),
    logger(context.log(name))
{
    VICODYN_REGISTER_LOGGER(context.log("debug_logger"));
    VICODYN_DEBUG("creating vicodyn service");
}

vicodyn_t::~vicodyn_t() {
    for (auto& proxy_pair: proxy_map.unsafe()) {
        proxy_pair.second->actor->terminate();
    }
    proxy_map.unsafe().clear();
}

auto vicodyn_t::resolve(const std::string& name) const -> std::shared_ptr<service_description_t> {
    VICODYN_DEBUG("resolve called with {}", name);
    return proxy_map.apply([&](const proxy_map_t& proxies){
        auto it = proxies.find(name);
        if(it == proxies.end()) {
            throw error_t(error::service_not_available, "service {} not found in vicodyn", name);
        }
        return it->second;
    });
}


auto vicodyn_t::on_local_service_exposed(const std::string& name,
                                         unsigned int version,
                                         const std::vector<asio::ip::tcp::endpoint>& endpoints,
                                         const io::graph_root_t& protocol) -> void {
    on_remote_service_exposed("local", name, version, endpoints, protocol);
}

auto vicodyn_t::on_remote_service_exposed(const std::string& uuid,
                               const std::string& name,
                               unsigned int version,
                               const std::vector<asio::ip::tcp::endpoint>& endpoints,
                               const io::graph_root_t& protocol) -> void {
    COCAINE_LOG_INFO(logger, "exposing {} service to vicodyn", name);
    proxy_map.apply([&](proxy_map_t& proxies){
        // lookup it in our table
        auto it = proxies.find(name);
        if(it != proxies.end()) {
            //TODO: FIXME!
            COCAINE_LOG_INFO(logger, "registering real in existing virtual service");
            it->second->proxy.register_real(uuid, endpoints, uuid.empty());
        } else {
            COCAINE_LOG_INFO(logger, "creating new virtual service");
            auto asio = std::make_shared<asio::io_service>();
            auto proxy = std::make_unique<vicodyn::proxy_t>(context,
                                                            *asio,
                                                            "virtual_" + name,
                                                            dynamic_t(),
                                                            version,
                                                            protocol);
            auto& proxy_ref = *proxy;
            proxy->register_real(uuid, endpoints, uuid.empty());
            auto actor = std::make_unique<actor_t>(context, asio, std::move(proxy));
            actor->run();
            proxies[name] = std::make_shared<proxy_description_t>(std::move(actor), proxy_ref);
        }
    });
}

auto vicodyn_t::on_remote_service_removed(const std::string& uuid, const std::string& name) -> void {
    proxy_map.apply([&](proxy_map_t& proxies){
        auto it = proxies.find(name);
        if(it == proxies.end()) {
            COCAINE_LOG_ERROR(logger, "remote service {} from {} scheduled for removal not found in vicodyn", name, uuid);
            return;
        }
        it->second->proxy.unregister_real(uuid);
        if(it->second->proxy.empty()) {
            proxies.erase(it);
        }
    });
}

auto vicodyn_t::on_local_service_removed(const std::string& name) -> void {
    on_remote_service_removed({}, name);
}

auto vicodyn_t::cleanup(const std::string& uuid) -> void {
    proxy_map.apply([&](proxy_map_t& proxies) {
        for (auto it = proxies.begin(); it != proxies.end();) {
            it->second->proxy.unregister_real(uuid);
            if(it->second->proxy.empty()) {
                auto copy = it;
                it++;
                proxies.erase(copy);
            } else {
                it++;
            }
        }
    });
}

auto vicodyn_t::total_count(const std::string& name) -> size_t {
    return proxy_map.apply([&](proxy_map_t& proxies) -> size_t {
        auto it = proxies.find(name);
        if(it == proxies.end()) {
            return 0ul;
        }
        return it->second->proxy.size();
    });
}

}
}
