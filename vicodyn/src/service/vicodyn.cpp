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

vicodyn_t::vicodyn_t(context_t& _context,
                     asio::io_service& io_loop,
                     const std::string& name,
                     const dynamic_t& args) :
    api::service_t(_context, io_loop, name, args),
    dispatch(name),
    context(_context),
    logger(context.log(name)),
    signal_dispatcher(std::make_shared<dispatch<io::context_tag>>(name))
{

    VICODYN_REGISTER_LOGGER(context.log("debug_logger"));
    VICODYN_DEBUG("creating vicodyn service");
    auto on_exposed = std::bind(&vicodyn_t::on_local_service_exposed, this, ph::_1, ph::_2);
    signal_dispatcher->on<io::context::service::exposed>(std::move(on_exposed));
    context.signal_hub().listen(signal_dispatcher, io_loop);
}


auto vicodyn_t::on_local_service_exposed(const std::string& name, const service_description_t& meta) -> void {
    COCAINE_LOG_INFO(logger, "exposing {} service to vicodyn");
    proxy_map.apply([&](proxy_map_t& proxies){
        auto versioned_name = std::make_pair(std::get<1>(meta), name);
        //check if this is a virtual service and we have it in our map
        if(proxies.count(versioned_name)){
            COCAINE_LOG_INFO(logger, "service is virtual - skipping");
            return;
        }

        // assign a new name for virtual service
        versioned_name.second = "virtual_" + versioned_name.second;

        // and lookup it in our table
        auto it = proxies.find(versioned_name);
        if(it != proxies.end()) {
            //TODO: FIXME!
            COCAINE_LOG_INFO(logger, "registering real in existing virtual service");
            it->second->register_real("LOCAL", std::get<0>(meta), true);
        } else {
            COCAINE_LOG_INFO(logger, "creating new virtual service");
            auto asio = std::make_shared<asio::io_service>();
            auto proxy = std::make_unique<vicodyn::proxy_t>(context,
                                                            *asio,
                                                            versioned_name.second,
                                                            dynamic_t(),
                                                            versioned_name.first,
                                                            std::get<2>(meta));
            proxy->register_real("LOCAL", std::get<0>(meta), true);
            auto actor = std::make_unique<actor_t>(context, asio, std::move(proxy));
            auto p = std::dynamic_pointer_cast<const vicodyn::proxy_t>(actor->prototype());
            proxies[versioned_name] = std::const_pointer_cast<vicodyn::proxy_t>(p);
            context.insert(versioned_name.second, std::move(actor));
        }
    });
}

}
}
