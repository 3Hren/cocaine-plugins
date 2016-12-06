#pragma once

#include "cocaine/idl/vicodyn.hpp"
#include "cocaine/vicodyn/forwards.hpp"

#include <cocaine/api/service.hpp>
#include <cocaine/api/cluster.hpp>
#include <cocaine/idl/context.hpp>
#include <cocaine/locked_ptr.hpp>
#include <cocaine/rpc/graph.hpp>
#include <cocaine/rpc/dispatch.hpp>


#include <asio/ip/tcp.hpp>

#include <map>

namespace cocaine {
namespace service {

class vicodyn_t :
    public api::gateway_t,
    public api::service_t,
    //public api::cluster_t::interface,
    //TODO: ugly hack for now. This service serves NOTHING
    public dispatch<io::vicodyn_tag>
{
public:
    typedef std::tuple<std::vector<asio::ip::tcp::endpoint>, unsigned int, io::graph_root_t> service_description_t;

    vicodyn_t(context_t& context, const std::string& name, const dynamic_t& args);
    ~vicodyn_t();

    auto
    prototype() const -> const io::basic_dispatch_t& {
        return *this;
    }

private:
    auto on_local_service_exposed(const std::string& name, const service_description_t& meta) -> void;
    auto on_local_service_removed(const std::string& name, const service_description_t& meta) -> void;


    typedef std::shared_ptr<vicodyn::proxy_t> proxy_ptr;
    typedef std::pair<unsigned int, std::string> versioned_name_t;
    typedef std::map<versioned_name_t, proxy_ptr> proxy_map_t;

    context_t& context;
    std::unique_ptr<logging::logger_t> logger;
    synchronized<proxy_map_t> proxy_map;
    std::shared_ptr<dispatch<io::context_tag>> signal_dispatcher;
};

}
}
