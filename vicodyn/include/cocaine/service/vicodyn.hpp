#pragma once

#include "cocaine/idl/vicodyn.hpp"
#include "cocaine/vicodyn/forwards.hpp"

#include <cocaine/api/cluster.hpp>
#include <cocaine/api/gateway.hpp>
#include <cocaine/api/service.hpp>
#include <cocaine/idl/context.hpp>
#include <cocaine/locked_ptr.hpp>
#include <cocaine/rpc/graph.hpp>
#include <cocaine/rpc/dispatch.hpp>


#include <asio/ip/tcp.hpp>

#include <map>

namespace cocaine {
namespace service {

class vicodyn_t : public api::gateway_t {
public:
    vicodyn_t(context_t& context, const std::string& name, const dynamic_t& args);
    ~vicodyn_t();

    struct proxy_description_t;

    auto resolve(const std::string& name) const -> std::shared_ptr<service_description_t> override;

    auto on_remote_service_exposed(const std::string& uuid,
                                   const std::string& name,
                                   unsigned int version,
                                   const std::vector<asio::ip::tcp::endpoint>& endpoints,
                                   const io::graph_root_t& protocol) -> void override;

    auto on_local_service_exposed(const std::string& name,
                                  unsigned int version,
                                  const std::vector<asio::ip::tcp::endpoint>& endpoints,
                                  const io::graph_root_t& protocol) -> void override;

    auto on_remote_service_removed(const std::string& uuid, const std::string& name) -> void override;

    auto on_local_service_removed(const std::string& name) -> void override;

    auto cleanup(const std::string& uuid) -> void override;

    auto total_count(const std::string& name) -> size_t override;

private:
    typedef std::map<std::string, std::shared_ptr<proxy_description_t>> proxy_map_t;

    context_t& context;
    std::unique_ptr<logging::logger_t> logger;
    synchronized<proxy_map_t> proxy_map;
};

}
}
