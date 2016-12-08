#pragma once

#include "cocaine/api/peer/pool.hpp"

#include <cocaine/api/service.hpp>
#include <cocaine/forwards.hpp>
#include <cocaine/rpc/basic_dispatch.hpp>

namespace cocaine {
namespace vicodyn {

class proxy_t : public io::basic_dispatch_t{
public:
    proxy_t(context_t& context,
            asio::io_service& io_loop,
            const std::string& name,
            const dynamic_t& args,
            unsigned int version,
            io::graph_root_t protocol);

    auto root() const -> const io::graph_root_t& override {
        return m_protocol;
    }

    auto version() const -> int override {
        return m_version;
    }

    auto process(const io::decoder_t::message_type& incoming_message, const io::upstream_ptr_t& upstream) const ->
        boost::optional<io::dispatch_ptr_t> override;

    auto register_real(std::string uuid, std::vector<asio::ip::tcp::endpoint> endpoints, bool local) -> void {
        pool->register_real(uuid, endpoints, local);
    }

    auto unregister_real(const std::string& uuid) -> void {
        pool->unregister_real(uuid);
    }

    auto empty() -> bool {
        return pool->empty();
    }

    auto size() -> size_t {
        return pool->size();
    }

private:
    std::unique_ptr<logging::logger_t> logger;
    io::graph_root_t m_protocol;
    unsigned int m_version;
    api::peer::pool_ptr pool;
};

} // namespace vicodyn
} // namespace cocaine
