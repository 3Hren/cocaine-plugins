#pragma once

#include "cocaine/vicodyn/forwards.hpp"

#include <cocaine/forwards.hpp>

#include <asio/ip/tcp.hpp>

namespace cocaine {
namespace api {
namespace peer {

class pool_t {
public:
    typedef pool_t category_type;
    virtual ~pool_t() {}

    virtual
    std::shared_ptr<vicodyn::peer_t>
    choose_peer(const std::string& service_name, const hpack::header_storage_t& headers) = 0;

    virtual
    void
    register_real(std::string uuid, std::vector<asio::ip::tcp::endpoint> endpoints, bool local) = 0;

    virtual
    void
    unregister_real(const std::string& uuid) = 0;
};

typedef std::shared_ptr<pool_t> pool_ptr;

pool_ptr
pool(context_t& context, asio::io_service& io_loop, const std::string& name);

} // namespace peer
} // namespace api
} // namespace cocaine
