#pragma once

#include "cocaine/vicodyn/queue/send.hpp"
#include "cocaine/vicodyn/queue/invocation.hpp"

#include <asio/ip/tcp.hpp>

namespace cocaine {
namespace vicodyn {

class peer_t {
public:
    ~peer_t();
    //TODO: why do we need this?
    static auto make_peer(context_t& context,
                          asio::io_service& loop,
                          std::vector<asio::ip::tcp::endpoint> endpoints) -> std::unique_ptr<peer_t>;

    auto invoke(const io::aux::decoded_message_t& incoming_message,
                const io::graph_node_t& protocol,
                io::upstream_ptr_t downstream) -> std::shared_ptr<queue::send_t>;
private:
    peer_t(context_t& context, asio::io_service& loop);
    auto connect(std::vector<asio::ip::tcp::endpoint> endpoints) -> void;

    context_t& context;
    asio::io_service& loop;
    std::unique_ptr<logging::logger_t> logger;
    std::unique_ptr<queue::invocation_t> queue;
};

} // namespace vicodyn
} // namespace cocaine
