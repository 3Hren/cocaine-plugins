#include "cocaine/vicodyn/peer.hpp"

#include <cocaine/context.hpp>
#include <cocaine/engine.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/rpc/asio/decoder.hpp>
#include <cocaine/rpc/graph.hpp>
#include <cocaine/rpc/session.hpp>
#include <cocaine/memory.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/connect.hpp>

#include <blackhole/logger.hpp>

namespace cocaine {
namespace vicodyn {

peer_t::~peer_t() = default;

peer_t::peer_t(context_t& _context, asio::io_service& _loop) :
    context(_context),
    loop(_loop),
    logger(context.log("TODO:NAME PEER")),
    queue(new queue::invocation_t)
{}

auto peer_t::invoke(const io::decoder_t::message_type& incoming_message,
                    const io::graph_node_t& protocol,
                    io::upstream_ptr_t downstream) -> std::shared_ptr<queue::send_t>
{
    return queue->append(incoming_message.args(),
                        incoming_message.type(),
                        incoming_message.headers(),
                        protocol,
                        downstream);
}

auto peer_t::make_peer(context_t& context,
                       asio::io_service& loop,
                       std::vector<asio::ip::tcp::endpoint> endpoints) -> std::unique_ptr<peer_t> {
    std::unique_ptr<peer_t> p(new peer_t(context, loop));
    p->connect(std::move(endpoints));
    return p;
}

auto peer_t::connect(std::vector<asio::ip::tcp::endpoint> endpoints) -> void {
    typedef asio::ip::tcp tcp;
    auto socket = std::make_shared<tcp::socket>(loop);

    //TODO: Big problem here if connection was unsuccessfull. We need to find another peer in pool and pass queue to it.
    asio::async_connect(*socket, endpoints.begin(), endpoints.end(),
        [=](const std::error_code& ec, std::vector<asio::ip::tcp::endpoint>::const_iterator /*endpoint*/) {
            if(ec) {
                COCAINE_LOG_ERROR(logger, "could not connect - {}({})", ec.message(), ec.value());
                std::terminate();
            }
            auto ptr = std::make_unique<tcp::socket>(std::move(*socket));
            auto session = context.engine().attach(std::move(ptr), nullptr);
            queue->attach(std::move(session));
        });
}

} // namespace vicodyn
} // namespace cocaine
