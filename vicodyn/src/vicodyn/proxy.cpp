#include "cocaine/vicodyn/proxy.hpp"

#include "cocaine/vicodyn/peer.hpp"
#include "cocaine/vicodyn/proxy/dispatch.hpp"

#include <cocaine/context.hpp>
#include <cocaine/errors.hpp>
#include <cocaine/format.hpp>
#include <cocaine/logging.hpp>

#include <blackhole/logger.hpp>

namespace cocaine {
namespace vicodyn {

proxy_t::proxy_t(context_t& context,
                 //TODO: Do we need io loop?
                 asio::io_service& io_loop ,
                 const std::string& name,
                 //TODO: Do we need args?
                 const dynamic_t& /*args*/,
                 unsigned int version,
                 io::graph_root_t protocol) :
    io::basic_dispatch_t(name),
    logger(context.log(name)),
    m_protocol(protocol),
    m_version(version),
    pool(api::peer::pool(context, io_loop, "basic"))
{
    VICODYN_DEBUG("create proxy");
}

boost::optional<io::dispatch_ptr_t>
proxy_t::process(const io::decoder_t::message_type& incoming_message, const io::upstream_ptr_t& upstream) const {

    auto event_span = incoming_message.type();
    auto protocol_span = m_protocol.find(event_span);
    COCAINE_LOG_DEBUG(logger, "graph has {} handles", m_protocol.size());
    COCAINE_LOG_DEBUG(logger, "graph handle is {}", m_protocol.begin()->first);
    if(protocol_span == m_protocol.end()) {
        auto msg = cocaine::format("could not find event with id {} in protocol for {}", event_span, name());
        COCAINE_LOG_ERROR(logger, msg);
        throw error_t(error::slot_not_found, msg);
    }
    const auto& protocol_tuple = protocol_span->second;
    const auto& forward_protocol = std::get<1>(protocol_tuple);
    if(!forward_protocol) {
        auto msg = cocaine::format("logical error - initial event is recurrent for span {}, dispatch {}", event_span, name());
        COCAINE_LOG_ERROR(logger, msg);
        throw error_t(error::slot_not_found, msg);
    }

    const auto& backward_protocol = std::get<2>(protocol_tuple);
    if(!backward_protocol) {
        auto msg = cocaine::format("logical error - backward initial event is recurrent for span {}, dispatch {}", event_span, name());
        COCAINE_LOG_ERROR(logger, msg);
        throw error_t(error::slot_not_found, msg);
    }
    auto peer = pool->choose_peer(name(), incoming_message.headers());

    auto queue = peer->invoke(incoming_message, *backward_protocol, upstream);

    // terminal transition
    if(forward_protocol->empty()) {
        return boost::optional<io::dispatch_ptr_t>(nullptr);
    }
    auto dispatch_name = cocaine::format("{}/{}", name(), std::get<0>(protocol_tuple));
    auto dispatch = std::make_shared<proxy::dispatch_t>(std::move(dispatch_name), queue, *forward_protocol);
    return boost::optional<io::dispatch_ptr_t>(std::move(dispatch));
}

} // namespace vicodyn
} // namespace cocaine
