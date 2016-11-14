#include "cocaine/vicodyn/proxy/dispatch.hpp"

#include <cocaine/errors.hpp>
#include <cocaine/format.hpp>

namespace cocaine {
namespace vicodyn {
namespace proxy {

dispatch_t::dispatch_t(const std::string& name,
                       appendable_ptr _downstream,
                       const io::graph_node_t& _current_state) :
    io::basic_dispatch_t(name),
    downstream(std::move(_downstream)),
    full_name(name),
    current_state(&_current_state)
{}

boost::optional<io::dispatch_ptr_t>
dispatch_t::process(const io::decoder_t::message_type& incoming_message, const io::upstream_ptr_t&) const {
    VICODYN_DEBUG("processing dispatch {}/{}", incoming_message.span(), incoming_message.type());
    downstream->append(incoming_message.args(), incoming_message.type(), incoming_message.headers());
    auto event_span = incoming_message.type();
    auto protocol_span = current_state->find(event_span);
    if(protocol_span == current_state->end()) {
        auto msg = cocaine::format("could not find event with id {} in protocol for {}", event_span, name());
        //COCAINE_LOG_ERROR(logger, msg);
        throw error_t(error::slot_not_found, msg);
    }
    auto protocol_tuple = protocol_span->second;
    const auto& incoming_protocol = std::get<1>(protocol_tuple);

    //recurrent transition
    if(!incoming_protocol) {
        return boost::none;
    }

    // terminal transition
    if(incoming_protocol->empty()) {
        return boost::optional<io::dispatch_ptr_t>(nullptr);
    }

    //next transition
    current_state = &(*incoming_protocol);
    full_name = cocaine::format("{}/{}", full_name, std::get<0>(protocol_tuple));
    return boost::optional<io::dispatch_ptr_t>(shared_from_this());
}

}
}
} // namesapce cocaine
