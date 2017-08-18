#include "cocaine/vicodyn/proxy.hpp"

#include "cocaine/api/vicodyn/balancer.hpp"
#include "cocaine/repository/vicodyn/balancer.hpp"

#include "cocaine/vicodyn/peer.hpp"
#include "../../../node/include/cocaine/service/node/slave/error.hpp"

#include <cocaine/context.hpp>
#include <cocaine/dynamic.hpp>
#include <cocaine/errors.hpp>
#include <cocaine/format.hpp>
#include <cocaine/logging.hpp>

#include <blackhole/logger.hpp>
#include <cocaine/rpc/slot.hpp>

namespace cocaine {
namespace vicodyn {

using app_tag = io::stream_of<std::string>::tag;

class forward_dispatch_t : public dispatch<app_tag>, public std::enable_shared_from_this<forward_dispatch_t> {
    using protocol = io::protocol<app_tag>::scope;

    proxy_t& proxy;
    std::shared_ptr<peer_t> peer;

    struct data_t {

        upstream<io::stream_of<std::string>::tag> backward_stream;

        io::upstream_ptr_t forward_stream;
        std::string enqueue_frame;
        hpack::headers_t enqueue_headers;

        std::vector<std::string> chunks;
        std::vector<hpack::headers_t> chunk_headers;

        bool choke_sent;
        hpack::headers_t choke_headers;

        bool buffering_enabled;

        size_t retry_counter;


        data_t(upstream<app_tag> backward_stream):
            backward_stream(std::move(backward_stream)),
            buffering_enabled(true),
            choke_sent(false),
            retry_counter(0)
        {}
    };

    synchronized<data_t> d;

public:
    forward_dispatch_t(proxy_t& proxy, const std::string& name, upstream<app_tag> backward_stream, std::shared_ptr<peer_t> _peer) :
        dispatch(name),
        proxy(proxy),
        peer(std::move(_peer)),
        d(std::move(backward_stream))
    {
        on<protocol::chunk>().execute([this](const hpack::headers_t& headers, std::string chunk){
            d.apply([&](data_t& d){
                try {
                    d.chunks.push_back(chunk);
                    d.chunk_headers.push_back(headers);
                    d.forward_stream->send<protocol::chunk>(headers, std::move(chunk));
                } catch (const std::system_error& e) {
                    peer->schedule_reconnect();
                }
            });
        });

        on<protocol::choke>().execute([this](const hpack::headers_t& headers){
            d.apply([&](data_t& d){
                try {
                    d.choke_sent = true;
                    d.choke_headers = headers;
                    d.forward_stream->send<protocol::choke>(headers);
                } catch (const std::system_error& e) {
                    peer->schedule_reconnect();
                }
            });
        });

        on<protocol::error>().execute([this](const hpack::headers_t& headers, std::error_code ec, std::string msg){
            d.apply([&](data_t& d){
                d.buffering_enabled = false;
                try {
                    d.forward_stream->send<protocol::error>(headers, ec, msg);
                } catch (const std::system_error& e) {
                    peer->schedule_reconnect();
                }
            });
        });
    }

    auto attach(io::upstream_ptr_t stream) -> void {
        d.apply([&](data_t& d){
            d.forward_stream = std::move(stream);
        });
    }

    auto disable_buffering() -> void {
        d.apply([&](data_t& d){
            disable_buffering(d);
        });
    }

    auto disable_buffering(data_t& d) -> void {
        d.buffering_enabled = false;
        d.enqueue_frame.clear();
        d.enqueue_headers.clear();
        d.chunk_headers.clear();
        d.chunks.clear();
    }

    auto send_discard_frame() -> void {
        d.apply([&](data_t& d) {
            try {
                auto ec = make_error_code(error::dispatch_errors::not_connected);
                d.forward_stream->send<protocol::error>(hpack::headers_t{}, ec, "vicodyn client was disconnected");
            } catch (const std::system_error& e) {
                // pass
            }
        });
    }

    auto enqueue(const hpack::headers_t& headers, std::string event) -> void {
        d.apply([&](data_t& d){
            try {
                d.enqueue_frame = std::move(event);
                d.enqueue_headers = headers;
                d.forward_stream->send<io::node::enqueue>(headers, proxy.app_name, d.enqueue_frame);
            } catch (const std::system_error& e) {
                peer->schedule_reconnect();
            }
        });
    }

    auto retry() -> void {
        d.apply([&](data_t& d){
            d.retry_counter++;
            if(!d.buffering_enabled) {
                throw error_t("buffering is already disabled - response chunk was sent");
            }
            if(d.retry_counter > proxy.balancer->retry_count()) {
                throw error_t("maximum number of retries reached");
            }
            peer = proxy.choose_peer(d.enqueue_headers, d.enqueue_frame);
            auto dispatch_name = format("{}/{}/streaming/backward", d.proxy.name(), d.enqueue_frame);
            auto backward_dispatch = std::make_shared<backward_dispatch_t>(dispatch_name, d.backward_stream, shared_from_this());
            d.forward_stream = d.peer->open_stream(std::move(backward_dispatch));
            d.forward_stream->send<io::node::enqueue>(d.enqueue_headers, d.proxy.app_name, d.enqueue_frame);
            for(size_t i = 0; i < d.chunks.size(); i++) {
                d.forward_stream->send<protocol::chunk>(d.chunk_headers[i], d.chunks[i]);
            }
            if(d.choke_sent) {
                d.forward_stream->send<protocol::choke>(d.choke_headers);
            }
        });
    }

    auto on_error(std::error_code ec, const std::string& msg) -> void {
        return proxy.balancer->on_error(ec, msg);
    }

    auto is_recoverable(std::error_code ec) -> bool {
        return proxy.balancer->is_recoverable(ec, peer);
    }
};

class backward_dispatch_t : public dispatch<app_tag> {
    using protocol = io::protocol<app_tag>::scope;

    upstream<app_tag> backward_stream;
    std::shared_ptr<forward_dispatch_t> forward_dispatch;

public:
    backward_dispatch_t(const std::string& name, upstream<app_tag> back_stream,
                        std::shared_ptr<forward_dispatch_t> f_dispatch
    ):
        dispatch(name),
        backward_stream(std::move(back_stream)),
        forward_dispatch(std::move(f_dispatch)),
        finished(false)
    {
        on<protocol::chunk>().execute([this](const hpack::headers_t& headers, std::string chunk) mutable {
            forward_dispatch->disable_buffering();
            try {
                backward_stream = backward_stream.send<protocol::chunk>(headers, std::move(chunk));
            } catch (const std::system_error& e) {
                forward_dispatch->send_discard_frame();
            }
        });

        on<protocol::error>().execute([this](const hpack::headers_t& headers, std::error_code ec, std::string msg) mutable {
            forward_dispatch->on_error(ec, msg);
            if(forward_dispatch->is_recoverable(ec)) {
                try {
                    forward_dispatch->retry();
                } catch(const std::system_error& e) {
                    backward_stream.send<protocol::error>(hpack::headers_t{}, e.code(), e.what());
                }
            } else {
                try {
                    backward_stream.send<protocol::error>(headers, ec, msg);
                } catch (const std::system_error& e) {
                    forward_dispatch->send_discard_frame();
                }
            }
        });

        on<protocol::choke>().execute([this](const hpack::headers_t& headers) mutable {
            forward_dispatch->disable_buffering();
            try {
                backward_stream.send<protocol::choke>(headers);
            } catch (const std::system_error& e) {
                forward_dispatch->send_discard_frame();
            }
        });

    }

    auto discard(const std::error_code& ec) -> void override {
        backward_stream.send<protocol::error>(ec, "vicodyn upstream has been disconnected");
    }
};

auto proxy_t::make_balancer(const dynamic_t& args) -> api::vicodyn::balancer_ptr {
    auto balancer_conf = args.as_object().at("balancer", dynamic_t::empty_object);
    auto name = balancer_conf.as_object().at("type", "simple").as_string();
    auto balancer_args = balancer_conf.as_object().at("args", dynamic_t::empty_object).as_object();
    return context.repository().get<api::vicodyn::balancer_t>(name, context, executor.asio(), app_name, args);
}

proxy_t::proxy_t(context_t& context, const std::string& name, const dynamic_t& args) :
    dispatch(name),
    context(context),
    app_name(name.substr(sizeof("virtual::") - 1)),
    balancer(make_balancer(args)),
    logger(context.log(name))
{
    on<event_t>([&](const hpack::headers_t& headers, slot_t::tuple_type&& args, slot_t::upstream_type&& backward_stream){
        auto event = std::get<0>(args);
        try {
            auto peer = choose_peer(headers, event);
            auto dispatch_name = format("{}/{}/streaming/forward", this->name(), event);
            auto forward_dispatch = std::make_shared<forward_dispatch_t>(*this, dispatch_name, app_name,
                                                                         backward_stream, peer);

            dispatch_name = format("{}/{}/streaming/backward", this->name(), event);
            auto backward_dispatch = std::make_shared<backward_dispatch_t>(dispatch_name, backward_stream,
                                                                           forward_dispatch);
            auto forward_stream = peer->open_stream(backward_dispatch);
            forward_dispatch->attach(forward_stream);
            forward_dispatch->enqueue(headers, event);
            return result_t(forward_dispatch);
        } catch (const std::system_error& e) {
            backward_stream.send<app_protocol::error>(e.code(), e.what());
            auto dispatch = std::make_shared<enqueue_slot_t::dispatch_type>(format("{}/{}/empty", app_name, event_name));
            dispatch->on<protocol::error>([this](std::error_code, std::string){});
            dispatch->on<protocol::chunk>([this](std::string){});
            dispatch->on<protocol::choke>([this](){});
            return result_t(dispatch);
        }
    });
}

auto proxy_t::choose_peer(const hpack::headers_t& headers, const std::string& event) -> std::shared_ptr<peer_t> {
    return balancer->choose_peer(mapping, headers, event);
}

auto proxy_t::register_node(const std::string& uuid, std::vector<asio::ip::tcp::endpoint> endpoints) -> void {
    mapping.apply([&](mapping_t& mapping){
        auto peer = std::make_shared<peer_t>(context, name(), executor.asio(), std::move(endpoints), uuid);
        peer->connect();
        mapping.node_peers[uuid] = std::move(peer);
    });
}

auto proxy_t::deregister_node(const std::string& uuid) -> void {
    mapping.apply([&](mapping_t& mapping){
        mapping.node_peers.erase(uuid);
    });
}

auto proxy_t::register_real(std::string uuid) -> void {
    mapping.apply([&](mapping_t& mapping){
        auto it = std::find(mapping.peers_with_app.begin(), mapping.peers_with_app.end(), uuid);
        if(it == mapping.peers_with_app.end()) {
            mapping.peers_with_app.push_back(std::move(uuid));
        }
    });
}

auto proxy_t::deregister_real(const std::string& uuid) -> void {
    mapping.apply([&](mapping_t& mapping){
        auto it = std::find(mapping.peers_with_app.begin(), mapping.peers_with_app.end(), uuid);
        if(it != mapping.peers_with_app.end()) {
            mapping.peers_with_app.erase(it);
        }
    });
}

auto proxy_t::empty() -> bool {
    return mapping.apply([&](mapping_t& mapping){
        return mapping.peers_with_app.empty();
    });
}

auto proxy_t::size() -> size_t {
    return mapping.apply([&](mapping_t& mapping){
        return mapping.peers_with_app.size();
    });
}

} // namespace vicodyn
} // namespace cocaine
