/*
* 2015+ Copyright (c) Anton Matveenko <antmat@yandex-team.ru>
* All rights reserved.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*/

#include "cocaine/detail/zookeeper/connection.hpp"
#include "cocaine/detail/zookeeper/handler.hpp"
#include "cocaine/detail/zookeeper/session.hpp"

#include "cocaine/detail/zookeeper/errors.hpp"

#include <cocaine/executor/asio.hpp>

#include <boost/optional/optional.hpp>

#include <zookeeper/zookeeper.h>

#include <algorithm>
#include <errno.h>
#include <stdexcept>
#include <string>

namespace zookeeper {
namespace {

template <class Ptr>
void* c_ptr(Ptr p) {
    return reinterpret_cast<void*>(p);
}

template <class Ptr>
void* mc_ptr(Ptr p) {
    return reinterpret_cast<void*>(p->get_id());
}

template <class T>
auto pack_ptr(std::shared_ptr<T> shared) -> std::unique_ptr<std::shared_ptr<T>> {
    return std::unique_ptr<std::shared_ptr<T>>(new std::shared_ptr<T>(std::move(shared)));
}

template <class T>
auto unpack_ptr(const void* raw_ptr) -> replier_ptr<T> {
    std::unique_ptr<const replier_ptr<T>> managed_ptr(reinterpret_cast<const replier_ptr<T>*>(raw_ptr));
    return replier_ptr<T>(std::move(*managed_ptr));
}

}

cfg_t::endpoint_t::endpoint_t(std::string _hostname, unsigned int _port) :
    hostname(std::move(_hostname)),
    port(_port)
{}

std::string
cfg_t::endpoint_t::to_string() const {
    if(hostname.empty() || port == 0) {
        throw std::system_error(cocaine::error::invalid_connection_endpoint);
    }
    //Zookeeper handles even ipv6 addresses correctly in this case
    return hostname + ':' + std::to_string(port);
}


cfg_t::cfg_t(std::vector<cfg_t::endpoint_t> _endpoints, unsigned int _recv_timeout_ms, std::string _prefix):
    recv_timeout_ms(_recv_timeout_ms),
    prefix(std::move(_prefix)),
    endpoints(std::move(_endpoints))
{
}

std::string
cfg_t::connection_string() const {
    std::string result;
    for(size_t i = 0; i < endpoints.size(); i++) {
        if(!result.empty()) {
            result += ',';
        }
        result += endpoints[i].to_string();
    }
    return result;
}

zookeeper::connection_t::connection_t(const cfg_t& _cfg, const session_t& _session) :
    cfg(_cfg),
    session(_session),
    executor(new cocaine::executor::owning_asio_t()),
    _zhandle(),
    w_scope(),
    watcher(w_scope.get_handler<reconnect_action_t>(*this))
{
    if(!cfg.prefix.empty() && cfg.prefix[0] != '/') {
        throw std::system_error(std::make_error_code(std::errc::invalid_argument), "invalid prefix");
    }
    while(!cfg.prefix.empty() && cfg.prefix.back() == '/') {
        cfg.prefix.resize(cfg.prefix.size() - 1);
    }
    _zhandle.apply([&](handle_ptr& h) {
        reconnect(h);
    });
}

path_t zookeeper::connection_t::format_path(const path_t& path) {
    if(path.empty() || path[0] != '/') {
        throw std::system_error(ZBADARGUMENTS,  cocaine::error::zookeeper_category());
    }
    return cfg.prefix + path;
}

connection_t::handle_ptr connection_t::zhandle(){
    return _zhandle.apply([&](handle_ptr& handle){
        return handle;
    });
}

auto put_cb(int rc, const connection_t::stat_t* stat, const void* data) -> void {
    auto replier = unpack_ptr<put_reply_t>(data);
    connection_t::stat_t empty_stat = {};
    replier->operator()({rc, rc ? empty_stat : *stat});
}

auto get_cb(int rc, const char* value, int value_len, const connection_t::stat_t* stat, const void* data) -> void {
    auto replier = unpack_ptr<get_reply_t>(data);
    connection_t::stat_t empty_stat = {};
    replier->operator()({rc, rc ? std::string() : std::string(value, value_len), rc ? empty_stat : *stat});
}

auto create_cb(int rc, const char* path, const void* data) -> void {
    auto replier = unpack_ptr<create_reply_t>(data);
    replier->operator()({rc, rc ? std::string() : path});
}

auto delete_cb(int rc, const void* data) -> void {
    auto replier = unpack_ptr<del_reply_t>(data);
    replier->operator()({rc});
}

auto exists_cb(int rc, const connection_t::stat_t* stat, const void* data) -> void {
    auto replier = unpack_ptr<exists_reply_t>(data);
    connection_t::stat_t empty_stat = {};
    replier->operator()({rc, rc ? empty_stat : *stat});
}

auto children_cb(int rc, const struct String_vector* strings, const connection_t::stat_t* stat, const void* data) -> void {
    auto replier = unpack_ptr<children_reply_t>(data);
    std::vector<std::string> children;
    connection_t::stat_t empty_stat = {};
    if(!rc) {
        for(int i = 0; i < strings->count; i++) {
            children.emplace_back(strings->data[i]);
        }
    }
    replier->operator()({rc, std::move(children), rc ? empty_stat : *stat});
}

//TODO: rework this
auto watch_cb(zhandle_t* /*zh*/, int type, int state, const char* path, void* watch_data) -> void {
    auto replier = unpack_ptr<watch_reply_t>(watch_data);
    replier->operator()({type, state, path});
}

template<class ZooFunction, class Replier, class CCallback, class... Args>
auto connection_t::zoo_command(ZooFunction f, const path_t& path, Replier replier, CCallback cb, Args... args) -> void {
    check_connectivity();
    auto prefixed_path = format_path(path);
    auto ctx = pack_ptr(std::forward<Replier>(replier));
    check_rc(
            f(zhandle().get(), prefixed_path.c_str(), std::forward<Args>(args)..., cb, ctx.get())
    );
    ctx.release();
};

template<class ZooFunction, class Replier, class CCallback, class Watcher, class... Args>
auto connection_t::zoo_watched_command(ZooFunction f, const path_t& path, Replier replier, CCallback cb,
                                       Watcher watcher, Args... args) -> void
{
    if(watcher) {
        auto ctx = pack_ptr(std::move(watcher));
        zoo_command(f, path, std::forward<Replier>(replier), cb, std::forward<Args>(args)..., watch_cb, ctx.get());
        ctx.release();
    } else {
        zoo_command(f, path, std::forward<Replier>(replier), cb, std::forward<Args>(args)..., nullptr, nullptr);
    }
};


auto connection_t::put(const path_t& path, const value_t& value, version_t version, replier_ptr<put_reply_t> handler) -> void {
    zoo_command(zoo_aset, path, std::move(handler), put_cb, value.c_str(), value.size(), version);
}

auto connection_t::get(const path_t& path, replier_ptr<get_reply_t> handler) -> void {
    get(path, std::move(handler), nullptr);
}

auto connection_t::get(const path_t& path, replier_ptr<get_reply_t> handler, replier_ptr<watch_reply_t> watcher) -> void {
    zoo_watched_command(zoo_awget, path, std::move(handler), get_cb, std::move(watcher));
}

auto connection_t::create(const path_t& path, const value_t& value, bool ephemeral, bool sequence,
                          replier_ptr<create_reply_t> handler) -> void
{
    auto acl = ZOO_OPEN_ACL_UNSAFE;
    int flag = ephemeral ? ZOO_EPHEMERAL : 0;
    flag = flag | (sequence ? ZOO_SEQUENCE : 0);
    zoo_command(zoo_acreate, path, std::move(handler), create_cb, value.c_str(), value.size(), &acl, flag);
}

auto connection_t::del(const path_t& path, replier_ptr<del_reply_t> handler) -> void {
    del(path, -1, std::move(handler));
}

auto connection_t::del(const path_t& path, version_t version, replier_ptr<del_reply_t> handler) -> void {
    zoo_command(zoo_adelete, path, std::move(handler), delete_cb, version);
}

auto connection_t::exists(const path_t& path, replier_ptr<exists_reply_t> handler) -> void {
    exists(path, std::move(handler), nullptr);
}

auto connection_t::exists(const path_t& path, replier_ptr<exists_reply_t> handler, replier_ptr<watch_reply_t> watcher) -> void {
    zoo_watched_command(zoo_awexists, path, std::move(handler), exists_cb, std::move(watcher));
}

auto connection_t::childs(const path_t& path, replier_ptr<children_reply_t> handler) -> void {
    childs(path, std::move(handler), nullptr);
}

auto connection_t::childs(const path_t& path, replier_ptr<children_reply_t> handler, replier_ptr<watch_reply_t> watcher) -> void {
    zoo_watched_command(zoo_awget_children2, path, std::move(handler), children_cb, std::move(watcher));
}

auto connection_t::check_connectivity() -> void {
    _zhandle.apply([&](handle_ptr& handle) {
        if(!handle.get() || is_unrecoverable(handle.get())) {
            reconnect(handle);
        }
    });
}

auto connection_t::check_rc(int rc) const -> void {
    if(rc != ZOK) {
        auto code = cocaine::error::make_error_code(static_cast<cocaine::error::zookeeper_errors>(rc));
        throw std::system_error(code);
    }
}

auto connection_t::reconnect() -> void {
    _zhandle.apply([&](handle_ptr& h) {
        reconnect(h);
    });
}

auto connection_t::reconnect(handle_ptr& old_zhandle) -> void {
    handle_ptr new_zhandle = init();
    if(!new_zhandle.get() || is_unrecoverable(new_zhandle.get())) {
        if(session.valid()) {
            //Try to reset session before second attempt
            session.reset();
            new_zhandle = init();
        }
        if(!new_zhandle.get() || is_unrecoverable(new_zhandle.get())) {
            // Swap in any case.
            // Sometimes we really want to force reconnect even when zk is unavailable at all. For example on lock release.
            old_zhandle.swap(new_zhandle);
            throw std::system_error(cocaine::error::could_not_connect);
        }
    } else {
        if(!session.valid()) {
            session.assign(*zoo_client_id(new_zhandle.get()));
        }
    }
    old_zhandle.swap(new_zhandle);
}

auto connection_t::init() -> connection_t::handle_ptr {
    zhandle_t* new_zhandle = zookeeper_init(cfg.connection_string().c_str(),
                                            &handler_dispatcher_t::watcher_cb,
                                            cfg.recv_timeout_ms,
                                            session.native(),
                                            mc_ptr(&watcher),
                                            0);
    return handle_ptr(new_zhandle, std::bind(&connection_t::close, this, std::placeholders::_1));
}

auto connection_t::close(zhandle_t* handle) -> void{
    executor->spawn([=]{
        zookeeper_close(handle);
    });
}

auto connection_t::create_prefix() -> void {
    if (!cfg.prefix.empty()) {
        auto count = std::count(cfg.prefix.begin(), cfg.prefix.end(), '/');
        auto acl = ZOO_OPEN_ACL_UNSAFE;
        int flag = 0;
        for (size_t i = count; i > 0; i--) {
            auto path = path_parent(cfg.prefix, i - 1);
            int rc = zoo_create(zhandle().get(), path.c_str(), "", 0, &acl, flag, NULL, 0);
            if (rc && rc != ZNODEEXISTS) {
                _zhandle.apply([&](handle_ptr& h){
                    reconnect(h);
                });
            }
        }
    }
}

auto connection_t::reconnect_action_t::watch_event(int type, int state, path_t /*path*/) -> void {
    if(type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            parent.create_prefix();
        }
        if(state == ZOO_EXPIRED_SESSION_STATE) {
            try {
                parent.session.reset();
                parent._zhandle.apply([&](handle_ptr& h){
                    parent.reconnect(h);
                });
            } catch (const std::system_error& e) {
                // Swallow it and leave connection in disconnected state.
                // That's all we can do if ZK is unavailable.
            }
        }
    }
}

}
