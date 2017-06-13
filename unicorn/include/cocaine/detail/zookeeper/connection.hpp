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

#pragma once

#include "cocaine/detail/zookeeper/handler.hpp"
#include "cocaine/detail/zookeeper/session.hpp"

#include <cocaine/locked_ptr.hpp>
#include <cocaine/api/executor.hpp>

#include <boost/optional/optional.hpp>

#include <zookeeper/zookeeper.h>

#include <vector>
#include <string>
#include <thread>

namespace zookeeper {

class cfg_t {
public:
    class endpoint_t {
    public:
        endpoint_t(std::string _hostname, unsigned int _port);

        std::string
        to_string() const;

    private:
        std::string hostname;
        unsigned int port;
    };

    cfg_t(std::vector<endpoint_t> endpoints, unsigned int recv_timeout_ms, std::string prefix);

    /**
    * ZK connection string.
    * ZK accepts several host:port values of a cluster splitted by comma.
    */
    std::string
    connection_string() const;

    const unsigned int recv_timeout_ms;
    std::string prefix;
private:
    std::vector<endpoint_t> endpoints;
};
template<class... Args>
struct replier {
    virtual
    auto operator()(Args... args) -> void = 0;

    virtual
    ~replier(){}
};

template<class... Args>
using replier_ptr = std::shared_ptr<replier<Args...>>;

struct put_reply_t {
    int rc;
    const node_stat& stat;
};

struct get_reply_t {
    int rc;
    std::string data;
    const node_stat& stat;
};

struct watch_reply_t {
    int type;
    int state;
    path_t path;
};

struct create_reply_t {
    int rc;
    path_t created_path;
};

struct del_reply_t {
    int rc;
};

struct exists_reply_t {
    int rc;
    const node_stat& stat;
};

struct children_reply_t {
    int rc;
    std::vector<std::string> children;
    const node_stat& stat;
};

/**
* Adapter class to zookeeper C api.
* Add ability to pass std::unique_ptr of handler object instead of function callback and void*
*/
class connection_t {
public:
    typedef std::shared_ptr<zhandle_t> handle_ptr;

    connection_t(const cfg_t& cfg, const session_t& session);
    connection_t(const connection_t&) = delete;
    connection_t& operator=(const connection_t&) = delete;

    /**
    * put value to path. If version in ZK is different returns an error.
    * See zoo_aset.
    */
    void
    put(const path_t& path, const value_t& value, version_t version, replier_ptr<put_reply_t> handler);

    void
    get(const path_t& path, replier_ptr<get_reply_t> handler);

    void
    get(const path_t& path, replier_ptr<get_reply_t> handler, replier_ptr<watch_reply_t> watcher);

    void
    create(const path_t& path, const value_t& value, bool ephemeral, bool sequence, replier_ptr<create_reply_t> handler);

    void
    del(const path_t& path, version_t version, replier_ptr<del_reply_t> handler);

    void
    del(const path_t& path, replier_ptr<del_reply_t> handler);


    void
    exists(const path_t& path, replier_ptr<exists_reply_t> handler, replier_ptr<watch_reply_t> watcher);

    void
    childs(const path_t& path, replier_ptr<children_reply_t>);

    void
    childs(const path_t& path, replier_ptr<children_reply_t>, replier_ptr<watch_reply_t> watcher);

    void
    reconnect();

private:
    struct reconnect_action_t :
        public managed_watch_handler_base_t
    {
        reconnect_action_t(const handler_tag& tag, connection_t& _parent) :
            managed_handler_base_t(tag),
            managed_watch_handler_base_t(tag),
            parent(_parent)
        {}

        virtual void
        watch_event(int type, int state, path_t path);

        connection_t& parent;
    };

    void reconnect(handle_ptr& old_handle);
    path_t format_path(const path_t path);
    handle_ptr zhandle();

    cfg_t cfg;
    session_t session;

    // executor for closing connections to avoid deadlocks
    std::unique_ptr<cocaine::api::executor_t> executor;

    cocaine::synchronized<handle_ptr> _zhandle;
    void check_rc(int rc) const;
    void check_connectivity();
    handle_ptr init();
    void close(zhandle_t* handle);
    void create_prefix();
    handler_scope_t w_scope;
    managed_watch_handler_base_t& watcher;
};
}

