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

#include "cocaine/api/unicorn.hpp"

#include "cocaine/detail/zookeeper/connection.hpp"

#include <cocaine/idl/unicorn.hpp>

namespace cocaine { namespace unicorn {

template<class T>
class scoped_wrapper;

//zookeeper::cfg_t make_zk_config(const dynamic_t& args);

/**
* Serializes service representation of value to zookepeers representation.
* Currently ZK store msgpacked data, and service uses cocaine::dynamic_t
*/
zookeeper::value_t
serialize(const value_t& val);

/**
* Unserializes zookepeers representation to service representation.
*/
value_t
unserialize(const zookeeper::value_t& val);


//TODO: Remove
struct zookeeper_scope_t: public api::unicorn_scope_t {
    std::shared_ptr<zookeeper::handler_scope_t> handler_scope;
    virtual
    ~zookeeper_scope_t() {}
};

class zookeeper_t :
    public api::unicorn_t
{
    cocaine::context_t& context;
    std::unique_ptr<api::executor_t> executor;
    const std::string name;
    const std::unique_ptr<logging::logger_t> log;
    zookeeper::session_t zk_session;
    zookeeper::connection_t zk;

public:
    //TODO: remove
    struct context_t {
        std::shared_ptr<logging::logger_t> log;
        zookeeper::connection_t& zk;
    };

    struct put_action_t;

    using callback = api::unicorn_t::callback;
    using scope_ptr = api::unicorn_scope_ptr;

    zookeeper_t(cocaine::context_t& context, const std::string& name, const dynamic_t& args);

    ~zookeeper_t();

    auto put(callback::put callback, const path_t& path, const value_t& value, version_t version) -> scope_ptr override;

    auto get(callback::get callback, const path_t& path) -> scope_ptr override;

    auto create(callback::create callback, const path_t& path, const value_t& value, bool ephemeral, bool sequence)
            -> scope_ptr override;

    auto del(callback::del callback, const path_t& path, version_t version) -> scope_ptr override;

    auto subscribe(callback::subscribe callback, const path_t& path) -> scope_ptr override;

    auto children_subscribe(callback::children_subscribe callback, const path_t& path) -> scope_ptr override;

    auto increment(callback::increment callback, const path_t& path, const value_t& value) -> scope_ptr override;

    auto lock(callback::lock callback, const path_t& path) -> scope_ptr override;

private:
    auto put_impl(scoped_wrapper<response::put> callback, const path_t& path, const value_t& value, version_t version) -> void;

    auto get_impl(scoped_wrapper<response::get> callback, const path_t& path) -> void;

    auto create_impl(scoped_wrapper<response::create> callback, const path_t& path, const value_t& value,
                     bool ephemeral, bool sequence) -> void;

    auto del_impl(scoped_wrapper<response::del> callback, const path_t& path, version_t version) -> void;

    auto subscribe_impl(scoped_wrapper<response::subscribe> callback, const path_t& path) -> void;

    auto children_subscribe_impl(scoped_wrapper<response::children_subscribe> callback, const path_t& path) -> void;

    auto increment_impl(scoped_wrapper<response::increment> callback, const path_t& path, const value_t& value) -> void;

    auto lock_impl(scoped_wrapper<response::lock> callback, const path_t& path) -> void;

    template<class T>
    auto async_abort_callback(scoped_wrapper<T>& cb) -> void;

    template<class Callback, class Method, class... Args>
    auto run_command(Callback cb, Method method, Args&& ...args) -> api::unicorn_scope_ptr;
};

}}
