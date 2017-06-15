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

#include "cocaine/detail/unicorn/zookeeper.hpp"
#include "cocaine/detail/zookeeper/errors.hpp"
#include "cocaine/detail/zookeeper/handler.hpp"
#include "cocaine/service/unicorn.hpp"
#include "cocaine/traits/dynamic.hpp"
#include "cocaine/unicorn/value.hpp"

#include <cocaine/context.hpp>
#include <cocaine/executor/asio.hpp>
#include <cocaine/errors.hpp>
#include <cocaine/format.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/utility/future.hpp>

#include <asio/io_service.hpp>

#include <blackhole/logger.hpp>

#include <memory>
#include <blackhole/wrapper.hpp>

#include <zookeeper/zookeeper.h>

namespace cocaine {
namespace unicorn {

template<class T>
using future_callback = std::function<void(std::future<T>)>;

using put_result_t = std::tuple<bool, unicorn::versioned_value_t>;

typedef api::unicorn_scope_ptr scope_ptr;

namespace {

zookeeper::cfg_t make_zk_config(const dynamic_t& args) {
    const auto& cfg = args.as_object();
    const auto& endpoints_cfg = cfg.at("endpoints", dynamic_t::empty_array).as_array();
    std::vector<zookeeper::cfg_t::endpoint_t> endpoints;
    for(size_t i = 0; i < endpoints_cfg.size(); i++) {
        endpoints.emplace_back(endpoints_cfg[i].as_object().at("host").as_string(),
                               endpoints_cfg[i].as_object().at("port").as_uint());
    }
    if(endpoints.empty()) {
        endpoints.emplace_back("localhost", 2181);
    }
    return zookeeper::cfg_t(endpoints, cfg.at("recv_timeout_ms", 1000u).as_uint(), cfg.at("prefix", "").as_string());
}

auto map_error(int rc) -> std::error_code {
    return cocaine::error::make_error_code(cocaine::error::zookeeper_errors(rc));
}
}


zookeeper::value_t serialize(const value_t& val) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(buffer);
    cocaine::io::type_traits<cocaine::dynamic_t>::pack(packer, val);
    return std::string(buffer.data(), buffer.size());
}

value_t unserialize(const zookeeper::value_t& val) {
    msgpack::object obj;
    std::unique_ptr<msgpack::zone> z(new msgpack::zone());

    msgpack_unpack_return ret = msgpack_unpack(
            val.c_str(), val.size(), nullptr, z.get(),
            reinterpret_cast<msgpack_object*>(&obj)
    );

    //Only strict unparse.
    if(static_cast<msgpack::unpack_return>(ret) != msgpack::UNPACK_SUCCESS) {
        throw std::system_error(cocaine::error::unicorn_errors::invalid_value);
    }
    value_t target;
    cocaine::io::type_traits<cocaine::dynamic_t>::unpack(obj, target);
    return target;
}

















class scope_t: public api::unicorn_scope_t {
public:
    synchronized<bool> closed;

    auto close() -> void override {
        closed.apply([&](bool& closed){
            closed = true;
        });
    }

    virtual
    auto on_abort() -> void {}
};

using node_stat = Stat;

struct abortable_t {
    virtual
    ~abortable_t() {}

    virtual
    auto abort_with_current_exception() -> void = 0;
};

template<class Reply>
class action: public zookeeper::replier<Reply>, public virtual abortable_t {
public:
    action() {}

    virtual
    auto on_reply(Reply reply) -> void = 0;

    auto operator()(Reply reply) -> void override {
        try {
            on_reply(std::move(reply));
        } catch(...) {
            abort_with_current_exception();
        }
    }
};

template <class T, class... Args>
class safe: public action<Args>... {
public:

private:
    std::shared_ptr<scope_t> _scope;
    future_callback<T> wrapped;

public:
    virtual ~safe() {}

    auto scope() -> std::shared_ptr<scope_t> {
        return _scope;
    }

    safe(future_callback<T> wrapped) :
            _scope(std::make_shared<scope_t>()),
            wrapped(std::move(wrapped))
    {}

    safe(std::shared_ptr<scope_t> scope, future_callback<T> wrapped) :
            _scope(std::move(scope)),
            wrapped(std::move(wrapped))
    {}

    template<class Result>
    auto satisfy(Result&& result) -> void {
        _scope->closed.apply([&](bool& closed){
            if(!closed) {
                wrapped(make_ready_future<T>(std::forward<Result>(result)));
            }
        });
    }

    virtual
    auto abort_with_current_exception() -> void override {
        _scope->closed.apply([&](bool& closed){
            _scope->on_abort();
            if(!closed) {
                wrapped(make_exceptional_future<T>());
            }
            closed = true;
        });
    }

    auto abort(std::exception_ptr eptr) -> void {
        try {
            std::rethrow_exception(eptr);
        } catch(...){
            abort_with_current_exception();
        }
    }
};

class zookeeper_t::put_t:
        public std::enable_shared_from_this<put_t>,
        public safe<put_result_t, zookeeper::put_reply_t, zookeeper::get_reply_t>
{
    zookeeper_t& parent;
    path_t path;
    value_t value;
    version_t version;
public:
    put_t(callback::put wrapped, zookeeper_t& parent, path_t path, value_t value, version_t version):
            safe(std::move(wrapped)),
            parent(parent),
            path(std::move(path)),
            value(std::move(value)),
            version(version)
    {}

    using safe::abort_with_current_exception;

    auto run() -> void {
        parent.zk.put(path, serialize(value), version, shared_from_this());
    }

    auto on_reply(zookeeper::put_reply_t reply) -> void override {
        if(reply.rc == ZBADVERSION) {
            parent.zk.get(path, shared_from_this());
        } else if(reply.rc != 0) {
            throw error_t(map_error(reply.rc), "failure during writing node value");
        } else {
            satisfy(std::make_tuple(true, versioned_value_t(value, reply.stat.version)));
        }
    }

    auto on_reply(zookeeper::get_reply_t reply) -> void override {
        if (reply.rc) {
            throw error_t(map_error(reply.rc), "failure during getting new node value");
        } else {
            satisfy(std::make_tuple(false, versioned_value_t(unserialize(reply.data), reply.stat.version)));
        }
    }
};


class zookeeper_t::get_t:
        public std::enable_shared_from_this<get_t>,
        public safe<versioned_value_t, zookeeper::get_reply_t>
{
    zookeeper_t& parent;
    path_t path;
public:
    get_t(callback::get wrapped, zookeeper_t& parent, path_t path):
        safe(std::move(wrapped)),
        parent(parent),
        path(std::move(path))
    {}

    auto run() -> void {
        parent.zk.get(path, shared_from_this());
    }

    auto on_reply(zookeeper::get_reply_t reply) -> void override {
        if (reply.rc != 0) {
            throw error_t(map_error(reply.rc), "failure during getting node value");
        } else if (reply.stat.numChildren != 0) {
            throw error_t(cocaine::error::child_not_allowed, "trying to read value of the node with childs");
        } else {
            satisfy(versioned_value_t(unserialize(reply.data), reply.stat.version));
        }
    }
};

class zookeeper_t::create_t:
        public std::enable_shared_from_this<create_t>,
        public safe<bool, zookeeper::create_reply_t>
{
    zookeeper_t& parent;
    path_t path;
    value_t value;
    bool ephemeral;
    bool sequence;
    size_t depth;

public:
    create_t(callback::create wrapped, zookeeper_t& parent, path_t path, value_t value, bool ephemeral, bool sequence) :
            safe(std::move(wrapped)),
            parent(parent),
            path(std::move(path)),
            value(std::move(value)),
            ephemeral(ephemeral),
            sequence(sequence),
            depth(0)
    {}

    auto run() -> void {
        parent.zk.create(path, serialize(value), ephemeral, sequence, shared_from_this());
    }

    auto on_reply(zookeeper::create_reply_t reply) -> void override {
        if(reply.rc == ZOK) {
            if(depth == 0) {
                satisfy(true);
            } else if(depth == 1) {
                depth--;
                parent.zk.create(path, serialize(value), ephemeral, sequence, shared_from_this());
            } else {
                depth--;
                parent.zk.create(zookeeper::path_parent(path, depth), "", false, false, shared_from_this());
            }
        } else if(reply.rc == ZNONODE) {
            depth++;
            parent.zk.create(zookeeper::path_parent(path, depth), "", false, false, shared_from_this());
        } else {
            throw error_t(map_error(reply.rc), "failure during creating node");
        }
    }
};

class zookeeper_t::del_t:
        public std::enable_shared_from_this<del_t>,
        public safe<bool, zookeeper::del_reply_t>
{
    zookeeper_t& parent;
    path_t path;
    version_t version;

public:
    del_t(callback::del wrapped, zookeeper_t& parent, path_t path, version_t version) :
        safe(std::move(wrapped)),
        parent(parent),
        path(std::move(path)),
        version(version)
    {}

    auto on_reply(zookeeper::del_reply_t reply) -> void override {
        if (reply.rc != 0) {
            throw error_t(map_error(reply.rc), "failure during deleting node");
        } else {
            satisfy(true);
        }
    }

    auto run() -> void {
        parent.zk.del(path, version, shared_from_this());
    }
};

class zookeeper_t::subscribe_t:
        public std::enable_shared_from_this<subscribe_t>,
        public safe<versioned_value_t, zookeeper::exists_reply_t, zookeeper::get_reply_t, zookeeper::watch_reply_t>
{
    zookeeper_t& parent;
    path_t path;

public:
    subscribe_t(callback::subscribe wrapped, zookeeper_t& parent, path_t path):
            safe(std::move(wrapped)),
            parent(parent),
            path(std::move(path))
    {}

    auto run() -> void {
        parent.zk.exists(path, shared_from_this(), shared_from_this());
    }

    auto on_reply(zookeeper::get_reply_t reply) -> void override {
        if(reply.rc) {
            throw error_t(map_error(reply.rc), "node was removed");
        } else if (reply.stat.numChildren != 0) {
            throw error_t(error::child_not_allowed, "trying to subscribe on node with childs");
        } else {
            satisfy(versioned_value_t(unserialize(reply.data), reply.stat.version));
        }
    }

    auto on_reply(zookeeper::exists_reply_t reply) -> void override {
        if(reply.rc == ZOK) {
            parent.zk.get(path, shared_from_this(), shared_from_this());
        } else {
            satisfy(versioned_value_t(value_t(), unicorn::not_existing_version));
        }
    }

    auto on_reply(zookeeper::watch_reply_t reply) -> void override {
        //TODO: is it ok?
        if(scope()->closed.unsafe()){
            return;
        }
        auto type = reply.type;
        if(type == ZOO_CREATED_EVENT || type == ZOO_CHANGED_EVENT) {
            return parent.zk.get(path, shared_from_this());
        }
        if(type == ZOO_DELETED_EVENT) {
            throw error_t(map_error(ZNONODE), "node was removed");
        }
        if(type == ZOO_CHILD_EVENT) {
            throw error_t(error::child_not_allowed, "child created on watched node");
        }
        if(type == ZOO_SESSION_EVENT) {
            COCAINE_LOG_WARNING(parent.log, "session event {} {} on {}", reply.type, reply.state, reply.path);
            return;
        }
        throw error_t(error::connection_loss, "watch was cancelled with {}, {} event on {}", reply.type, reply.state, reply.path);
    }
};

class zookeeper_t::children_subscribe_t:
        public std::enable_shared_from_this<children_subscribe_t>,
        public safe<response::children_subscribe, zookeeper::children_reply_t, zookeeper::watch_reply_t>
{
    zookeeper_t& parent;
    path_t path;

public:
    children_subscribe_t(callback::children_subscribe wrapped, zookeeper_t& parent, path_t path) :
            safe(std::move(wrapped)),
            parent(parent),
            path(std::move(path))
    {}

    auto run() -> void {
        parent.zk.childs(path, shared_from_this(), shared_from_this());
    }

    auto on_reply(zookeeper::children_reply_t reply) -> void override {
        if(reply.rc) {
            throw error_t(map_error(reply.rc), "can not fetch children");
        } else {
            satisfy(response::children_subscribe(reply.stat.cversion, std::move(reply.children)));
        }
    }

    auto on_reply(zookeeper::watch_reply_t reply) -> void override {
        //TODO: is it ok?
        if(scope()->closed.unsafe()){
            return;
        }
        if(reply.type == ZOO_CHILD_EVENT) {
            return parent.zk.childs(path, shared_from_this(), shared_from_this());
        }
        if(reply.type == ZOO_SESSION_EVENT) {
            COCAINE_LOG_WARNING(parent.log, "session event {} {} on {}", reply.type, reply.state, reply.path);
            return;
        }
        throw error_t(error::connection_loss, "event {}, {} on {} happened", reply.type, reply.state, reply.path);
    }
};

class zookeeper_t::increment_t:
    public std::enable_shared_from_this<increment_t>,
    public safe<versioned_value_t, zookeeper::get_reply_t, zookeeper::create_reply_t, zookeeper::put_reply_t>
{
    zookeeper_t& parent;
    path_t path;
    value_t value;
public:
    increment_t(callback::increment wrapped, zookeeper_t& parent, path_t path, value_t value) :
        safe(std::move(wrapped)),
        parent(parent),
        path(std::move(path)),
        value(std::move(value))
    {}

    auto run() -> void {
        if (!value.is_double() && !value.is_int() && !value.is_uint()) {
            throw error_t(error::unicorn_errors::invalid_type, "invalid value type for increment");
        }
        parent.zk.get(path, shared_from_this());
    }

    auto on_reply(zookeeper::get_reply_t reply) -> void override {
        if(reply.rc == ZNONODE) {
            return parent.zk.create(path, serialize(value), false, false, shared_from_this());
        } else if(reply.rc) {
            throw error_t(map_error(reply.rc), "failed to get node value");
        }
        value_t parsed = unserialize(reply.data);
        if (reply.stat.numChildren != 0) {
            throw error_t(error::child_not_allowed, "can not increment node with children");
        } else if (!parsed.is_double() && !parsed.is_int() && !parsed.is_uint()) {
            throw error_t(error::invalid_type, "can not increment non-numeric value");
        } else if (parsed.is_double() || value.is_double()) {
            value = parsed.to<double>() + value.to<double>();
        } else {
            value = parsed.to<int64_t>() + value.to<int64_t>();
        }
        parent.zk.put(path, serialize(value), reply.stat.version, shared_from_this());
    }

    auto on_reply(zookeeper::put_reply_t reply) -> void override {
        if(reply.rc) {
            throw error_t(map_error(reply.rc), "failed to put new node value");
        }
        satisfy(versioned_value_t(value, reply.stat.version));
    }

    auto on_reply(zookeeper::create_reply_t reply) -> void override {
        if(reply.rc) {
            throw error_t(map_error(reply.rc), "could not create value");
        } else {
            satisfy(versioned_value_t(value, version_t()));
        }
    }
};

class zookeeper_t::lock_t :
    public std::enable_shared_from_this<lock_t>,
    public safe<bool, zookeeper::create_reply_t, zookeeper::children_reply_t, zookeeper::exists_reply_t, zookeeper::del_reply_t, zookeeper::watch_reply_t>
{
public:
    struct lock_scope_t: public scope_t {
        lock_t* parent;

        lock_scope_t(lock_t* parent):
                parent(parent)
        {}

        auto close() -> void override {
            closed.apply([&](bool& closed){
                if(!closed) {
                    if(!parent->created_sequence_path.empty()) {
                        parent->parent.zk.del(parent->created_sequence_path, parent->shared_from_this());
                    }
                    closed = true;
                }
            });
        }

        auto on_abort() -> void override {
            if(!parent->created_sequence_path.empty()) {
                parent->parent.zk.del(parent->created_sequence_path, parent->shared_from_this());
            }
        }
    };

private:
    std::shared_ptr<lock_scope_t> _scope;
    callback::lock wrapped;
    zookeeper_t& parent;
    path_t folder;
    path_t path;
    path_t created_sequence_path;
    value_t value;
    uint64_t depth;

public:
    lock_t(callback::lock wrapped, zookeeper_t& parent, path_t folder) :
        safe(std::make_shared<lock_scope_t>(this), std::move(wrapped)),
        wrapped(std::move(wrapped)),
        parent(parent),
        folder(std::move(folder)),
        path(folder + "/lock"),
        value(time(nullptr)),
        depth(0)
    {}

    auto scope() -> std::shared_ptr<lock_scope_t> {
        return _scope;
    }

    auto run() -> void {
        parent.zk.create(path, serialize(value), true, true, shared_from_this());
    }

    auto on_reply(zookeeper::create_reply_t reply) -> void override {
        if(reply.rc == ZOK) {
            if(depth == 0) {
                return _scope->closed.apply([&](bool& closed){
                    created_sequence_path = reply.created_path;
                    if(!closed) {
                        parent.zk.childs(folder, shared_from_this());
                    } else {
                        parent.zk.del(created_sequence_path, shared_from_this());
                    }
                });
            } else if(depth == 1) {
                depth--;
                parent.zk.create(path, serialize(value), true, true, shared_from_this());
            } else {
                depth--;
                parent.zk.create(zookeeper::path_parent(path, depth), "", false, false, shared_from_this());
            }
        } else if(reply.rc == ZNONODE) {
            depth++;
            parent.zk.create(zookeeper::path_parent(path, depth), "", false, false, shared_from_this());
        } else {
            throw error_t(map_error(reply.rc), "failure during creating node");
        }
    }

    auto on_reply(zookeeper::children_reply_t reply) -> void override {
        if(reply.rc) {
            throw error_t(map_error(reply.rc), "failed to get lock folder children");
        }
        auto& children = reply.children;
        std::sort(children.begin(), children.end());
        auto it = std::find(children.begin(), children.end(), created_sequence_path);
        if(it == children.end()) {
            throw error_t("created path is not found in children");
        }
        if(it == children.begin()) {
            return satisfy(true);
        } else {
            it--;
            if(!zookeeper::is_valid_sequence_node(*it)) {
                throw error_t("trash data in lock folder");
            }
            auto next_node = folder + "/" + *it;
            parent.zk.exists(next_node, shared_from_this(), shared_from_this());
        }
    }

    auto on_reply(zookeeper::exists_reply_t reply) -> void override {
        if(reply.rc == ZNONODE) {
            satisfy(true);
        } else if(reply.rc) {
            throw error_t(map_error(reply.rc), "error during fetching previous lock");
        }
    }

    auto on_reply(zookeeper::watch_reply_t reply) -> void override {
        if(reply.type == ZOO_DELETED_EVENT) {
            satisfy(true);
        } else if(reply.type == ZOO_SESSION_EVENT){
            COCAINE_LOG_WARNING(parent.log, "session event {} {} on {}", reply.type, reply.state, reply.path);
            return;
        } else {
            throw error_t("invalid lock watch event {} {} on {}", reply.type, reply.state, reply.path);
        }
    }

    auto on_reply(zookeeper::del_reply_t reply) -> void override {
        if(reply.rc) {
            COCAINE_LOG_ERROR(parent.log, "failed to delete lock, reconnecting");
            parent.zk.reconnect();
        }
    }
};


template<class Action, class Callback, class... Args>
auto zookeeper_t::run_command(Callback callback, Args&& ...args) -> api::unicorn_scope_ptr {
    auto action = std::make_shared<Action>(std::move(callback), *this, std::forward<Args>(args)...);
    try {
        action->run();
    } catch (...) {
        auto eptr = std::current_exception();
        executor->spawn([=](){
            action->abort(eptr);
        });
    }
    return action->scope();
}

zookeeper_t::zookeeper_t(cocaine::context_t& _context, const std::string& _name, const dynamic_t& args) :
    api::unicorn_t(_context, name, args),
    context(_context),
    executor(new cocaine::executor::owning_asio_t()),
    name(_name),
    log(context.log(cocaine::format("unicorn/{}", name))),
    zk_session(),
    zk(make_zk_config(args), zk_session)
{
}

zookeeper_t::~zookeeper_t() = default;

auto zookeeper_t::put(callback::put callback, const path_t& path, const value_t& value, version_t version) -> scope_ptr {
    return run_command<put_t>(std::move(callback), path, value, version);
}

auto zookeeper_t::get(callback::get callback, const path_t& path) -> scope_ptr {
    return run_command<get_t>(std::move(callback), path);
}

auto zookeeper_t::create(callback::create callback, const path_t& path, const value_t& value, bool ephemeral, bool sequence) -> scope_ptr {
    return run_command<create_t>(std::move(callback), path, value, ephemeral, sequence);
}

auto zookeeper_t::del(callback::del callback, const path_t& path, version_t version) -> scope_ptr {
    return run_command<del_t>(std::move(callback), path, version);
}

auto zookeeper_t::subscribe(callback::subscribe callback, const path_t& path) -> scope_ptr {
    return run_command<subscribe_t>(std::move(callback), path);
}

auto zookeeper_t::children_subscribe(callback::children_subscribe callback, const path_t& path) -> scope_ptr {
    return run_command<children_subscribe_t>(std::move(callback), path);
}

auto zookeeper_t::increment(callback::increment callback, const path_t& path, const value_t& value) -> scope_ptr {
    return run_command<increment_t>(std::move(callback), path, value);
}

auto zookeeper_t::lock(callback::lock callback, const path_t& path) -> scope_ptr {
    return run_command<lock_t>(std::move(callback), path);
}

}}
