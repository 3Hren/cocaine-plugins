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
    struct {
        boost::shared_mutex mutex;
        bool closed;
    } d;

public:
    scope_t():
        d{{},false}
    {}

    auto close() -> void {
        d.mutex.lock();
        d.closed = true;
    }

    auto closed() -> bool {
        return d.closed;
    };

    auto lock() -> void {
        d.mutex.lock_shared();
    }

    auto unlock() -> void {
        d.mutex.unlock_shared();
    }
};

using node_stat = Stat;

template <class T>
class safe {
public:

private:
    std::shared_ptr<scope_t> _scope;
    future_callback<T> wrapped;

public:
    auto scope() -> std::shared_ptr<scope_t> {
        return _scope;
    }

    safe(future_callback<T> wrapped) :
            _scope(std::make_shared<scope_t>()),
            wrapped(std::move(wrapped))
    {}

    template<class Result>
    auto satisfy(Result&& result) -> void {
        std::lock_guard<scope_t> guard(*_scope);
        if(!_scope->closed()) {
            wrapped(make_ready_future<T>(std::forward<Result>(result)));
        }
    }

    auto abort_with_current_exception() -> void {
        std::lock_guard<scope_t> guard(*_scope);
        if(!_scope->closed()) {
            _scope->close();
            wrapped(make_exceptional_future<T>());
        }
    }

    auto abort(std::exception_ptr eptr) -> void {
        try {
            std::rethrow_exception(eptr);
        } catch(...){
            abort_with_current_exception();
        }
    }

//    auto closed() -> bool {
//        return scope->closed();
//    }
};

template<class T, class... Args>
class action: virtual public safe<T>, public zookeeper::connection_t::replier<Args...> {
public:
//    action() : safe<T>(nullptr) {}
    action() {}

    virtual
    auto on_reply(Args... args) -> void = 0;

    auto operator()(Args... args) -> void override {
        try {
            on_reply(std::forward<Args>(args)...);
        } catch(...) {
            safe<T>::abort_with_current_exception();
        }
    }
};

class zookeeper_t::put_t:
        public std::enable_shared_from_this<put_t>,
        public action<put_result_t, int, const node_stat&>,
        public action<put_result_t, int, std::string, const node_stat&>
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

    auto run() -> void {
        parent.zk.put(path, serialize(value), version, shared_from_this());
    }

    auto on_reply(int rc, const node_stat& stat) -> void override {
        if(rc == ZBADVERSION) {
            parent.zk.get(path, shared_from_this());
        } else if(rc != 0) {
            throw error_t(map_error(rc), "failure during writing node value");
        } else {
            satisfy(std::make_tuple(true, versioned_value_t(value, stat.version)));
        }
    }

    auto on_reply(int rc, std::string data, const node_stat& stat) -> void override {
        if (rc) {
            throw error_t(map_error(rc), "failure during getting new node value");
        } else {
            satisfy(std::make_tuple(false, versioned_value_t(unserialize(data), stat.version)));
        }
    }
};


class zookeeper_t::get_t:
        public std::enable_shared_from_this<get_t>,
        public action<versioned_value_t, int, std::string, const node_stat&>
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

    auto on_reply(int rc, std::string data, const node_stat& stat) -> void override {
        if (rc != 0) {
            throw error_t(map_error(rc), "failure during getting node value");
        } else if (stat.numChildren != 0) {
            throw error_t(cocaine::error::child_not_allowed, "trying to read value of the node with childs");
        } else {
            satisfy(versioned_value_t(unserialize(data), stat.version));
        }
    }
};

class zookeeper_t::create_t:
        public std::enable_shared_from_this<create_t>,
        public action<bool, int, zookeeper::path_t>
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

    auto on_reply(int rc, zookeeper::path_t) -> void override {
        if(rc == ZOK) {
            if(depth == 0) {
                satisfy(true);
            } else if(depth == 1) {
                depth--;
                parent.zk.create(path, serialize(value), ephemeral, sequence, shared_from_this());
            } else {
                depth--;
                parent.zk.create(zookeeper::path_parent(path, depth), "", false, false, shared_from_this());
            }
        } else if(rc == ZNONODE) {
            depth++;
            parent.zk.create(zookeeper::path_parent(path, depth), "", false, false, shared_from_this());
        } else {
            throw error_t(map_error(rc), "failure during creating node");
        }
    }
};

class zookeeper_t::del_t: public std::enable_shared_from_this<del_t>, public action<bool, int> {
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

    auto on_reply(int rc) -> void override {
        if (rc != 0) {
            throw error_t(map_error(rc), "failure during deleting node");
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
        public action<versioned_value_t, int, std::string, const node_stat&>,
        public action<versioned_value_t, int, const node_stat&>,
        public action<versioned_value_t, int, int, path_t>
{
    zookeeper_t& parent;
    path_t path;

public:
    auto run() -> void {
        parent.zk.exists(path, shared_from_this(), shared_from_this());
    }

    subscribe_t(callback::subscribe wrapped, zookeeper_t& parent, path_t path):
            safe(std::move(wrapped)),
            path(std::move(path)),
            parent(parent)
    {}

    auto on_reply(int rc, std::string data, const node_stat& stat) -> void override {
        if(rc) {
            throw error_t(map_error(rc), "node was removed");
        } else if (stat.numChildren != 0) {
            throw error_t(error::child_not_allowed, "trying to subscribe on node with childs");
        } else {
            satisfy(versioned_value_t(unserialize(data), stat.version));
        }
    }

    auto on_reply(int rc, const zookeeper::node_stat& stat) -> void override {
        if(rc == ZOK) {
            parent.zk.get(path, shared_from_this(), shared_from_this());
        } else {
            satisfy(versioned_value_t(value_t(), unicorn::not_existing_version));
        }
    }

    auto on_reply(int type, int state, zookeeper::path_t) -> void override {
        if(scope()->closed()){
            return;
        }
        switch(type) {
            case ZOO_CREATED_EVENT:
            case ZOO_CHANGED_EVENT:
                parent.zk.get(path, shared_from_this());
            case ZOO_DELETED_EVENT:
                throw error_t(map_error(ZNONODE), "node was removed");
            case ZOO_CHILD_EVENT:
                throw error_t(error::child_not_allowed, "child created on watched node");
            case ZOO_SESSION_EVENT:
                COCAINE_LOG_WARNING(log, "session event {} {} on {}", type, state, path);
                return;
            case ZOO_NOTWATCHING_EVENT:
            default:
                throw error_t(error::connection_loss, "watch was cancelled");
        }
    }
};

class zookeeper_t::children_subscribe_t: public std::enable_shared_from_this<children_subscribe_t>,
    public action<response::children_subscribe, int, std::vector<std::string>, const zookeeper::node_stat&>,
    public action<response::children_subscribe, int, int, zookeeper::path_t>
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

    auto on_reply(int rc, std::vector<std::string> children, const zookeeper::node_stat& stat) -> void override {
        if(rc) {
            throw error_t(map_error(rc), "can not fetch children");
        } else {
            satisfy(response::children_subscribe(stat.cversion, std::move(children)));
        }
    }

    auto on_reply(int type, int state, zookeeper::path_t) -> void override {
        if(scope()->closed()){
            return;
        }
        switch(type) {
            case ZOO_CHILD_EVENT:
                parent.zk.childs(path, shared_from_this(), shared_from_this());
            case ZOO_SESSION_EVENT:
                COCAINE_LOG_WARNING(log, "session event {} {} on {}", type, state, path);
                return;
            case ZOO_DELETED_EVENT:
            case ZOO_CREATED_EVENT:
            case ZOO_CHANGED_EVENT:
            case ZOO_NOTWATCHING_EVENT:
            default:
                throw error_t(error::connection_loss, "event {} happened", type);
        }
    }
};

class zookeeper_t::increment_t:
    public std::enable_shared_from_this<increment_t>,
    public action<versioned_value_t, int, std::string, const node_stat&>,
    public action<versioned_value_t, int, const node_stat&>,
    public action<versioned_value_t, int, zookeeper::path_t>
{
    zookeeper_t& parent;
    path_t path;
    value_t value;
public:
    increment_t(callback::children_subscribe wrapped, zookeeper_t& parent, path_t path, value_t value) :
        safe(std::move(wrapped)),
        parent(parent),
        path(std::move(path)),
        value(std::move(value))
    {}

    auto run() -> void {
        if (!value.is_double() && !value.is_int() && !value.is_uint()) {
            throw error_t(error::unicorn_errors::invalid_type, "invalid value type for increment")
        }
        parent.zk.get(path, shared_from_this());
    }

    auto on_reply(int rc, std::string data, const node_stat& stat) -> void override {
        if(rc == ZNONODE) {
            return parent.zk.create(path, serialize(value), false, false, shared_from_this());
        } else if(rc) {
            throw error_t(map_error(rc), "failed to get node value");
        }
        value_t parsed = unserialize(data);
        if (stat.numChildren != 0) {
            throw error_t(error::child_not_allowed, "can not increment node with children");
        } else if (!parsed.is_double() && !parsed.is_int() && !parsed.is_uint()) {
            throw error_t(error::invalid_type, "can not increment non-numeric value");
        } else if (parsed.is_double() || value.is_double()) {
            value = parsed.to<double>() + value.to<double>();
        } else {
            value = parsed.to<int64_t>() + value.to<int64_t>();
        }
        parent.zk.put(path, serialize(value), stat.version, shared_from_this);
    }

    auto on_reply(int rc, const node_stat& stat) -> void override {
        if(rc) {
            throw error_t(map_error(rc), "failed to put new node value");
        }
        satisfy(versioned_value_t(value, stat.version));
    }

    auto on_reply(int rc, zookeeper::path_t) -> void override {
        if(rc) {
            throw error_t(map_error(rc), "could not create value");
        } else {
            satisfy(versioned_value_t(value, version_t()));
        }
    }
};

class zookeeper_t::lock_t :
    public
{

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




//TODO: do we need impl functions? lamdas maybe will be ok
auto zookeeper_t::put(callback::put callback, const path_t& path, const value_t& value, version_t version) -> scope_ptr {
    return run_command<put_t>(std::move(callback), path, value, version);
}

auto zookeeper_t::get(callback::get callback, const path_t& path) -> scope_ptr {
    return run_command<get_t>(std::move(callback), path);
}

auto zookeeper_t::create(callback::create callback, const path_t& path, const value_t& value,
                         bool ephemeral, bool sequence) -> scope_ptr
{
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

auto zookeeper_t::del_impl(scoped_wrapper<response::del> wrapper, const path_t& path, version_t version) -> void {
}

auto zookeeper_t::subscribe_impl(scoped_wrapper<response::subscribe> wrapper, const path_t& path) -> void {
    auto on_data = make_safe_wrapper(wrapper, [=](int rc, std::string value, const zookeeper::node_stat& stat) mutable {
        if(rc) {
            throw error_t(map_error(rc), "node was removed");
        } else if (stat.numChildren != 0) {
            throw error_t(error::child_not_allowed, "trying to subscribe on node with childs");
        } else {
            wrapper.satisfy(versioned_value_t(unserialize(value), stat.version));
        }
    });

    auto on_exist = make_safe_wrapper(wrapper, [=](int rc, const zookeeper::node_stat& stat){
        if(rc == ZOK) {
            zk.get(path, on_data);
        } else {
            //wait for watch
        }
    });

    auto on_watch = make_safe_wrapper(wrapper, [=](int type, int state, zookeeper::path_t path){
        if(wrapper.closed()){
            return;
        }
        switch(type) {
            case ZOO_CREATED_EVENT:
            case ZOO_CHANGED_EVENT:
                zk.get(path, on_data);
            case ZOO_DELETED_EVENT:
                throw error_t(map_error(ZNONODE), "node was removed");
            case ZOO_CHILD_EVENT:
                throw error_t(error::child_not_allowed, "child created on watched node");
            case ZOO_SESSION_EVENT:
                COCAINE_LOG_WARNING(log, "session event {} {} on {}", type, state, path);
                return;
            case ZOO_NOTWATCHING_EVENT:
            default:
                throw error_t(error::connection_loss, "watch was cancelled");
        }
    });

    zk.exists(path, std::move(on_exist), std::move(on_watch));

    std::shared_ptr<logging::logger_t> logger = context.log(cocaine::format("unicorn/{}", name), {
        {"method", "subscribe"},
        {"path", path},
        {"command_id", rand()}});
    auto scope = std::make_shared<zk_scope_t>();
    auto& handler = scope->handler_scope.get_handler<subscribe_action_t>(
        std::move(callback),
        context_t({logger, zk}),
        path
    );
    COCAINE_LOG_DEBUG(logger, "start processing");
    try_run([&]{
        zk.get(handler.path, handler, handler);
    }, handler.callback, *executor);
    COCAINE_LOG_DEBUG(logger, "enqueued");
    return scope;
}

auto zookeeper_t::children_subscribe_impl(scoped_wrapper<response::children_subscribe> wrapper, const path_t& path) -> void {
    std::shared_ptr<logging::logger_t> logger = context.log(cocaine::format("unicorn/{}", name), {
        {"method", "children_subscribe"},
        {"path", path},
        {"command_id", rand()}});
    auto scope = std::make_shared<zk_scope_t>();
    auto& handler = scope->handler_scope.get_handler<children_subscribe_action_t>(
        std::move(callback),
        context_t({logger, zk}),
        path
    );
    COCAINE_LOG_DEBUG(logger, "start processing");
    try_run([&]{
        zk.childs(handler.path, handler, handler);
    }, handler.callback, *executor);
    COCAINE_LOG_DEBUG(logger, "enqueued");
    return scope;
}

auto zookeeper_t::increment_impl(scoped_wrapper<response::increment> wrapper, const path_t& path, const value_t& value) -> void {
    auto scope = std::make_shared<zk_scope_t>();
    if (!value.is_double() && !value.is_int() && !value.is_uint()) {
        executor->spawn(std::bind([](callback::increment& cb){
            auto ec = make_error_code(cocaine::error::unicorn_errors::invalid_type);
            auto future = make_exceptional_future<response::increment>(ec);
            cb(std::move(future));
        }, std::move(callback)));
        return scope;
    }
    std::shared_ptr<logging::logger_t> logger = context.log(cocaine::format("unicorn/{}", name), {
        {"method", "increment"},
        {"path", path},
        {"command_id", rand()}});
    auto& handler = scope->handler_scope.get_handler<increment_action_t>(
        context_t({logger, zk}),
        std::move(callback),
        path,
        value
    );
    COCAINE_LOG_DEBUG(logger, "start processing");
    try_run([&]{
        zk.get(handler.path, handler);
    }, handler.callback, *executor);
    COCAINE_LOG_DEBUG(logger, "enqueued");
    return scope;
}

auto zookeeper_t::lock_impl(scoped_wrapper<response::lock> wrapper, const path_t& folder) -> void{
    path_t path = folder + "/lock";
    std::shared_ptr<logging::logger_t> logger = context.log(cocaine::format("unicorn/{}", name), {
        {"method", "lock"},
        {"folder", folder},
        {"command_id", rand()}});
    auto lock_state = std::make_shared<lock_state_t>(context_t({logger, zk}));

    auto& handler = lock_state->handler_scope.get_handler<lock_action_t>(
        context_t({logger, zk}),
        lock_state,
        path,
        std::move(folder),
        value_t(time(nullptr)),
        std::move(callback)
    );
    COCAINE_LOG_DEBUG(logger, "start processing");
    try_run([&]{
   //     zk.create(path, handler.encoded_value, handler.ephemeral, handler.sequence, handler);
    }, handler.callback, *executor);
    COCAINE_LOG_DEBUG(logger, "enqueued");
    return lock_state;
}

}}
