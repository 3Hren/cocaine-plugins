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
#include "cocaine/detail/unicorn/zookeeper/children_subscribe.hpp"
#include "cocaine/detail/unicorn/zookeeper/create.hpp"
#include "cocaine/detail/unicorn/zookeeper/del.hpp"
#include "cocaine/detail/unicorn/zookeeper/increment.hpp"
#include "cocaine/detail/unicorn/zookeeper/lock.hpp"
#include "cocaine/detail/unicorn/zookeeper/lock_state.hpp"
#include "cocaine/detail/unicorn/zookeeper/put.hpp"
#include "cocaine/detail/unicorn/zookeeper/subscribe.hpp"
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

typedef api::unicorn_t::callback callback;

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











//TODO: remove
struct zk_scope_t : public api::unicorn_scope_t {
    zookeeper::handler_scope_t handler_scope;
    virtual void close()  override {};
};

template<class F, class Result>
auto try_run(F runnable, std::function<void(std::future<Result>)>& callback, api::executor_t& executor) -> void {
    typedef std::function<void(std::future<Result>)> callback_t;
    try {
        runnable();
    } catch (...) {
        auto future = make_exceptional_future<Result>();
        executor.spawn(std::bind([](callback_t& cb, std::future<Result>& f) {
            cb(std::move(f));
        }, std::move(callback), std::move(future)));
    }
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


//TODO: performance issues?
template<class T>
class scoped_wrapper {
    std::shared_ptr<scope_t> scope;
    std::function<void(std::future<T>)> f;

public:
    using argument_type = T;

    scoped_wrapper(std::shared_ptr<scope_t> scope, std::function<void(std::future<T>)> f):
        scope(std::move(scope)), f(std::move(f))
    {}

    template<class Result>
    auto satisfy(Result&& result) -> void {
        std::lock_guard<scope_t> guard(*scope);
        if(!scope->closed()) {
            f(make_ready_future<T>(std::forward<Result>(result)));
        }
    }

    auto abort_with_current_exception() -> void {
        std::lock_guard<scope_t> guard(*scope);
        if(!scope->closed()) {
            f(make_exceptional_future<T>());
        }
    }

    auto abort(std::exception_ptr eptr) -> void {
        try {
            std::rethrow_exception(eptr);
        } catch(...){
            abort_with_current_exception();
        }
    }

    auto closed() -> bool {
        return scope->closed();
    }
};

template<class T, class F>
class safe_wrapper {
    scoped_wrapper<T> scoped;
    F f;
public:
    safe_wrapper(scoped_wrapper<T> scoped, F f): scoped(std::move(scoped)), f(std::move(f)) {}

    template<class... Args>
    auto operator()(Args&&... args) -> void {
        try {
            f(std::forward<Args>(args)...);
        } catch(...) {
            scoped.abort_with_current_exception();
        }
    }
};


template<class T, class... Args>
class action_t: public zookeeper::connection_t::callback<Args...> {
    std::shared_ptr<scope_t> scope;
    std::function<void(std::future<T>)> f;

public:
    virtual
    auto process(Args... args) = 0;

    auto operator()(Args... args) -> void override {
        try {
            process(std::forward<Args>(args)...);
        } catch(...) {
            abort_with_current_exception();
        }
    }

private:
    template<class Result>
    auto satisfy(Result&& result) -> void {
        std::lock_guard<scope_t> guard(*scope);
        if(!scope->closed()) {
            f(make_ready_future<T>(std::forward<Result>(result)));
        }
    }

    auto abort_with_current_exception() -> void {
        std::lock_guard<scope_t> guard(*scope);
        if(!scope->closed()) {
            f(make_exceptional_future<T>());
        }
    }

    auto abort(std::exception_ptr eptr) -> void {
        try {
            std::rethrow_exception(eptr);
        } catch(...){
            abort_with_current_exception();
        }
    }

    auto closed() -> bool {
        return scope->closed();
    }
};


template <class T, class F>
auto make_safe_wrapper(scoped_wrapper<T> scoped, F f) -> safe_wrapper<T, F> {
    return safe_wrapper<T, F>(std::move(scoped), std::move(f));
};

template<class T>
auto make_scoped_wrapper(std::shared_ptr<scope_t> scope, std::function<void(std::future<T>)> f) -> scoped_wrapper<T> {
    return scoped_wrapper<T>(std::move(scope), std::move(f));
}

template<class T>
auto zookeeper_t::async_abort_callback(scoped_wrapper<T>& callback) -> void {
    auto eptr = std::current_exception();
    executor->spawn(std::bind([=](scoped_wrapper<T>& cb){
        cb.abort(std::move(eptr));
    }, std::move(callback)));
}

template<class Callback, class Method, class... Args>
auto zookeeper_t::run_command(Callback cb, Method method, Args&& ...args) -> api::unicorn_scope_ptr {
    auto scope = std::make_shared<scope_t>();
    auto wrapper = make_scoped_wrapper(scope, std::move(cb));
    try {
        (this->*method)(wrapper, std::forward<Args>(args)...);
    } catch (...) {
        async_abort_callback(wrapper);
    }
    return scope;
}
//template<class T>
//auto zookeeper_t::async_abort_callback(scoped_wrapper<T>& callback, std::error_code ec, std::string reason) {
//    executor->spawn(std::bind([](scoped_wrapper<T>& cb){
//        auto future = make_exceptional_future<T>(std::move(ec), std::move(reason));
//        cb(std::move(future));
//    }, std::move(callback)));
//}

//template<class F, class T>
//auto try_run(F runnable, scoped_wrapper<T>& callback, api::executor_t& executor) -> void {
//    try {
//        runnable();
//    } catch (...) {
//        async_abort_callback(callback);
//    }
//}

typedef api::unicorn_scope_ptr scope_ptr;
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
    return run_command(std::move(callback), &zookeeper_t::put_impl, path, value, version);
}

auto zookeeper_t::get(callback::get callback, const path_t& path) -> scope_ptr {
    return run_command(std::move(callback), &zookeeper_t::get_impl, path);
}

auto zookeeper_t::create(callback::create callback, const path_t& path, const value_t& value,
                         bool ephemeral, bool sequence) -> scope_ptr
{
    return run_command(std::move(callback), &zookeeper_t::create_impl, path, value, ephemeral, sequence);
}

auto zookeeper_t::del(callback::del callback, const path_t& path, version_t version) -> scope_ptr {
    return run_command(std::move(callback), &zookeeper_t::del_impl, path, version);
}

auto zookeeper_t::subscribe(callback::subscribe callback, const path_t& path) -> scope_ptr {
    return run_command(std::move(callback), &zookeeper_t::subscribe_impl, path);
}

auto zookeeper_t::children_subscribe(callback::children_subscribe callback, const path_t& path) -> scope_ptr {
    return run_command(std::move(callback), &zookeeper_t::children_subscribe_impl, path);
}

auto zookeeper_t::increment(callback::increment callback, const path_t& path, const value_t& value) -> scope_ptr {
    return run_command(std::move(callback), &zookeeper_t::increment_impl, path, value);
}

auto zookeeper_t::lock(callback::lock callback, const path_t& path) -> scope_ptr {
    return run_command(std::move(callback), &zookeeper_t::lock_impl, path);
}

auto zookeeper_t::put_impl(scoped_wrapper<response::put> wrapper, const path_t& path, const value_t& value, version_t version)
    -> void
{

    if (version < 0) {
        throw error_t(error::version_not_allowed, "version should be positive");
    }

    auto on_get = make_safe_wrapper(wrapper, [=](int rc, zookeeper::value_t value, zookeeper::node_stat const& stat) mutable {
        if (rc) {
            throw error_t(map_error(rc), "failure during getting new node value");
        } else {
            wrapper.satisfy(std::make_tuple(false, versioned_value_t(unserialize(value), stat.version)));
        }
    });

    auto on_put = make_safe_wrapper(wrapper, [=](int rc, const zookeeper::node_stat& stat) mutable {
        if(rc == ZBADVERSION) {
                //zk.get(path, );
        } else if(rc != 0) {
            throw error_t(map_error(rc), "failure during writing node value");
        } else {
            wrapper.satisfy(std::make_tuple(true, versioned_value_t(value, stat.version)));
        }
    });

    zk.put(path, serialize(value), version, std::move(on_put));
}

auto zookeeper_t::get_impl(scoped_wrapper<response::get> wrapper, const path_t& path) -> void {
    auto on_get = make_safe_wrapper(wrapper, [=](int rc, zookeeper::value_t value, const zookeeper::node_stat& stat) mutable {
        if (rc != 0) {
            throw error_t(map_error(rc), "failure during getting node value");
        } else if (stat.numChildren != 0) {
            throw error_t(cocaine::error::child_not_allowed, "trying to read value of the node with childs");
        } else {
            version_t new_version(stat.version);
            wrapper.satisfy(versioned_value_t(unserialize(value), new_version));
        }
    });
    zk.get(path, std::move(on_get));
}

struct on_create_t {
    using wrapper_t = scoped_wrapper<zookeeper_t::response::create>;
    struct data_t {
        size_t depth;
        wrapper_t wrapper;
        value_t value;
        bool ephemeral;
        bool sequence;
        path_t path;
        zookeeper::connection_t& zk;
    };
    std::shared_ptr<data_t> d;

    on_create_t(wrapper_t wrapper, value_t value, bool ephemeral, bool sequence, path_t path, zookeeper::connection_t& zk) :
        d(std::make_shared<data_t>(data_t{0, std::move(wrapper), std::move(value), ephemeral, sequence, std::move(path), zk}))
    {}

    auto operator()(int rc, zookeeper::path_t) -> void {
        if(rc == ZOK) {
            if(d->depth == 0) {
                d->wrapper.satisfy(true);
            } else if(d->depth == 1) {
                d->depth--;
                d->zk.create(d->path, serialize(d->value), d->ephemeral, d->sequence, *this);
            } else {
                d->depth--;
                d->zk.create(zookeeper::path_parent(d->path, d->depth), "", false, false, *this);
            }
        } else if(rc == ZNONODE) {
            d->depth++;
            d->zk.create(zookeeper::path_parent(d->path, d->depth), "", false, false, *this);
        } else {
            throw error_t(map_error(rc), "failure during creating node");
        }
    }
};

auto zookeeper_t::create_impl(scoped_wrapper<response::create> wrapper, const path_t& path, const value_t& value,
                              bool ephemeral, bool sequence) -> void
{
    auto on_create = make_safe_wrapper(wrapper, on_create_t(wrapper, value, ephemeral, sequence, path, zk));
    zk.create(path, serialize(value), ephemeral, sequence, std::move(on_create));
}

auto zookeeper_t::del_impl(scoped_wrapper<response::del> wrapper, const path_t& path, version_t version) -> void {
    auto on_del = make_safe_wrapper(wrapper, [=](int rc) mutable {
        if (rc != 0) {
            throw error_t(map_error(rc), "failure during deleting node");
        } else {
            wrapper.satisfy(true);
        }
    });
    zk.del(path, version, std::move(on_del));
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

}}
