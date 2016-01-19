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

#include "cocaine/detail/unicorn/zookeeper/subscribe.hpp"

#include <cocaine/logging.hpp>

#include "cocaine/unicorn/errors.hpp"

namespace cocaine { namespace unicorn {

subscribe_action_t::subscribe_action_t(const zookeeper::handler_tag& tag,
                                       api::unicorn_t::writable_ptr::subscribe _result,
                                       const zookeeper_t::context_t& _ctx,
                                       path_t _path
) : managed_handler_base_t(tag),
    managed_data_handler_base_t(tag),
    managed_watch_handler_base_t(tag),
    managed_stat_handler_base_t(tag),
    result(std::move(_result)),
    ctx(_ctx),
    write_lock(),
    last_version(unicorn::MIN_VERSION),
    path(std::move(_path))
{}

void
subscribe_action_t::data_event(int rc, std::string value, const zookeeper::node_stat& stat) {
    if(rc == ZNONODE) {
        if(last_version != MIN_VERSION && last_version != NOT_EXISTING_VERSION) {
            auto code = cocaine::error::make_error_code(static_cast<cocaine::error::zookeeper_errors>(rc));
            result->abort(code);
        } else {
            // Write that node is not exist to client only first time.
            // After that set a watch to see when it will appear
            if(last_version == MIN_VERSION) {
                std::lock_guard<std::mutex> guard(write_lock);
                if (NOT_EXISTING_VERSION > last_version) {
                    result->write(versioned_value_t(value_t(), NOT_EXISTING_VERSION));
                }
            }
            try {
                ctx.zk.exists(path, *this, *this);
            } catch(const std::system_error& e) {
                COCAINE_LOG_WARNING(ctx.log, "failure during subscription(get): {}", e.what());
                result->abort(e.code());
            }
        }
    } else if (rc != 0) {
        auto code = cocaine::error::make_error_code(static_cast<cocaine::error::zookeeper_errors>(rc));
        result->abort(code);
    } else if (stat.numChildren != 0) {
        result->abort(cocaine::error::CHILD_NOT_ALLOWED);
    } else {
        version_t new_version(stat.version);
        std::lock_guard<std::mutex> guard(write_lock);
        if (new_version > last_version) {
            last_version = new_version;
            value_t val;
            try {
                result->write(versioned_value_t(unserialize(value), new_version));
            } catch(const std::system_error& e) {
                result->abort(e.code());
            }
        }
    }
}

void
subscribe_action_t::stat_event(int rc, zookeeper::node_stat const&) {
    // Someone created a node in a gap between
    // we received nonode and issued exists
    if(rc == ZOK) {
        try {
            ctx.zk.get(path, *this, *this);
        } catch(const std::system_error& e)  {
            COCAINE_LOG_WARNING(ctx.log, "failure during subscription(stat): {}", e.what());
            result->abort(e.code());
        }
    }
}

void
subscribe_action_t::watch_event(int /* type */, int /* state */, zookeeper::path_t) {
    try {
        ctx.zk.get(path, *this, *this);
    } catch(const std::system_error& e)  {
        result->abort(e.code());
        COCAINE_LOG_WARNING(ctx.log, "failure during subscription(watch): {}", e.what());
    }
}

}} // namespace cocaine::unicorn
