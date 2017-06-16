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

#include "cocaine/zookeeper.hpp"
#include "cocaine/traits/unicorn.hpp"

#include <cocaine/dynamic.hpp>
#include <cocaine/errors.hpp>

namespace cocaine {
namespace zookeeper {

path_t
path_parent(const path_t& path, unsigned int depth) {
    if(depth == 0) {
        return path;
    }
    size_t last_char = path.size();
    size_t pos = std::string::npos;
    for (size_t i = 0; i < depth; i++) {
        pos = path.find_last_of('/', last_char);
        if (pos == path.size() - 1) {
            last_char = pos - 1;
            pos = path.find_last_of('/', last_char);
        }
        if (pos == std::string::npos || pos == 0) {
            throw std::runtime_error("could not get " + std::to_string(depth) + " level parent from path: " + path);
        }
        last_char = pos - 1;
    }
    return path.substr(0, pos);
}

bool is_valid_sequence_node(const path_t& path) {
    return !path.empty() && isdigit(static_cast<unsigned char>(path[path.size()-1]));
}

unsigned long
get_sequence_from_node_name_or_path(const path_t& path) {
    if(!is_valid_sequence_node(path)) {
        throw std::system_error(cocaine::error::invalid_node_name);
    }
    auto pos = path.size()-1;
    unsigned char ch = static_cast<unsigned char>(path[pos]);
    while(isdigit(ch)) {
        pos--;
        ch =  static_cast<unsigned char>(path[pos]);
    }
    pos++;
    return std::stoul(path.substr(pos));
}

std::string
get_node_name(const path_t& path) {
    auto pos = path.find_last_of('/');
    if(pos == std::string::npos || pos == path.size()-1) {
        throw std::system_error(cocaine::error::invalid_path);
    }
    return path.substr(pos+1);
}

auto serialize(const unicorn::value_t& val) -> std::string {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(buffer);
    cocaine::io::type_traits<cocaine::dynamic_t>::pack(packer, val);
    return std::string(buffer.data(), buffer.size());
}

auto unserialize(const std::string& val) -> unicorn::value_t {
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
    unicorn::value_t target;
    cocaine::io::type_traits<cocaine::dynamic_t>::unpack(obj, target);
    return target;
}

auto map_zoo_error(int rc) -> std::error_code {
    if(rc == ZCONNECTIONLOSS || rc == ZSESSIONEXPIRED || rc == ZCLOSING) {
        return make_error_code(error::unicorn_errors::connection_loss);
    }
    if(rc == ZNONODE) {
        return make_error_code(error::unicorn_errors::no_node);
    }
    if(rc == ZNODEEXISTS) {
        return make_error_code(error::unicorn_errors::node_exists);
    }
    if(rc == ZBADVERSION) {
        return make_error_code(error::unicorn_errors::version_not_allowed);
    }
    return make_error_code(error::unicorn_errors::backend_internal_error);
}

auto event_to_string(int event) -> std::string {
    if(event == ZOO_CREATED_EVENT) {
        return "created";
    } else if(event == ZOO_DELETED_EVENT) {
        return "deleted";
    } else if(event == ZOO_CHANGED_EVENT) {
        return "changed";
    } else if(event == ZOO_CHILD_EVENT) {
        return "child";
    } else if(event == ZOO_SESSION_EVENT) {
        return "session";
    } else if(event == ZOO_NOTWATCHING_EVENT) {
        return "not watching";
    }
    return "unknown";
}

auto state_to_string(int state) -> std::string {
    if(state == ZOO_EXPIRED_SESSION_STATE) {
        return "expired session";
    } else if(state == ZOO_AUTH_FAILED_STATE) {
        return "auth failed";
    } else if(state == ZOO_CONNECTING_STATE) {
        return "connecting";
    } else if(state == ZOO_ASSOCIATING_STATE) {
        return "associating";
    } else if(state == ZOO_CONNECTED_STATE) {
        return "connected";
    }
    return "unknown";
}


} // namespace zookeeper
} // namespace cocaine
