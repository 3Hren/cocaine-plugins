/*
    Copyright (c) 2011-2015 Anton Matveenko <antmat@yandex-team.ru>
    Copyright (c) 2011-2015 Other contributors as noted in the AUTHORS file.

    This file is part of Cocaine.

    Cocaine is free software; you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    Cocaine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#include "cocaine/detail/zookeeper/errors.hpp"

#include <cocaine/errors.hpp>

#include <string>
#include <map>

namespace cocaine { namespace error {

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
    return make_error_code(error::unicorn_errors::backend_internal_error);
}

}}
