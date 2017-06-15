/*
    Copyright (c) 2015+ Anton Matveenko <antmat@yandex-team.ru>
    Copyright (c) 2015+ Other contributors as noted in the AUTHORS file.
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

#pragma once

#include "zookeeper/zookeeper.h"

#include <system_error>

namespace cocaine { namespace error {

auto map_zoo_error(int rc) -> std::error_code;

} // namespace error
} // namespace cocaine
