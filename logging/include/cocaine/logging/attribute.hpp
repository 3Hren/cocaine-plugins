/*
    Copyright (c) 2016 Anton Matveenko <antmat@yandex-team.ru>
    Copyright (c) 2016 Other contributors as noted in the AUTHORS file.

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

#include <cocaine/dynamic.hpp>

#include <string>

namespace cocaine { namespace logging {
struct attribute_t {
    typedef std::string name_t;
    typedef dynamic_t value_t;
    name_t name;
    value_t value;
};

typedef std::vector<attribute_t> attributes_t;

}} // namespace cocaine::logging
