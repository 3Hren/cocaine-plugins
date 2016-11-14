/*
    Copyright (c) 2016+ Anton Matveenko <antmat@me.com>
    Copyright (c) 2016+ Other contributors as noted in the AUTHORS file.

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
#include <cocaine/idl/primitive.hpp>

#include <map>
#include <string>

namespace cocaine {
namespace io {

struct vicodyn_tag;

struct vicodyn {
    struct nop {
        typedef vicodyn_tag tag;

        constexpr static auto alias() noexcept -> const char* {
            return "nop";
        }
    };
};

template<>
struct protocol<vicodyn_tag> {
    typedef boost::mpl::int_<
        1
    >::type version;

    typedef boost::mpl::list<
        vicodyn::nop
    >::type messages;
};

}  // namespace io
}  // namespace cocaine
