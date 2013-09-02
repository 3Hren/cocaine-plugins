/*
    Copyright (c) 2011-2013 Andrey Sibiryov <me@kobology.ru>
    Copyright (c) 2011-2013 Other contributors as noted in the AUTHORS file.

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

#ifndef COCAINE_CGROUP_ISOLATE_HPP
#define COCAINE_CGROUP_ISOLATE_HPP

#include <cocaine/api/isolate.hpp>

struct cgroup;

namespace cocaine { namespace isolate {

class cgroups_t:
    public api::isolate_t
{
    public:
        typedef api::isolate_t category_type;

    public:
        cgroups_t(context_t& context, const std::string& name, const Json::Value& args);

        virtual
       ~cgroups_t();

        virtual
        std::unique_ptr<api::handle_t>
        spawn(const std::string& path, const api::string_map_t& args, const api::string_map_t& environment);

    private:
        std::unique_ptr<logging::log_t> m_log;
        const std::string m_working_directory;

        cgroup * m_cgroup;
};

}} // namespace cocaine::storage

#endif
