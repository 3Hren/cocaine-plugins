/*
    Copyright (c) 2011-2012 Andrey Sibiryov <me@kobology.ru>
    Copyright (c) 2011-2012 Other contributors as noted in the AUTHORS file.

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

#ifndef COCAINE_LOGGING_SERVICE_HPP
#define COCAINE_LOGGING_SERVICE_HPP

#include <cocaine/api/service.hpp>
#include <cocaine/asio/reactor.hpp>
#include <swarm/networkmanager.h>

namespace cocaine {

namespace service {

typedef ioremap::swarm::network_request network_request_t;
typedef ioremap::swarm::network_reply network_reply_t;

} // namespace service

namespace io {
    namespace tags {
        struct urlfetch_tag;
    }

    namespace urlfetch {
        struct get {
            typedef tags::urlfetch_tag tag;

            typedef boost::mpl::list<

                /* request */   service::network_request_t
            > tuple_type;
        };
    }

    template<>
    struct type_traits<service::network_request_t>
    {
        static inline
        void
        unpack(const msgpack::object& object,
               service::network_request_t& target)
        {
            if(object.type != msgpack::type::ARRAY ||
               object.via.array.size != 3)
            {
                throw msgpack::type_error();
            }

            object.via.array.ptr[0] >> target.url;
            object.via.array.ptr[1] >> target.headers;
            object.via.array.ptr[2] >> target.follow_location;
        }
    };

    template<>
    struct type_traits<service::network_reply_t>
    {
        template<class Stream>
        static inline
        void
        pack(msgpack::packer<Stream>& packer,
             const service::network_reply_t& source)
        {
            packer.pack_array(5);

            packer << source.url;
            packer << source.headers;
            packer << source.code;
            packer << source.error;
            packer << source.data;
        }
    };

    template<>
    struct protocol<tags::urlfetch_tag> {
        typedef mpl::list<
            urlfetch::get
        > type;
    };
} // namespace io

namespace service {

class urlfetch_t:
    public api::service_t
{
    public:
        urlfetch_t(context_t& context,
                   cocaine::io::reactor_t &reactor,
                   const std::string& name,
                   const Json::Value& args);

    private:
        deferred<network_reply_t> get(const network_request_t &request);

    private:
        context_t& m_context;
        ioremap::swarm::network_manager m_manager;
};

} // namespace service

} // namespace cocaine

#endif
