#pragma once

#include <cocaine/dynamic.hpp>
#include <cocaine/idl/primitive.hpp>

#include <map>
#include <string>

namespace cocaine {
namespace io {

struct metrics_tag;

struct metrics {

    struct fetch {
        typedef metrics_tag tag;

        constexpr static auto alias() noexcept -> const char* {
            return "fetch";
        }

        typedef option_of<
            dynamic_t
        >::tag upstream_type;
    };

};

template<>
struct protocol<metrics_tag> {
    typedef boost::mpl::int_<
        1
    >::type version;

    typedef boost::mpl::list<
        metrics::fetch
    >::type messages;
};

}  // namespace io
}  // namespace cocaine
