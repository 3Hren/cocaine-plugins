#pragma once

#include <string>

#include <boost/optional/optional.hpp>

namespace cocaine {
namespace detail {
namespace service {
namespace node {

class splitter_t {
public:
    std::string unparsed;

    auto consume(const std::string& data) -> void {
        unparsed.append(data);
    }

    auto next() -> boost::optional<std::string> {
        auto pos = unparsed.find('\n');
        if (pos == std::string::npos) {
            return boost::none;
        }

        auto line = unparsed.substr(0, pos);
        unparsed.erase(0, pos + 1);
        return boost::make_optional(line);
    }
};

}  // namespace node
}  // namespace service
}  // namespace detail
}  // namespace cocaine
