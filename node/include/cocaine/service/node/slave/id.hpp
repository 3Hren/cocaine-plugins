#pragma once

#include <string>

namespace cocaine {
namespace service {
namespace node {
namespace slave {

class id_t {
    std::string id;

public:
    explicit id_t(std::string id);
    static auto generate() -> id_t;

    auto get() const noexcept -> const std::string&;
};

}  // namespace slave
}  // namespace node
}  // namespace service
}  // namespace cocaine
