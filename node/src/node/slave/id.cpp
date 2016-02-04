#include "cocaine/service/node/slave/id.hpp"

#include <cocaine/unique_id.hpp>

namespace cocaine {
namespace service {
namespace node {
namespace slave {

id_t::id_t(std::string id) :
    id(std::move(id))
{}

auto id_t::generate() -> id_t {
    return id_t(unique_id_t().string());
}

auto id_t::get() const noexcept -> const std::string& {
    return id;
}

}  // namespace slave
}  // namespace node
}  // namespace service
}  // namespace cocaine
