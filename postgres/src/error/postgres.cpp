#include "cocaine/error/postgres.hpp"

namespace cocaine {
namespace error {

auto
postgres_category() -> const std::error_category& {
    static postgres_category_t instance;
    return instance;
}

auto make_error_code(postgres_errors err) -> std::error_code {
    return std::error_code(static_cast<int>(err), postgres_category());
}

} // namespace error
} // namespace cocaine