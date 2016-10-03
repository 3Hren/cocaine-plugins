#pragma once

#include <cstddef>
#include <string>
#include <system_error>

namespace cocaine {
namespace error {

enum postgres_errors {
    unknown_pg_error = 1
};

struct postgres_category_t : public std::error_category {
    constexpr static auto id() -> std::size_t {
        return 0x40BC;
    }

    auto name() const noexcept -> const char* {
        return "postgres category";
    }

    auto message(int ec) const noexcept -> std::string {
        switch (ec) {
            case unknown_pg_error:
                return "postgres error";
            default:
                return std::string(name()) + ": " + std::to_string(ec);
        }
    }
};

auto make_error_code(postgres_errors err) -> std::error_code;

}  // namespace error
}  // namespace cocaine

namespace std {

/// Extends the type trait std::is_error_code_enum to identify `postgres_errors` error codes.
template<>
struct is_error_code_enum<cocaine::error::postgres_errors> : public true_type {};

}  // namespace std
