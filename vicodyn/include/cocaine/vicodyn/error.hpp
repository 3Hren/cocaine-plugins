#pragma once

#include <system_error>

namespace cocaine {
namespace vicodyn {

enum class vicodyn_errors {
    /// Upstream has been disconnected
    upstream_disconnected,

    /// Failed to retry enqueue
    failed_to_retry_enqueue,

    /// Failed to send error to forward dispatch
    failed_to_send_error_to_forward,
};

auto vicodyn_category() -> const std::error_category&;
auto make_error_code(vicodyn_errors err) -> std::error_code;
size_t constexpr vicodyn_category_id = 0x54FF;

} // namespace vicodyn
} // namespace cocaine

namespace std {

/// Extends the type trait `std::is_error_code_enum` to identify `vicodyn_errors` error codes.
template<>
struct is_error_code_enum<cocaine::vicodyn::vicodyn_errors> : public true_type {};

} // namespace std
