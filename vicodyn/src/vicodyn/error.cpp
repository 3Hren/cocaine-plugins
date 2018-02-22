#include "cocaine/vicodyn/error.hpp"

#include <cocaine/format.hpp>

namespace {
using namespace cocaine;
using namespace cocaine::vicodyn;

class vicodyn_category_t : public std::error_category {
public:
    auto name() const noexcept -> const char * {
        return "vicodyn category";
    }

    auto message(int code) const -> std::string {
        switch (static_cast<vicodyn_errors>(code)) {
        case vicodyn_errors::upstream_disconnected:
            return "vicodyn upstream has been disconnected";
        case vicodyn_errors::failed_to_retry_enqueue:
            return "vicodyn failed to retry enqueue";
        case vicodyn_errors::failed_to_send_error_to_forward:
            return "failed to send error to forward dispatch";
        default:
            return format("{}: {}", name(), code);
        }
    }
};

} // namespace

namespace cocaine {
namespace vicodyn {

auto vicodyn_category() -> const std::error_category & {
    static ::vicodyn_category_t category;
    return category;
}

auto make_error_code(vicodyn_errors err) -> std::error_code {
    return std::error_code(static_cast<int>(err), vicodyn_category());
}

} // namespace vicodyn
} // namespace cocaine
