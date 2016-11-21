#pragma once

#include <cocaine/context.hpp>
#include <cocaine/hpack/header.hpp>

#include "cocaine/api/auth.hpp"

namespace cocaine {
namespace middleware {

class auth_t {
    std::shared_ptr<api::auth_t> auth;
    std::string service;

public:
    explicit
    auth_t(context_t& context, std::string service) :
        auth(api::auth(context, "core")),
        service(std::move(service))
    {}

    template<typename Event, typename F, typename... Args>
    auto
    operator()(F& fn, Event, const std::vector<hpack::header_t>& meta, Args&&... args) ->
        decltype(fn(meta, std::forward<Args>(args)...))
    {
        std::string authorization;
        if (auto header = hpack::header::find_first(meta, "authorization")) {
            authorization = header.get().value();
        }

        const auto result = auth->check_permissions(service, Event::alias(), {});

        if (auto ec = boost::get<std::error_code>(&result)) {
            throw std::system_error(*ec, "permission denied");
        }

        return fn(meta, std::forward<Args>(args)...);
    }
};

}  // namespace middleware
}  // namespace cocaine
