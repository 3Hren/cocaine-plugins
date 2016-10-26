#pragma once

#include "cocaine/api/sender.hpp"

#include <metrics/tags.hpp>
#include <metrics/registry.hpp>

#include <cocaine/api/service.hpp>
#include <cocaine/context.hpp>
#include <cocaine/idl/metrics.hpp>
#include <cocaine/rpc/dispatch.hpp>

namespace cocaine {
namespace service {

class metrics_t : public api::service_t, public dispatch<io::metrics_tag> {
public:
    metrics_t(context_t& context,
              asio::io_service& asio,
              const std::string& name,
              const dynamic_t& args);

    auto prototype() const -> const io::basic_dispatch_t& {
        return *this;
    }

    /// Returns metrics dump.
    auto metrics() const -> dynamic_t;

private:
    metrics::registry_t& hub;
    api::sender_ptr sender;
};

}  // namespace service
}  // namespace cocaine
