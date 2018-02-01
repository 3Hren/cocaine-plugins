#pragma once

#include <string>

#include <cocaine/api/service.hpp>
#include <cocaine/idl/context.hpp>
#include <cocaine/rpc/dispatch.hpp>
#include <cocaine/executor/asio.hpp>

#include "cocaine/idl/uniresis.hpp"
#include "cocaine/uniresis/resources.hpp"

namespace cocaine {
namespace service {

class uniresis_t : public api::service_t, public dispatch<io::uniresis_tag> {
    class updater_t;

    std::string uuid;
    uniresis::resources_t resources;
    std::shared_ptr<updater_t> updater;
    std::shared_ptr<logging::logger_t> log;

    // Note that executor `must die` before updater as it contains io loop for
    // updater's inner timer.
    std::shared_ptr<executor::owning_asio_t> executor;

    // Slot for context signals.
    std::shared_ptr<dispatch<io::context_tag>> signal;
public:
    uniresis_t(context_t& context, asio::io_service& loop, const std::string& name, const dynamic_t& args);

    auto
    prototype() -> io::basic_dispatch_t& {
        return *this;
    }
private:
    auto on_context_shutdown() -> void;
};

} // namespace uniresis
} // namespace cocaine
