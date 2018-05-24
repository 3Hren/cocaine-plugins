#pragma once

#include <string>

#include <cocaine/api/service.hpp>
#include <cocaine/dynamic.hpp>
#include <cocaine/rpc/dispatch.hpp>

#include "cocaine/idl/uniresis.hpp"
#include "cocaine/uniresis/resources.hpp"

namespace cocaine {
namespace service {

class uniresis_t : public api::service_t, public dispatch<io::uniresis_tag> {
    class updater_t;

    std::string uuid;
    uniresis::resources_t resources;
    std::unique_ptr<logging::logger_t> log;
    std::unique_ptr<updater_t> updater;
    dynamic_t::object_t extra;
public:
    uniresis_t(context_t& context, asio::io_service& loop, const std::string& name, const dynamic_t& args);

    auto
    prototype() -> io::basic_dispatch_t& {
        return *this;
    }
};

} // namespace uniresis
} // namespace cocaine
