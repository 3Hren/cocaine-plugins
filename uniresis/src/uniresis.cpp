#include <cassert>
#include <future>
#include <utility>

#include <unistd.h>

#include "cocaine/service/uniresis.hpp"

#include <asio/io_service.hpp>
#include <asio/ip/tcp.hpp>

#include <blackhole/logger.hpp>

#include <cocaine/api/unicorn.hpp>
#include <cocaine/context.hpp>
#include <cocaine/context/signal.hpp>
#include <cocaine/context/config.hpp>
#include <cocaine/dynamic.hpp>
#include <cocaine/executor/asio.hpp>
#include <cocaine/unicorn/value.hpp>
#include <cocaine/unique_id.hpp>

#include <cocaine/traits/dynamic.hpp>
#include <cocaine/traits/endpoint.hpp>
#include <cocaine/traits/vector.hpp>

#include "cocaine/uniresis/error.hpp"


namespace cocaine {
namespace service {

using namespace asio::ip;

namespace {
namespace defaults {

const std::string locator_name = "locator";
const std::string unicorn_name = "core";
const std::string resources_path = "/resources";

} // namespace defaults
} // namespace

namespace {

namespace ph = std::placeholders;

auto resolve(const std::string& hostname) -> std::vector<tcp::endpoint> {
    std::vector<tcp::endpoint> endpoints;

    const tcp::resolver::query::flags flags =
        tcp::resolver::query::address_configured;

    asio::io_service loop;
    tcp::resolver::iterator begin = tcp::resolver(loop).resolve(
        tcp::resolver::query(hostname, "", flags)
    );

    std::transform(
        begin,
        tcp::resolver::iterator(),
        std::back_inserter(endpoints),
        std::bind(&tcp::resolver::iterator::value_type::endpoint, ph::_1)
    );

    return endpoints;
}

} // namespace

/// A task that will try to notify about resources information on the machine.
class uniresis_t::updater_t : public std::enable_shared_from_this<uniresis_t::updater_t> {
    std::string path;
    std::string hostname;
    std::vector<tcp::endpoint> endpoints;
    dynamic_t::object_t extra;
    uniresis::resources_t resources;
    std::shared_ptr<api::unicorn_t> unicorn;
    api::unicorn_scope_ptr scope;
    api::unicorn_scope_ptr subscope;
    executor::owning_asio_t executor;
    asio::deadline_timer timer;
    std::shared_ptr<logging::logger_t> log;

public:
    updater_t(std::string path,
              std::string hostname,
              std::vector<tcp::endpoint> endpoints,
              dynamic_t::object_t extra,
              uniresis::resources_t resources,
              std::shared_ptr<api::unicorn_t> unicorn,
              std::shared_ptr<logging::logger_t> log) :
        path(std::move(path)),
        hostname(std::move(hostname)),
        endpoints(std::move(endpoints)),
        extra(std::move(extra)),
        resources(std::move(resources)),
        unicorn(std::move(unicorn)),
        scope(),
        subscope(),
        executor(),
        timer(executor.asio()),
        log(std::move(log))
    {}

    ~updater_t() {
        std::cerr << "~updater_t" << "\n";
    }

    auto
    notify() -> void {
        COCAINE_LOG_DEBUG(log, "schedule resource notification on `{}` ...", path);
        std::cerr << "notify\n";
        scope = unicorn->create(
            std::bind(&weak_from_self_t::on_create, self_as_weak(), ph::_1),
            // std::bind(&updater_t::on_create, shared_from_this(), ph::_1),
            path,
            make_value(),
            true,
            false
        );
    }

    auto
    closes_scopes() -> void {
        timer.cancel();
        subscope.reset();
        scope.reset();
    }

private:
    using self_type = updater_t;

    //
    // Unicorn holds callback pointer even after callback completion and
    // scope close, so as a temporary workaround wrapper struct for weak_ptr
    // introduced.
    //
    struct weak_from_self_t {
        std::weak_ptr<self_type> weak_self;

        weak_from_self_t(std::shared_ptr<self_type> self_shared) :
            weak_self(std::move(self_shared))
        {}

        auto
        on_create(std::future<bool> future) -> void {
            if (const auto strong = weak_self.lock()) {
                strong->on_create(std::move(future));
            }
        }

        auto
        on_subscribe(std::future<unicorn::versioned_value_t> future) -> void {
            if (const auto strong = weak_self.lock()) {
                strong->on_subscribe(std::move(future));
            }
        }
    };

    auto self_as_weak() -> weak_from_self_t {
        return weak_from_self_t(shared_from_this());
    }

    auto
    make_value() const -> dynamic_t {
        dynamic_t::object_t result;

        std::vector<dynamic_t> endpoints;
        for (auto& endpoint : this->endpoints) {
            endpoints.push_back({
                std::vector<dynamic_t>{{endpoint.address().to_string()}, {endpoint.port()}}
            });
        }
        result["hostname"] = hostname;
        result["endpoints"] = endpoints;
        result["extra"] = extra;

        result["resources"].as_object()["cpu"] = resources.cpu;
        result["resources"].as_object()["mem"] = resources.mem;

        return result;
    }

    auto
    notify_later() -> void {
        std::cerr << "notify later\n";

        auto self = shared_from_this();
        COCAINE_LOG_DEBUG(log, "schedule resource notification after {} sec ...", 1);
        timer.expires_from_now(boost::posix_time::seconds(1));
        timer.async_wait([&, self](std::error_code ec) {
            std::cerr << "async timer\n";
            if (ec) {
                std::cerr << "canceled\n";
                return;
            }

            notify();
        });
    }

    auto
    on_create(std::future<bool> future) -> void {
        try {
            auto created = future.get();

            if (created) {
                std::cerr << "on_create::created\n";
                COCAINE_LOG_INFO(log, "registered machine's resources on `{}` path", path);
                subscribe();
            } else {
                std::cerr << "on_create::failed\n";
                COCAINE_LOG_ERROR(log, "failed to create `{}` node: already exists", path);
                notify_later();
            }
        } catch (const std::system_error& err) {
            COCAINE_LOG_ERROR(log, "failed to create `{}` node: {}", path, error::to_string(err));
            notify_later();
        } catch (const std::exception& err) {
            COCAINE_LOG_ERROR(log, "failed to create `{}` node: {}", path, err.what());
            notify_later();
        }
    }

    auto
    subscribe() -> void {
        std::cerr << "subscribe\n";

        COCAINE_LOG_DEBUG(log, "schedule resource node subscription on `{}` ...", path);
        scope = unicorn->subscribe(
            std::bind(&weak_from_self_t::on_subscribe, self_as_weak(), ph::_1),
            // std::bind(&updater_t::on_subscribe, shared_from_this(), ph::_1),
            path
        );
    }

    auto
    on_subscribe(std::future<unicorn::versioned_value_t> future) -> void {
        COCAINE_LOG_DEBUG(log, "received node update on `{}` path", path);

        std::cerr << "on_subscribe\n";

        try {
            std::cerr << "waiting...\n";
            auto value = future.get();
            std::cerr << "got it\n";

            // sleep(2);
            // throw std::runtime_error("boo");

            if (value.version() == 0) {
                std::cerr << "zero version\n";
                return;
            }

            COCAINE_LOG_WARNING(log, "received node update on `{}`, but it shouldn't", path);
        } catch (const std::exception& err) {
            COCAINE_LOG_ERROR(log, "failed to hold subscription on `{}` node: {}", path, err.what());
            notify();
        }
    }
};

uniresis_t::uniresis_t(context_t& context, asio::io_service& loop, const std::string& name, const dynamic_t& args) :
    api::service_t(context, loop, name, args),
    dispatch<io::uniresis_tag>(name),
    uuid(context.uuid()),
    resources(),
    updater(nullptr),
    log(context.log("uniresis"))
{
    if (resources.cpu == 0) {
        throw std::system_error(uniresis::uniresis_errc::failed_calculate_cpu_count);
    }

    if (resources.mem == 0) {
        throw std::system_error(uniresis::uniresis_errc::failed_calculate_system_memory);
    }

    auto restrictions = args.as_object().at("restrictions", dynamic_t::empty_object).as_object();

    auto cpu_restricted = std::min(
        resources.cpu,
        static_cast<uint>(restrictions.at("cpu", resources.cpu).as_uint())
    );

    if (resources.cpu != cpu_restricted) {
        resources.cpu = cpu_restricted;
        COCAINE_LOG_INFO(log, "restricted available CPU count to {}", resources.cpu);
    }

    auto mem_restricted = std::min(
        resources.mem,
        static_cast<std::uint64_t>(restrictions.at("mem", resources.mem).as_uint())
    );

    if (resources.mem != mem_restricted) {
        resources.mem = mem_restricted;
        COCAINE_LOG_INFO(log, "restricted available system memory to {}", resources.mem);
    }

    auto prefix = args.as_object().at("prefix", defaults::resources_path).as_string();
    auto path = format("{}/{}", prefix, uuid);

    auto hostname = context.config().network().hostname();
    auto endpoints = resolve(hostname);
    dynamic_t::object_t extra;
    if (auto locator = context.config().services().get("locator")) {
        extra = dynamic_converter<dynamic_t::object_t>::convert(
            locator->args().as_object().at("extra_param", dynamic_t::empty_object)
        );
    }
    auto unicorn = api::unicorn(context, args.as_object().at("unicorn", defaults::unicorn_name).as_string());
    updater = std::make_shared<updater_t>(
        std::move(path),
        std::move(hostname),
        std::move(endpoints),
        std::move(extra),
        resources,
        std::move(unicorn),
        log
    );
    updater->notify();

    on<io::uniresis::cpu_count>([&] {
        return resources.cpu;
    });

    on<io::uniresis::memory_count>([&] {
        return resources.mem;
    });

    on<io::uniresis::uuid>([&] {
        return uuid;
    });

    // Context signal/slot.
    signal = std::make_shared<dispatch<io::context_tag>>(name);
    signal->on<io::context::shutdown>(std::bind(&uniresis_t::on_context_shutdown, this));

    context.signal_hub().listen(signal, loop);
    std::cerr << "updater use count(3): " << updater.use_count() << '\n';
}

auto uniresis_t::on_context_shutdown() -> void {
    assert(updater);

    updater->closes_scopes();

    std::cerr
        << "uniresis_t::on_context_shutdown(2), updater use_count: "
        << updater.use_count() << '\n';

    updater.reset();

    signal = nullptr;
}

} // namespace service
} // namespace cocaine
