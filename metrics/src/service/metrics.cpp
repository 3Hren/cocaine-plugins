#include "cocaine/service/metrics.hpp"

#include <cocaine/context/config.hpp>
#include <cocaine/format.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/traits/dynamic.hpp>
#include <cocaine/traits/map.hpp>

#include <cocaine/postgres/pool.hpp>

#include <blackhole/logger.hpp>
#include <metrics/accumulator/sliding/window.hpp>
#include <metrics/accumulator/snapshot/uniform.hpp>
#include <metrics/meter.hpp>
#include <metrics/timer.hpp>
#include <pqxx/pqxx>


namespace cocaine {
namespace service {

metrics_t::metrics_t(context_t& context,
                     asio::io_service& asio,
                     const std::string& name,
                     const dynamic_t& args) :
    api::service_t(context, asio, name, args),
    dispatch<io::metrics_tag>(name),
    hub(context.metrics_hub()),
    sender()
{
    auto sender_name = args.as_object().at("sender", "core").as_string();

    api::sender_t::data_provider_ptr provider(new api::sender_t::function_data_provider_t([=](){
        return metrics();
    }));
    sender = api::sender(context, asio, sender_name, std::move(provider));

    on<io::metrics::fetch>([&]() -> dynamic_t {
        return metrics();
    });
}

auto metrics_t::metrics() const -> dynamic_t {
    dynamic_t::object_t result;

    for (const auto& item : hub.counters<std::int64_t>()) {
        const auto& name = std::get<0>(item).name();
        const auto& counter = std::get<1>(item);

        result[name] = counter.get()->load();
    }

    for (const auto& item : hub.meters()) {
        const auto& name = std::get<0>(item).name();
        const auto& meter = std::get<1>(item);

        result[name + ".count"] = meter.get()->count();
        result[name + ".m01rate"] = meter.get()->m01rate();
        result[name + ".m05rate"] = meter.get()->m05rate();
        result[name + ".m15rate"] = meter.get()->m15rate();
    }

    for (const auto& item : hub.timers()) {
        const auto& name = std::get<0>(item).name();
        const auto& timer = std::get<1>(item);

        result[name + ".count"] = timer.get()->count();
        result[name + ".m01rate"] = timer.get()->m01rate();
        result[name + ".m05rate"] = timer.get()->m05rate();
        result[name + ".m15rate"] = timer.get()->m15rate();

        const auto snapshot = timer->snapshot();

        result[name + ".mean"] = snapshot.mean() / 1e6;
        result[name + ".stddev"] = snapshot.stddev() / 1e6;
        result[name + ".p50"] = snapshot.median() / 1e6;
        result[name + ".p75"] = snapshot.p75() / 1e6;
        result[name + ".p90"] = snapshot.p90() / 1e6;
        result[name + ".p95"] = snapshot.p95() / 1e6;
        result[name + ".p98"] = snapshot.p98() / 1e6;
        result[name + ".p99"] = snapshot.p99() / 1e6;
    }

    return result;
}

}  // namespace service
}  // namespace cocaine
