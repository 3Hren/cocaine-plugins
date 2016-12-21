#include "cocaine/service/metrics.hpp"

#include <cocaine/context/config.hpp>
#include <cocaine/format.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/traits/dynamic.hpp>
#include <cocaine/traits/map.hpp>

#include <cocaine/postgres/pool.hpp>

#include <blackhole/logger.hpp>

#include "metrics/factory.hpp"
#include "metrics/filter/and.hpp"
#include "metrics/filter/contains.hpp"
#include "metrics/filter/eq.hpp"
#include "metrics/filter/or.hpp"
#include "metrics/visitor/dendroid.hpp"
#include "metrics/visitor/plain.hpp"

namespace cocaine {
namespace service {

metrics_t::metrics_t(context_t& context,
                     asio::io_service& asio,
                     const std::string& _name,
                     const dynamic_t& args) :
    api::service_t(context, asio, _name, args),
    dispatch<io::metrics_tag>(_name),
    hub(context.metrics_hub()),
    senders(),
    factory(std::make_shared<metrics::factory_t>())
{
    factory->add(std::make_shared<metrics::filter::eq_t>());
    factory->add(std::make_shared<metrics::filter::or_t>());
    factory->add(std::make_shared<metrics::filter::and_t>());
    factory->add(std::make_shared<metrics::filter::contains_t>());

    auto sender_names = args.as_object().at("senders", dynamic_t::empty_array).as_array();

    for(auto& sender_name: sender_names) {
        api::sender_t::data_provider_ptr provider(new api::sender_t::function_data_provider_t([=]() {
            return metrics({}, {});
        }));
        senders.push_back(api::sender(context, asio, sender_name.as_string(), std::move(provider)));
    }

    on<io::metrics::fetch>([&](const std::string& type, const dynamic_t& query) -> dynamic_t {
        return metrics(type, query);
    });
}

auto metrics_t::metrics(std::string type, const dynamic_t& query) const -> dynamic_t {
    if (type.empty()) {
        type = "plain";
    }

    libmetrics::query_t filter = [](const libmetrics::tags_t&) -> bool {
        return true;
    };

    if (!query.is_null()) {
        filter = factory->construct_query(query);
    }

    dynamic_t::object_t out;
    if (type == "json") {
        for (const auto& metric : hub.select(filter)) {
            metrics::dendroid_t visitor(metric->name(), out);
            metric->apply(visitor);
        }
    } else {
        for (const auto& metric : hub.select(filter)) {
            metrics::plain_t visitor(metric->name(), out);
            metric->apply(visitor);
        }
    }

    return out;
}

}  // namespace service
}  // namespace cocaine
