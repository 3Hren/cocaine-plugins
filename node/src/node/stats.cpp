#include "cocaine/detail/service/node/stats.hpp"

#include <cmath>

#include <cocaine/context.hpp>
#include <cocaine/format.hpp>

#include <metrics/factory.hpp>
#include <metrics/registry.hpp>

namespace cocaine {

stats_t::stats_t(context_t& context, const std::string& name, std::chrono::high_resolution_clock::duration interval):
    name{name},
    metrics_hub{context.metrics_hub()},
    requests{
        metrics_hub.counter<std::int64_t>(cocaine::format("{}.requests.accepted", name)),
        metrics_hub.counter<std::int64_t>(cocaine::format("{}.requests.rejected", name))
    },
    slaves{
        metrics_hub.counter<std::int64_t>(cocaine::format("{}.slaves.spawned", name)),
        metrics_hub.counter<std::int64_t>(cocaine::format("{}.slaves.crashed", name))
    },
    meter(metrics_hub.meter(cocaine::format("{}.rate", name))),
    queue_depth(std::make_shared<metrics::usts::ewma_t>(interval)),
    queue_depth_gauge(metrics_hub
        .register_gauge<double>(
            cocaine::format("{}.queue.depth_average", name),
            {},
            std::bind(&metrics::usts::ewma_t::get, queue_depth)
        )
    ),
    timer(metrics_hub.timer<metrics::accumulator::decaying::exponentially_t>(cocaine::format("{}.timings", name)))
{
    queue_depth->add(0);
}

stats_t::~stats_t() {
    metrics_hub.remove<std::atomic<std::int64_t>>(cocaine::format("{}.requests.accepted", name), {});
    metrics_hub.remove<std::atomic<std::int64_t>>(cocaine::format("{}.requests.rejected", name), {});
    metrics_hub.remove<std::atomic<std::int64_t>>(cocaine::format("{}.slaves.spawned", name), {});
    metrics_hub.remove<std::atomic<std::int64_t>>(cocaine::format("{}.slaves.crashed", name), {});
    metrics_hub.remove<metrics::meter_t>(cocaine::format("{}.rate", name), {});
    metrics_hub.remove<metrics::gauge<double>>(cocaine::format("{}.queue.depth_average", name), {});
    metrics_hub.remove<metrics::timer<metrics::accumulator::decaying::exponentially_t>>(cocaine::format("{}.timings", name), {});
}

}  // namespace cocaine
