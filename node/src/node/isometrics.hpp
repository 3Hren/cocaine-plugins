#pragma once

#include <chrono>
#include <memory>

#include <blackhole/logger.hpp>
#include <blackhole/scope/holder.hpp>

#include <cocaine/format.hpp>
#include <cocaine/context.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/repository.hpp>

#include "cocaine/service/node/manifest.hpp"
#include "cocaine/service/node/profile.hpp"

#include "cocaine/api/isolate.hpp"

#include <metrics/factory.hpp>
#include <metrics/registry.hpp>

#include "node/pool_observer.hpp"

#include "engine.hpp"

namespace cocaine {
namespace detail {
namespace service {
namespace node {

namespace conf {
    constexpr auto METRICS_POLL_INTERVAL_S = 2u;
}

enum class CounterType : unsigned {
        instant,   // treat value `as is`
        aggregate, // does isolate returns accumalated value on each request (ioread, etc)
};

struct metrics_aggregate_proxy_t;

struct worker_metrics_t {
    // <not monotonic (instant); shared_metric; delta from prev. value (for monotonic increasing value)>
    struct counter_metric_t {
        using value_type = std::uint64_t;

        CounterType type;
        metrics::shared_metric<std::atomic<value_type>> value;
        value_type delta; // if is_accumulated = true then delta = value - prev(value), used for app-wide aggration
    };

    using clock_type = std::chrono::system_clock;
    using counters_table_type = std::unordered_map<std::string, counter_metric_t>;
    using counters_record_type = counters_table_type::value_type;

    counters_table_type common_counters;
    clock_type::time_point update_stamp;

    worker_metrics_t(context_t& ctx, const std::string& name_prefix);
    worker_metrics_t(context_t& ctx, const std::string& app_name, const std::string& id);

    friend auto
    operator+(worker_metrics_t& src, metrics_aggregate_proxy_t& proxy) -> metrics_aggregate_proxy_t&;

    auto
    assign(metrics_aggregate_proxy_t&& init) -> void;
};

struct metrics_aggregate_proxy_t {
    // see worker_metrics_t::counter_metric_t
    struct counter_metric_t {
        using value_type = worker_metrics_t::counter_metric_t::value_type;
        // TODO: union?
        CounterType type;
        value_type values; // summation of worker_metrics values
        value_type deltas; // summation of worker_metrics deltas
    };

    std::unordered_map<std::string, counter_metric_t> common_counters;

    auto
    operator+(const worker_metrics_t& worker_metrics) -> metrics_aggregate_proxy_t&;
};

/// Isolation daemon's workers metrics sampler.
///
/// Poll sequence should be initialized explicitly with
/// metrics_retriever_t::ignite_poll method or implicitly
/// within metrics_retriever_t::make_and_ignite.
class metrics_retriever_t :
    public std::enable_shared_from_this<metrics_retriever_t>
{
public:
    using stats_table_type = std::unordered_map<std::string, worker_metrics_t>;
private:
    using pool_type = engine_t::pool_type;

    context_t& context;

    asio::deadline_timer metrics_poll_timer;
    std::shared_ptr<api::isolate_t> isolate;

    synchronized<engine_t::pool_type>& pool;

    const std::unique_ptr<cocaine::logging::logger_t> log;

    //
    // Poll intervals could be quite large (should be configurable), so the
    // Isolation Daemon supports metrics `in memory` persistance for dead workers
    // (at least for 30 seconds) and it will be possible to query for workers which
    // have passed away not too long ago. Their uuids will be taken and added to
    // request from `purgatory`.
    //
    using purgatory_pot_type = std::set<std::string>;
    synchronized<purgatory_pot_type> purgatory;

    // `synchronized` not needed within current design, but shouldn't do any harm
    // <uuid, metrics>
    synchronized<stats_table_type> metrics;

    struct self_metrics_t {
        metrics::shared_metric<std::atomic<std::uint64_t>> uuid_requested;
        metrics::shared_metric<std::atomic<std::uint64_t>> uuid_recieved;
        metrics::shared_metric<std::atomic<std::uint64_t>> requests_send;
        metrics::shared_metric<std::atomic<std::uint64_t>> responses_received;
        metrics::shared_metric<std::atomic<std::uint64_t>> receive_errors;
        metrics::shared_metric<std::atomic<std::uint64_t>> posmortem_queue_size;
        self_metrics_t(context_t& ctx, const std::string& name);
    } self_metrics;

    std::string app_name;
    boost::posix_time::seconds poll_interval;

    worker_metrics_t app_aggregate_metrics;
public:

    metrics_retriever_t(
        context_t& ctx,
        const std::string& name,
        std::shared_ptr<api::isolate_t> isolate,
        synchronized<engine_t::pool_type>& pool,
        asio::io_service& loop,
        const std::uint64_t poll_interval);

    ///
    /// Reads following section from Cocaine-RT configuration:
    ///  ```
    ///  "node" : {
    ///     ...
    ///     "args" : {
    ///        "isolate_metrics:" : true,
    ///        "isolate_metrics_poll_period_s" : 10
    ///     }
    ///  }
    ///  ```
    /// if `isolate_metrics` is false (default), returns nullptr,
    /// and polling sequence wouldn't start.
    ///
    // TODO: it seems that throw on construction error would be a better choice.
    template<typename Observers>
    static
    auto
    make_and_ignite(
        context_t& ctx,
        const std::string& name,
        std::shared_ptr<api::isolate_t> isolate,
        synchronized<engine_t::pool_type>& pool,
        asio::io_service& loop,
        synchronized<Observers>& observers) -> std::shared_ptr<metrics_retriever_t>;

    auto
    ignite_poll() -> void;

    auto
    make_observer() -> std::shared_ptr<pool_observer>;

    //
    // Should be called on every pool::erase(id) invocation
    //
    // usually isolation daemon will hold metrics of despawned workers for some
    // reasonable period (at least 30 sec), so it is allowed to request metrics
    // of dead workers on next `poll` invocation.
    //
    // Note: on high despawn rates stat of some unlucky workers will be lost
    //
    auto
    add_post_mortem(const std::string& id) -> void;

private:

    auto
    poll_metrics(const std::error_code& ec) -> void;

private:

    // TODO: wip, possibility of redesign
    struct metrics_handle_t : public api::metrics_handle_base_t
    {
        metrics_handle_t(std::shared_ptr<metrics_retriever_t> parent) :
            parent{parent}
        {}

        auto
        on_data(const dynamic_t& data) -> void override;

        auto
        on_error(const std::error_code&, const std::string& what) -> void override;

        std::shared_ptr<metrics_retriever_t> parent;
    };

    // TODO: wip, possibility of redesign
    struct metrics_pool_observer_t : public pool_observer {

        metrics_pool_observer_t(metrics_retriever_t& p) :
            parent(p)
        {}

        auto
        spawned(const std::string&) -> void override
        {}

        auto
        despawned(const std::string& id) -> void override {
            parent.add_post_mortem(id);
        }

    private:
        metrics_retriever_t& parent;
    };

}; // metrics_retriever_t

template<typename Observers>
auto
metrics_retriever_t::make_and_ignite(
    context_t& ctx,
    const std::string& name,
    std::shared_ptr<api::isolate_t> isolate,
    synchronized<engine_t::pool_type>& pool,
    asio::io_service& loop,
    synchronized<Observers>& observers) -> std::shared_ptr<metrics_retriever_t>
{
    const auto node_config = ctx.config().component_group("services").get("node");

    if (!node_config) {
        return nullptr;
    }

    if (isolate) {
        const auto args = node_config->args().as_object();

        const auto& should_start = args.at("isolate_metrics", false).as_bool();
        const auto& poll_interval = args.at("isolate_metrics_poll_period_s", conf::METRICS_POLL_INTERVAL_S).as_uint();

        if (should_start) {
            auto retriever = std::make_shared<metrics_retriever_t>(
                ctx,
                name,
                std::move(isolate),
                pool,
                loop,
                poll_interval);

                observers->emplace_back(retriever->make_observer());
                retriever->ignite_poll();

                return retriever;
        }
    }

    return nullptr;
}

}  // namespace node
}  // namespace service
}  // namespace detail
}  // namespace cocaine
