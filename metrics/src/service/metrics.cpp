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
#include <metrics/visitor.hpp>

namespace cocaine {
namespace service {

class plain_t : public metrics::visitor_t {
    const std::string& m_name;
    dynamic_t::object_t& m_out;

public:
    plain_t(const std::string& name, dynamic_t::object_t& out) :
        m_name(name),
        m_out(out)
    {}

    auto visit(const metrics::gauge<std::int64_t>& metric) -> void override {
        do_visit(metric);
    }

    auto visit(const metrics::gauge<std::uint64_t>& metric) -> void override {
        do_visit(metric);
    }

    auto visit(const metrics::gauge<std::double_t>& metric) -> void override {
        do_visit(metric);
    }

    auto visit(const std::atomic<std::int64_t>& metric) -> void override {
        do_visit(metric);
    }

    auto visit(const std::atomic<std::uint64_t>& metric) -> void override {
        do_visit(metric);
    }

    auto visit(const metrics::meter_t& metric) -> void override {
        m_out[m_name + ".count"] = metric.count();
        m_out[m_name + ".m01rate"] = metric.m01rate();
        m_out[m_name + ".m05rate"] = metric.m05rate();
        m_out[m_name + ".m15rate"] = metric.m15rate();
    }

    auto visit(const metrics::timer<metrics::accumulator::sliding::window_t>& metric) -> void override {
        do_visit(metric);
    }

private:
    template<typename T>
    auto do_visit(const metrics::gauge<T>& metric) -> void {
        m_out[m_name] = metric();
    }

    template<typename T>
    auto do_visit(const std::atomic<T>& metric) -> void {
        m_out[m_name] = metric.load();
    }

    template<typename T>
    auto do_visit(const metrics::timer<T>& metric) -> void {
        m_out[m_name + ".count"] = metric.count();
        m_out[m_name + ".m01rate"] = metric.m01rate();
        m_out[m_name + ".m05rate"] = metric.m05rate();
        m_out[m_name + ".m15rate"] = metric.m15rate();

        const auto snapshot = metric.snapshot();
        m_out[m_name + ".p50"] = snapshot.median() / 1e6;
        m_out[m_name + ".p75"] = snapshot.p75() / 1e6;
        m_out[m_name + ".p90"] = snapshot.p90() / 1e6;
        m_out[m_name + ".p95"] = snapshot.p95() / 1e6;
        m_out[m_name + ".p98"] = snapshot.p98() / 1e6;
        m_out[m_name + ".p99"] = snapshot.p99() / 1e6;
        m_out[m_name + ".mean"] = snapshot.mean() / 1e6;
        m_out[m_name + ".stddev"] = snapshot.stddev() / 1e6;
    }
};

metrics_t::metrics_t(context_t& context,
                     asio::io_service& asio,
                     const std::string& _name,
                     const dynamic_t& args) :
    api::service_t(context, asio, _name, args),
    dispatch<io::metrics_tag>(_name),
    hub(context.metrics_hub()),
    senders()
{

    auto sender_names = args.as_object().at("senders", dynamic_t::empty_array).as_array();

    for(auto& sender_name: sender_names) {
        api::sender_t::data_provider_ptr provider(new api::sender_t::function_data_provider_t([=]() {
            return metrics();
        }));
        senders.push_back(api::sender(context, asio, sender_name.as_string(), std::move(provider)));
    }

    on<io::metrics::fetch>([&]() -> dynamic_t {
        return metrics();
    });
}

auto metrics_t::metrics() const -> dynamic_t {
    dynamic_t::object_t out;
    for (const auto& metric : hub.select()) {
        plain_t visitor(metric->name(), out);
        metric->apply(visitor);
    }

    return out;
}

}  // namespace service
}  // namespace cocaine
