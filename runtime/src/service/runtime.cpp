#include "cocaine/service/runtime.hpp"

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

class pg_sender_t : public runtime_t::sender_t {
public:
    pg_sender_t(std::unique_ptr<postgres::pool_t> _pool,
                std::string _hostname,
                std::string _table_name,
                std::unique_ptr<blackhole::logger_t> _logger):
        pool(std::move(_pool)),
        hostname(std::move(_hostname)),
        table_name(std::move(_table_name)),
        logger(std::move(_logger))
    {}

    virtual auto send(const dynamic_t& data) -> void {
        pool->execute([=](pqxx::connection_base& connection){
            try {
                auto data_string = boost::lexical_cast<std::string>(data);

                pqxx::work transaction(connection);
                auto query = cocaine::format("INSERT INTO {} (ts, host, data) VALUES(now(), {}, {});",
                                             transaction.esc(table_name),
                                             transaction.quote(hostname),
                                             transaction.quote(data_string));
                COCAINE_LOG_DEBUG(logger, "executing {}", query);
                auto sql_result = transaction.exec(query);
                transaction.commit();
            } catch (const std::exception& e) {
                COCAINE_LOG_ERROR(logger, "metric sending failed - {}", e.what());
            }
        });
    }
private:
    std::unique_ptr<postgres::pool_t> pool;
    std::string hostname;
    std::string table_name;
    std::unique_ptr<blackhole::logger_t> logger;
};

runtime_t::runtime_t(context_t& context,
                     asio::io_service& asio,
                     const std::string& name,
                     const dynamic_t& args) :
    api::service_t(context, asio, name, args),
    dispatch<io::runtime_tag>(name),
    hub(context.metrics_hub()),
    send_period(args.as_object().at("pg_send_period_s", 0u).as_uint()),
    send_timer(asio)
{
    if(send_period.ticks() != 0) {
        std::unique_ptr<postgres::pool_t> pool(
            new postgres::pool_t(args.as_object().at("pg_pool_size", 1u).as_uint(),
                                 args.as_object().at("pg_conn_string", "").as_string()));
        sender = std::unique_ptr<sender_t>(new pg_sender_t(std::move(pool),
                                                           context.config().network().hostname(),
                                                           args.as_object().at("pg_table_name", "cocaine_metrics").as_string(),
                                                           context.log("runtime/pg_sender")));
        send_timer.expires_from_now(send_period);
        send_timer.async_wait(std::bind(&runtime_t::on_send_timer, this, std::placeholders::_1));
    }
    on<io::runtime::metrics>([&]() -> dynamic_t {
        return metrics();
    });
}

void runtime_t::on_send_timer(const std::error_code& ec) {
    if(!ec) {
        sender->send(metrics());
        send_timer.expires_from_now(send_period);
        send_timer.async_wait(std::bind(&runtime_t::on_send_timer, this, std::placeholders::_1));
    }
}



auto runtime_t::metrics() const -> dynamic_t {
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
