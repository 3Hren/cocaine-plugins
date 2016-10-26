#include "cocaine/sender/postgres.hpp"

#include <cocaine/context.hpp>
#include <cocaine/context/config.hpp>
#include <cocaine/errors.hpp>
#include <cocaine/format.hpp>
#include <cocaine/logging.hpp>

#include <blackhole/logger.hpp>

#include <pqxx/transaction>

namespace cocaine {
namespace sender {

pg_sender_t::pg_sender_t(context_t& context,
                         asio::io_service& io_loop,
                         const std::string& name,
                         data_provider_ptr _data_provier,
                         const dynamic_t& args
) :
    sender_t(context, io_loop, name, nullptr, args),
    data_provider(std::move(_data_provier)),
    pool(new postgres::pool_t(args.as_object().at("pg_pool_size", 1u).as_uint(),
                              args.as_object().at("pg_conn_string", "").as_string())),
    hostname(context.config().network().hostname()),
    table_name(args.as_object().at("pg_table_name", "cocaine_metrics").as_string()),
    logger(context.log(format("pg_sender/{}", name))),
    send_period(args.as_object().at("pg_send_period_s", 1u).as_uint()),
    send_timer(io_loop)
{
    if(send_period.ticks() == 0) {
        throw error_t("pg_send_period can not be zero");
    }
    send_timer.expires_from_now(send_period);
    send_timer.async_wait(std::bind(&pg_sender_t::on_send_timer, this, std::placeholders::_1));
}

auto pg_sender_t::on_send_timer(const std::error_code& ec) -> void {
    if(!ec) {
        send(data_provider->fetch());
        send_timer.expires_from_now(send_period);
        send_timer.async_wait(std::bind(&pg_sender_t::on_send_timer, this, std::placeholders::_1));
    } else {
        COCAINE_LOG_WARNING(logger, "sender timer was cancelled");
    }
}

auto pg_sender_t::send(dynamic_t data) -> void {
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

} // namespace sender
} // namespace cocaine
