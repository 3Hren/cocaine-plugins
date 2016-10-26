#pragma once

#include "cocaine/api/sender.hpp"

#include "cocaine/postgres/pool.hpp"

#include <asio/deadline_timer.hpp>

namespace cocaine {
namespace sender {

class pg_sender_t : public api::sender_t {
public:
    pg_sender_t(context_t& context,
                asio::io_service& io_service,
                const std::string& name,
                data_provider_ptr data_provider,
                const dynamic_t& args);

private:
    auto on_send_timer(const std::error_code& ec) -> void;
    auto send(dynamic_t data) -> void;

    data_provider_ptr data_provider;
    std::unique_ptr<postgres::pool_t> pool;
    std::string hostname;
    std::string table_name;
    std::unique_ptr<blackhole::logger_t> logger;
    boost::posix_time::seconds send_period;
    asio::deadline_timer send_timer;
};

}
}
