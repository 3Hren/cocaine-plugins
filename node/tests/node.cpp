#include <iostream>

#include <blackhole/builder.hpp>
#include <blackhole/extensions/writer.hpp>
#include <blackhole/formatter/string.hpp>
#include <blackhole/handler/blocking.hpp>
#include <blackhole/logger.hpp>
#include <blackhole/root.hpp>
#include <blackhole/sink/console.hpp>

#include <cocaine/trace/trace.hpp>
#include <cocaine/api/stream.hpp>
#include <cocaine/context.hpp>
#include <cocaine/context/config.hpp>
#include <cocaine/rpc/actor.hpp>
#include <cocaine/service/node.hpp>
#include <cocaine/service/node/overseer.hpp>

class stream_t : public cocaine::api::stream_t {
public:
    auto write(cocaine::hpack::header_storage_t headers, const std::string& chunk) -> stream_t& {
        std::cout << chunk << std::endl;
    }

    auto error(cocaine::hpack::header_storage_t headers, const std::error_code& ec, const std::string& reason) -> void {
        throw std::runtime_error("#");
    }

    auto close(cocaine::hpack::header_storage_t headers) -> void {
    }
};

int main() {
    auto config = cocaine::make_config("/Users/esafronov/.cocaine/cocaine-runtime.conf");
    auto log = blackhole::experimental::builder<blackhole::root_logger_t>()
        .handler<blackhole::handler::blocking_t>()
            .set<blackhole::formatter::string_t>("{severity}, [{timestamp}]: {message} [{...}]")
            .add<blackhole::sink::console_t>()
            .build()
        .build();

    auto context = cocaine::get_context(
        std::move(config),
        std::unique_ptr<blackhole::logger_t>(new blackhole::root_logger_t(std::move(log)))
    );

    auto logger = context->log("#");

    logger->log(1, "sleeping for 10 secs ...");
    ::sleep(1);
    if (auto actor = context->locate("node")) {
        const auto& prototype = actor.get().prototype();
        const auto& node = static_cast<const cocaine::service::node_t&>(prototype);

        logger->log(1, "                ");
        auto overseer = node.overseer("echo-cpp");
        auto rx = std::shared_ptr<cocaine::api::stream_t>(new stream_t);

        cocaine::trace_t::current() = cocaine::trace_t::generate("#");
        auto tx = overseer->enqueue(rx, {"meta", {}}, boost::none);
        ::sleep(5);
    } else {
        logger->log(4, "fuck you");
        return 1;
    }

    return 0;
}
