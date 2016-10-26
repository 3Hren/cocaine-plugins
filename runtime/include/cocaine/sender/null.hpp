#pragma once

#include "cocaine/api/sender.hpp"

namespace cocaine {
namespace sender {

class null_sender_t : public api::sender_t {
public:
    null_sender_t(context_t& context,
                  asio::io_service& io_service,
                  const std::string& name,
                  data_provider_ptr data_provider,
                  const dynamic_t& args
    ) :
        api::sender_t(context, io_service, name, std::move(data_provider), args)
    {}
};

}
}
