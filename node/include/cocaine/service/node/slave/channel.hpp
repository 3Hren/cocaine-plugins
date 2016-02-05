#pragma once

#include <memory>

#include "cocaine/service/node/event.hpp"

namespace cocaine {

class client_rpc_dispatch_t;

namespace api {
class stream_t;
}

namespace slave {

struct channel_t {
    /// Event to be processed.
    app::event_t event;

    std::shared_ptr<client_rpc_dispatch_t> dispatch;

    /// An RX stream provided from user. The slave will call its callbacks on every incoming event.
    std::shared_ptr<api::stream_t> downstream;
};

}
}
