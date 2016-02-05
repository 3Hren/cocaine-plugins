#include "cocaine/service/node.hpp"

#include "cocaine/detail/isolate/process.hpp"

extern "C" {
    auto validation() -> cocaine::api::preconditions_t {
        return cocaine::api::preconditions_t { COCAINE_MAKE_VERSION(0, 12, 5) };
    }

    auto initialize(cocaine::api::repository_t& repository) -> void {
        repository.insert<cocaine::isolate::process_t>("process");
        repository.insert<cocaine::service::node_t>("node::v2");
    }
}
