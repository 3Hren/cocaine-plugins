#include "cocaine/service/node/slave/stats.hpp"

namespace cocaine {
namespace service {
namespace node {
namespace slave {

stats_t::stats_t():
     tx{},
     rx{},
     load{},
     total{},
     age{boost::none}
{}

}  // namespace slave
}  // namespace node
}  // namespace service
}  // namespace cocaine
