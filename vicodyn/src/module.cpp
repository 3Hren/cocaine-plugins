
/*
* 2015+ Copyright (c) Anton Matveenko <antmat@yandex-team.ru>
* All rights reserved.
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*/

#include "cocaine/api/vicodyn/balancer.hpp"
#include "cocaine/gateway/vicodyn.hpp"
#include "cocaine/vicodyn/balancer/simple.hpp"
#include "cocaine/vicodyn/error.hpp"
#include "cocaine/repository/vicodyn/balancer.hpp"

#include <cocaine/errors.hpp>
#include <cocaine/repository.hpp>
#include <cocaine/repository/gateway.hpp>

using namespace cocaine;
extern "C" {
auto
validation() -> api::preconditions_t {
    return api::preconditions_t{ COCAINE_VERSION };
}

void
initialize(api::repository_t& repository) {
    cocaine::error::registrar::add(cocaine::vicodyn::vicodyn_category(), cocaine::vicodyn::vicodyn_category_id);

    repository.insert<vicodyn::balancer::simple_t>("simple");
    repository.insert<gateway::vicodyn_t>("vicodyn");
}

}
