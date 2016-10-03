/*
* 2016+ Copyright (c) Anton Matveenko <antmat@yandex-team.ru>
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

#pragma once

#include "cocaine/postgres/pool.hpp"

#include <cocaine/api/storage.hpp>

#include <pqxx/pqxx>

namespace cocaine {
namespace storage {
class postgres_t : public api::storage_t {
public:
    postgres_t(context_t& context, const std::string& name, const dynamic_t& args);

    virtual
    void
    read(const std::string& collection, const std::string& key, callback<std::string> cb);

    virtual
    void
    write(const std::string& collection,
          const std::string& key,
          const std::string& blob,
          const std::vector<std::string>& tags,
          callback<void> cb);

    virtual
    void
    remove(const std::string& collection, const std::string& key, callback<void> cb);

    virtual
    void
    find(const std::string& collection, const std::vector<std::string>& tags, callback<std::vector<std::string>> cb);

private:
    std::shared_ptr<logging::logger_t> log;
    std::string table_name;
    api::storage_ptr wrapped;
    postgres::pool_t pg_pool;
};
} // namespace storage
} // namespace cocaine