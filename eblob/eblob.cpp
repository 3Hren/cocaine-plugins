/*
    Copyright (c) 2011-2012 Andrey Sibiryov <me@kobology.ru>
    Copyright (c) 2011-2012 Other contributors as noted in the AUTHORS file.

    This file is part of Cocaine.

    Cocaine is free software; you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    Cocaine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

#include "eblob.hpp"

#include <cocaine/context.hpp>
#include <cocaine/registry.hpp>

#include <boost/tuple/tuple.hpp>

using namespace cocaine::core;
using namespace cocaine::storages;

namespace fs = boost::filesystem;

bool eblob_collector_t::callback(const zbr::eblob_disk_control * dco, const void * data, int) {
    if(dco->flags & BLOB_DISK_CTL_REMOVE) {
        return true;
    }

    std::string value(
        static_cast<const char*>(data),
        dco->data_size);

    Json::Value object;

    if(!m_reader.parse(value, object)) {
        // TODO: Have to find out the storage name somehow.
        throw storage_error_t("corrupted data");
    }

    // TODO: Have to find out the key somehow.
    m_root[unique_id_t().id()] = object;

    return true;
}

bool eblob_purger_t::callback(const zbr::eblob_disk_control * dco, const void * data, int) {
    m_keys.push_back(dco->key);
    return true;
}

void eblob_purger_t::complete(uint64_t, uint64_t) {
    for(key_list_t::const_iterator it = m_keys.begin(); it != m_keys.end(); ++it) {
        // XXX: Is there a possibility for an exception here?
        m_eblob->remove_all(*it);
    }
}

eblob_storage_t::eblob_storage_t(context_t& context):
    storage_t(context),
    m_storage_path(context.config.storage.uri),
    m_logger(NULL, EBLOB_LOG_NOTICE)
{
    if(!fs::exists(m_storage_path)) {
        try {
            fs::create_directories(m_storage_path);
        } catch(const std::runtime_error& e) {
            throw storage_error_t("unable to create " + m_storage_path.string());
        }
    } else if(fs::exists(m_storage_path) && !fs::is_directory(m_storage_path)) {
        throw storage_error_t(m_storage_path.string() + " is not a directory");
    }
}

eblob_storage_t::~eblob_storage_t() {
    m_eblobs.clear();
}

void eblob_storage_t::put(const std::string& ns, const std::string& key, const Json::Value& value) {
    eblob_map_t::iterator it(m_eblobs.find(ns));

    if(it == m_eblobs.end()) {
        zbr::eblob_config cfg;

        memset(&cfg, 0, sizeof(cfg));

        cfg.file = const_cast<char*>((m_storage_path / ns).string().c_str());
        cfg.iterate_threads = 1;
        cfg.sync = 5;
        cfg.log = m_logger.log();

        try {
            boost::tie(it, boost::tuples::ignore) = m_eblobs.insert(ns, new zbr::eblob(&cfg));
        } catch(const std::runtime_error& e) {
            // TODO: Have to do something more sophisticated here.
            throw storage_error_t(e.what());
        }
    }

    Json::FastWriter writer;
    std::string object(writer.write(value));

    try {
        it->second->write_hashed(key, object, 0);
    } catch(const std::runtime_error& e) {
        // TODO: Have to do something more sophisticated here.
        throw storage_error_t(e.what());
    }
}

bool eblob_storage_t::exists(const std::string& ns, const std::string& key) {
    eblob_map_t::iterator it(m_eblobs.find(ns));

    if(it != m_eblobs.end()) {
        std::string object;

        try {
            object = it->second->read_hashed(key, 0, 0);
        } catch(const std::runtime_error& e) {
            // TODO: Have to do something more sophisticated here.
            throw storage_error_t(e.what());
        }

        return !object.empty();
    }

    return false;
}

Json::Value eblob_storage_t::get(const std::string& ns, const std::string& key) {
    Json::Value result(Json::objectValue);
    eblob_map_t::iterator it(m_eblobs.find(ns));

    if(it != m_eblobs.end()) {
        Json::Reader reader(Json::Features::strictMode());
        std::string object;

        try {
            object = it->second->read_hashed(key, 0, 0);
        } catch(const std::runtime_error& e) {
            // TODO: Have to do something more sophisticated here.
            throw storage_error_t(e.what());
        }

        if(!object.empty() && !reader.parse(object, result)) {
            throw storage_error_t("corrupted data in '" + ns + "'");
        }
    }

    return result;
}

Json::Value eblob_storage_t::all(const std::string& ns) {
    eblob_collector_t collector;

    try {
        zbr::eblob_iterator iterator((m_storage_path / ns).string(), true);
        iterator.iterate(collector, 1);
    } catch(...) {
        // XXX: Does it only mean that the blob is empty?
        return Json::Value(Json::objectValue);
    }

    return collector.seal();
}

void eblob_storage_t::remove(const std::string& ns, const std::string& key) {
    eblob_map_t::iterator it(m_eblobs.find(ns));

    if(it != m_eblobs.end()) {
        try {
            it->second->remove_hashed(key);
        } catch(const std::runtime_error& e) {
            throw storage_error_t("unable to remove from '" + ns + "'");
        }
    }
}

void eblob_storage_t::purge(const std::string& ns) {
    eblob_map_t::iterator it(m_eblobs.find(ns));

    if(it != m_eblobs.end()) {
        eblob_purger_t purger(it->second);

        try {
            zbr::eblob_iterator iterator((m_storage_path / ns).string(), true);
            iterator.iterate(purger, 1);
        } catch(...) {
            // FIXME: I have no idea what this means.
        }
    }
}

extern "C" {
    void initialize(registry_t& registry) {
        registry.insert<eblob_storage_t, storage_t>("eblob");
    }
}
