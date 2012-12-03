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

#include "service.hpp"

#include <cocaine/app.hpp>
#include <cocaine/context.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/session.hpp>

#include <cocaine/api/storage.hpp>

#include <cocaine/traits/json.hpp>

#include <boost/tuple/tuple.hpp>

using namespace cocaine;
using namespace cocaine::io;
using namespace cocaine::service;

server_t::server_t(context_t& context, 
                   const std::string& name,
                   const Json::Value& args):
    category_type(context, name, args),
    m_context(context),
    m_log(context.log("server")),
    m_server(context, ZMQ_REP),
    m_runlist(args.get("runlist", "unspecified").asString()),
    m_auth(context),
    m_birthstamp(m_loop.now()) /* I don't like it. */,
    m_announce_interval(args.get("announce-interval", 5.0f).asDouble()),
    m_infostamp(0.0f)
{
    int minor, major, patch;
    zmq_version(&major, &minor, &patch);

    COCAINE_LOG_INFO(m_log, "using libev version %d.%d", ev_version_major(), ev_version_minor());
    COCAINE_LOG_INFO(m_log, "using libmsgpack version %s", msgpack_version());
    COCAINE_LOG_INFO(m_log, "using libzmq version %d.%d.%d", major, minor, patch);

    // Server socket

    m_server.setsockopt(
        ZMQ_IDENTITY,
        context.config.network.hostname.data(),
        context.config.network.hostname.size()
    );

    COCAINE_LOG_INFO(m_log, "identity of this node is '%s'", m_server.identity());
    
    for(Json::Value::const_iterator it = args["listen"].begin();
        it != args["listen"].end();
        ++it)
    {
        std::string endpoint((*it).asString());

        try {
            m_server.bind(endpoint);
        } catch(const zmq::error_t& e) {
            throw configuration_error_t("invalid listen endpoint - %s", e.what());
        }
            
        COCAINE_LOG_INFO(m_log, "listening on %s", endpoint);
    }
    
    m_watcher.set<server_t, &server_t::on_event>(this);
    m_watcher.start(m_server.fd(), ev::READ);
    m_checker.set<server_t, &server_t::on_check>(this);
    m_checker.start();

    // Autodiscovery

    if(!args["announce"].empty()) {
        m_announces.reset(new socket_t(m_context, ZMQ_PUB));
        
        for(Json::Value::const_iterator it = args["announce"].begin();
            it != args["announce"].end();
            ++it)
        {
            std::string endpoint((*it).asString());

            try {
                m_announces->bind(endpoint);
            } catch(const zmq::error_t& e) {
                throw configuration_error_t("invalid announce endpoint - %s", e.what());
            }

            COCAINE_LOG_INFO(m_log, "announcing on %s", endpoint);
        }

        m_announce_timer.reset(new ev::timer());
        m_announce_timer->set<server_t, &server_t::on_announce>(this);
        m_announce_timer->start(0.0f, m_announce_interval);
    }

    // Signals

    m_sigint.set<server_t, &server_t::on_terminate>(this);
    m_sigint.start(SIGINT);

    m_sigterm.set<server_t, &server_t::on_terminate>(this);
    m_sigterm.start(SIGTERM);

    m_sigquit.set<server_t, &server_t::on_terminate>(this);
    m_sigquit.start(SIGQUIT);

    m_sighup.set<server_t, &server_t::on_reload>(this);
    m_sighup.start(SIGHUP);
   
    recover();
}

server_t::~server_t() {
    // Empty.
}

void
server_t::run() {
    m_loop.loop();
}

void
server_t::terminate() {
    // TODO: No-op.
}

void
server_t::on_terminate(ev::sig&, int) {
    if(!m_apps.empty()) {
        COCAINE_LOG_INFO(m_log, "stopping the apps");
        m_apps.clear();
    }

    m_loop.unloop(ev::ALL);
}

void
server_t::on_reload(ev::sig&, int) {
    COCAINE_LOG_INFO(m_log, "reloading the apps");

    try {
        recover();
    } catch(const std::exception& e) {
        COCAINE_LOG_ERROR(m_log, "unable to reload the apps - %s", e.what());
    } catch(...) {
        COCAINE_LOG_ERROR(m_log, "unable to reload the apps - unexpected exception");
    }
}

void
server_t::on_event(ev::io&, int) {
    m_checker.stop();

    if(m_server.pending()) {
        m_checker.start();
        process();
    }
}

void
server_t::on_check(ev::prepare&, int) {
    m_loop.feed_fd_event(m_server.fd(), ev::READ);
}

void
server_t::on_announce(ev::timer&, int) {
    COCAINE_LOG_DEBUG(m_log, "announcing the node");

    m_announces->send_multipart(
        protect(m_server.endpoint()),
        info()
    );
}

void
server_t::process() {
    zmq::message_t message;
    
    {
        scoped_option<
            options::receive_timeout
        > option(m_server, 0);
        
        if(!m_server.recv(message)) {
            return;
        }
    }

    Json::Reader reader(Json::Features::strictMode());
    Json::Value root;
    std::string response;

    if(reader.parse(
        static_cast<const char*>(message.data()),
        static_cast<const char*>(message.data()) + message.size(),
        root)) 
    {
        try {
            if(!root.isObject()) {
                throw configuration_error_t("json root must be an object");
            }

            unsigned int version = root["version"].asUInt();
            
            if(version < 2 || version > 3) {
                throw configuration_error_t("unsupported protocol version");
            }
  
            if(version == 3) {
                zmq::message_t signature;

                if(m_server.more()) {
                    m_server.recv(signature);
                }

                std::string username(root["username"].asString());
                
                if(!username.empty()) {
                    m_auth.verify(
                        std::string(static_cast<const char*>(message.data()), message.size()),
                        std::string(static_cast<const char*>(signature.data()), signature.size()),
                        username
                    );
                } else {
                    throw authorization_error_t("username expected");
                }
            }

            response = dispatch(root);
        } catch(const std::exception& e) {
            response = json::serialize(json::build("error", e.what()));
        } catch(...) {
            response = json::serialize(json::build("error", "unexpected exception"));
        }
    } else {
        response = json::serialize(
            json::build("error", reader.getFormattedErrorMessages())
        );
    }

    // Serialize and send the response.
    message.rebuild(response.size());
    memcpy(message.data(), response.data(), response.size());

    scoped_option<
        options::send_timeout
    > option(m_server, 0);
    
    if(!m_server.send(message)) {
        COCAINE_LOG_ERROR(m_log, "unable to respond to an RPC request - client timed out");
    }
}

std::string server_t::dispatch(const Json::Value& root) {
    std::string action(root["action"].asString());

    if(action == "create") {
        Json::Value apps(root["apps"]),
                    result(Json::objectValue);

        if(!apps.isObject() || apps.empty()) {
            throw configuration_error_t("no apps have been specified");
        }

        Json::Value::Members names(apps.getMemberNames());

        for(Json::Value::Members::const_iterator it = names.begin(); it != names.end(); ++it) {
            std::string name(*it),
                        profile(apps[name].asString());

            try {
                result[name] = create_app(name, profile);
            } catch(const std::exception& e) {
                result[name]["error"] = e.what();
            } catch(...) {
                result[name]["error"] = "unexpected exception";
            }
        }

        return json::serialize(result);
    } else if(action == "delete") {
        Json::Value apps(root["apps"]),
                    result(Json::objectValue);

        if(!apps.isArray() || apps.empty()) {
            throw configuration_error_t("no apps have been specified");
        }

        for(Json::Value::iterator it = apps.begin(); it != apps.end(); ++it) {
            std::string name((*it).asString());

            try {
                result[name] = delete_app(name);                
            } catch(const std::exception& e) {
                result[name]["error"] = e.what();
            } catch(...) {
                result[name]["error"] = "unexpected exception";
            }
        }

        return json::serialize(result);
    } else if(action == "info") {
        if(m_loop.now() >= (m_infostamp + 5.0f)) {
            m_infostamp = m_loop.now();
            m_infocache = json::serialize(info());
        }

        return m_infocache;
    } else {
        throw configuration_error_t("unsupported action");
    }
}

// Commands

Json::Value server_t::create_app(const std::string& name, const std::string& profile) {
    if(m_apps.find(name) != m_apps.end()) {
        throw configuration_error_t("the specified app already exists");
    }

    app_map_t::iterator it;

    boost::tie(it, boost::tuples::ignore) = m_apps.emplace(
        name,
        boost::make_shared<app_t>(
            m_context,
            name,
            profile
        )
    );

    try {
        it->second->start();
    } catch(...) {
        m_apps.erase(it);
        throw;
    }

    return json::build("success", true);
}

Json::Value server_t::delete_app(const std::string& name) {
    app_map_t::iterator app(m_apps.find(name));

    if(app == m_apps.end()) {
        throw configuration_error_t("the specified app doesn't exist");
    }

    m_apps.erase(app);

    return json::build("success", true);
}

Json::Value server_t::info() const {
    Json::Value result(Json::objectValue);

    for(app_map_t::const_iterator it = m_apps.begin();
        it != m_apps.end(); 
        ++it) 
    {
        result["apps"][it->first] = it->second->info();
    }

    result["identity"] = m_context.config.network.hostname;

    size_t total = engine::session_t::objects_created(),
           current = engine::session_t::objects_alive();

    result["sessions"]["pending"] = static_cast<Json::LargestUInt>(current);
    result["sessions"]["processed"] = static_cast<Json::LargestUInt>(total - current);

    if(m_announce_interval) {
        result["announce-interval"] = m_announce_interval;
    }
    
    result["uptime"] = m_loop.now() - m_birthstamp;

    return result;
}

void server_t::recover() {
    typedef std::map<
        std::string,
        std::string
    > runlist_t;

    auto storage = api::storage(m_context, "core");

    COCAINE_LOG_INFO(m_log, "reading the '%s' runlist", m_runlist);
    
    // NOTE: Allowing the exception to propagate here, as this is a fatal error.
    runlist_t runlist(
        storage->get<runlist_t>("runlists", m_runlist)
    );

    std::set<std::string> active,
                          available;

    // Currently running apps.
    for(app_map_t::const_iterator it = m_apps.begin();
        it != m_apps.end();
        ++it)
    {
        active.insert(it->first);
    }

    // Runnable apps.
    for(runlist_t::const_iterator it = runlist.begin();
        it != runlist.end();
        ++it)
    {
        available.insert(it->first);
    }

    std::vector<std::string> diff;

    // Generate a list of apps which are either new or dead.
    std::set_symmetric_difference(active.begin(), active.end(),
                                  available.begin(), available.end(),
                                  std::back_inserter(diff));

    for(std::vector<std::string>::const_iterator it = diff.begin();
        it != diff.end(); 
        ++it)
    {
        if(m_apps.find(*it) == m_apps.end()) {
            try {
                create_app(*it, runlist[*it]);
            } catch(const std::exception& e) {
                COCAINE_LOG_ERROR(
                    m_log,
                    "unable to initialize the '%s' app - %s",
                    *it,
                    e.what()
                );

                throw configuration_error_t("unable to initialize the apps");
            }
        } else {
            m_apps.find(*it)->second->stop();
        }
    }
}

