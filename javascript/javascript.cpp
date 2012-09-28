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

#include <boost/filesystem/fstream.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/format.hpp>

#include <cocaine/context.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/manifest.hpp>

#include <cocaine/api/sandbox.hpp>

#include <sstream>

#include <v8.h>

namespace cocaine { namespace sandbox {

using namespace v8;

class javascript_t:
    public api::sandbox_t
{
    public:
        typedef api::sandbox_t category_type;

    public:
        javascript_t(context_t& context, const manifest_t& manifest, const std::string& spool):
            category_type(context, manifest, spool),
            m_log(context.log(
                (boost::format("app/%1%")
                    % manifest.name
                ).str()
            ))
        {
            boost::filesystem::path source(spool);
            boost::filesystem::ifstream input(source);
    
            if(!input) {
                throw configuration_error_t("unable to open " + source.string());
            }

            std::stringstream stream;
            stream << input.rdbuf();
            
            compile(stream.str(), "iterate");
        }

        ~javascript_t() {
            m_function.Dispose();
            m_v8_context.Dispose();
        }

        virtual void invoke(const std::string& event,
                            api::io_t& io)
        {
            Json::Value result;

            HandleScope handle_scope;
            Context::Scope context_scope(m_v8_context);
            
            TryCatch try_catch;
            Handle<Value> rv(m_function->Call(m_v8_context->Global(), 0, NULL));

            if(!rv.IsEmpty()) {
                result["result"] = "success";
            } else if(try_catch.HasCaught()) {
                String::AsciiValue exception(try_catch.Exception());
                result["error"] = std::string(*exception, exception.length());
            }

            Json::FastWriter writer;
            std::string response(writer.write(result));

            io.write(response.data(), response.size());
        }

    private:
        void compile(const std::string& code,
                     const std::string& name)
        {
            HandleScope handle_scope;

            m_v8_context = Context::New();

            Context::Scope context_scope(m_v8_context);
            
            TryCatch try_catch;

            Handle<String> source(String::New(code.c_str()));
            Handle<Script> script(Script::Compile(source));

            if(script.IsEmpty()) {
                String::AsciiValue exception(try_catch.Exception());
                throw unrecoverable_error_t(*exception);
            }

            Handle<Value> result(script->Run());

            if(result.IsEmpty()) {
                String::AsciiValue exception(try_catch.Exception());
                throw unrecoverable_error_t(*exception);
            }

            Handle<String> target(String::New(name.c_str()));
            Handle<Value> object(m_v8_context->Global()->Get(target));

            if(!object->IsFunction()) {
                throw configuration_error_t("target object is not a function");
            }

            Handle<Function> function(Handle<Function>::Cast(object));
            m_function = Persistent<Function>::New(function);
        }

    private:
        const logging::logger_t& log() const {
            return *m_log;
        }
    
    private:
        boost::shared_ptr<logging::logger_t> m_log;

        Persistent<Context> m_v8_context;
        Persistent<Function> m_function;
};

extern "C" {
    void initialize(api::repository_t& repository) {
        repository.insert<javascript_t>("javascript");
    }
}

}}
