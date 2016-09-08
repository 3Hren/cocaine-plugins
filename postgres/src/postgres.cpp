#include "cocaine/storage/postgres.hpp"

#include <cocaine/dynamic.hpp>

namespace cocaine {
namespace storage {

postgres_t::postgres_t(context_t& context, const std::string& name, const dynamic_t& args) :
    api::storage_t(context, name, args),
    table_name(args.as_object().at("pg_table_name", "cocaine_index").as_string()),
    wrapped(api::storage(context, args.as_object().at("pg_underlying_storage", "core").as_string())),
    pg_pool(args.as_object().at("pg_pool_size", 1ul).as_uint(),
            args.as_object().at("pg_connection_string", "").as_string())
{}

void
postgres_t::read(const std::string& collection, const std::string& key, callback<std::string> cb) {
    wrapped->read(collection, key, std::move(cb));
}

void
postgres_t::write(const std::string& collection,
                   const std::string& key,
                   const std::string& blob,
                   const std::vector<std::string>& tags,
                   callback<void> cb)
{
    wrapped->write(collection, key, blob, {}, [=](std::future<void> fut){
        try {
            // First we check if underlying write succeed
            fut.get();
            pg_pool.execute([=](pqxx::connection_base& connection){
                try {
                    // TODO: possible overhead with dynamic can be removed via usage
                    // of RapidJson or even manual formatting
                    dynamic_t tags_obj(tags);
                    auto tag_string = boost::lexical_cast<std::string>(tags_obj);
                    pqxx::work transaction(connection);
                    transaction.exec(
                        "INSERT INTO " + transaction.esc(table_name) + "(collection, key, tags) VALUES(" +
                        transaction.quote(collection) + ", " +
                        transaction.quote(key) + ", " +
                        transaction.quote(tag_string) + ");");
                    transaction.commit();
                    cb(make_ready_future());
                } catch (...) {
                    cb(make_exceptional_future<void>());
                }
            });
        } catch(...) {
            cb(make_exceptional_future<void>());
        }
    });
}

void
postgres_t::remove(const std::string& collection, const std::string& key, callback<void> cb) {
    wrapped->remove(collection, key, [=](std::future<void> fut){
        try {
            // First we check if underlying write succeed
            fut.get();
            pg_pool.execute([=](pqxx::connection_base& connection){
                try {
                    pqxx::work transaction(connection);
                    transaction.exec("DELETE FROM " + transaction.esc(table_name) + "WHERE " +
                                     "collection = " + transaction.quote(collection) + " AND " +
                                     "key = " + transaction.quote(key) + ";");
                    transaction.commit();
                    cb(make_ready_future());
                } catch (...) {
                    cb(make_exceptional_future<void>());
                }
            });
        } catch(...) {
            cb(make_exceptional_future<void>());
        }
    });
}

void
postgres_t::find(const std::string& collection, const std::vector<std::string>& tags, callback<std::vector<std::string>> cb) {
    pg_pool.execute([=](pqxx::connection_base& connection){
        try {
            // TODO: possible overhead with dynamic can be removed via usage
            // of RapidJson or even manual formatting
            dynamic_t tags_obj(tags);
            auto tag_string = boost::lexical_cast<std::string>(tags_obj);

            pqxx::work transaction(connection);
            auto sql_result = transaction.exec("SELECT key FROM " + transaction.esc(table_name) + " WHERE " +
                             "collection = " + transaction.quote(collection) + " AND " +
                             "tags @> " + transaction.quote(tag_string) + ";");

            std::vector<std::string> result;
            for(const auto& row : sql_result) {
                std::string key;
                row.at(0).to(key);
                result.push_back(std::move(key));
            }

            cb(make_ready_future(std::move(result)));
        } catch (...) {
            cb(make_exceptional_future<std::vector<std::string>>());
        }
    });
}

} // namespace storage
} // namespace cocaine