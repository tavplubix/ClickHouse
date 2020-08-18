#include <Databases/DatabasesCommon.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageFactory.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DATABASE;
}

DatabaseWithOwnTablesBase::DatabaseWithOwnTablesBase(const String & name_, const String & logger, const Context & context)
        : IDatabase(name_), log(&Poco::Logger::get(logger)), global_context(context.getGlobalContext())
{
}

bool DatabaseWithOwnTablesBase::isTableExist(const String & table_name, const Context &) const
{
    std::lock_guard lock(mutex);
    return tables.find(table_name) != tables.end();
}

StoragePtr DatabaseWithOwnTablesBase::tryGetTable(const String & table_name, const Context &) const
{
    std::lock_guard lock(mutex);
    auto it = tables.find(table_name);
    if (it != tables.end())
        return it->second;
    return {};
}

DatabaseTablesIteratorPtr DatabaseWithOwnTablesBase::getTablesIterator(const Context &, const FilterByNameFunction & filter_by_table_name)
{
    std::lock_guard lock(mutex);
    if (!filter_by_table_name)
        return std::make_unique<DatabaseTablesSnapshotIterator>(tables, database_name);

    Tables filtered_tables;
    for (const auto & [table_name, storage] : tables)
        if (filter_by_table_name(table_name))
            filtered_tables.emplace(table_name, storage);

    return std::make_unique<DatabaseTablesSnapshotIterator>(std::move(filtered_tables), database_name);
}

bool DatabaseWithOwnTablesBase::empty() const
{
    std::lock_guard lock(mutex);
    LOG_WARNING(log, "Check empty, {} tables", tables.size());
    return tables.empty();
}

StoragePtr DatabaseWithOwnTablesBase::detachTable(const String & table_name)
{
    std::unique_lock lock(mutex);
    return detachTableUnlocked(table_name, lock);
}

StoragePtr DatabaseWithOwnTablesBase::detachTableUnlocked(const String & table_name, std::unique_lock<std::mutex> &)
{
    LOG_WARNING(log, "Detaching {}", table_name);
    assertDatabaseStartedUp();
    StoragePtr res;

    auto it = tables.find(table_name);
    if (it == tables.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist.",
                        backQuote(database_name), backQuote(table_name));
    res = it->second;
    tables.erase(it);

    auto table_id = res->getStorageID();
    if (table_id.hasUUID())
    {
        assert(database_name == DatabaseCatalog::TEMPORARY_DATABASE || getEngineName() == "Atomic");
        DatabaseCatalog::instance().removeUUIDMapping(table_id.uuid);
    }

    LOG_WARNING(log, "Detached {}", table_name);
    return res;
}

void DatabaseWithOwnTablesBase::attachTable(const String & table_name, const StoragePtr & table, const String &)
{
    std::unique_lock lock(mutex);
    attachTableUnlocked(table_name, table, lock);
}

void DatabaseWithOwnTablesBase::attachTableUnlocked(const String & table_name, const StoragePtr & table, std::unique_lock<std::mutex> &)
{
    LOG_WARNING(log, "Attaching {}", table_name);
    assertDatabaseStartedUp();
    auto table_id = table->getStorageID();
    if (table_id.database_name != database_name)
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database was renamed to `{}`, cannot create table in `{}`",
                        database_name, table_id.database_name);

    if (!tables.emplace(table_name, table).second)
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {} already exists.", table_id.getFullTableName());

    if (table_id.hasUUID())
    {
        assert(database_name == DatabaseCatalog::TEMPORARY_DATABASE || getEngineName() == "Atomic");
        DatabaseCatalog::instance().addUUIDMapping(table_id.uuid, shared_from_this(), table);
    }
    LOG_WARNING(log, "Attached {}", table_name);
}

void DatabaseWithOwnTablesBase::shutdown()
{
    LOG_WARNING(log, "Shutdown 1");
    /// You can not hold a lock during shutdown.
    /// Because inside `shutdown` function tables can work with database, and mutex is not recursive.

    started_up = false;

    Tables tables_snapshot;
    {
        std::lock_guard lock(mutex);
        tables_snapshot = tables;
    }

    for (const auto & kv : tables_snapshot)
    {
        auto table_id = kv.second->getStorageID();
        kv.second->shutdown();
        if (table_id.hasUUID())
        {
            assert(getDatabaseName() == DatabaseCatalog::TEMPORARY_DATABASE || getEngineName() == "Atomic");
            DatabaseCatalog::instance().removeUUIDMapping(table_id.uuid);
        }
    }

    std::lock_guard lock(mutex);
    tables.clear();

    LOG_WARNING(log, "Shutdown 2");
}

DatabaseWithOwnTablesBase::~DatabaseWithOwnTablesBase()
{
    try
    {
        DatabaseWithOwnTablesBase::shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

StoragePtr DatabaseWithOwnTablesBase::getTableUnlocked(const String & table_name, std::unique_lock<std::mutex> &) const
{
    auto it = tables.find(table_name);
    if (it != tables.end())
        return it->second;
    throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist.",
                    backQuote(database_name), backQuote(table_name));
}

void DatabaseWithOwnTablesBase::assertDatabaseStartedUp() const
{
    //if (!started_up)
    //    throw Exception("Database is not started up.", ErrorCodes::UNKNOWN_DATABASE);
    DatabaseCatalog::instance().assertDatabaseExists(database_name);
}

}
