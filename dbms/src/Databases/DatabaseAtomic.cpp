#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseOnDisk.h>
#include <Poco/File.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
}

DatabaseAtomic::DatabaseAtomic(String name_, String metadata_path_, Context & context_)
    : DatabaseOrdinary(name_, metadata_path_, context_)
{
    data_path = "store/";
    auto log_name = "DatabaseAtomic (" + name_ + ")";
    log = &Logger::get(log_name);
    drop_task = context_.getSchedulePool().createTask(log_name, [this](){ this->dropTableDataTask(); });
}

String DatabaseAtomic::getTableDataPath(const String & table_name) const
{
    std::lock_guard lock(mutex);
    auto it = table_name_to_path.find(table_name);
    if (it == table_name_to_path.end())
        throw Exception("Table " + table_name + " not found in database " + getDatabaseName(), ErrorCodes::UNKNOWN_TABLE);
    assert(it->second != data_path && !it->second.empty());
    return it->second;
}

String DatabaseAtomic::getTableDataPath(const ASTCreateQuery & query) const
{
    //stringToUUID(query.uuid);   /// Check UUID is valid
    const size_t uuid_prefix_len = 3;
    auto tmp = data_path + toString(query.uuid).substr(0, uuid_prefix_len) + '/' + toString(query.uuid) + '/';
    assert(tmp != data_path && !tmp.empty());
    return tmp;

}

void DatabaseAtomic::drop(const Context &)
{
    Poco::File(getMetadataPath()).remove(false);
}

void DatabaseAtomic::attachTable(const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assert(relative_table_path != data_path && !relative_table_path.empty());
    DatabaseWithDictionaries::attachTable(name, table, relative_table_path);
    std::lock_guard lock(mutex);
    table_name_to_path.emplace(std::make_pair(name, relative_table_path));
}

StoragePtr DatabaseAtomic::detachTable(const String & name)
{
    {
        std::lock_guard lock(mutex);
        table_name_to_path.erase(name);
    }
    return DatabaseWithDictionaries::detachTable(name);
}

void DatabaseAtomic::dropTable(const Context & /*context*/, const String & /*table_name*/)
{
    //auto table = detachTable(table_name);
    //auto detach_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

}

void DatabaseAtomic::renameTable(const Context & context, const String & table_name, IDatabase & to_database,
                                 const String & to_table_name)
{
    if (typeid(*this) != typeid(to_database))
    {
        if (!typeid_cast<DatabaseOrdinary *>(&to_database))
            throw Exception("Moving tables between databases of different engines is not supported", ErrorCodes::NOT_IMPLEMENTED);
        /// Allow moving tables from Atomic to Ordinary (with table lock)
        DatabaseOnDisk::renameTable(context, table_name, to_database, to_table_name);
        return;
    }

    StoragePtr table = tryGetTable(context, table_name);

    if (!table)
        throw Exception("Table " + backQuote(getDatabaseName()) + "." + backQuote(table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    /// Update database and table name in memory without moving any data on disk
    table->renameInMemory(to_database.getDatabaseName(), to_table_name);

    /// NOTE Non-atomic.
    to_database.attachTable(to_table_name, table, getTableDataPath(table_name));
    detachTable(table_name);
    Poco::File(getObjectMetadataPath(table_name)).renameTo(to_database.getObjectMetadataPath(to_table_name));
}

void DatabaseAtomic::loadStoredObjects(Context & context, bool has_force_restore_data_flag)
{
    DatabaseOrdinary::loadStoredObjects(context, has_force_restore_data_flag);
    drop_task->activateAndSchedule();
}

void DatabaseAtomic::shutdown()
{
    drop_task->deactivate();
    DatabaseWithDictionaries::shutdown();
}

void DatabaseAtomic::dropTableDataTask()
{
    try
    {
        StoragePtr table_to_drop;
        {
            std::lock_guard lock(tables_to_drop_mutex);
            time_t current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            auto it = std::find_if(tables_to_drop.begin(), tables_to_drop.end(), [current_time](const TableWithDropTime & elem)
            {
                return elem.first.unique() && elem.second + drop_delay_s < current_time;
            });
            if (it == tables_to_drop.end())
                return;
        }
        if (!table_to_drop)
            return;

        table_to_drop->drop();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }
}


}

