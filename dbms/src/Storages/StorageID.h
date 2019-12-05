#pragma once
#include <Core/Types.h>
#include <Common/quoteString.h>
#include <tuple>
#include <Interpreters/DatabaseAndTableWithAlias.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct StorageID
{
    String database_name;
    String table_name;
    String uuid;

    StorageID() = delete;

    //TODO StorageID(const ASTPtr & query_with_one_table, const Context & context) to get db and table names (and maybe uuid) from query
    //But there are a lot of different ASTs with db and table name
    //And it looks like it depends on https://github.com/ClickHouse/ClickHouse/pull/7774

    StorageID(const String & database, const String & table, const String & uuid_ = {})
            : database_name(database), table_name(table), uuid(uuid_)
    {
        assert_not_empty();
    }

    String getFullTableName() const
    {
        return (database_name.empty() ? "" : database_name + ".") + table_name;
    }

    String getNameForLogs() const
    {
        return (database_name.empty() ? "" : backQuoteIfNeed(database_name) + ".") + backQuoteIfNeed(table_name) + " (UUID " + uuid + ")";
    }

    String getId() const
    {
        //if (uuid.empty())
        return getFullTableName();
        //else
        //    return uuid;
    }

    bool operator<(const StorageID & rhs) const
    {
        /// It's needed for ViewDependencies
        if (uuid.empty() && rhs.uuid.empty())
            /// If both IDs don't have UUID, compare them like pair of strings
            return std::tie(database_name, table_name) < std::tie(rhs.database_name, rhs.table_name);
        else if (!uuid.empty() && !rhs.uuid.empty())
            /// If both IDs have UUID, compare UUIDs and ignore database and table name
            return uuid < rhs.uuid;
        else
            /// All IDs without UUID are less, then all IDs with UUID
            return uuid.empty();
    }

    void assert_not_empty() const
    {
        if (table_name.empty())
            throw Exception("empty table name", ErrorCodes::LOGICAL_ERROR);
    }
};

}
