#include "DatabaseMemoryAtomic.h"

namespace DB
{

DatabaseMemoryAtomic::DatabaseMemoryAtomic(const String & name_)
    : DatabaseMemory(name_)
{
}

void DatabaseMemoryAtomic::dropTable(const Context & context, const String & table_name)
{
    std::lock_guard lock{mutex};
    StoragePtr table = detachTableUnlocked(table_name);
    ASTPtr create =

}

String DatabaseMemoryAtomic::getTableDataPath(const String & table_name) const
{
    getCreateTableQuery()
}

String DatabaseMemoryAtomic::getTableDataPath(const ASTCreateQuery & query) const
{
    constexpr size_t uuid_prefix_len = 3;
    return "store/" + toString(query.uuid).substr(0, uuid_prefix_len) + '/' + toString(query.uuid) + '/';
}

void DatabaseMemoryAtomic::dropTableDataTask()
{

}

}
