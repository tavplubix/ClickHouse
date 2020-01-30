#pragma once
#include <Databases/DatabaseMemory.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

class DatabaseMemoryAtomic : public DatabaseMemory
{
public:
    DatabaseMemoryAtomic(const String & name_);
    String getEngineName() const override { return "MemoryAtomic"; }

    void dropTable(const Context & context, const String & table_name) override;

    String getTableDataPath(const String & table_name) const override;
    String getTableDataPath(const ASTCreateQuery & query) const override;

private:
    void dropTableDataTask();

    using TablesToDrop = std::list<std::pair<StoragePtr, ASTPtr>>;
    TablesToDrop tables_to_drop;
    BackgroundSchedulePoolTaskHolder drop_task;
};

}
