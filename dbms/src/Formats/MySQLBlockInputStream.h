#pragma once

#include <string>
#include <variant>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <mysqlxx/PoolWithFailover.h>
#include <mysqlxx/Query.h>
#include <Core/ExternalResultDescription.h>


namespace DB
{
/// Allows processing results of a MySQL query as a sequence of Blocks, simplifies chaining
class MySQLBlockInputStream final : public IBlockInputStream
{
public:
    /// mysqlxx::Pool object must be alive while we use an entry from it, so we have to hold shared_ptr to poll object
    /// (otherwise pool from StorageMySQL may be destructed while MySQLBlockInputStream is alive, because of table functions).
    /// However, there is also mysqlxx::PoolWithFailover, which has no common base classes with mysqlxx::Pool
    using SharedPoolOrEntryFromPoolWithFilover = std::variant<std::shared_ptr<mysqlxx::Pool>, mysqlxx::PoolWithFailover::Entry>;

    MySQLBlockInputStream(
        SharedPoolOrEntryFromPoolWithFilover pool_or_entry,
        const std::string & query_str,
        const Block & sample_block,
        const UInt64 max_block_size_,
        const bool auto_close_ = false);

    String getName() const override { return "MySQL"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    Block readImpl() override;

    /// pool must be alive while poll entry is used
    std::shared_ptr<mysqlxx::Pool> pool;
    mysqlxx::PoolWithFailover::Entry entry;
    mysqlxx::Query query;
    mysqlxx::UseQueryResult result;
    const UInt64 max_block_size;
    const bool auto_close;
    ExternalResultDescription description;
};

}
