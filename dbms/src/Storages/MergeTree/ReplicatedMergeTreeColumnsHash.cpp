#include <Storages/MergeTree/ReplicatedMergeTreeColumnsHash.h>
#include <Core/NamesAndTypes.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Common/SipHash.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/hex.h>

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ZNODE_CONTENTS;

}

ReplicatedMergeTreeColumnsHash ReplicatedMergeTreeColumnsHash::fromZNode(const String & zk_columns_node)
{
    if (startsWith(zk_columns_node, "columns"))
    {
        SipHash hash;
        hash.update(zk_columns_node.data(), zk_columns_node.size());
        char hash_data[16];
        hash.get128(hash_data);

        String result;
        result.resize(32);
        for (size_t i = 0; i < 16; ++i)
            writeHexByteLowercase(hash_data[i], &result[2 * i]);

        return ReplicatedMergeTreeColumnsHash(std::move(result));
    }
    else if (zk_columns_node.length() == 32)
        return ReplicatedMergeTreeColumnsHash(zk_columns_node);
    else
        throw Exception("Suspiciously looking columns znode, length: " + DB::toString(zk_columns_node.length()),
            ErrorCodes::BAD_ZNODE_CONTENTS);
}

ReplicatedMergeTreeColumnsHash ReplicatedMergeTreeColumnsHash::fromColumns(const NamesAndTypesList & columns)
{
    return fromZNode(columns.toString());
}

void ReplicatedMergeTreeColumnsHash::write(WriteBuffer & out) const
{
    out << hash_hex;
}

}
