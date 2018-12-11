#pragma once

#include <Core/Types.h>
#include <IO/WriteBuffer.h>


namespace DB
{

class NamesAndTypesList;

class ReplicatedMergeTreeColumnsHash
{
public:
    static ReplicatedMergeTreeColumnsHash fromZNode(const String & zk_columns_node);
    static ReplicatedMergeTreeColumnsHash fromColumns(const NamesAndTypesList & columns);

    void write(WriteBuffer & out) const;
    String toString() const { return hash_hex; }

    bool operator==(const ReplicatedMergeTreeColumnsHash & other) const { return hash_hex == other.hash_hex; }
    bool operator!=(const ReplicatedMergeTreeColumnsHash & other) const { return !(*this == other); }

private:
    ReplicatedMergeTreeColumnsHash(String hash_hex_) : hash_hex(std::move(hash_hex_)) {}

    String hash_hex;
};

}
