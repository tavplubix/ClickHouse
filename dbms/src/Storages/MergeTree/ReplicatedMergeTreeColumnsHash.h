#pragma once

#include <Core/Types.h>
#include <IO/WriteBuffer.h>
#include <array>


namespace DB
{

class NamesAndTypesList;

class ReplicatedMergeTreeColumnsHash
{
public:
    static ReplicatedMergeTreeColumnsHash fromZNode(const String & zk_columns_node);
    static ReplicatedMergeTreeColumnsHash fromColumns(const NamesAndTypesList & columns);

    void write(WriteBuffer & out) const;
    String toString() const { return String(hash.begin(), hash.end()); }

    const std::array<char, 16> & get() const { return hash; }

    bool operator==(const ReplicatedMergeTreeColumnsHash & other) const { return hash == other.hash; }
    bool operator!=(const ReplicatedMergeTreeColumnsHash & other) const { return !(*this == other); }

private:
    ReplicatedMergeTreeColumnsHash(std::array<char, 16> hash_) : hash(std::move(hash_)) {}

    std::array<char, 16> hash;
};

}
