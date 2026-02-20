#pragma once

#include "common/common.hpp"
#include "common/types.hpp"

#include <string>
#include <vector>
#include <unordered_map>
#include <functional>

namespace duckdb {
    #define MIN_MAX_ARRAY_SIZE 1024

    struct minmax_t {
        int64_t min = 0;
        int64_t max = 0;
    };

    struct welford_t {
        idx_t n = 0;
        int64_t mean = 0;
        int64_t M2 = 0;
    };

    // Standard C++ type aliases (replacing former boost::interprocess shared-memory types)
    using char_string = std::string;
    using idx_t_vector = std::vector<idx_t>;
    using uint16_t_vector = std::vector<uint16_t>;
    using int64_t_vector = std::vector<int64_t>;
    using uint64_t_vector = std::vector<uint64_t>;
    using void_pointer_vector = std::vector<void*>;
    using string_vector = std::vector<std::string>;
    using PartitionID_vector = std::vector<PartitionID>;
    using PropertyKeyID_vector = std::vector<PropertyKeyID>;
    using PropertySchemaID_vector = std::vector<PropertySchemaID>;
    using ChunkDefinitionID_vector = std::vector<ChunkDefinitionID>;
    using LogicalTypeId_vector = std::vector<LogicalTypeId>;
    using minmax_t_vector = std::vector<minmax_t>;
    using welford_t_vector = std::vector<welford_t>;
    using idx_t_pair_vector = std::vector<std::pair<idx_t, idx_t>>;

    // Map types (replacing former boost::unordered_map with SHM allocators)
    using PropertyToPropertySchemaVecUnorderedMap = std::unordered_map<PropertyKeyID, PropertySchemaID_vector>;
    using PropertyToPropertySchemaPairVecUnorderedMap = std::unordered_map<PropertyKeyID, idx_t_pair_vector>;
    using PropertyToIdxUnorderedMap = std::unordered_map<PropertyKeyID, idx_t>;
    using PropertyNameToColumnIdxUnorderedMap = std::unordered_map<std::string, idx_t>;

} // namespace duckdb
