#pragma once

#include "common/common.hpp"
#include "common/vector.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/boost_typedefs.hpp"
#include <queue>
#include <unordered_set>
#include <algorithm>

namespace duckdb {

class ClientContext;
class ExtentIterator;
class LogicalType;
class DataChunk;
class PartitionCatalogEntry;

// Square-root, Sturges, Freedman–Diaconis
enum class BinningMethod : uint8_t { SQRT, STURGES, RICE, SCOTT, CONST };

struct SimpleHistogram {
    std::vector<int64_t> boundaries;  // bin edges
    std::vector<uint64_t> counts;     // counts per bin
    void fill(int64_t value) {
        // upper_bound gives the first boundary strictly > value, which is the
        // end-boundary of the half-open bin [b_i, b_{i+1}) containing value.
        auto it = std::upper_bound(boundaries.begin(), boundaries.end(), value);
        size_t bin = std::distance(boundaries.begin(), it);
        if (bin > 0) bin--;
        if (bin < counts.size()) counts[bin]++;
    }
    uint64_t at(size_t i) const { return counts[i]; }
};

//! Class for creating histogram
class HistogramGenerator {
private:
    std::queue<ExtentIterator *> ext_its;
    std::vector<std::vector<int64_t>*> accms;
    std::vector<idx_t> target_cols;

public:
    //! ctor
    HistogramGenerator() {}

    //! dtor
    ~HistogramGenerator() {
        _clear_accms();
    }

    //! Create histogram for all partitions in the database
    void CreateHistogram(std::shared_ptr<ClientContext> client);

    //! Create histogram for the specific partition
    void CreateHistogram(std::shared_ptr<ClientContext> client, string &part_name);

    //! Create histogram for the specific partition
    void CreateHistogram(std::shared_ptr<ClientContext> client, idx_t partition_oid);

private:
    //! Create histogram internal function
    void _create_histogram(std::shared_ptr<ClientContext> client, PartitionCatalogEntry *partition_cat);

    //! Initialize accumulator for target types
    void _init_accumulators(vector<LogicalType> &universal_schema, std::vector<std::vector<double>>& probs_per_column);

    //! Iterate data chunk & accumulate values
    void _accumulate_data_for_hist(DataChunk &chunk, vector<LogicalType> &universal_schema, vector<idx_t> &target_cols_in_univ_schema);

    //! create buckets for each column
    void _create_bucket(DataChunk &chunk, vector<LogicalType> &universal_schema, vector<idx_t> &target_cols_in_univ_schema,
        std::vector<SimpleHistogram> &histograms);

    //! generate group info
    void _generate_group_info(PartitionCatalogEntry *partition_cat, PropertySchemaID_vector *ps_oids,
        vector<uint64_t> &num_buckets_for_each_column, vector<vector<uint64_t>> &frequency_values_for_each_column);

    //! clustering
    template <typename T>
    void _cluster_column(size_t num_histograms, uint64_t& num_buckets, vector<uint64_t>& frequency_values, uint64_t& num_groups, vector<uint64_t>& group_info) {
        T clustering;
        if (frequency_values.empty()) {
            frequency_values.push_back(0);
        }
        clustering.run(num_histograms, num_buckets, frequency_values);
        num_groups = clustering.num_groups;
        group_info = std::move(clustering.group_info);
    }

    //! calcualte bin boundaries
    void _calculate_bin_boundaries(std::vector<std::vector<double>>& probs_per_column, vector<uint64_t>& bin_sizes);

    //! calculate bin size. note that bin_size follows universal_schema order
    void _calculate_bin_sizes(std::shared_ptr<ClientContext> client, PartitionCatalogEntry *partition_cat, vector<uint64_t>& bin_sizes);
    uint64_t _calculate_bin_size(uint64_t num_rows, BinningMethod method = BinningMethod::STURGES);

    //! Iterate data chunk & accumulate values for NDV counting
    void _accumulate_data_for_ndv(DataChunk& chunk, vector<LogicalType> types, std::vector<std::unordered_set<uint64_t>>& ndv_counters, size_t& num_total_tuples);

    //! calculate NDV
    void _store_ndv(PropertySchemaCatalogEntry *ps_cat, vector<LogicalType> types, std::vector<std::unordered_set<uint64_t>>& ndv_counters, size_t& num_total_tuples);

    //! clear accms
    void _clear_accms() {
        for (auto &accm : accms) {
            if (accm != nullptr) {
                delete accm;
            }
        }
        accms.clear();
    }
};

} // namespace duckdb
