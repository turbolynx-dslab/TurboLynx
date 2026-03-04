#pragma once
#include <filesystem>
#include <string>
#include <vector>
#include <algorithm>
#include <stdexcept>

#include "main/database.hpp"
#include "main/client_context.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "catalog/catalog_entry/partition_catalog_entry.hpp"
#include "catalog/catalog_entry/property_schema_catalog_entry.hpp"
#include "common/constants.hpp"
#include "common/enums/graph_component_type.hpp"

#include "dataset_registry.hpp"

namespace fs = std::filesystem;

namespace bulktest {

class DbVerifier {
public:
    explicit DbVerifier(const fs::path& workspace)
        : db_(workspace.string().c_str())
        , ctx_(std::make_shared<duckdb::ClientContext>(db_.instance))
        , catalog_(db_.instance->GetCatalog())
    {
        graph_ = catalog_.GetEntry<duckdb::GraphCatalogEntry>(
            *ctx_, DEFAULT_SCHEMA, DEFAULT_GRAPH);
        if (!graph_)
            throw std::runtime_error("GraphCatalogEntry not found in workspace: " + workspace.string());
    }

    // Check that all expected vertex labels exist in the catalog
    void check_labels(const DatasetConfig& cfg) {
        std::vector<std::string> actual_labels;
        graph_->GetVertexLabels(actual_labels);
        for (const auto& v : cfg.vertices) {
            bool found = std::find(actual_labels.begin(), actual_labels.end(), v.label)
                         != actual_labels.end();
            if (!found)
                throw std::runtime_error("Missing vertex label in catalog: " + v.label);
        }
    }

    // Check that all expected edge types exist in the catalog
    void check_edge_types(const DatasetConfig& cfg) {
        std::vector<std::string> actual_types;
        graph_->GetEdgeTypes(actual_types);
        for (const auto& e : cfg.edges) {
            bool found = std::find(actual_types.begin(), actual_types.end(), e.type)
                         != actual_types.end();
            if (!found)
                throw std::runtime_error("Missing edge type in catalog: " + e.type);
        }
    }

    // Check vertex counts (skips if expected_count == 0)
    void check_vertex_counts(const DatasetConfig& cfg) {
        for (const auto& v : cfg.vertices) {
            if (v.expected_count == 0) continue;
            uint64_t actual = count_label(v.label, duckdb::GraphComponentType::VERTEX);
            if (actual != v.expected_count) {
                throw std::runtime_error(
                    "Vertex count mismatch for label " + v.label +
                    ": expected " + std::to_string(v.expected_count) +
                    ", got " + std::to_string(actual));
            }
        }
    }

    // Measure vertex counts and return them (for --generate mode)
    std::vector<uint64_t> measure_vertex_counts(const DatasetConfig& cfg) {
        std::vector<uint64_t> counts;
        for (const auto& v : cfg.vertices)
            counts.push_back(count_label(v.label, duckdb::GraphComponentType::VERTEX));
        return counts;
    }

    // Check forward edge counts (skips if expected_fwd_count == 0).
    // count_label(type, EDGE) returns the forward edge count only; backward edges
    // are stored in the adjacency-list index, not in PropertySchemaCatalogEntry rows.
    void check_edge_counts(const DatasetConfig& cfg) {
        for (const auto& e : cfg.edges) {
            if (e.expected_fwd_count == 0) continue;
            uint64_t actual = count_label(e.type, duckdb::GraphComponentType::EDGE);
            if (actual != e.expected_fwd_count) {
                throw std::runtime_error(
                    "Edge count mismatch for type " + e.type +
                    ": expected " + std::to_string(e.expected_fwd_count) +
                    ", got " + std::to_string(actual));
            }
        }
    }

    // Measure forward edge counts for --generate mode.
    std::vector<uint64_t> measure_edge_counts(const DatasetConfig& cfg) {
        std::vector<uint64_t> counts;
        for (const auto& e : cfg.edges)
            counts.push_back(count_label(e.type, duckdb::GraphComponentType::EDGE));
        return counts;
    }

    uint64_t count_label(const std::string& label, duckdb::GraphComponentType gtype) {
        std::vector<duckdb::idx_t> part_oids =
            graph_->LookupPartition(*ctx_, {label}, gtype);
        uint64_t total = 0;
        for (auto oid : part_oids) {
            auto* part = (duckdb::PartitionCatalogEntry*)
                catalog_.GetEntry(*ctx_, DEFAULT_SCHEMA, oid);
            if (!part) continue;
            std::vector<duckdb::idx_t> ps_oids;
            part->GetPropertySchemaIDs(ps_oids);
            for (auto psoid : ps_oids) {
                auto* ps = (duckdb::PropertySchemaCatalogEntry*)
                    catalog_.GetEntry(*ctx_, DEFAULT_SCHEMA, psoid);
                if (!ps || ps->is_fake) continue;
                total += ps->GetNumberOfRowsApproximately();
            }
        }
        return total;
    }

private:
    duckdb::DuckDB                         db_;
    std::shared_ptr<duckdb::ClientContext> ctx_;
    duckdb::Catalog&                       catalog_;
    duckdb::GraphCatalogEntry*             graph_ = nullptr;
};

} // namespace bulktest
