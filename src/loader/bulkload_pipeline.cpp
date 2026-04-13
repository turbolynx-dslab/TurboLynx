#include "loader/bulkload_pipeline.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "main/capi/turbolynx.h"
#include "storage/graph_storage_wrapper.hpp"
#include "storage/extent/extent_manager.hpp"
#include "storage/extent/extent_iterator.hpp"
#include "storage/cache/chunk_cache_manager.h"
#include "storage/statistics/histogram_generator.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_manager.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_extent_info.hpp"
#include "parser/parsed_data/create_chunkdefinition_info.hpp"
#include "common/typedef.hpp"
#include "common/range.hpp"
#include "common/logger.hpp"
#include "common/disk_aio_init.hpp"
#include "common/graph_csv_reader.hpp"
#include "common/graph_simdcsv_parser.hpp"
#include "common/graph_simdjson_parser.hpp"
#include "common/flat_hash_map.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"

#include <filesystem>
#include <set>
#include <fstream>
#include <thread>
#include <mutex>
#include <omp.h>

namespace fs = std::filesystem;

namespace duckdb {

// ---------------------------------------------------------------------------
// BulkloadContext — internal detail, not exposed in the header
// ---------------------------------------------------------------------------

struct BulkloadContext {
    BulkloadOptions input_options;
    std::shared_ptr<ClientContext> client;
    int64_t conn_id = -1;
    Catalog &catalog;
    GraphCatalogEntry *graph_cat;
    ExtentManager ext_mng;
    vector<
        std::pair<string, FlatHashMap<LidPair, idx_t, LidPairHash>>>
        lid_to_pid_map;  // For Forward & Backward AdjList
    unordered_map<string, size_t> lid_to_pid_map_index;  // label → lid_to_pid_map index (exact match)

    std::vector<std::string> skipped_labels;

    BulkloadContext(BulkloadOptions input_options, std::shared_ptr<ClientContext> client,
                    Catalog &catalog, CatalogWrapper &catalog_wrapper,
                    CreateGraphInfo &graph_info)
        : input_options(std::move(input_options)),
          client(std::move(client)),
          catalog(catalog)
    {
        // Always use GetEntry — the DuckDB constructor already creates a
        // default empty graph entry via LoadCatalog().  Calling CreateGraph()
        // again would allocate a *new* GraphCatalogEntry that is NOT stored
        // in the CatalogSet (because the mapping already exists), so any
        // mutations (AddVertexPartition, etc.) would go to a dangling copy
        // and SaveCatalog would serialize the original empty one.
        if (this->input_options.incremental) {
            spdlog::info("[BulkloadContext] Incremental mode detected; loading existing graph catalog");
        } else {
            spdlog::info("[BulkloadContext] Creating new graph catalog");
        }
        graph_cat = (GraphCatalogEntry*)catalog.GetEntry(*(this->client.get()), CatalogType::GRAPH_ENTRY,
                    graph_info.schema, graph_info.graph);
        if (!graph_cat) {
            throw std::runtime_error("[BulkloadContext] Failed to get graph catalog entry");
        }
        duckdb::SetClientWrapper(this->client, make_shared<CatalogWrapper>(catalog_wrapper));
    }
};

// ---------------------------------------------------------------------------
// Utility helpers (file-scope static)
// ---------------------------------------------------------------------------

static bool FolderExists(const std::string& folder_path) {
    return fs::exists(folder_path) && fs::is_directory(folder_path);
}

static bool CreateDirectoryIfNotExists(const std::string& folder_path) {
    try {
        if (!FolderExists(folder_path)) {
            spdlog::info("[CreateDirectory] Creating directory: {}", folder_path);
            return fs::create_directories(folder_path);
        }
        spdlog::info("[CreateDirectory] Directory already exists: {}", folder_path);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error creating directory: " << e.what() << std::endl;
        return false;
    }
}

static void RemoveAllFilesInDirectory(const std::string& directory) {
    try {
        if (fs::exists(directory) && fs::is_directory(directory)) {
            for (const auto& entry : fs::directory_iterator(directory)) {
                fs::remove_all(entry.path());
            }
            spdlog::info("[BulkloadPipeline] Cleaned output directory: {}", directory);
        }
    } catch (const std::exception& e) {
        spdlog::error("[BulkloadPipeline] Failed to clean output directory: {}. Error: {}", directory, e.what());
    }
}

static inline bool isSkippedLabel(BulkloadContext& bulkload_ctx, string &label) {
    return std::find(bulkload_ctx.skipped_labels.begin(), bulkload_ctx.skipped_labels.end(), label) != bulkload_ctx.skipped_labels.end();
}

static inline bool isJSONFile(const std::string &file_path_str) {
    std::filesystem::path file_path(file_path_str);
    std::string ext = file_path.extension().string();
    return ext == ".json";
}

static void SeperateFilesByExtension(vector<LabeledFile>& files,
                                     vector<LabeledFile>& json_files,
                                     vector<LabeledFile>& csv_files) {
    for (auto &file: files) {
        if (isJSONFile(std::get<1>(file))) {
            json_files.push_back(file);
        } else {
            csv_files.push_back(file);
        }
    }
}

static void ParseLabelSet(string &labelset, vector<string> &parsed_labelset) {
    std::istringstream iss(labelset);
    string label;
    while (std::getline(iss, label, ':')) {
        parsed_labelset.push_back(label);
    }
}

// ---------------------------------------------------------------------------
// Multi-label CSV expansion: auto-detect Neo4j ":LABEL" column
// ---------------------------------------------------------------------------
// Scans each vertex CSV header for a ":LABEL" column (Neo4j standard).
// If found, splits the CSV by label values.  Neo4j format uses semicolons
// to separate labels, e.g. "Organisation;Company".  The LAST label is the
// sub-type and preceding labels are parents.  The split produces entries
// like "Company:Organisation" with temp CSV files per sub-type.
//
// Files without a ":LABEL" column are passed through unchanged.
// ---------------------------------------------------------------------------

static void ExpandMultiLabelVertexFiles(vector<LabeledFile> &csv_vertex_files,
                                         const string &workspace_path) {
    vector<LabeledFile> expanded;
    for (auto &vf : csv_vertex_files) {
        string &labelset   = std::get<0>(vf);
        string &file_path  = std::get<1>(vf);

        // Read the CSV header
        std::ifstream infile(file_path);
        if (!infile.is_open()) {
            expanded.push_back(std::move(vf));
            continue;
        }

        string header_line;
        std::getline(infile, header_line);

        // Parse header fields (pipe-delimited)
        vector<string> header_fields;
        {
            std::istringstream hss(header_line);
            string field;
            while (std::getline(hss, field, '|')) header_fields.push_back(field);
        }

        // Find ":LABEL" column — Neo4j standard header name
        int label_col_idx = -1;
        for (size_t i = 0; i < header_fields.size(); i++) {
            if (header_fields[i] == ":LABEL") { label_col_idx = (int)i; break; }
        }

        // No :LABEL column — pass through unchanged
        if (label_col_idx < 0) {
            infile.close();
            expanded.push_back(std::move(vf));
            continue;
        }

        spdlog::info("[ExpandMultiLabel] Detected :LABEL column in {} (col {})",
                     file_path, label_col_idx);

        // Build header without the :LABEL column
        string new_header;
        for (size_t i = 0; i < header_fields.size(); i++) {
            if ((int)i == label_col_idx) continue;
            if (!new_header.empty()) new_header += '|';
            new_header += header_fields[i];
        }

        // Read all data lines, categorize by sub-label
        // Neo4j :LABEL value: "Parent;Child" → sub_label = last token
        // Labelset becomes "Child:Parent" (colon-separated, child first)
        unordered_map<string, vector<string>> sublabel_to_lines;
        unordered_map<string, string> sublabel_to_labelset;  // "Company" → "Company:Organisation"
        string line;
        while (std::getline(infile, line)) {
            if (line.empty()) continue;
            vector<string> fields;
            {
                std::istringstream lss(line);
                string f;
                while (std::getline(lss, f, '|')) fields.push_back(f);
            }
            if (label_col_idx >= (int)fields.size()) continue;

            string label_value = fields[label_col_idx];

            // Parse semicolon-separated labels: "Organisation;Company"
            // Last token = sub-label, everything else = parent labels
            vector<string> labels;
            {
                std::istringstream lss(label_value);
                string tok;
                while (std::getline(lss, tok, ';')) labels.push_back(tok);
            }
            string sub_label = labels.back();

            // Build labelset: "Child:Parent1:Parent2" (child first, then parents)
            if (sublabel_to_labelset.find(sub_label) == sublabel_to_labelset.end()) {
                string ls = sub_label;
                for (int j = (int)labels.size() - 2; j >= 0; j--) {
                    ls += ":" + labels[j];
                }
                sublabel_to_labelset[sub_label] = ls;
            }

            // Build line without the :LABEL column
            string new_line;
            for (size_t i = 0; i < fields.size(); i++) {
                if ((int)i == label_col_idx) continue;
                if (!new_line.empty()) new_line += '|';
                new_line += fields[i];
            }
            sublabel_to_lines[sub_label].push_back(std::move(new_line));
        }
        infile.close();

        // Create temp directory for split files
        string split_dir = workspace_path + "/.bulkload_split";
        fs::create_directories(split_dir);

        // Write a temp CSV per sub-label and add to expanded list
        for (auto &[sub_label, lines] : sublabel_to_lines) {
            string tmp_path = split_dir + "/" + sub_label + ".csv";
            std::ofstream out(tmp_path);
            out << new_header << "\n";
            for (auto &l : lines) out << l << "\n";
            out.close();

            string new_labelset = sublabel_to_labelset[sub_label];
            spdlog::info("[ExpandMultiLabel]   {} → {} ({} rows)", new_labelset, tmp_path, lines.size());
            expanded.emplace_back(LabeledFile{new_labelset, tmp_path, std::nullopt});
        }
    }
    csv_vertex_files = std::move(expanded);
}


// ---------------------------------------------------------------------------
// Multi-label edge CSV expansion: split edge files by sub-partition
// ---------------------------------------------------------------------------
// When vertex CSVs were split by :LABEL (e.g., Place → City, Country, Continent),
// edge CSVs that reference the parent label (e.g., :END_ID(Place)) must be split
// so that each (src_sub, dst_sub) combination becomes a separate edge file.
// This keeps the downstream edge loading code unchanged (1 src, 1 dst partition).
//
// Uses lid_to_pid_map to determine which sub-partition each row belongs to,
// then routes rows to per-(src_sub, dst_sub) temp CSV files.
// ---------------------------------------------------------------------------

static void ExpandMultiLabelEdgeFiles(vector<LabeledFile> &csv_edge_files,
                                       BulkloadContext &bulkload_ctx) {
    // Phase 1: Build PartitionID → primary label mapping from vertex partitions.
    // Primary label = first token of the labelset (e.g., "City" from "City:Place").
    // This matches what lid_to_pid_map_index uses as keys.
    unordered_map<PartitionID, string> pid_to_primary_label;
    {
        auto *vp_oids = bulkload_ctx.graph_cat->GetVertexPartitionOids();
        for (auto oid : *vp_oids) {
            auto *pcat = (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(
                *(bulkload_ctx.client.get()), DEFAULT_SCHEMA, oid);
            if (!pcat) continue;
            PartitionID pid = pcat->GetPartitionID();
            // Strip "vpart_" prefix to get labelset (e.g., "City:Place")
            string name = pcat->GetName();
            string prefix = DEFAULT_VERTEX_PARTITION_PREFIX;
            if (name.substr(0, prefix.size()) == prefix)
                name = name.substr(prefix.size());
            // Extract primary label (first token before ':')
            auto colon = name.find(':');
            string primary = (colon != string::npos) ? name.substr(0, colon) : name;
            pid_to_primary_label[pid] = primary;
        }
    }

    // Phase 2: For each edge file, check if src/dst labels have multiple partitions.
    string split_dir = bulkload_ctx.input_options.output_dir + "/.bulkload_split";

    vector<LabeledFile> expanded;
    for (auto &ef : csv_edge_files) {
        string &edge_type  = std::get<0>(ef);
        string &file_path  = std::get<1>(ef);

        // Lightweight header-only parse: read first line to extract src/dst labels
        // from ":START_ID(Label)" / ":END_ID(Label)" columns.
        // This avoids the expensive full-file load that InitCSVFile performs.
        string src_label, dst_label;
        {
            std::ifstream hdr_in(file_path);
            if (hdr_in.is_open()) {
                string header_line;
                std::getline(hdr_in, header_line);
                // Parse pipe-delimited header fields
                std::istringstream hss(header_line);
                string field;
                while (std::getline(hss, field, '|')) {
                    // Match START_ID variants: ":START_ID(Label)", ":START_ID_1(Label)", etc.
                    auto start_pos = field.find("START_ID");
                    if (start_pos != string::npos) {
                        auto paren_start = field.find('(', start_pos);
                        auto paren_end = field.find(')', paren_start != string::npos ? paren_start : 0);
                        if (paren_start != string::npos && paren_end != string::npos)
                            src_label = field.substr(paren_start + 1, paren_end - paren_start - 1);
                        continue;
                    }
                    // Match END_ID variants: ":END_ID(Label)", ":END_ID_1(Label)", etc.
                    auto end_pos = field.find("END_ID");
                    if (end_pos != string::npos) {
                        auto paren_start = field.find('(', end_pos);
                        auto paren_end = field.find(')', paren_start != string::npos ? paren_start : 0);
                        if (paren_start != string::npos && paren_end != string::npos)
                            dst_label = field.substr(paren_start + 1, paren_end - paren_start - 1);
                    }
                }
            }
        }

        auto src_oids = bulkload_ctx.graph_cat->LookupPartition(
            *(bulkload_ctx.client.get()), { src_label }, GraphComponentType::VERTEX);
        auto dst_oids = bulkload_ctx.graph_cat->LookupPartition(
            *(bulkload_ctx.client.get()), { dst_label }, GraphComponentType::VERTEX);
        bool src_multi = src_oids.size() > 1;
        bool dst_multi = dst_oids.size() > 1;

        if (!src_multi && !dst_multi) {
            expanded.push_back(std::move(ef));
            continue;
        }

        spdlog::info("[ExpandMultiLabelEdge] Splitting {} ({}): src={}{} dst={}{}",
                     file_path, edge_type,
                     src_label, src_multi ? " [MULTI]" : "",
                     dst_label, dst_multi ? " [MULTI]" : "");

        // Get lid_to_pid maps for src and dst
        auto src_map_it = bulkload_ctx.lid_to_pid_map_index.find(src_label);
        auto dst_map_it = bulkload_ctx.lid_to_pid_map_index.find(dst_label);
        if (src_map_it == bulkload_ctx.lid_to_pid_map_index.end() ||
            dst_map_it == bulkload_ctx.lid_to_pid_map_index.end()) {
            spdlog::warn("[ExpandMultiLabelEdge] Missing lid_to_pid_map for src={} or dst={}, skipping split",
                         src_label, dst_label);
            expanded.push_back(std::move(ef));
            continue;
        }
        auto &src_pid_map = bulkload_ctx.lid_to_pid_map[src_map_it->second].second;
        auto &dst_pid_map = bulkload_ctx.lid_to_pid_map[dst_map_it->second].second;

        // Read the CSV header line
        std::ifstream infile(file_path);
        if (!infile.is_open()) {
            expanded.push_back(std::move(ef));
            continue;
        }
        string header_line;
        std::getline(infile, header_line);

        // Parse header fields to find src/dst column positions (pipe-delimited)
        vector<string> header_fields;
        {
            std::istringstream hss(header_line);
            string field;
            while (std::getline(hss, field, '|')) header_fields.push_back(field);
        }

        // Find src ID columns (":START_ID(Label)")  and dst ID columns (":END_ID(Label)")
        vector<int> src_id_cols, dst_id_cols;
        for (int i = 0; i < (int)header_fields.size(); i++) {
            if (header_fields[i].find(":START_ID") != string::npos) src_id_cols.push_back(i);
            if (header_fields[i].find(":END_ID") != string::npos) dst_id_cols.push_back(i);
        }

        // Route rows to per-(src_partition, dst_partition) buckets
        // Key: (src_labelset, dst_labelset) → vector of lines
        using PairKey = std::pair<string, string>;
        struct PairHash {
            size_t operator()(const PairKey &p) const {
                return std::hash<string>()(p.first) ^ (std::hash<string>()(p.second) << 32);
            }
        };
        unordered_map<PairKey, vector<string>, PairHash> buckets;

        string line;
        idx_t total_rows = 0, skipped_rows = 0;
        while (std::getline(infile, line)) {
            if (line.empty()) continue;
            total_rows++;

            // Parse fields
            vector<string> fields;
            {
                std::istringstream lss(line);
                string f;
                while (std::getline(lss, f, '|')) fields.push_back(f);
            }

            // Look up src LID → PID → PartitionID → labelset
            string src_ls = src_label;  // default: unchanged
            if (src_multi && src_id_cols.size() >= 1) {
                LidPair src_lid{0, 0};
                src_lid.first = std::stoull(fields[src_id_cols[0]]);
                if (src_id_cols.size() >= 2) src_lid.second = std::stoull(fields[src_id_cols[1]]);
                const idx_t *sp = src_pid_map.get_ptr(src_lid);
                if (!sp) { skipped_rows++; continue; }
                PartitionID src_pid = (PartitionID)(*sp >> 48);
                auto sit = pid_to_primary_label.find(src_pid);
                if (sit != pid_to_primary_label.end()) src_ls = sit->second;
            }

            // Look up dst LID → PID → PartitionID → labelset
            string dst_ls = dst_label;  // default: unchanged
            if (dst_multi && dst_id_cols.size() >= 1) {
                LidPair dst_lid{0, 0};
                dst_lid.first = std::stoull(fields[dst_id_cols[0]]);
                if (dst_id_cols.size() >= 2) dst_lid.second = std::stoull(fields[dst_id_cols[1]]);
                const idx_t *dp = dst_pid_map.get_ptr(dst_lid);
                if (!dp) { skipped_rows++; continue; }
                PartitionID dst_pid = (PartitionID)(*dp >> 48);
                auto dit = pid_to_primary_label.find(dst_pid);
                if (dit != pid_to_primary_label.end()) dst_ls = dit->second;
            }

            buckets[{src_ls, dst_ls}].push_back(line);
        }
        infile.close();

        if (skipped_rows > 0) {
            spdlog::warn("[ExpandMultiLabelEdge] Skipped {} / {} rows (LID not found)", skipped_rows, total_rows);
        }

        // Write split files
        fs::create_directories(split_dir);
        for (auto &[key, lines] : buckets) {
            auto &[src_ls, dst_ls] = key;

            // Build header with updated :START_ID/:END_ID labels
            string new_header;
            for (int i = 0; i < (int)header_fields.size(); i++) {
                if (i > 0) new_header += '|';
                if (header_fields[i].find(":START_ID") != string::npos) {
                    new_header += ":START_ID(" + src_ls + ")";
                } else if (header_fields[i].find(":END_ID") != string::npos) {
                    new_header += ":END_ID(" + dst_ls + ")";
                } else {
                    new_header += header_fields[i];
                }
            }

            string tmp_name = edge_type + "_" + src_ls + "_" + dst_ls + ".csv";
            string tmp_path = split_dir + "/" + tmp_name;

            std::ofstream out(tmp_path);
            out << new_header << "\n";
            for (auto &l : lines) out << l << "\n";
            out.close();

            spdlog::info("[ExpandMultiLabelEdge]   {} → {} ({} rows, src={} dst={})",
                         edge_type, tmp_path, lines.size(), src_ls, dst_ls);
            expanded.emplace_back(LabeledFile{edge_type, tmp_path, std::nullopt});
        }
    }
    csv_edge_files = std::move(expanded);
}

// Flat CSR per-extent adjacency buffer (12d).
// Phase A: count()+store() per edge.  Phase B: build_offsets() fills data from raw_edges.
struct ExtentAdjBuffer {
    std::vector<uint32_t> degree;   // degree[seqno] = edge count for this src vertex
    std::vector<idx_t>    data;     // flat: [dst_pid, epid, dst_pid, epid, ...]
    std::vector<uint32_t> offset;   // write cursor during build_offsets fill pass
    bool offsets_ready = false;

    struct RawEdge { uint32_t src_seqno; idx_t dst_pid; idx_t epid; };
    std::vector<RawEdge> raw_edges; // accumulated in Phase 1, consumed in build_offsets

    void count(uint32_t src_seqno, uint32_t n_edges) {
        if (src_seqno >= degree.size()) degree.resize(src_seqno + 1, 0);
        degree[src_seqno] += n_edges;
    }
    void store(uint32_t src_seqno, idx_t dst_pid, idx_t epid) {
        raw_edges.push_back({src_seqno, dst_pid, epid});
    }
    void build_offsets() {
        uint32_t n = (uint32_t)degree.size();
        offset.resize(n, 0);
        uint32_t cur = 0;
        for (uint32_t i = 0; i < n; i++) { offset[i] = cur; cur += degree[i] * 2; }
        data.resize(cur);
        // Fill CSR from raw_edges; eliminates Phase 3 ExtentIterator scan.
        for (auto& e : raw_edges) {
            uint32_t &pos = offset[e.src_seqno];
            data[pos++] = e.dst_pid;
            data[pos++] = e.epid;
        }
        raw_edges.clear();
        raw_edges.shrink_to_fit();
        offsets_ready = true;
    }
};

// ---------------------------------------------------------------------------
// Catalog helpers
// ---------------------------------------------------------------------------

static void CreateVertexCatalogInfos(BulkloadContext& bulkload_ctx, std::string &vertex_labelset_name, vector<string> &vertex_labels, vector<string> &key_names,
                                     vector<LogicalType> &types, vector<idx_t> &key_column_idxs, PartitionCatalogEntry *&partition_cat, PropertySchemaCatalogEntry *&property_schema_cat) {
    string partition_name = DEFAULT_VERTEX_PARTITION_PREFIX + vertex_labelset_name;
    string property_schema_name = DEFAULT_VERTEX_PROPERTYSCHEMA_PREFIX + vertex_labelset_name;
    vector<PropertyKeyID> property_key_ids;

    spdlog::trace("[CreateVertexCatalogInfos] vertex_labelset_name: {}, vertex_labels: [{}], key_names: [{}], partition_name: {}, property_schema_name: {}",
                  vertex_labelset_name, join_vector(vertex_labels), join_vector(key_names), partition_name, property_schema_name);

    spdlog::debug("[CreateVertexCatalogInfos] CreatePartitionInfo");
    CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str());
    partition_cat =
        (PartitionCatalogEntry *)bulkload_ctx.catalog.CreatePartition(*(bulkload_ctx.client.get()), &partition_info);
    PartitionID new_pid = bulkload_ctx.graph_cat->GetNewPartitionID();
    spdlog::trace("[CreateVertexCatalogInfos] new_pid: {}", new_pid);

    spdlog::debug("[CreateVertexCatalogInfos] CreatePropertySchemaInfo");
    CreatePropertySchemaInfo propertyschema_info(DEFAULT_SCHEMA, property_schema_name.c_str(),
                                                  new_pid, partition_cat->GetOid());
    property_schema_cat = (PropertySchemaCatalogEntry *)bulkload_ctx.catalog.CreatePropertySchema(*(bulkload_ctx.client.get()), &propertyschema_info);

    spdlog::debug("[CreateVertexCatalogInfos] CreateIndexInfo");
    CreateIndexInfo idx_info(DEFAULT_SCHEMA, vertex_labelset_name + "_id", IndexType::PHYSICAL_ID,
                             partition_cat->GetOid(), property_schema_cat->GetOid(), 0, {-1});
    IndexCatalogEntry *index_cat = (IndexCatalogEntry *)bulkload_ctx.catalog.CreateIndex(*(bulkload_ctx.client.get()), &idx_info);

    spdlog::debug("[CreateVertexCatalogInfos] Set up catalog informations");
    bulkload_ctx.graph_cat->AddVertexPartition(*(bulkload_ctx.client.get()), new_pid, partition_cat->GetOid(), vertex_labels);
    bulkload_ctx.graph_cat->GetPropertyKeyIDs(*(bulkload_ctx.client.get()), key_names, types, property_key_ids);

    partition_cat->AddPropertySchema(*(bulkload_ctx.client.get()), property_schema_cat->GetOid(), property_key_ids);
    partition_cat->SetSchema(*(bulkload_ctx.client.get()), key_names, types, property_key_ids);
    partition_cat->SetIdKeyColumnIdxs(key_column_idxs);
    partition_cat->SetPhysicalIDIndex(index_cat->GetOid());
    partition_cat->SetPartitionID(new_pid);

    property_schema_cat->SetSchema(*(bulkload_ctx.client.get()), key_names, types, property_key_ids);
    property_schema_cat->SetPhysicalIDIndex(index_cat->GetOid());
}

static std::string MakeInternalEdgeTypeName(const std::string &edge_type,
                                             const std::string &src_label,
                                             const std::string &dst_label) {
    return edge_type + "@" + src_label + "@" + dst_label;
}

static bool IsEdgeCatalogInfoExist(BulkloadContext& bulkload_ctx, const std::string &edge_type,
                                   const std::string &src_label, const std::string &dst_label) {
    string internal_name = MakeInternalEdgeTypeName(edge_type, src_label, dst_label);
    return bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), CatalogType::PARTITION_ENTRY, DEFAULT_SCHEMA, DEFAULT_EDGE_PARTITION_PREFIX + internal_name) != nullptr;
}

static void CreateEdgeCatalogInfos(BulkloadContext& bulkload_ctx, std::string &edge_type, vector<string> &key_names, vector<LogicalType> &types, string &src_vertex_label,
                                   string &dst_vertex_label, PartitionCatalogEntry *&partition_cat, PropertySchemaCatalogEntry *&property_schema_cat,
                                   LogicalType edge_direction_type, idx_t num_src_columns,
                                   const string &fwd_src_label, const string &fwd_dst_label) {
    // Internal name is always based on the forward (canonical) direction.
    string internal_name = MakeInternalEdgeTypeName(edge_type, fwd_src_label, fwd_dst_label);
    string partition_name = DEFAULT_EDGE_PARTITION_PREFIX + internal_name;
    string property_schema_name = DEFAULT_EDGE_PROPERTYSCHEMA_PREFIX + internal_name;
    vector<PropertyKeyID> property_key_ids;
    vector<idx_t> vertex_ps_cat_oids;
    PartitionID new_pid;

    if (edge_direction_type == LogicalType::FORWARD_ADJLIST) {
        CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str());
        partition_cat =
            (PartitionCatalogEntry *)bulkload_ctx.catalog.CreatePartition(*(bulkload_ctx.client.get()), &partition_info);
        PartitionID new_pid = bulkload_ctx.graph_cat->GetNewPartitionID();

        CreatePropertySchemaInfo propertyschema_info(DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid, partition_cat->GetOid());
        property_schema_cat = (PropertySchemaCatalogEntry *)bulkload_ctx.catalog.CreatePropertySchema(*(bulkload_ctx.client.get()), &propertyschema_info);

        CreateIndexInfo id_idx_info(DEFAULT_SCHEMA, internal_name + "_id", IndexType::PHYSICAL_ID,
                                    partition_cat->GetOid(), property_schema_cat->GetOid(), 0, {-1});
        IndexCatalogEntry *id_index_cat =
            (IndexCatalogEntry *)bulkload_ctx.catalog.CreateIndex(*(bulkload_ctx.client.get()), &id_idx_info);

        bulkload_ctx.graph_cat->AddEdgePartition(*(bulkload_ctx.client.get()), new_pid, partition_cat->GetOid(), edge_type);
        bulkload_ctx.graph_cat->GetPropertyKeyIDs(*(bulkload_ctx.client.get()), key_names, types, property_key_ids);

        partition_cat->AddPropertySchema(*(bulkload_ctx.client.get()), property_schema_cat->GetOid(), property_key_ids);
        partition_cat->SetSchema(*(bulkload_ctx.client.get()), key_names, types, property_key_ids);
        partition_cat->SetPhysicalIDIndex(id_index_cat->GetOid());
        partition_cat->SetPartitionID(new_pid);

        property_schema_cat->SetSchema(*(bulkload_ctx.client.get()), key_names, types, property_key_ids);
        property_schema_cat->SetPhysicalIDIndex(id_index_cat->GetOid());

        vector<idx_t> src_vertex_part_cat_oids =
            bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { src_vertex_label }, GraphComponentType::VERTEX);
        if (src_vertex_part_cat_oids.size() != 1) throw InvalidInputException("The input src key corresponds to multiple vertex partitions.");
        PartitionCatalogEntry *src_vertex_part_cat_entry =
            (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, src_vertex_part_cat_oids[0]);

        vector<idx_t> dst_vertex_part_cat_oids =
            bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { dst_vertex_label }, GraphComponentType::VERTEX);
        if (dst_vertex_part_cat_oids.size() != 1) throw InvalidInputException("The input dst key corresponds to multiple vertex partitions.");
        PartitionCatalogEntry *dst_vertex_part_cat_entry =
            (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, dst_vertex_part_cat_oids[0]);
        src_vertex_part_cat_entry->GetPropertySchemaIDs(vertex_ps_cat_oids);
        bulkload_ctx.graph_cat->AddEdgeConnectionInfo(*(bulkload_ctx.client.get()), src_vertex_part_cat_entry->GetOid(), partition_cat->GetOid());
        partition_cat->SetSrcDstPartOid(src_vertex_part_cat_entry->GetOid(), dst_vertex_part_cat_entry->GetOid());
    } else if (edge_direction_type == LogicalType::BACKWARD_ADJLIST) {
        partition_cat =
            (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), CatalogType::PARTITION_ENTRY, DEFAULT_SCHEMA, partition_name);
        property_schema_cat =
            (PropertySchemaCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), CatalogType::PROPERTY_SCHEMA_ENTRY, DEFAULT_SCHEMA, property_schema_name);

        vector<idx_t> src_vertex_part_cat_oids =
            bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { src_vertex_label }, GraphComponentType::VERTEX);
        if (src_vertex_part_cat_oids.size() != 1) throw InvalidInputException("The input src key corresponds to multiple vertex partitions.");
        PartitionCatalogEntry *src_vertex_part_cat_entry =
            (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, src_vertex_part_cat_oids[0]);

        src_vertex_part_cat_entry->GetPropertySchemaIDs(vertex_ps_cat_oids);
    } else {
        D_ASSERT(false);
    }

    idx_t adj_col_idx; // TODO bug fix
    for (auto i = 0; i < vertex_ps_cat_oids.size(); i++) {
        PropertySchemaCatalogEntry *vertex_ps_cat_entry =
            (PropertySchemaCatalogEntry*)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, vertex_ps_cat_oids[i]);
        vertex_ps_cat_entry->AppendAdjListType({ edge_direction_type });
        adj_col_idx = vertex_ps_cat_entry->AppendAdjListKey(*(bulkload_ctx.client.get()), { internal_name });
    }

    duckdb::IndexType index_type = edge_direction_type == LogicalType::FORWARD_ADJLIST ?
        IndexType::FORWARD_CSR : IndexType::BACKWARD_CSR;
    string adj_idx_name = internal_name + (edge_direction_type == LogicalType::FORWARD_ADJLIST ?
        "_fwd" : "_bwd");
    int64_t src_key_col_idx = 1;
    int64_t dst_key_col_idx = src_key_col_idx + num_src_columns;
    vector<int64_t> adj_key_col_idxs = { src_key_col_idx, dst_key_col_idx };
    CreateIndexInfo adj_idx_info(DEFAULT_SCHEMA, adj_idx_name, index_type,
                                  partition_cat->GetOid(), property_schema_cat->GetOid(), adj_col_idx, adj_key_col_idxs);
    IndexCatalogEntry *adj_index_cat =
        (IndexCatalogEntry *)bulkload_ctx.catalog.CreateIndex(*(bulkload_ctx.client.get()), &adj_idx_info);

    partition_cat->AddAdjIndex(adj_index_cat->GetOid());
}

// ---------------------------------------------------------------------------
// AdjList helpers
// ---------------------------------------------------------------------------

// Phase 2 adj list writer (12d flat CSR).
// build_offsets() fills the CSR data from raw_edges accumulated in Phase 1,
// then writes each vertex extent's CSR chunk in the same wire format as before.
static void AppendFlatAdjListChunk(
    BulkloadContext& bulkload_ctx,
    LogicalType edge_direction_type,
    PartitionID part_id,
    ExtentID max_extent_id,
    unordered_map<ExtentID, ExtentAdjBuffer> &adj_list_buffers)
{
    spdlog::trace("[AppendFlatAdjListChunk] part_id: {}, max_extent_id: {}", part_id, max_extent_id);

    // Phase 2: build offsets and fill CSR from raw_edges accumulated in Phase 1.
    for (auto& [local_ext_id, buf] : adj_list_buffers)
        buf.build_offsets();

    // Write CSR chunks to vertex extents (same wire format as old AppendAdjListChunk)
    for (ExtentID idx = 0; idx < max_extent_id; idx++) {
        auto it2 = adj_list_buffers.find(idx);
        size_t num_adj_list = 0;
        size_t adj_len_total = 0;
        ExtentAdjBuffer* buf = nullptr;
        if (it2 != adj_list_buffers.end() && it2->second.offsets_ready) {
            buf = &it2->second;
            num_adj_list = buf->degree.size();
            adj_len_total = buf->data.size();
        }
        if (adj_len_total == 0) num_adj_list = 0;

        const size_t slot_for_num_adj = 1;
        vector<idx_t> tmp;
        tmp.resize(slot_for_num_adj + num_adj_list + adj_len_total);
        tmp[0] = num_adj_list;
        if (buf && num_adj_list > 0) {
            // offset[k] after fill = end position for vertex k in data[] (0-based).
            // Wire format: tmp[k+1] = num_adj_list + offset[k].
            for (size_t k = 0; k < num_adj_list; k++)
                tmp[k + slot_for_num_adj] = num_adj_list + buf->offset[k];
            std::copy(buf->data.begin(), buf->data.end(),
                      tmp.begin() + slot_for_num_adj + num_adj_list);
        }

        DataChunk adj_chunk;
        vector<LogicalType> adj_types = { edge_direction_type };
        vector<data_ptr_t> adj_datas(1);
        adj_datas[0] = (data_ptr_t)tmp.data();
        adj_chunk.Initialize(adj_types, adj_datas, STORAGE_STANDARD_VECTOR_SIZE);
        ExtentID cur_vertex_extentID = idx | (((uint32_t)part_id) << 16);
        bulkload_ctx.ext_mng.AppendChunkToExistingExtent(
            *(bulkload_ctx.client.get()), adj_chunk, cur_vertex_extentID);
        adj_chunk.Destroy();
    }
}

// Phase 1 (count-pass) for forward adj buffer (12d).
// Counts degree per src_seqno; still resolves dst LIDs→PIDs so they get stored in the
// DataChunk (needed for CreateExtent) and populates lid_pair_to_epid_map for bwd lookup.
static inline void FillAdjListBuffer(idx_t &begin_idx, idx_t &end_idx, idx_t &src_seqno, idx_t cur_src_pid,
                   idx_t &vertex_seqno, std::vector<int64_t> &dst_column_idx, vector<idx_t *> dst_key_columns,
                   FlatHashMap<LidPair, idx_t, LidPairHash> &dst_lid_to_pid_map_instance,
                   FlatHashMap<LidPair, idx_t, LidPairHash> *lid_pair_to_epid_map_instance,
                   unordered_map<ExtentID, ExtentAdjBuffer> &adj_list_buffers,
                   idx_t epid_base, idx_t src_lid = 0) {
    idx_t cur_src_seqno = GET_SEQNO_FROM_PHYSICAL_ID(cur_src_pid);
    ExtentID cur_vertex_localextentID = (static_cast<ExtentID>(cur_src_pid >> 32)) & 0xFFFF;

    end_idx = src_seqno;
    if (end_idx <= begin_idx) return;

    // Resolve dst LIDs → PIDs.  Skip edges whose dst vertex is absent (dangling
    // edges in heterogeneous/noisy graphs such as DBpedia).  We must count only
    // valid edges before calling count(), because count() pre-allocates storage.
    idx_t dst_seqno;
    LidPair dst_key{0, 0};
    LidPair pid_pair{cur_src_pid, 0};

    // Collect valid (dst_pid, epid) pairs in one pass.
    struct ValidEdge { idx_t dst_pid; idx_t epid; idx_t dst_seqno; };
    std::vector<ValidEdge> valid_edges;
    valid_edges.reserve((size_t)(end_idx - begin_idx));

    if (dst_column_idx.size() == 1) {
        for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
            dst_key.first = dst_key_columns[0][dst_seqno];
            const idx_t *p = dst_lid_to_pid_map_instance.get_ptr(dst_key);
            if (!p) continue;  // dangling edge — dst vertex not loaded
            dst_key_columns[0][dst_seqno] = *p;
            valid_edges.push_back({*p, epid_base + dst_seqno, dst_seqno});
        }
    } else if (dst_column_idx.size() == 2) {
        for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
            dst_key.first = dst_key_columns[0][dst_seqno];
            dst_key.second = dst_key_columns[1][dst_seqno];
            const idx_t *p = dst_lid_to_pid_map_instance.get_ptr(dst_key);
            if (!p) continue;
            dst_key_columns[0][dst_seqno] = *p;
            valid_edges.push_back({*p, epid_base + dst_seqno, dst_seqno});
        }
    }

    uint32_t n_valid = (uint32_t)valid_edges.size();
    adj_list_buffers[cur_vertex_localextentID].count((uint32_t)cur_src_seqno, n_valid);
    if (n_valid == 0) return;

    for (auto &ve : valid_edges) {
        pid_pair.second = ve.dst_pid;
        lid_pair_to_epid_map_instance->emplace(pid_pair, ve.epid);
        adj_list_buffers[cur_vertex_localextentID].store((uint32_t)cur_src_seqno, ve.dst_pid, ve.epid);
    }
}

// Phase 1 (count+store pass) for backward adj buffer (12d).
// Resolves bwd-dst LIDs→PIDs, looks up epids from lid_pair_to_epid_map, and stores
// raw edges directly — eliminating the Phase 3 ExtentIterator scan entirely.
static inline void FillBwdAdjListBuffer(idx_t &begin_idx, idx_t &end_idx, idx_t &src_seqno, idx_t cur_src_pid,
                   idx_t &vertex_seqno, std::vector<int64_t> &dst_column_idx, vector<idx_t *> dst_key_columns,
                   FlatHashMap<LidPair, idx_t, LidPairHash> &dst_lid_to_pid_map_instance,
                   FlatHashMap<LidPair, idx_t, LidPairHash> &lid_pair_to_epid_map_instance,
                   unordered_map<ExtentID, ExtentAdjBuffer> &adj_list_buffers) {
    idx_t cur_src_seqno = GET_SEQNO_FROM_PHYSICAL_ID(cur_src_pid);
    ExtentID cur_vertex_localextentID = (static_cast<ExtentID>(cur_src_pid >> 32)) & 0xFFFF;

    end_idx = src_seqno;
    if (end_idx <= begin_idx) return;

    // In bwd context: bwd-src = fwd-dst (cur_src_pid), bwd-dst = fwd-src.
    // lid_pair_to_epid_map key is (fwd_src_pid, fwd_dst_pid) = (bwd_dst_pid, cur_src_pid).
    // Skip edges where the bwd-dst (fwd-src) vertex is dangling or the epid wasn't
    // recorded during the forward pass (can happen for dangling fwd edges).
    LidPair dst_key{0, 0};
    LidPair fwd_key{0, cur_src_pid};
    idx_t dst_seqno;

    // Count valid edges before registering degree (same approach as FillAdjListBuffer).
    struct BwdValidEdge { idx_t bwd_dst_pid; idx_t epid; };
    std::vector<BwdValidEdge> valid_edges;
    valid_edges.reserve((size_t)(end_idx - begin_idx));

    if (dst_column_idx.size() == 1) {
        for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
            dst_key.first = dst_key_columns[0][dst_seqno];
            const idx_t *dp = dst_lid_to_pid_map_instance.get_ptr(dst_key);
            if (!dp) continue;
            fwd_key.first = *dp;
            const idx_t *ep = lid_pair_to_epid_map_instance.get_ptr(fwd_key);
            if (!ep) continue;  // dangling fwd edge (no epid registered)
            valid_edges.push_back({*dp, *ep});
        }
    } else if (dst_column_idx.size() == 2) {
        for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
            dst_key.first = dst_key_columns[0][dst_seqno];
            dst_key.second = dst_key_columns[1][dst_seqno];
            const idx_t *dp = dst_lid_to_pid_map_instance.get_ptr(dst_key);
            if (!dp) continue;
            fwd_key.first = *dp;
            const idx_t *ep = lid_pair_to_epid_map_instance.get_ptr(fwd_key);
            if (!ep) continue;
            valid_edges.push_back({*dp, *ep});
        }
    }

    uint32_t n_valid = (uint32_t)valid_edges.size();
    adj_list_buffers[cur_vertex_localextentID].count((uint32_t)cur_src_seqno, n_valid);
    for (auto &ve : valid_edges)
        adj_list_buffers[cur_vertex_localextentID].store((uint32_t)cur_src_seqno, ve.bwd_dst_pid, ve.epid);
}

// ---------------------------------------------------------------------------
// LID-to-PID map helpers
// ---------------------------------------------------------------------------

static void PopulateLidToPidMap(BulkloadContext &bulkload_ctx, std::string &label_name, std::string &query, size_t num_id_columns) {
    spdlog::trace("[PopulateLidToPidMap] Query: {}", query);
    turbolynx_prepared_statement* prep_stmt = turbolynx_prepare(bulkload_ctx.conn_id, const_cast<char*>(query.c_str()));
    turbolynx_resultset_wrapper *resultset_wrapper;
    turbolynx_num_rows rows = turbolynx_execute(bulkload_ctx.conn_id, prep_stmt, &resultset_wrapper);
    spdlog::trace("[PopulateLidToPidMap] Query returned {} rows", rows);

    if (num_id_columns > 2) throw InvalidInputException("Do not support # of compound keys >= 3 currently");

    bulkload_ctx.lid_to_pid_map_index[label_name] = bulkload_ctx.lid_to_pid_map.size();
    auto& lid_pid_map = bulkload_ctx.lid_to_pid_map.emplace_back(label_name, FlatHashMap<LidPair, idx_t, LidPairHash>()).second;
    lid_pid_map.reserve(rows);

    while (turbolynx_fetch_next(resultset_wrapper) != TURBOLYNX_END_OF_RESULT) {
        uint64_t pid = turbolynx_get_id(resultset_wrapper, 0);
        uint64_t id1 = turbolynx_get_uint64(resultset_wrapper, 1);
        uint64_t id2 = (num_id_columns == 2) ? turbolynx_get_uint64(resultset_wrapper, 2) : 0;
        lid_pid_map.emplace(LidPair(id1, id2), pid);
    }
}

static void PopulateLidToPidMap(FlatHashMap<LidPair, idx_t, LidPairHash> *lid_to_pid_map_instance, const vector<idx_t> &key_column_idxs, DataChunk &data, ExtentID new_eid) {
    idx_t pid_base = static_cast<idx_t>(new_eid) << 32;

    if (key_column_idxs.empty()) return;

    LidPair lid_key{0, 0};
    if (key_column_idxs.size() == 1) {
        auto key_column = reinterpret_cast<idx_t *>(data.data[key_column_idxs[0]].GetData());
        for (idx_t seqno = 0; seqno < data.size(); seqno++) {
            lid_key.first = key_column[seqno];
            lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
        }
    } else if (key_column_idxs.size() == 2) {
        auto key_column_1 = reinterpret_cast<idx_t *>(data.data[key_column_idxs[0]].GetData());
        auto key_column_2 = reinterpret_cast<idx_t *>(data.data[key_column_idxs[1]].GetData());

        // Determine the multiplier for combined key: 10^(digits of max key2).
        // Edge files reference composite keys as a single combined value
        // (e.g., ORDERKEY=1, LINENUMBER=3 → 13 with multiplier=10).
        idx_t max_key2 = 0;
        for (idx_t seqno = 0; seqno < data.size(); seqno++) {
            if (key_column_2[seqno] > max_key2) max_key2 = key_column_2[seqno];
        }
        idx_t multiplier = 1;
        while (multiplier <= max_key2) multiplier *= 10;

        for (idx_t seqno = 0; seqno < data.size(); seqno++) {
            lid_key.first = key_column_1[seqno];
            lid_key.second = key_column_2[seqno];
            lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);

            // Also register combined single-key form for edge lookup compatibility.
            // Edge CSVs with single :START_ID use combined value = key1 * multiplier + key2.
            LidPair combined_key{key_column_1[seqno] * multiplier + key_column_2[seqno], 0};
            lid_to_pid_map_instance->emplace(combined_key, pid_base + seqno);
        }
    } else {
        throw InvalidInputException("Do not support # of compound keys >= 3 currently");
    }
}

// ---------------------------------------------------------------------------
// Vertex loading
// ---------------------------------------------------------------------------

static void ReadVertexCSVFileAndCreateVertexExtents(vector<LabeledFile> &csv_vertex_files, BulkloadContext &bulkload_ctx) {
    SCOPED_TIMER_SIMPLE(ReadVertexCSVFileAndCreateVertexExtents, spdlog::level::info, spdlog::level::debug);
    for (auto &vertex_file: csv_vertex_files) {
        SCOPED_TIMER_SIMPLE(ReadSingleVertexCSVFile, spdlog::level::info, spdlog::level::debug);

        string &vertex_labelset = std::get<0>(vertex_file);
        string &vertex_file_path = std::get<1>(vertex_file);
        OptionalFileSize &file_size = std::get<2>(vertex_file);

        spdlog::info("[ReadVertexCSVFileAndCreateVertexExtents] Start to load {} with label set {}", vertex_file_path, vertex_labelset);

        vector<string> vertex_labels;
        vector<string> key_names;
        vector<idx_t> key_column_idxs;
        vector<LogicalType> types;
        ParseLabelSet(vertex_labelset, vertex_labels);

        spdlog::debug("[ReadVertexCSVFileAndCreateVertexExtents] InitCSVFile");
        SUBTIMER_START(ReadSingleVertexCSVFile, "InitCSVFile");
        GraphSIMDCSVFileParser reader;
        size_t approximated_num_rows =
            file_size.has_value() ?
            reader.InitCSVFile(vertex_file_path.c_str(), GraphComponentType::VERTEX, '|', file_size.value()) :
            reader.InitCSVFile(vertex_file_path.c_str(), GraphComponentType::VERTEX, '|');

        if (!reader.GetSchemaFromHeader(key_names, types)) { throw InvalidInputException("Invalid Schema Information"); }
        key_column_idxs = reader.GetKeyColumnIndexFromHeader();
        SUBTIMER_STOP(ReadSingleVertexCSVFile, "InitCSVFile");

        spdlog::debug("[ReadVertexCSVFileAndCreateVertexExtents] CreateVertexCatalogInfos");
        PartitionCatalogEntry *partition_cat;
        PropertySchemaCatalogEntry *property_schema_cat;
        CreateVertexCatalogInfos(bulkload_ctx, vertex_labelset, vertex_labels, key_names, types, key_column_idxs, partition_cat, property_schema_cat);

        spdlog::debug("[ReadVertexCSVFileAndCreateVertexExtents] Initialize LID_TO_PID_MAP");
        SUBTIMER_START(ReadSingleVertexCSVFile, "InitLIDToPIDMap");
        FlatHashMap<LidPair, idx_t, LidPairHash> *lid_to_pid_map_instance;
        // Additional maps to populate for shared parent labels.
        // e.g., Company:Organisation and University:Organisation both need to
        // populate the "Organisation" map so edge CSVs with :START_ID(Organisation)
        // can resolve IDs from both sub-partitions.
        // NOTE: Store indices, not pointers — emplace_back may realloc the vector.
        vector<size_t> shared_lid_map_indices;
        if (!bulkload_ctx.input_options.edge_files.empty()) {
            size_t map_idx = bulkload_ctx.lid_to_pid_map.size();
            bulkload_ctx.lid_to_pid_map_index[vertex_labelset] = map_idx;
            for (const auto& lbl : vertex_labels) {
                auto it = bulkload_ctx.lid_to_pid_map_index.find(lbl);
                if (it == bulkload_ctx.lid_to_pid_map_index.end()) {
                    bulkload_ctx.lid_to_pid_map_index[lbl] = map_idx;
                } else if (it->second != map_idx) {
                    shared_lid_map_indices.push_back(it->second);
                    spdlog::info("[LID_TO_PID_MAP] Shared label '{}': also populating map[{}]",
                                 lbl, it->second);
                }
            }
            bulkload_ctx.lid_to_pid_map.emplace_back(vertex_labelset, FlatHashMap<LidPair, idx_t, LidPairHash>());
            lid_to_pid_map_instance = &bulkload_ctx.lid_to_pid_map.back().second;
            lid_to_pid_map_instance->reserve(approximated_num_rows);
        }
        SUBTIMER_STOP(ReadSingleVertexCSVFile, "InitLIDToPIDMap");

        DataChunk data;
        data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

        spdlog::debug("[ReadVertexCSVFileAndCreateVertexExtents] Start to read and create vertex extents");
        SUBTIMER_START(ReadSingleVertexCSVFile, "ReadCSVFile and CreateVertexExtents");
        while (true) {
            SCOPED_TIMER_SIMPLE(ReadCSVFileAndCreateExtents, spdlog::level::debug, spdlog::level::trace);

            SUBTIMER_START(ReadCSVFileAndCreateExtents, "ReadCSVFile");
            spdlog::trace("[ReadVertexCSVFileAndCreateVertexExtents] ReadCSVFile");
            bool eof = reader.ReadCSVFile(key_names, types, data);
            SUBTIMER_STOP(ReadCSVFileAndCreateExtents, "ReadCSVFile");
            if (eof) break;

            SUBTIMER_START(ReadCSVFileAndCreateExtents, "CreateExtent");
            spdlog::trace("[ReadVertexCSVFileAndCreateVertexExtents] CreateExtent");
            ExtentID new_eid = bulkload_ctx.ext_mng.CreateExtent(*(bulkload_ctx.client.get()), data, *partition_cat, *property_schema_cat);
            property_schema_cat->AddExtent(new_eid, data.size());
            SUBTIMER_STOP(ReadCSVFileAndCreateExtents, "CreateExtent");

            if (!bulkload_ctx.input_options.edge_files.empty()) {
                SUBTIMER_START(ReadCSVFileAndCreateExtents, "PopulateLidToPidMap");
                spdlog::trace("[ReadVertexCSVFileAndCreateVertexExtents] PopulateLidToPidMap");
                PopulateLidToPidMap(lid_to_pid_map_instance, key_column_idxs, data, new_eid);
                // Also populate shared parent-label maps so that edge CSVs
                // referencing the parent label can resolve this partition's IDs.
                for (auto idx : shared_lid_map_indices) {
                    PopulateLidToPidMap(&bulkload_ctx.lid_to_pid_map[idx].second, key_column_idxs, data, new_eid);
                }
                SUBTIMER_STOP(ReadCSVFileAndCreateExtents, "PopulateLidToPidMap");
            }
        }
        SUBTIMER_STOP(ReadSingleVertexCSVFile, "ReadCSVFile and CreateVertexExtents");
        spdlog::info("[ReadVertexCSVFileAndCreateVertexExtents] Load {} Done", vertex_file_path);
        ChunkCacheManager::ccm->ThrottleIfNeeded();
    }
    spdlog::info("[ReadVertexCSVFileAndCreateVertexExtents] Load CSV Vertex Files Done");
}

static void ReadVertexJSONFileAndCreateVertexExtents(vector<LabeledFile> &json_vertex_files, BulkloadContext &bulkload_ctx) {
    SCOPED_TIMER_SIMPLE(ReadVertexJSONFiles, spdlog::level::info, spdlog::level::debug);
    spdlog::info("[ReadVertexJSONFileAndCreateVertexExtents] Start to load {} JSON Vertex Files", json_vertex_files.size());
    for (auto &vertex_file: json_vertex_files) {
        SCOPED_TIMER_SIMPLE(ReadSingleVertexJSONFile, spdlog::level::info, spdlog::level::debug);

        string &vertex_labelset = std::get<0>(vertex_file);
        string &vertex_file_path = std::get<1>(vertex_file);
        spdlog::info("[ReadVertexJSONFileAndCreateVertexExtents] Start to load {} with label set {}", vertex_file_path, vertex_labelset);

        SUBTIMER_START(ReadSingleVertexJSONFile, "GraphSIMDJSONFileParser Init");
        GraphSIMDJSONFileParser reader(bulkload_ctx.client, &bulkload_ctx.ext_mng, &bulkload_ctx.catalog);
        // Capture index before LoadJson appends to lid_to_pid_map
        size_t lid_map_idx = bulkload_ctx.lid_to_pid_map.size();
        if (!bulkload_ctx.input_options.edge_files.empty()) reader.SetLidToPidMap(&bulkload_ctx.lid_to_pid_map);
        reader.InitJsonFile(vertex_file_path.c_str());
        SUBTIMER_STOP(ReadSingleVertexJSONFile, "GraphSIMDJSONFileParser Init");

        SUBTIMER_START(ReadSingleVertexJSONFile, "GraphSIMDJSONFileParser LoadJSON");
        DataChunk data;
        vector<string> label_set;
        ParseLabelSet(vertex_labelset, label_set);
        reader.LoadJson(vertex_labelset, label_set, bulkload_ctx.graph_cat, GraphComponentType::VERTEX);
        SUBTIMER_STOP(ReadSingleVertexJSONFile, "GraphSIMDJSONFileParser LoadJSON");

        // Register labelset and individual labels in lid_to_pid_map_index so
        // edge CSVs using :START_ID(Label) can find this vertex file.
        if (!bulkload_ctx.input_options.edge_files.empty()) {
            bulkload_ctx.lid_to_pid_map_index[vertex_labelset] = lid_map_idx;
            for (const auto& lbl : label_set) {
                if (bulkload_ctx.lid_to_pid_map_index.find(lbl) == bulkload_ctx.lid_to_pid_map_index.end())
                    bulkload_ctx.lid_to_pid_map_index[lbl] = lid_map_idx;
            }
        }

        spdlog::info("[ReadVertexJSONFileAndCreateVertexExtents] Load {} Done", vertex_file_path);
        ChunkCacheManager::ccm->ThrottleIfNeeded();
    }
    spdlog::info("[ReadVertexJSONFileAndCreateVertexExtents] Load JSON Vertex Files Done");
}

static void ReadVertexFilesAndCreateVertexExtents(BulkloadContext &bulkload_ctx) {
    vector<LabeledFile> json_vertex_files;
    vector<LabeledFile> csv_vertex_files;
    SeperateFilesByExtension(bulkload_ctx.input_options.vertex_files, json_vertex_files, csv_vertex_files);
    spdlog::info("[ReadVertexFileAndCreateVertexExtents] {} JSON Vertex Files and {} CSV Vertex Files", json_vertex_files.size(), csv_vertex_files.size());

    if (json_vertex_files.size() > 0) {
        ReadVertexJSONFileAndCreateVertexExtents(json_vertex_files, bulkload_ctx);
    }
    if (csv_vertex_files.size() > 0) {
        ExpandMultiLabelVertexFiles(csv_vertex_files, bulkload_ctx.input_options.output_dir);
        ReadVertexCSVFileAndCreateVertexExtents(csv_vertex_files, bulkload_ctx);
    }
}

// ---------------------------------------------------------------------------
// Incremental ID reconstruction
// ---------------------------------------------------------------------------

static void PrepareClient(BulkloadContext &bulkload_ctx) {
    spdlog::info("[PrepareClient] Try to connect to the workspace");
    bulkload_ctx.conn_id = turbolynx_connect_with_client_context(&bulkload_ctx.client);
    if (bulkload_ctx.conn_id < 0 || turbolynx_is_connected(bulkload_ctx.conn_id) != TURBOLYNX_CONNECTED) {
        throw InvalidInputException("Failed to connect to the workspace");
    }
    spdlog::info("[PrepareClient] Connected to the workspace");
}

static vector<std::string> ObtainIdColumnNames(BulkloadContext &bulkload_ctx, std::string &vertex_label) {
    vector<idx_t> vertex_part_cat_oids =
        bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { vertex_label }, GraphComponentType::VERTEX);

    if (vertex_part_cat_oids.size() != 1) {
        spdlog::error("[ObtainIdColumnNames] {} vertex partitions found", vertex_part_cat_oids.size());
        throw InvalidInputException("The input src key corresponds to multiple vertex partitions.");
    }

    PartitionCatalogEntry *vertex_part_cat_entry =
        (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, vertex_part_cat_oids[0]);

    if (vertex_part_cat_entry == nullptr) {
        spdlog::error("[ObtainIdColumnNames] vertex_part_cat_entry is nullptr");
        throw InvalidInputException("The input src key corresponds to multiple vertex partitions.");
    }

    auto part_key_names = vertex_part_cat_entry->GetUniversalPropertyKeyNames();
    auto part_id_column_idxs = vertex_part_cat_entry->GetIdKeyColumnIdxs();

    vector<std::string> id_column_names;
    for (size_t i = 0; i < part_id_column_idxs->size(); i++) {
        std::string id_column_name = part_key_names->at(part_id_column_idxs->at(i));
        spdlog::trace("[ObtainIdColumnNames] part_key_names[part_id_column_idxs[{}]] = {}", i, id_column_name);
        id_column_names.push_back(id_column_name);
    }

    return std::move(id_column_names);
}

static std::string ConstructLidPidRetrievalQuery(BulkloadContext &bulkload_ctx, std::string &vertex_label, vector<std::string> &id_col_names) {
    std::string query = "MATCH (n:" + vertex_label + ") RETURN n._id";
    for (size_t i = 0; i < id_col_names.size(); i++) {
        query += ", n." + id_col_names[i];
    }
    return query;
}

static void ReconstructIDMappings(BulkloadContext &bulkload_ctx) {
    SCOPED_TIMER_SIMPLE(ReconstructIDMappings, spdlog::level::info, spdlog::level::debug);
    PrepareClient(bulkload_ctx);
    spdlog::info("[ReconstructIDMappings] Start to reconstruct ID mappings for {} edge files", bulkload_ctx.input_options.edge_files.size());
    for (auto &edge_file: bulkload_ctx.input_options.edge_files) {
        SCOPED_TIMER_SIMPLE(ReconstructIDMappingForFile, spdlog::level::info, spdlog::level::debug);

        string src_vertex_label;
        string dst_vertex_label;
        vector<int64_t> src_column_idx;
        vector<int64_t> dst_column_idx;
        string &edge_file_path = std::get<1>(edge_file);

        spdlog::info("[ReconstructIDMappings] Start to reconstruct ID mappings for {}", edge_file_path);

        if (isJSONFile(edge_file_path)) throw NotImplementedException("JSON Edge File is not supported yet");

        SUBTIMER_START(ReconstructIDMappingForFile, "GraphSIMDCSVFileParser InitCSVFile");
        GraphSIMDCSVFileParser reader;
        reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|');
        reader.GetSrcColumnInfo(src_column_idx, src_vertex_label);
        reader.GetDstColumnInfo(dst_column_idx, dst_vertex_label);
        SUBTIMER_STOP(ReconstructIDMappingForFile, "GraphSIMDCSVFileParser InitCSVFile");

        spdlog::debug("[ReconstructIDMappings] Reconstruct Src Vertex Label = {}", src_vertex_label);
        SUBTIMER_START(ReconstructIDMappingForFile, "Reconstruct Src Vertex Label");
        if (bulkload_ctx.lid_to_pid_map_index.find(src_vertex_label) == bulkload_ctx.lid_to_pid_map_index.end()) {
            auto id_col_names = ObtainIdColumnNames(bulkload_ctx, src_vertex_label);
            std::string src_lid_pid_query = ConstructLidPidRetrievalQuery(bulkload_ctx, src_vertex_label, id_col_names);
            PopulateLidToPidMap(bulkload_ctx, src_vertex_label, src_lid_pid_query, id_col_names.size());
        } else {
            spdlog::trace("[ReconstructIDMappings] Mapping for Src Vertex Label = {} is found, skipped", src_vertex_label);
        }
        SUBTIMER_STOP(ReconstructIDMappingForFile, "Reconstruct Src Vertex Label");

        spdlog::debug("[ReconstructIDMappings] Reconstruct Dst Vertex Label = {}", dst_vertex_label);
        SUBTIMER_START(ReconstructIDMappingForFile, "Reconstruct Dst Vertex Label");
        if (bulkload_ctx.lid_to_pid_map_index.find(dst_vertex_label) == bulkload_ctx.lid_to_pid_map_index.end()) {
            auto id_col_names = ObtainIdColumnNames(bulkload_ctx, dst_vertex_label);
            std::string dst_lid_pid_query = ConstructLidPidRetrievalQuery(bulkload_ctx, dst_vertex_label, id_col_names);
            PopulateLidToPidMap(bulkload_ctx, dst_vertex_label, dst_lid_pid_query, id_col_names.size());
        } else {
            spdlog::trace("[ReconstructIDMappings] Mapping for Dst Vertex Label = {} is found, skipped", dst_vertex_label);
        }
        SUBTIMER_STOP(ReconstructIDMappingForFile, "Reconstruct Dst Vertex Label");
    }
    turbolynx_disconnect(bulkload_ctx.conn_id);
    spdlog::info("[ReconstructIDMappings] Reconstruct ID mappings for edge files done");
}

// ---------------------------------------------------------------------------
// Forward edge loading
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Interleaved edge loading (fwd+bwd per file) — replaces separate fwd/bwd loops.
// Each file's forward edges are loaded first, then backward edges using the
// locally-built epid map.  The epid map is released at end of each file scope,
// drastically reducing peak memory vs the old approach (which kept all epid maps
// until the backward pass completed for all files).
// Thread count is capped at 2 to bound simultaneous raw_edges memory footprint.
// ---------------------------------------------------------------------------

static void ReadEdgeCSVFilesInterleaved(vector<LabeledFile> &csv_edge_files, BulkloadContext &bulkload_ctx) {
    SCOPED_TIMER_SIMPLE(ReadEdgeCSVFilesInterleaved, spdlog::level::info, spdlog::level::debug);
    spdlog::info("[ReadEdgeCSVFilesInterleaved] Start to load {} CSV Edge Files", csv_edge_files.size());

    const size_t n_files = csv_edge_files.size();
    // Backward edges are always loaded (interleaved with forward pass).

    // Serial pre-scan: read fwd src/dst labels from CSV headers.
    vector<string> fwd_src_labels(n_files), fwd_dst_labels(n_files);
    for (size_t i = 0; i < n_files; i++) {
        auto &f = csv_edge_files[i];
        GraphSIMDCSVFileParser probe;
        vector<int64_t> si, di;
        probe.InitCSVFile(std::get<1>(f).c_str(), GraphComponentType::EDGE, '|');
        probe.GetSrcColumnInfo(si, fwd_src_labels[i]);
        probe.GetDstColumnInfo(di, fwd_dst_labels[i]);
    }

    // Process files sequentially (n_t = 1).
    // Rationale: the interleaved fwd+bwd-per-file design requires that
    // AppendChunkToExistingExtent for the backward adj list is called
    // sequentially — in a homogeneous graph (e.g. DBpedia: all NODE→NODE),
    // multiple threads would concurrently append to the same vertex-partition
    // extents, which is unsafe even though CCM is internally locked at a
    // coarser granularity.  The primary goal of this refactoring is memory
    // reduction (local_epid_map per file, not cross-file retention); parallelism
    // can be added later once AppendChunkToExistingExtent is verified to be
    // safe for concurrent calls on the same extent ID.
    int n_t = 1;
    std::mutex catalog_mu;

    #pragma omp parallel for schedule(dynamic,1) num_threads(n_t)
    for (int fi = 0; fi < static_cast<int>(n_files); fi++) {
        auto &edge_file      = csv_edge_files[fi];
        string &edge_type      = std::get<0>(edge_file);
        string &edge_file_path = std::get<1>(edge_file);
        OptionalFileSize &file_size = std::get<2>(edge_file);
        string fwd_src_label = fwd_src_labels[fi];
        string fwd_dst_label = fwd_dst_labels[fi];

        spdlog::info("[ReadEdgeCSVFilesInterleaved] Processing {} ({})", edge_file_path, edge_type);

        // Skip if edge catalog already exists (incremental mode).
        bool skip = false;
        {
            std::lock_guard<std::mutex> lk(catalog_mu);
            if (IsEdgeCatalogInfoExist(bulkload_ctx, edge_type, fwd_src_label, fwd_dst_label)) {
                spdlog::info("[ReadEdgeCSVFilesInterleaved] Edge catalog for {}@{}@{} already exists", edge_type, fwd_src_label, fwd_dst_label);
                bulkload_ctx.skipped_labels.push_back(edge_type);
                skip = true;
            }
        }
        if (skip) continue;

        // Guard: skip edge types whose forward CSV is empty (0 bytes).
        // An empty forward file means no edge property data and no epid_map,
        // so neither the forward nor the backward adj list can be built.
        // This situation arises in DBpedia for rdf:type, where only the
        // .backward file contains data.  Log a warning and continue.
        {
            std::error_code ec;
            auto fwd_sz = fs::file_size(fs::path(edge_file_path), ec);
            if (!ec && fwd_sz == 0) {
                spdlog::warn("[ReadEdgeCSVFilesInterleaved] Forward file is empty for '{}' ({}): "
                             "skipping forward+backward pass — no adj list will be built for this edge type.",
                             edge_type, edge_file_path);
                continue;
            }
        }

        // Per-file epid map: lives only for this file's fwd+bwd passes,
        // then destroyed automatically — no cross-file memory retention.
        FlatHashMap<LidPair, idx_t, LidPairHash> local_epid_map;

        // ==================================================================
        // FORWARD PASS
        // ==================================================================
        {
            SCOPED_TIMER_SIMPLE(ReadSingleEdgeCSVFileFwd, spdlog::level::info, spdlog::level::debug);

            GraphSIMDCSVFileParser reader;
            vector<int64_t> src_column_idx, dst_column_idx;
            vector<string> key_names;
            vector<LogicalType> types;

            SUBTIMER_START(ReadSingleEdgeCSVFileFwd, "InitCSVFile");
            size_t approximated_num_rows =
                file_size.has_value() ?
                reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|', file_size.value()) :
                reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|');
            if (!reader.GetSchemaFromHeader(key_names, types)) throw InvalidInputException("Invalid Schema Information");
            reader.GetSrcColumnInfo(src_column_idx, fwd_src_label);
            reader.GetDstColumnInfo(dst_column_idx, fwd_dst_label);
            if (src_column_idx.size() == 0 || dst_column_idx.size() == 0)
                throw InvalidInputException("Invalid Edge File Format");
            SUBTIMER_STOP(ReadSingleEdgeCSVFileFwd, "InitCSVFile");

            spdlog::trace("Fwd src={} dst={}", fwd_src_label, fwd_dst_label);

            SUBTIMER_START(ReadSingleEdgeCSVFileFwd, "InitLIDToPIDMap");
            auto src_idx_it = bulkload_ctx.lid_to_pid_map_index.find(fwd_src_label);
            if (src_idx_it == bulkload_ctx.lid_to_pid_map_index.end())
                throw InvalidInputException("Corresponding src vertex file was not loaded");
            FlatHashMap<LidPair, idx_t, LidPairHash> &src_lid_to_pid_map_instance =
                bulkload_ctx.lid_to_pid_map[src_idx_it->second].second;

            auto dst_idx_it = bulkload_ctx.lid_to_pid_map_index.find(fwd_dst_label);
            if (dst_idx_it == bulkload_ctx.lid_to_pid_map_index.end())
                throw InvalidInputException("Corresponding dst vertex file was not loaded");
            FlatHashMap<LidPair, idx_t, LidPairHash> &dst_lid_to_pid_map_instance =
                bulkload_ctx.lid_to_pid_map[dst_idx_it->second].second;
            SUBTIMER_STOP(ReadSingleEdgeCSVFileFwd, "InitLIDToPIDMap");

            local_epid_map.reserve(approximated_num_rows);
            FlatHashMap<LidPair, idx_t, LidPairHash> *epid_map_ptr = &local_epid_map;

            DataChunk data;
            data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

            PartitionCatalogEntry *partition_cat;
            PropertySchemaCatalogEntry *property_schema_cat;
            {
                std::lock_guard<std::mutex> lk(catalog_mu);
                CreateEdgeCatalogInfos(bulkload_ctx, edge_type, key_names, types, fwd_src_label, fwd_dst_label,
                    partition_cat, property_schema_cat, LogicalType::FORWARD_ADJLIST, src_column_idx.size(),
                    fwd_src_label, fwd_dst_label);
            }

            unordered_map<ExtentID, ExtentAdjBuffer> adj_list_buffers;

            LidPair prev_id {0, 0};
            idx_t cur_src_pid = 0, prev_src_pid = 0;
            ExtentID cur_vertex_extentID;
            ExtentID cur_vertex_localextentID;
            idx_t vertex_seqno;
            bool is_first_tuple_processed = false;
            PartitionID cur_part_id = 0;

            SUBTIMER_START(ReadSingleEdgeCSVFileFwd, "ReadCSVFile and CreateEdgeExtents");
            while (true) {
                bool eof = reader.ReadCSVFile(key_names, types, data);
                if (eof) break;

                ExtentID new_eid = partition_cat->GetNewExtentID();
                idx_t epid_base = (idx_t)new_eid << 32;

                vector<idx_t *> src_key_columns, dst_key_columns;
                src_key_columns.resize(src_column_idx.size());
                dst_key_columns.resize(dst_column_idx.size());
                for (size_t i = 0; i < src_key_columns.size(); i++)
                    src_key_columns[i] = (idx_t *)data.data[src_column_idx[i]].GetData();
                for (size_t i = 0; i < dst_key_columns.size(); i++)
                    dst_key_columns[i] = (idx_t *)data.data[dst_column_idx[i]].GetData();

                idx_t src_seqno = 0;
                idx_t begin_idx = 0, end_idx;
                idx_t max_seqno = data.size();
                LidPair src_key{0, 0};
                bool chunk_has_valid_src = true;

                if (!is_first_tuple_processed) {
                    if (src_column_idx.size() == 1) {
                        prev_id.first = src_key_columns[0][src_seqno];
                    } else if (src_column_idx.size() == 2) {
                        prev_id.first = src_key_columns[0][src_seqno];
                        prev_id.second = src_key_columns[1][src_seqno];
                    }
                    const idx_t *p = src_lid_to_pid_map_instance.get_ptr(prev_id);
                    if (!p) {
                        src_seqno++;
                        bool found = false;
                        while (src_seqno < max_seqno) {
                            LidPair candidate{src_key_columns[0][src_seqno],
                                              (src_column_idx.size() == 2 ? src_key_columns[1][src_seqno] : (idx_t)0)};
                            p = src_lid_to_pid_map_instance.get_ptr(candidate);
                            if (p) { prev_id = candidate; found = true; break; }
                            src_seqno++;
                        }
                        if (!found) { chunk_has_valid_src = false; }
                        else {
                            begin_idx = src_seqno;
                            prev_src_pid = *p;
                            cur_vertex_extentID = static_cast<ExtentID>(prev_src_pid >> 32);
                            cur_vertex_localextentID = cur_vertex_extentID & 0xFFFF;
                            cur_part_id = (PartitionID)(cur_vertex_extentID >> 16);
                            is_first_tuple_processed = true;
                        }
                    } else {
                        prev_src_pid = *p;
                        cur_vertex_extentID = static_cast<ExtentID>(prev_src_pid >> 32);
                        cur_vertex_localextentID = cur_vertex_extentID & 0xFFFF;
                        cur_part_id = (PartitionID)(cur_vertex_extentID >> 16);
                        is_first_tuple_processed = true;
                    }
                }

                if (chunk_has_valid_src) {
                SUBTIMER_START(ReadSingleEdgeCSVFileFwd, "FillAdjListBuffer");
                if (src_column_idx.size() == 1) {
                    while (src_seqno < max_seqno) {
                        src_key.first = src_key_columns[0][src_seqno];
                        const idx_t *sp = src_lid_to_pid_map_instance.get_ptr(src_key);
                        if (!sp) { src_seqno++; continue; }
                        cur_src_pid = *sp;
                        src_key_columns[0][src_seqno] = cur_src_pid;
                        if (src_key == prev_id) {
                            src_seqno++;
                        } else {
                            end_idx = src_seqno;
                            FillAdjListBuffer(begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
                                              dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
                                              epid_map_ptr, adj_list_buffers, epid_base);
                            prev_id = src_key;
                            prev_src_pid = cur_src_pid;
                            begin_idx = src_seqno;
                            src_seqno++;
                        }
                    }
                } else if (src_column_idx.size() == 2) {
                    while (src_seqno < max_seqno) {
                        src_key.first = src_key_columns[0][src_seqno];
                        src_key.second = src_key_columns[1][src_seqno];
                        const idx_t *sp = src_lid_to_pid_map_instance.get_ptr(src_key);
                        if (!sp) { src_seqno++; continue; }
                        cur_src_pid = *sp;
                        src_key_columns[0][src_seqno] = cur_src_pid;
                        if (src_key == prev_id) {
                            src_seqno++;
                        } else {
                            end_idx = src_seqno;
                            FillAdjListBuffer(begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
                                              dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
                                              epid_map_ptr, adj_list_buffers, epid_base);
                            prev_id = src_key;
                            prev_src_pid = cur_src_pid;
                            begin_idx = src_seqno;
                            src_seqno++;
                        }
                    }
                } else {
                    throw InvalidInputException("Do not support # of compound keys >= 3 currently");
                }
                SUBTIMER_STOP(ReadSingleEdgeCSVFileFwd, "FillAdjListBuffer");

                SUBTIMER_START(ReadSingleEdgeCSVFileFwd, "FillAdjListBuffer for Remaining");
                end_idx = src_seqno;
                FillAdjListBuffer(begin_idx, end_idx, src_seqno, cur_src_pid, vertex_seqno,
                                  dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
                                  epid_map_ptr, adj_list_buffers, epid_base);
                SUBTIMER_STOP(ReadSingleEdgeCSVFileFwd, "FillAdjListBuffer for Remaining");
                } // chunk_has_valid_src

                SUBTIMER_START(ReadSingleEdgeCSVFileFwd, "CreateExtent and AddExtent");
                bulkload_ctx.ext_mng.CreateExtent(*(bulkload_ctx.client.get()), data, *partition_cat, *property_schema_cat, new_eid);
                property_schema_cat->AddExtent(new_eid, data.size());
                SUBTIMER_STOP(ReadSingleEdgeCSVFileFwd, "CreateExtent and AddExtent");
            }
            SUBTIMER_STOP(ReadSingleEdgeCSVFileFwd, "ReadCSVFile and CreateEdgeExtents");

            SUBTIMER_START(ReadSingleEdgeCSVFileFwd, "AppendFlatAdjListChunk");
            PartitionCatalogEntry *fwd_src_part_cat;
            {
                std::lock_guard<std::mutex> lk(catalog_mu);
                vector<idx_t> src_part_oids = bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { fwd_src_label }, GraphComponentType::VERTEX);
                fwd_src_part_cat = (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, src_part_oids[0]);
            }
            AppendFlatAdjListChunk(bulkload_ctx, LogicalType::FORWARD_ADJLIST, cur_part_id,
                fwd_src_part_cat->GetLocalExtentID(), adj_list_buffers);
            SUBTIMER_STOP(ReadSingleEdgeCSVFileFwd, "AppendFlatAdjListChunk");
            ChunkCacheManager::ccm->ThrottleIfNeeded();
        } // fwd adj_list_buffers destroyed here

        // ==================================================================
        // BACKWARD PASS (uses local_epid_map built during forward pass)
        // ==================================================================
        // Backward pass follows.

        {
            SCOPED_TIMER_SIMPLE(ReadSingleEdgeCSVFileBwd, spdlog::level::info, spdlog::level::debug);

            // In backward direction: src = original dst, dst = original src.
            string bwd_src_label = fwd_dst_label;
            string bwd_dst_label = fwd_src_label;

            GraphSIMDCSVFileParser reader;
            vector<int64_t> src_column_idx, dst_column_idx;
            vector<string> key_names;
            vector<LogicalType> types;

            SUBTIMER_START(ReadSingleEdgeCSVFileBwd, "InitCSVFile");
            file_size.has_value() ?
                reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|', file_size.value()) :
                reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|');
            if (!reader.GetSchemaFromHeader(key_names, types)) throw InvalidInputException("Invalid Schema Information");
            reader.GetDstColumnInfo(src_column_idx, bwd_src_label); // Reverse
            reader.GetSrcColumnInfo(dst_column_idx, bwd_dst_label); // Reverse
            if (src_column_idx.size() == 0 || dst_column_idx.size() == 0)
                throw InvalidInputException("Invalid Edge File Format");
            SUBTIMER_STOP(ReadSingleEdgeCSVFileBwd, "InitCSVFile");

            spdlog::trace("Bwd src={} dst={}", bwd_src_label, bwd_dst_label);

            SUBTIMER_START(ReadSingleEdgeCSVFileBwd, "InitLIDToPIDMap");
            auto bwd_src_idx_it = bulkload_ctx.lid_to_pid_map_index.find(bwd_src_label);
            if (bwd_src_idx_it == bulkload_ctx.lid_to_pid_map_index.end())
                throw InvalidInputException("Corresponding bwd-src vertex file was not loaded");
            FlatHashMap<LidPair, idx_t, LidPairHash> &bwd_src_lid_to_pid_map_instance =
                bulkload_ctx.lid_to_pid_map[bwd_src_idx_it->second].second;

            auto bwd_dst_idx_it = bulkload_ctx.lid_to_pid_map_index.find(bwd_dst_label);
            if (bwd_dst_idx_it == bulkload_ctx.lid_to_pid_map_index.end())
                throw InvalidInputException("Corresponding bwd-dst vertex file was not loaded");
            FlatHashMap<LidPair, idx_t, LidPairHash> &bwd_dst_lid_to_pid_map_instance =
                bulkload_ctx.lid_to_pid_map[bwd_dst_idx_it->second].second;
            SUBTIMER_STOP(ReadSingleEdgeCSVFileBwd, "InitLIDToPIDMap");

            DataChunk data;
            data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

            PartitionCatalogEntry *partition_cat;
            PropertySchemaCatalogEntry *property_schema_cat;
            {
                std::lock_guard<std::mutex> lk(catalog_mu);
                CreateEdgeCatalogInfos(bulkload_ctx, edge_type, key_names, types, bwd_src_label, bwd_dst_label,
                    partition_cat, property_schema_cat, LogicalType::BACKWARD_ADJLIST, dst_column_idx.size(),
                    fwd_src_label, fwd_dst_label);
            }

            unordered_map<ExtentID, ExtentAdjBuffer> adj_list_buffers;

            LidPair prev_id {0, 0};
            idx_t cur_src_pid = 0, prev_src_pid = 0;
            ExtentID cur_vertex_extentID;
            ExtentID cur_vertex_localextentID;
            idx_t vertex_seqno;
            bool is_first_tuple_processed = false;
            PartitionID cur_part_id = 0;

            SUBTIMER_START(ReadSingleEdgeCSVFileBwd, "ReadCSVFile and CreateEdgeExtents");
            while (true) {
                bool eof = reader.ReadCSVFile(key_names, types, data);
                if (eof) break;

                vector<idx_t *> src_key_columns, dst_key_columns;
                src_key_columns.resize(src_column_idx.size());
                dst_key_columns.resize(dst_column_idx.size());
                for (size_t i = 0; i < src_key_columns.size(); i++)
                    src_key_columns[i] = (idx_t *)data.data[src_column_idx[i]].GetData();
                for (size_t i = 0; i < dst_key_columns.size(); i++)
                    dst_key_columns[i] = (idx_t *)data.data[dst_column_idx[i]].GetData();

                idx_t src_seqno = 0;
                idx_t begin_idx = 0, end_idx;
                idx_t max_seqno = data.size();
                LidPair src_key{0, 0};
                bool bwd_chunk_has_valid_src = true;

                if (!is_first_tuple_processed) {
                    if (src_column_idx.size() == 1) {
                        prev_id.first = src_key_columns[0][src_seqno];
                    } else if (src_column_idx.size() == 2) {
                        prev_id.first = src_key_columns[0][src_seqno];
                        prev_id.second = src_key_columns[1][src_seqno];
                    }
                    const idx_t *p = bwd_src_lid_to_pid_map_instance.get_ptr(prev_id);
                    if (!p) {
                        src_seqno++;
                        bool found = false;
                        while (src_seqno < max_seqno) {
                            LidPair candidate{src_key_columns[0][src_seqno],
                                              (src_column_idx.size() == 2 ? src_key_columns[1][src_seqno] : (idx_t)0)};
                            p = bwd_src_lid_to_pid_map_instance.get_ptr(candidate);
                            if (p) { prev_id = candidate; found = true; break; }
                            src_seqno++;
                        }
                        if (!found) { bwd_chunk_has_valid_src = false; }
                        else {
                            begin_idx = src_seqno;
                            prev_src_pid = *p;
                            cur_vertex_extentID = static_cast<ExtentID>(prev_src_pid >> 32);
                            cur_vertex_localextentID = cur_vertex_extentID & 0xFFFF;
                            cur_part_id = (PartitionID)(cur_vertex_extentID >> 16);
                            is_first_tuple_processed = true;
                        }
                    } else {
                        prev_src_pid = *p;
                        cur_vertex_extentID = static_cast<ExtentID>(prev_src_pid >> 32);
                        cur_vertex_localextentID = cur_vertex_extentID & 0xFFFF;
                        cur_part_id = (PartitionID)(cur_vertex_extentID >> 16);
                        is_first_tuple_processed = true;
                    }
                }

                if (bwd_chunk_has_valid_src) {
                SUBTIMER_START(ReadSingleEdgeCSVFileBwd, "FillBwdAdjListBuffer");
                if (src_column_idx.size() == 1) {
                    while (src_seqno < max_seqno) {
                        src_key.first = src_key_columns[0][src_seqno];
                        const idx_t *sp = bwd_src_lid_to_pid_map_instance.get_ptr(src_key);
                        if (!sp) { src_seqno++; continue; }
                        cur_src_pid = *sp;
                        if (src_key == prev_id) {
                            src_seqno++;
                        } else {
                            end_idx = src_seqno;
                            FillBwdAdjListBuffer(begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
                                                 dst_column_idx, dst_key_columns, bwd_dst_lid_to_pid_map_instance,
                                                 local_epid_map, adj_list_buffers);
                            prev_id = src_key;
                            prev_src_pid = cur_src_pid;
                            begin_idx = src_seqno;
                            src_seqno++;
                        }
                    }
                } else if (src_column_idx.size() == 2) {
                    while (src_seqno < max_seqno) {
                        src_key.first = src_key_columns[0][src_seqno];
                        src_key.second = src_key_columns[1][src_seqno];
                        const idx_t *sp = bwd_src_lid_to_pid_map_instance.get_ptr(src_key);
                        if (!sp) { src_seqno++; continue; }
                        cur_src_pid = *sp;
                        if (src_key == prev_id) {
                            src_seqno++;
                        } else {
                            end_idx = src_seqno;
                            FillBwdAdjListBuffer(begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
                                                 dst_column_idx, dst_key_columns, bwd_dst_lid_to_pid_map_instance,
                                                 local_epid_map, adj_list_buffers);
                            prev_id = src_key;
                            prev_src_pid = cur_src_pid;
                            begin_idx = src_seqno;
                            src_seqno++;
                        }
                    }
                } else {
                    throw InvalidInputException("Do not support # of compound keys >= 3 currently");
                }
                SUBTIMER_STOP(ReadSingleEdgeCSVFileBwd, "FillBwdAdjListBuffer");

                SUBTIMER_START(ReadSingleEdgeCSVFileBwd, "FillBwdAdjListBuffer for Remaining");
                end_idx = src_seqno;
                FillBwdAdjListBuffer(begin_idx, end_idx, src_seqno, cur_src_pid, vertex_seqno,
                                     dst_column_idx, dst_key_columns, bwd_dst_lid_to_pid_map_instance,
                                     local_epid_map, adj_list_buffers);
                SUBTIMER_STOP(ReadSingleEdgeCSVFileBwd, "FillBwdAdjListBuffer for Remaining");
                } // bwd_chunk_has_valid_src
            }
            SUBTIMER_STOP(ReadSingleEdgeCSVFileBwd, "ReadCSVFile and CreateEdgeExtents");

            SUBTIMER_START(ReadSingleEdgeCSVFileBwd, "AppendFlatAdjListChunk");
            PartitionCatalogEntry *bwd_src_part_cat;
            {
                std::lock_guard<std::mutex> lk(catalog_mu);
                vector<idx_t> src_part_oids = bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { bwd_src_label }, GraphComponentType::VERTEX);
                bwd_src_part_cat = (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, src_part_oids[0]);
            }
            AppendFlatAdjListChunk(bulkload_ctx, LogicalType::BACKWARD_ADJLIST, cur_part_id,
                bwd_src_part_cat->GetLocalExtentID(), adj_list_buffers);
            SUBTIMER_STOP(ReadSingleEdgeCSVFileBwd, "AppendFlatAdjListChunk");
            ChunkCacheManager::ccm->ThrottleIfNeeded();
        } // bwd adj_list_buffers destroyed here

        // local_epid_map destroyed here — no cross-file memory retention
    }
    spdlog::info("[ReadEdgeCSVFilesInterleaved] Load CSV Edge Files Done");
}

static void ReadEdgeFilesInterleaved(BulkloadContext &bulkload_ctx) {
    vector<LabeledFile> json_edge_files;
    vector<LabeledFile> csv_edge_files;
    SeperateFilesByExtension(bulkload_ctx.input_options.edge_files, json_edge_files, csv_edge_files);
    spdlog::info("[ReadEdgeFilesInterleaved] {} JSON Edge Files and {} CSV Edge Files",
                 json_edge_files.size(), csv_edge_files.size());

    if (json_edge_files.size() > 0) {
        throw NotImplementedException("JSON Edge File is not supported yet");
    }

    ExpandMultiLabelEdgeFiles(csv_edge_files, bulkload_ctx);
    ReadEdgeCSVFilesInterleaved(csv_edge_files, bulkload_ctx);
}


// ---------------------------------------------------------------------------
// BulkloadPipeline method implementations
// ---------------------------------------------------------------------------

BulkloadPipeline::BulkloadPipeline(BulkloadOptions opts)
    : opts_(std::move(opts)) {}

BulkloadPipeline::~BulkloadPipeline() = default;

void BulkloadPipeline::InitializeWorkspace() {
    CreateDirectoryIfNotExists(opts_.output_dir);
    if (!opts_.incremental) RemoveAllFilesInDirectory(opts_.output_dir);
    CatalogManager::CreateOrOpenCatalog(opts_.output_dir);
    InitializeDiskAio(opts_.output_dir);

    ChunkCacheManager::ccm = new ChunkCacheManager(opts_.output_dir.c_str());
    database_ = make_unique<DuckDB>(opts_.output_dir.c_str());
    CreateGraphInfo graph_info(DEFAULT_SCHEMA, DEFAULT_GRAPH);
    ctx_ = make_unique<BulkloadContext>(
        opts_,
        std::make_shared<ClientContext>(database_->instance->shared_from_this()),
        database_->instance->GetCatalog(),
        database_->instance->GetCatalogWrapper(),
        graph_info
    );
}

void BulkloadPipeline::LoadVertices() {
    ReadVertexFilesAndCreateVertexExtents(*ctx_);
}

void BulkloadPipeline::LoadEdges() {
    ReadEdgeFilesInterleaved(*ctx_);
}

// ---------------------------------------------------------------------------
// CreateVirtualUnifiedVertexPartitions
// ---------------------------------------------------------------------------
// For multi-partition vertex labels (e.g., Message = Comment ∪ Post),
// create a single virtual vertex partition so ORCA sees it as one table.
// This avoids CLogicalUnionAll and enables IndexApply on the vertex scan.

static void CreateVirtualUnifiedVertexPartitions(BulkloadContext &ctx) {
    auto *graph = ctx.graph_cat;
    auto &catalog = ctx.catalog;
    auto &client = *ctx.client;

    // Find vertex labels that map to >1 partitions
    for (auto &[label, label_id] : graph->vertexlabel_map) {
        auto it = graph->label_to_partition_index.find(label_id);
        if (it == graph->label_to_partition_index.end()) continue;
        if (it->second.size() <= 1) continue;

        auto sub_oids = it->second;  // copy before modifying

        string partition_name = string(DEFAULT_VERTEX_PARTITION_PREFIX) + label;
        string ps_name = string(DEFAULT_VERTEX_PROPERTYSCHEMA_PREFIX) + label;

        // Skip if already exists
        if (catalog.GetEntry(client, CatalogType::PARTITION_ENTRY,
                             DEFAULT_SCHEMA, partition_name) != nullptr)
            continue;

        // ---- Create PartitionCatalogEntry ----
        CreatePartitionInfo pi(DEFAULT_SCHEMA, partition_name.c_str());
        auto *vpart = static_cast<PartitionCatalogEntry *>(
            catalog.CreatePartition(client, &pi));
        PartitionID new_pid = graph->GetNewPartitionID();
        vpart->SetPartitionID(new_pid);

        // ---- Build union schema (superset of columns from all sub-partitions) ----
        vector<string> union_key_names;
        vector<LogicalType> union_types;
        vector<PropertyKeyID> union_key_ids;
        map<string, size_t> name_to_idx;

        for (auto sub_oid : sub_oids) {
            auto *sub = static_cast<PartitionCatalogEntry *>(
                catalog.GetEntry(client, DEFAULT_SCHEMA, sub_oid));
            if (!sub) continue;
            auto *key_names = sub->GetUniversalPropertyKeyNames();
            auto sub_types = sub->GetTypes();
            auto *key_ids = sub->GetUniversalPropertyKeyIds();
            if (!key_names) continue;

            for (size_t i = 0; i < key_names->size(); i++) {
                if (name_to_idx.find((*key_names)[i]) == name_to_idx.end()) {
                    name_to_idx[(*key_names)[i]] = union_key_names.size();
                    union_key_names.push_back((*key_names)[i]);
                    union_types.push_back(sub_types[i]);
                    union_key_ids.push_back((*key_ids)[i]);
                }
            }
        }

        // ---- Create PropertySchemaCatalogEntry ----
        CreatePropertySchemaInfo psi(DEFAULT_SCHEMA, ps_name.c_str(),
                                     new_pid, vpart->GetOid());
        auto *vps = static_cast<PropertySchemaCatalogEntry *>(
            catalog.CreatePropertySchema(client, &psi));

        vpart->AddPropertySchema(client, vps->GetOid(), union_key_ids);
        vpart->SetSchema(client, union_key_names, union_types, union_key_ids);
        vps->SetSchema(client, union_key_names, union_types, union_key_ids);

        // ---- Create PHYSICAL_ID index ----
        string idx_name = label + "_vid";
        CreateIndexInfo id_idx_info(DEFAULT_SCHEMA, idx_name,
            IndexType::PHYSICAL_ID, vpart->GetOid(), vps->GetOid(), 0, {-1});
        auto *id_idx = static_cast<IndexCatalogEntry *>(
            catalog.CreateIndex(client, &id_idx_info));
        vpart->SetPhysicalIDIndex(id_idx->GetOid());
        vps->SetPhysicalIDIndex(id_idx->GetOid());

        // ---- Set sub_partition_oids ----
        vpart->sub_partition_oids.assign(sub_oids.begin(), sub_oids.end());

        // ---- Compute combined row count ----
        uint64_t total_rows = 0;
        for (auto sub_oid : sub_oids) {
            auto *sub_part = static_cast<PartitionCatalogEntry *>(
                catalog.GetEntry(client, DEFAULT_SCHEMA, sub_oid));
            if (!sub_part) continue;
            PropertySchemaID_vector sub_ps_oids;
            sub_part->GetPropertySchemaIDs(sub_ps_oids);
            for (auto ps_id : sub_ps_oids) {
                auto *ps = static_cast<PropertySchemaCatalogEntry *>(
                    catalog.GetEntry(client, DEFAULT_SCHEMA, ps_id));
                if (ps && !ps->is_fake)
                    total_rows += ps->GetNumberOfRowsApproximately();
            }
        }
        vps->SetNumberOfLastExtentNumTuples(total_rows);

        // ---- Compute combined NDVs ----
        auto *ndvs = vps->GetNDVs();
        ndvs->clear();
        ndvs->push_back(total_rows);  // ndvs[0] = total rows (_id NDV)
        for (size_t col_i = 0; col_i < union_key_names.size(); col_i++) {
            uint64_t sum_ndv = 0;
            bool found = false;
            for (auto sub_oid : sub_oids) {
                auto *sub_part = static_cast<PartitionCatalogEntry *>(
                    catalog.GetEntry(client, DEFAULT_SCHEMA, sub_oid));
                if (!sub_part) continue;
                PropertySchemaID_vector sub_ps_oids;
                sub_part->GetPropertySchemaIDs(sub_ps_oids);
                if (sub_ps_oids.empty()) continue;
                auto *sub_ps = static_cast<PropertySchemaCatalogEntry *>(
                    catalog.GetEntry(client, DEFAULT_SCHEMA, sub_ps_oids[0]));
                if (!sub_ps) continue;
                auto *sub_ndvs = sub_ps->GetNDVs();
                if (!sub_ndvs || sub_ndvs->empty()) continue;

                auto *sub_key_names = sub_part->GetUniversalPropertyKeyNames();
                if (!sub_key_names) continue;
                for (size_t j = 0; j < sub_key_names->size(); j++) {
                    if ((*sub_key_names)[j] == union_key_names[col_i] &&
                        j + 1 < sub_ndvs->size()) {
                        sum_ndv += (*sub_ndvs)[j + 1];
                        found = true;
                        break;
                    }
                }
            }
            ndvs->push_back(found ? std::min(sum_ndv, total_rows) : total_rows);
        }

        // ---- Update label_to_partition_index: replace with virtual OID ----
        it->second = {vpart->GetOid()};
        graph->vertex_partitions.push_back(vpart->GetOid());

        spdlog::info("[Bulkload] Created virtual unified vertex partition: {} "
                     "(sub-partitions: {}, total rows: {})",
                     partition_name, sub_oids.size(), total_rows);
    }
}

// ---------------------------------------------------------------------------
// CreateVirtualUnifiedEdgePartitions
// ---------------------------------------------------------------------------
// For multi-partition edge types (e.g., HAS_CREATOR = Comment→Person + Post→Person),
// create virtual unified edge partitions with sub_partition_oids metadata.
// This enables AdjIdxJoin for union-typed vertices (e.g., Message = Comment ∪ Post).

static void CreateVirtualUnifiedEdgePartitions(BulkloadContext &ctx) {
    auto *graph = ctx.graph_cat;
    auto &catalog = ctx.catalog;
    auto &client = *ctx.client;

    // Reverse lookup: edge_type_id → edge_type name
    unordered_map<EdgeTypeID, string> etype_id_to_name;
    for (auto &[name, id] : graph->edgetype_map) {
        etype_id_to_name[id] = name;
    }

    // Reverse lookup: vertex partition OID → specific label name (the label that
    // maps to exactly 1 partition, i.e., the concrete label like "Comment", not "Message")
    unordered_map<idx_t, string> vpart_oid_to_label;
    for (auto &[label, label_id] : graph->vertexlabel_map) {
        auto it = graph->label_to_partition_index.find(label_id);
        if (it == graph->label_to_partition_index.end()) continue;
        if (it->second.size() == 1) {
            vpart_oid_to_label[it->second[0]] = label;
        }
    }

    // Find union labels: labels that map to multiple partitions
    // e.g., "Message" → [Comment_OID, Post_OID]
    // Handles virtual partitions by expanding sub_partition_oids.
    auto expand_partition_oids = [&](const vector<idx_t> &oids) -> set<idx_t> {
        set<idx_t> result;
        for (auto oid : oids) {
            auto *part = static_cast<PartitionCatalogEntry *>(
                catalog.GetEntry(client, DEFAULT_SCHEMA, oid));
            if (part && !part->sub_partition_oids.empty()) {
                result.insert(part->sub_partition_oids.begin(),
                              part->sub_partition_oids.end());
            } else {
                result.insert(oid);
            }
        }
        return result;
    };
    auto find_union_label = [&](const vector<idx_t> &part_oids) -> string {
        set<idx_t> target_set(part_oids.begin(), part_oids.end());
        for (auto &[label, label_id] : graph->vertexlabel_map) {
            auto it = graph->label_to_partition_index.find(label_id);
            if (it == graph->label_to_partition_index.end()) continue;
            set<idx_t> label_set = expand_partition_oids(it->second);
            if (label_set.size() != part_oids.size()) continue;
            if (label_set == target_set) {
                if (label_set.size() > 1) return label;
            }
        }
        return "";
    };

    // Iterate over edge types with multiple partitions
    for (auto &[etype_id, part_oids] : graph->type_to_partition_index) {
        if (part_oids.size() <= 1) continue;

        string edge_type = etype_id_to_name[etype_id];

        // Group sub-partitions by dst_part_oid (common case: same dst, different src)
        unordered_map<idx_t, vector<idx_t>> by_dst;
        for (auto ep_oid : part_oids) {
            auto *ep = static_cast<PartitionCatalogEntry *>(
                catalog.GetEntry(client, DEFAULT_SCHEMA, ep_oid));
            if (!ep) continue;
            by_dst[ep->GetDstPartOid()].push_back(ep_oid);
        }

        for (auto &[dst_oid, sub_oids] : by_dst) {
            if (sub_oids.size() <= 1) continue;

            // Collect src partition OIDs
            vector<idx_t> src_oids;
            for (auto eo : sub_oids) {
                auto *ep = static_cast<PartitionCatalogEntry *>(
                    catalog.GetEntry(client, DEFAULT_SCHEMA, eo));
                src_oids.push_back(ep->GetSrcPartOid());
            }

            // Find union src label. First try the full set, then try subsets.
            // For HAS_TAG: full set = [Comment, Forum, Post] → no union label.
            // Subset [Comment, Post] → "Message" union label ✓.
            string union_src = find_union_label(src_oids);
            vector<idx_t> matched_sub_oids = sub_oids; // default: all
            if (union_src.empty() && src_oids.size() > 2) {
                // Try all subsets of size 2+ by checking union labels
                for (auto &[label, label_id] : graph->vertexlabel_map) {
                    auto it = graph->label_to_partition_index.find(label_id);
                    if (it == graph->label_to_partition_index.end()) continue;
                    set<idx_t> label_set = expand_partition_oids(it->second);
                    if (label_set.size() < 2) continue;
                    // Check if all label_set members are in src_oids
                    vector<idx_t> matching_subs;
                    for (size_t i = 0; i < src_oids.size(); i++) {
                        if (label_set.count(src_oids[i])) {
                            matching_subs.push_back(sub_oids[i]);
                        }
                    }
                    if (matching_subs.size() == label_set.size() && matching_subs.size() >= 2) {
                        union_src = label;
                        matched_sub_oids = matching_subs;
                        break;
                    }
                }
            }
            if (union_src.empty()) continue;
            sub_oids = matched_sub_oids; // narrow to matched subset

            // Find dst label
            string dst_label;
            auto it_dst = vpart_oid_to_label.find(dst_oid);
            if (it_dst != vpart_oid_to_label.end()) {
                dst_label = it_dst->second;
            } else {
                continue; // can't determine dst label
            }

            // Create virtual partition: epart_EDGETYPE@UnionSrc@Dst
            string internal_name = MakeInternalEdgeTypeName(edge_type, union_src, dst_label);
            string partition_name = DEFAULT_EDGE_PARTITION_PREFIX + internal_name;
            string ps_name = DEFAULT_EDGE_PROPERTYSCHEMA_PREFIX + internal_name;

            // Skip if already exists (e.g., from previous bulkload)
            if (catalog.GetEntry(client, CatalogType::PARTITION_ENTRY, DEFAULT_SCHEMA, partition_name) != nullptr) {
                continue;
            }

            // Create the partition entry
            CreatePartitionInfo pi(DEFAULT_SCHEMA, partition_name.c_str());
            auto *vpart = static_cast<PartitionCatalogEntry *>(
                catalog.CreatePartition(client, &pi));
            PartitionID new_pid = graph->GetNewPartitionID();
            vpart->SetPartitionID(new_pid);

            // Create PropertySchema (empty — no data, just metadata for ORCA)
            CreatePropertySchemaInfo psi(DEFAULT_SCHEMA, ps_name.c_str(), new_pid, vpart->GetOid());
            auto *vps = static_cast<PropertySchemaCatalogEntry *>(
                catalog.CreatePropertySchema(client, &psi));

            // Copy schema from first sub-partition (all sub-partitions have same edge schema)
            auto *first_sub = static_cast<PartitionCatalogEntry *>(
                catalog.GetEntry(client, DEFAULT_SCHEMA, sub_oids[0]));
            auto types = first_sub->GetTypes();
            auto *key_names = first_sub->GetUniversalPropertyKeyNames();
            auto *key_ids = first_sub->GetUniversalPropertyKeyIds();
            if (key_names && !key_names->empty()) {
                vector<PropertyKeyID> prop_key_ids(key_ids->begin(), key_ids->end());
                vpart->AddPropertySchema(client, vps->GetOid(), prop_key_ids);
                vpart->SetSchema(client, *key_names, types, prop_key_ids);
                vps->SetSchema(client, *key_names, types, prop_key_ids);
            }

            // Create physical ID index (required for ORCA metadata)
            CreateIndexInfo id_idx_info(DEFAULT_SCHEMA, internal_name + "_id",
                IndexType::PHYSICAL_ID, vpart->GetOid(), vps->GetOid(), 0, {-1});
            auto *id_idx = static_cast<IndexCatalogEntry *>(
                catalog.CreateIndex(client, &id_idx_info));
            vpart->SetPhysicalIDIndex(id_idx->GetOid());
            vps->SetPhysicalIDIndex(id_idx->GetOid());

            // Copy adjacency index OIDs from ALL sub-partitions.
            // These are real CSR indexes — ORCA sees them and enables CXformJoin2IndexApply.
            for (auto sub_oid : sub_oids) {
                auto *sub = static_cast<PartitionCatalogEntry *>(
                    catalog.GetEntry(client, DEFAULT_SCHEMA, sub_oid));
                auto *adj_idxs = sub->GetAdjIndexOidVec();
                for (auto idx_oid : *adj_idxs) {
                    vpart->AddAdjIndex(idx_oid);
                }
            }

            // Set src/dst: use 0 as sentinel (virtual partition doesn't have a single src/dst).
            // The binder handles this via sub_partition_oids lookup.
            vpart->SetSrcDstPartOid(0, 0);

            // Set sub_partition_oids — the key metadata linking to real partitions
            vpart->sub_partition_oids.assign(sub_oids.begin(), sub_oids.end());

            // Mark PropertySchema as virtual (is_fake = true so it's not counted in edge counts)
            vps->is_fake = true;

            // Register in GraphCatalogEntry
            graph->AddEdgePartition(client, new_pid, vpart->GetOid(), edge_type);

            spdlog::info("[Bulkload] Created virtual unified edge partition: {} "
                         "(sub-partitions: {})", partition_name, sub_oids.size());
        }

        // Also check same-src, different-dst case (e.g., LIKES: Person→Comment + Person→Post)
        unordered_map<idx_t, vector<idx_t>> by_src;
        for (auto ep_oid : part_oids) {
            auto *ep = static_cast<PartitionCatalogEntry *>(
                catalog.GetEntry(client, DEFAULT_SCHEMA, ep_oid));
            if (!ep) continue;
            by_src[ep->GetSrcPartOid()].push_back(ep_oid);
        }

        for (auto &[src_oid, sub_oids] : by_src) {
            if (sub_oids.size() <= 1) continue;

            vector<idx_t> dst_oids;
            for (auto eo : sub_oids) {
                auto *ep = static_cast<PartitionCatalogEntry *>(
                    catalog.GetEntry(client, DEFAULT_SCHEMA, eo));
                dst_oids.push_back(ep->GetDstPartOid());
            }

            string union_dst = find_union_label(dst_oids);
            vector<idx_t> matched_sub_oids = sub_oids;
            if (union_dst.empty() && dst_oids.size() > 2) {
                for (auto &[label, label_id] : graph->vertexlabel_map) {
                    auto it = graph->label_to_partition_index.find(label_id);
                    if (it == graph->label_to_partition_index.end()) continue;
                    set<idx_t> label_set = expand_partition_oids(it->second);
                    if (label_set.size() < 2) continue;
                    vector<idx_t> matching_subs;
                    for (size_t i = 0; i < dst_oids.size(); i++) {
                        if (label_set.count(dst_oids[i])) {
                            matching_subs.push_back(sub_oids[i]);
                        }
                    }
                    if (matching_subs.size() == label_set.size() && matching_subs.size() >= 2) {
                        union_dst = label;
                        matched_sub_oids = matching_subs;
                        break;
                    }
                }
            }
            if (union_dst.empty()) continue;
            sub_oids = matched_sub_oids;

            string src_label;
            auto it_src = vpart_oid_to_label.find(src_oid);
            if (it_src != vpart_oid_to_label.end()) {
                src_label = it_src->second;
            } else {
                continue;
            }

            string internal_name = MakeInternalEdgeTypeName(edge_type, src_label, union_dst);
            string partition_name = DEFAULT_EDGE_PARTITION_PREFIX + internal_name;
            string ps_name = DEFAULT_EDGE_PROPERTYSCHEMA_PREFIX + internal_name;

            if (catalog.GetEntry(client, CatalogType::PARTITION_ENTRY, DEFAULT_SCHEMA, partition_name) != nullptr) {
                continue;
            }

            CreatePartitionInfo pi(DEFAULT_SCHEMA, partition_name.c_str());
            auto *vpart = static_cast<PartitionCatalogEntry *>(
                catalog.CreatePartition(client, &pi));
            PartitionID new_pid = graph->GetNewPartitionID();
            vpart->SetPartitionID(new_pid);

            CreatePropertySchemaInfo psi(DEFAULT_SCHEMA, ps_name.c_str(), new_pid, vpart->GetOid());
            auto *vps = static_cast<PropertySchemaCatalogEntry *>(
                catalog.CreatePropertySchema(client, &psi));

            auto *first_sub = static_cast<PartitionCatalogEntry *>(
                catalog.GetEntry(client, DEFAULT_SCHEMA, sub_oids[0]));
            auto types = first_sub->GetTypes();
            auto *key_names = first_sub->GetUniversalPropertyKeyNames();
            auto *key_ids = first_sub->GetUniversalPropertyKeyIds();
            if (key_names && !key_names->empty()) {
                vector<PropertyKeyID> prop_key_ids(key_ids->begin(), key_ids->end());
                vpart->AddPropertySchema(client, vps->GetOid(), prop_key_ids);
                vpart->SetSchema(client, *key_names, types, prop_key_ids);
                vps->SetSchema(client, *key_names, types, prop_key_ids);
            }

            CreateIndexInfo id_idx_info(DEFAULT_SCHEMA, internal_name + "_id",
                IndexType::PHYSICAL_ID, vpart->GetOid(), vps->GetOid(), 0, {-1});
            auto *id_idx = static_cast<IndexCatalogEntry *>(
                catalog.CreateIndex(client, &id_idx_info));
            vpart->SetPhysicalIDIndex(id_idx->GetOid());
            vps->SetPhysicalIDIndex(id_idx->GetOid());

            for (auto sub_oid : sub_oids) {
                auto *sub = static_cast<PartitionCatalogEntry *>(
                    catalog.GetEntry(client, DEFAULT_SCHEMA, sub_oid));
                auto *adj_idxs = sub->GetAdjIndexOidVec();
                for (auto idx_oid : *adj_idxs) {
                    vpart->AddAdjIndex(idx_oid);
                }
            }

            vpart->SetSrcDstPartOid(0, 0);
            vpart->sub_partition_oids.assign(sub_oids.begin(), sub_oids.end());
            vps->is_fake = true;

            graph->AddEdgePartition(client, new_pid, vpart->GetOid(), edge_type);

            spdlog::info("[Bulkload] Created virtual unified edge partition: {} "
                         "(sub-partitions: {})", partition_name, sub_oids.size());
        }
    }
}

void BulkloadPipeline::RunPostProcessing() {
    SCOPED_TIMER_SIMPLE(BulkloadPostProcessing, spdlog::level::info, spdlog::level::debug);
    if (ctx_->input_options.skip_histogram) {
        spdlog::info("[BulkloadPipeline] Skip Histogram Generation");
    } else {
        SUBTIMER_START(BulkloadPostProcessing, "CreateHistogram");
        HistogramGenerator hist_gen;
        hist_gen.CreateHistogram(ctx_->client);
        SUBTIMER_STOP(BulkloadPostProcessing, "CreateHistogram");
    }

    SUBTIMER_START(BulkloadPostProcessing, "FlushMetaInfo");
    ChunkCacheManager::ccm->FlushMetaInfo(ctx_->input_options.output_dir.c_str());
    SUBTIMER_STOP(BulkloadPostProcessing, "FlushMetaInfo");

    SUBTIMER_START(BulkloadPostProcessing, "DeleteChunkCacheManager");
    delete ChunkCacheManager::ccm;
    SUBTIMER_STOP(BulkloadPostProcessing, "DeleteChunkCacheManager");

    SUBTIMER_START(BulkloadPostProcessing, "CreateVirtualVertexPartitions");
    CreateVirtualUnifiedVertexPartitions(*ctx_);
    SUBTIMER_STOP(BulkloadPostProcessing, "CreateVirtualVertexPartitions");

    SUBTIMER_START(BulkloadPostProcessing, "CreateVirtualEdgePartitions");
    CreateVirtualUnifiedEdgePartitions(*ctx_);
    SUBTIMER_STOP(BulkloadPostProcessing, "CreateVirtualEdgePartitions");

    SUBTIMER_START(BulkloadPostProcessing, "SaveCatalog");
    database_->instance->GetCatalog().SaveCatalog();
    SUBTIMER_STOP(BulkloadPostProcessing, "SaveCatalog");

    SUBTIMER_START(BulkloadPostProcessing, "CloseCatalog");
    CatalogManager::CloseCatalog();
    SUBTIMER_STOP(BulkloadPostProcessing, "CloseCatalog");
}

void BulkloadPipeline::Run() {
    spdlog::info("[BulkloadPipeline] Initialization");
    InitializeWorkspace();

    spdlog::info("[BulkloadPipeline] Run Bulkload");
    try {
        SCOPED_TIMER_SIMPLE(Bulkload, spdlog::level::info, spdlog::level::debug);

        SUBTIMER_START(Bulkload, "Vertex Files");
        LoadVertices();
        SUBTIMER_STOP(Bulkload, "Vertex Files");

        if (ctx_->input_options.incremental) {
            SUBTIMER_START(Bulkload, "Reconstruct ID Mappings");
            ReconstructIDMappings(*ctx_);
            SUBTIMER_STOP(Bulkload, "Reconstruct ID Mappings");
        }

        SUBTIMER_START(Bulkload, "Edge Files (fwd+bwd interleaved)");
        LoadEdges();
        SUBTIMER_STOP(Bulkload, "Edge Files (fwd+bwd interleaved)");
    } catch (const std::system_error& e) {
        spdlog::error("[BulkloadPipeline::Run] Caught system_error with code [{}] meaning [{}]", e.code().value(), e.what());
    }

    spdlog::info("[BulkloadPipeline] Post Processing");
    RunPostProcessing();
}

} // namespace duckdb
