#include "loader/bulkload_pipeline.hpp"
#include "main/database.hpp"
#include "main/client_context.hpp"
#include "main/capi/s62.h"
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

    vector<
        std::pair<string, FlatHashMap<LidPair, idx_t, LidPairHash>>>
        lid_pair_to_epid_map;  // For Backward AdjList
    unordered_map<string, size_t> lid_pair_to_epid_map_index;  // edge_type → lid_pair_to_epid_map index
    std::vector<std::string> skipped_labels;

    BulkloadContext(BulkloadOptions input_options, std::shared_ptr<ClientContext> client,
                    Catalog &catalog, CatalogWrapper &catalog_wrapper,
                    CreateGraphInfo &graph_info)
        : input_options(std::move(input_options)),
          client(std::move(client)),
          catalog(catalog)
    {
        if (this->input_options.incremental) {
            spdlog::info("[BulkloadContext] Incremental mode detected; loading existing graph catalog");
            graph_cat = (GraphCatalogEntry*)catalog.GetEntry(*(this->client.get()), CatalogType::GRAPH_ENTRY,
                        graph_info.schema, graph_info.graph);
        }
        else {
            spdlog::info("[BulkloadContext] Creating new graph catalog");
            graph_cat = (GraphCatalogEntry *)catalog.CreateGraph(*(this->client.get()),
                                                                  &graph_info);
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

static bool IsEdgeCatalogInfoExist(BulkloadContext& bulkload_ctx, std::string &edge_type) {
    return bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), CatalogType::PARTITION_ENTRY, DEFAULT_SCHEMA, DEFAULT_EDGE_PARTITION_PREFIX + edge_type) != nullptr;
}

static void CreateEdgeCatalogInfos(BulkloadContext& bulkload_ctx, std::string &edge_type, vector<string> &key_names, vector<LogicalType> &types, string &src_vertex_label,
                                   string &dst_vertex_label, PartitionCatalogEntry *&partition_cat, PropertySchemaCatalogEntry *&property_schema_cat,
                                   LogicalType edge_direction_type, idx_t num_src_columns) {
    string partition_name = DEFAULT_EDGE_PARTITION_PREFIX + edge_type;
    string property_schema_name = DEFAULT_EDGE_PROPERTYSCHEMA_PREFIX + edge_type;
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

        CreateIndexInfo id_idx_info(DEFAULT_SCHEMA, edge_type + "_id", IndexType::PHYSICAL_ID,
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
        adj_col_idx = vertex_ps_cat_entry->AppendAdjListKey(*(bulkload_ctx.client.get()), { edge_type });
    }

    duckdb::IndexType index_type = edge_direction_type == LogicalType::FORWARD_ADJLIST ?
        IndexType::FORWARD_CSR : IndexType::BACKWARD_CSR;
    string adj_idx_name = edge_direction_type == LogicalType::FORWARD_ADJLIST ?
        edge_type + "_fwd" : edge_type + "_bwd";
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
static inline void FillAdjListBuffer(bool load_backward_edge, idx_t &begin_idx, idx_t &end_idx, idx_t &src_seqno, idx_t cur_src_pid,
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
        if (load_backward_edge) {
            pid_pair.second = ve.dst_pid;
            lid_pair_to_epid_map_instance->emplace(pid_pair, ve.epid);
        }
        adj_list_buffers[cur_vertex_localextentID].store((uint32_t)cur_src_seqno, ve.dst_pid, ve.epid);
    }
}

// Phase 1 (count+store pass) for backward adj buffer (12d).
// Resolves bwd-dst LIDs→PIDs, looks up epids from lid_pair_to_epid_map, and stores
// raw edges directly — eliminating the Phase 3 ExtentIterator scan entirely.
static inline void FillBwdAdjListBuffer(bool load_backward_edge, idx_t &begin_idx, idx_t &end_idx, idx_t &src_seqno, idx_t cur_src_pid,
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
    s62_prepared_statement* prep_stmt = s62_prepare(bulkload_ctx.conn_id, const_cast<char*>(query.c_str()));
    s62_resultset_wrapper *resultset_wrapper;
    s62_num_rows rows = s62_execute(bulkload_ctx.conn_id, prep_stmt, &resultset_wrapper);
    spdlog::trace("[PopulateLidToPidMap] Query returned {} rows", rows);

    if (num_id_columns > 2) throw InvalidInputException("Do not support # of compound keys >= 3 currently");

    bulkload_ctx.lid_to_pid_map_index[label_name] = bulkload_ctx.lid_to_pid_map.size();
    auto& lid_pid_map = bulkload_ctx.lid_to_pid_map.emplace_back(label_name, FlatHashMap<LidPair, idx_t, LidPairHash>()).second;
    lid_pid_map.reserve(rows);

    while (s62_fetch_next(resultset_wrapper) != S62_END_OF_RESULT) {
        uint64_t pid = s62_get_id(resultset_wrapper, 0);
        uint64_t id1 = s62_get_uint64(resultset_wrapper, 1);
        uint64_t id2 = (num_id_columns == 2) ? s62_get_uint64(resultset_wrapper, 2) : 0;
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

        for (idx_t seqno = 0; seqno < data.size(); seqno++) {
            lid_key.first = key_column_1[seqno];
            lid_key.second = key_column_2[seqno];
            lid_to_pid_map_instance->emplace(lid_key, pid_base + seqno);
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
        if (bulkload_ctx.input_options.load_edge) {
            size_t map_idx = bulkload_ctx.lid_to_pid_map.size();
            bulkload_ctx.lid_to_pid_map_index[vertex_labelset] = map_idx;
            // Also register each individual label in the colon-separated labelset
            // so that edge CSVs using :START_ID(Post) can find "Post:Message" vertices.
            for (const auto& lbl : vertex_labels) {
                if (bulkload_ctx.lid_to_pid_map_index.find(lbl) == bulkload_ctx.lid_to_pid_map_index.end())
                    bulkload_ctx.lid_to_pid_map_index[lbl] = map_idx;
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

            if (bulkload_ctx.input_options.load_edge) {
                SUBTIMER_START(ReadCSVFileAndCreateExtents, "PopulateLidToPidMap");
                spdlog::trace("[ReadVertexCSVFileAndCreateVertexExtents] PopulateLidToPidMap");
                PopulateLidToPidMap(lid_to_pid_map_instance, key_column_idxs, data, new_eid);
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
        if (bulkload_ctx.input_options.load_edge) reader.SetLidToPidMap(&bulkload_ctx.lid_to_pid_map);
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
        if (bulkload_ctx.input_options.load_edge) {
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
    if (csv_vertex_files.size() > 0) ReadVertexCSVFileAndCreateVertexExtents(csv_vertex_files, bulkload_ctx);
}

// ---------------------------------------------------------------------------
// Incremental ID reconstruction
// ---------------------------------------------------------------------------

static void PrepareClient(BulkloadContext &bulkload_ctx) {
    spdlog::info("[PrepareClient] Try to connect to the workspace");
    bulkload_ctx.conn_id = s62_connect_with_client_context(&bulkload_ctx.client);
    if (bulkload_ctx.conn_id < 0 || s62_is_connected(bulkload_ctx.conn_id) != S62_CONNECTED) {
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
    s62_disconnect(bulkload_ctx.conn_id);
    spdlog::info("[ReconstructIDMappings] Reconstruct ID mappings for edge files done");
}

// ---------------------------------------------------------------------------
// Forward edge loading
// ---------------------------------------------------------------------------

static void ReadFwdEdgeCSVFilesAndCreateEdgeExtents(vector<LabeledFile> &csv_edge_files, BulkloadContext &bulkload_ctx) {
    SCOPED_TIMER_SIMPLE(ReadFwdEdgeCSVFilesAndCreateEdgeExtents, spdlog::level::info, spdlog::level::debug);
    spdlog::info("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Start to load {} CSV Edge Files", csv_edge_files.size());

    const size_t n_files = csv_edge_files.size();

    // --- Serial pre-scan: read src/dst labels from CSV headers for grouping.
    // Use file_size hint when available to skip expensive row-count scan. ---
    vector<string> file_src_labels(n_files), file_dst_labels(n_files);
    for (size_t i = 0; i < n_files; i++) {
        auto &f = csv_edge_files[i];
        GraphSIMDCSVFileParser probe;
        vector<int64_t> si, di;
        // num_lines=0 (default) → buffer sized to p.size() (safe).
        // Passing 1 would allocate only (num_columns+1) entries while find_indexes
        // processes the full file → overflow → SIGSEGV.
        probe.InitCSVFile(std::get<1>(f).c_str(), GraphComponentType::EDGE, '|');
        probe.GetSrcColumnInfo(si, file_src_labels[i]);
        probe.GetDstColumnInfo(di, file_dst_labels[i]);
    }

    // Pre-register lid_pair_to_epid_map entries so the vector stays stable
    // (no reallocation) during the parallel phase — pointers must not move.
    if (bulkload_ctx.input_options.load_backward_edge) {
        bulkload_ctx.lid_pair_to_epid_map.reserve(
            bulkload_ctx.lid_pair_to_epid_map.size() + n_files);
        for (size_t i = 0; i < n_files; i++) {
            string &et = std::get<0>(csv_edge_files[i]);
            if (bulkload_ctx.lid_pair_to_epid_map_index.find(et) ==
                    bulkload_ctx.lid_pair_to_epid_map_index.end()) {
                bulkload_ctx.lid_pair_to_epid_map_index[et] =
                    bulkload_ctx.lid_pair_to_epid_map.size();
                bulkload_ctx.lid_pair_to_epid_map.emplace_back(
                    et, FlatHashMap<LidPair, idx_t, LidPairHash>());
            }
        }
    }

    int hw  = static_cast<int>(std::thread::hardware_concurrency());
    // Each file has its own adj_list_buffers and writes to a unique edge-type
    // partition (unique part_id), so files are fully independent and can all
    // run in parallel regardless of src_vertex_label.
    int n_t = std::max(1, std::min(static_cast<int>(n_files), hw / 2));
    std::mutex catalog_mu;

    // --- Parallel for over individual edge files ---
    // Each iteration owns a fresh adj_list_buffers and writes to a distinct
    // edge-type partition.  Catalog mutations are protected by catalog_mu.
    // Storage writes go through CCM which is internally locked.
    #pragma omp parallel for schedule(dynamic,1) num_threads(n_t)
    for (int fi = 0; fi < static_cast<int>(n_files); fi++) {
        unordered_map<ExtentID, ExtentAdjBuffer> adj_list_buffers;

        {
            SCOPED_TIMER_SIMPLE(ReadSingleEdgeCSVFile, spdlog::level::info, spdlog::level::debug);

            auto &edge_file = csv_edge_files[fi];
            string &edge_type      = std::get<0>(edge_file);
            string &edge_file_path = std::get<1>(edge_file);
            OptionalFileSize &file_size = std::get<2>(edge_file);
            string src_vertex_label = file_src_labels[fi];
            string dst_vertex_label = file_dst_labels[fi];

            spdlog::info("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Start to load {} with edge type {}",
                         edge_file_path, edge_type);

            bool skip = false;
            {
                std::lock_guard<std::mutex> lk(catalog_mu);
                if (IsEdgeCatalogInfoExist(bulkload_ctx, edge_type)) {
                    spdlog::info("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Edge Catalog Info for {} is already exists",
                                 edge_type);
                    bulkload_ctx.skipped_labels.push_back(edge_type);
                    skip = true;
                }
            }
            if (skip) continue;

            GraphSIMDCSVFileParser reader;
            vector<int64_t> src_column_idx;
            vector<int64_t> dst_column_idx;
            vector<string> key_names;
            vector<LogicalType> types;

            spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] InitCSVFile");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "InitCSVFile");
            size_t approximated_num_rows =
                file_size.has_value() ?
                reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|', file_size.value()) :
                reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|');
            if (!reader.GetSchemaFromHeader(key_names, types)) { throw InvalidInputException("Invalid Schema Information"); }
            reader.GetSrcColumnInfo(src_column_idx, src_vertex_label);
            reader.GetDstColumnInfo(dst_column_idx, dst_vertex_label);
            if (src_column_idx.size() == 0 || dst_column_idx.size() == 0) {
                throw InvalidInputException("Invalid Edge File Format");
            }
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitCSVFile");

            spdlog::trace("Src column name = {} (idxs = [{}])", src_vertex_label, join_vector(src_column_idx));
            spdlog::trace("Dst column name = {} (idxs = [{}])", dst_vertex_label, join_vector(dst_column_idx));

            spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] InitLIDToPIDMap");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "InitLIDToPIDMap");
            auto src_idx_it = bulkload_ctx.lid_to_pid_map_index.find(src_vertex_label);
            if (src_idx_it == bulkload_ctx.lid_to_pid_map_index.end()) throw InvalidInputException("Corresponding src vertex file was not loaded");
            FlatHashMap<LidPair, idx_t, LidPairHash> &src_lid_to_pid_map_instance = bulkload_ctx.lid_to_pid_map[src_idx_it->second].second;

            auto dst_idx_it = bulkload_ctx.lid_to_pid_map_index.find(dst_vertex_label);
            if (dst_idx_it == bulkload_ctx.lid_to_pid_map_index.end()) throw InvalidInputException("Corresponding dst vertex file was not loaded");
            FlatHashMap<LidPair, idx_t, LidPairHash> &dst_lid_to_pid_map_instance = bulkload_ctx.lid_to_pid_map[dst_idx_it->second].second;
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitLIDToPIDMap");

            spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] InitLIDPairToEPIDMap");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "InitLIDPairToEPIDMap");
            FlatHashMap<LidPair, idx_t, LidPairHash> *lid_pair_to_epid_map_instance = nullptr;
            if (bulkload_ctx.input_options.load_backward_edge) {
                // Pre-registered above; vector is stable, pointer will not move.
                auto epid_it = bulkload_ctx.lid_pair_to_epid_map_index.find(edge_type);
                lid_pair_to_epid_map_instance = &bulkload_ctx.lid_pair_to_epid_map[epid_it->second].second;
                lid_pair_to_epid_map_instance->reserve(approximated_num_rows);
            }
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitLIDPairToEPIDMap");

            DataChunk data;
            data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

            spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] CreateEdgeCatalogInfos");
            PartitionCatalogEntry *partition_cat;
            PropertySchemaCatalogEntry *property_schema_cat;
            {
                std::lock_guard<std::mutex> lk(catalog_mu);
                CreateEdgeCatalogInfos(bulkload_ctx, edge_type, key_names, types, src_vertex_label, dst_vertex_label,
                    partition_cat, property_schema_cat, LogicalType::FORWARD_ADJLIST, src_column_idx.size());
            }

            // adj_list_buffers is fresh (declared per-file in the outer loop)

            LidPair prev_id {0, 0};
            idx_t cur_src_pid = 0, prev_src_pid = 0;
            ExtentID cur_vertex_extentID;
            ExtentID cur_vertex_localextentID;
            idx_t vertex_seqno;
            bool is_first_tuple_processed = false;
            PartitionID cur_part_id = 0;

            spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Start to read and create edge extents");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "ReadCSVFile and CreateEdgeExtents");
            while (true) {
                SCOPED_TIMER_SIMPLE(ReadCSVFileAndCreateEdgeExtents, spdlog::level::debug, spdlog::level::trace);

                SUBTIMER_START(ReadCSVFileAndCreateEdgeExtents, "ReadCSVFile");
                spdlog::trace("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] ReadCSVFile");
                bool eof = reader.ReadCSVFile(key_names, types, data);
                SUBTIMER_STOP(ReadCSVFileAndCreateEdgeExtents, "ReadCSVFile");
                if (eof) break;

                ExtentID new_eid = partition_cat->GetNewExtentID();
                spdlog::trace("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] New Extent ID = {}", new_eid);

                idx_t epid_base = (idx_t) new_eid;
                epid_base = epid_base << 32;

                vector<idx_t *> src_key_columns, dst_key_columns;
                src_key_columns.resize(src_column_idx.size());
                dst_key_columns.resize(dst_column_idx.size());
                for (size_t i = 0; i < src_key_columns.size(); i++) {
                    src_key_columns[i] = (idx_t *)data.data[src_column_idx[i]].GetData();
                }
                for (size_t i = 0; i < dst_key_columns.size(); i++) {
                    dst_key_columns[i] = (idx_t *)data.data[dst_column_idx[i]].GetData();
                }

                idx_t src_seqno = 0;
                idx_t begin_idx = 0, end_idx;
                idx_t max_seqno = data.size();
                LidPair src_key{0, 0}, dst_key{0, 0};

                bool load_backward_edge = bulkload_ctx.input_options.load_backward_edge;
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
                        // First src vertex is dangling — scan forward to a valid one.
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
                SUBTIMER_START(ReadSingleEdgeCSVFile, "FillAdjListBuffer");
                if (src_column_idx.size() == 1) {
                    while (src_seqno < max_seqno) {
                        src_key.first = src_key_columns[0][src_seqno];
                        const idx_t *sp = src_lid_to_pid_map_instance.get_ptr(src_key);
                        if (!sp) { src_seqno++; continue; }  // dangling src — skip
                        cur_src_pid = *sp;
                        src_key_columns[0][src_seqno] = cur_src_pid;

                        if (src_key == prev_id) {
                            src_seqno++;
                        } else {
                            end_idx = src_seqno;
                            FillAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
                                              dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
                                              lid_pair_to_epid_map_instance, adj_list_buffers, epid_base);
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
                        if (!sp) { src_seqno++; continue; }  // dangling src — skip
                        cur_src_pid = *sp;
                        src_key_columns[0][src_seqno] = cur_src_pid;

                        if (src_key == prev_id) {
                            src_seqno++;
                        } else {
                            end_idx = src_seqno;
                            FillAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
                                              dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
                                              lid_pair_to_epid_map_instance, adj_list_buffers, epid_base);
                            prev_id = src_key;
                            prev_src_pid = cur_src_pid;
                            begin_idx = src_seqno;
                            src_seqno++;
                        }
                    }
                } else {
                    throw InvalidInputException("Do not support # of compound keys >= 3 currently");
                }
                SUBTIMER_STOP(ReadSingleEdgeCSVFile, "FillAdjListBuffer");

                SUBTIMER_START(ReadSingleEdgeCSVFile, "FillAdjListBuffer for Remaining");
                end_idx = src_seqno;
                FillAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, cur_src_pid, vertex_seqno,
                                  dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
                                  lid_pair_to_epid_map_instance, adj_list_buffers, epid_base);
                SUBTIMER_STOP(ReadSingleEdgeCSVFile, "FillAdjListBuffer for Remaining");
                } // chunk_has_valid_src

                SUBTIMER_START(ReadSingleEdgeCSVFile, "CreateExtent and AddExtent");
                spdlog::trace("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] CreateExtent and AddExtent");
                bulkload_ctx.ext_mng.CreateExtent(*(bulkload_ctx.client.get()), data, *partition_cat, *property_schema_cat, new_eid);
                property_schema_cat->AddExtent(new_eid, data.size());
                SUBTIMER_STOP(ReadSingleEdgeCSVFile, "CreateExtent and AddExtent");
            }
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "ReadCSVFile and CreateEdgeExtents");

            spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] AppendFlatAdjListChunk (Phase 2+3)");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "AppendFlatAdjListChunk");
            PartitionCatalogEntry *src_part_cat_entry;
            {
                // LookupPartition/GetEntry touch shared catalog state — must hold catalog_mu
                // even though we are reading: concurrent writes from CreateEdgeCatalogInfos
                // in other threads can trigger rehash/reallocation in the same container.
                std::lock_guard<std::mutex> lk(catalog_mu);
                vector<idx_t> src_part_oids = bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { src_vertex_label }, GraphComponentType::VERTEX);
                src_part_cat_entry = (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, src_part_oids[0]);
            }
            AppendFlatAdjListChunk(bulkload_ctx, LogicalType::FORWARD_ADJLIST, cur_part_id,
                src_part_cat_entry->GetLocalExtentID(), adj_list_buffers);
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "AppendFlatAdjListChunk");
            ChunkCacheManager::ccm->ThrottleIfNeeded();
        }
    }
    spdlog::info("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Load CSV Edge Files Done");
}

static void ReadFwdEdgeFilesAndCreateEdgeExtents(BulkloadContext &bulkload_ctx) {
    vector<LabeledFile> json_edge_files;
    vector<LabeledFile> csv_edge_files;
    SeperateFilesByExtension(bulkload_ctx.input_options.edge_files, json_edge_files, csv_edge_files);
    spdlog::info("[ReadEdgeFilesAndCreateEdgeExtents] {} JSON Edge Files and {} CSV Edge Files", json_edge_files.size(), csv_edge_files.size());

    if (json_edge_files.size() > 0) {
        throw NotImplementedException("JSON Edge File is not supported yet");
    }

    ReadFwdEdgeCSVFilesAndCreateEdgeExtents(csv_edge_files, bulkload_ctx);
}

// ---------------------------------------------------------------------------
// Backward edge loading
// ---------------------------------------------------------------------------

static void ReadBwdEdgeCSVFilesAndCreateEdgeExtents(vector<LabeledFile> &csv_edge_files, BulkloadContext &bulkload_ctx) {
    SCOPED_TIMER_SIMPLE(ReadBwdEdgesCSVFileAndCreateEdgeExtents, spdlog::level::info, spdlog::level::debug);
    spdlog::info("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Start to load {} CSV Edge Files", csv_edge_files.size());

    const size_t n_files = csv_edge_files.size();

    // --- Serial pre-scan: read reversed src/dst labels for grouping.
    // In the bwd direction: src = original dst vertex, dst = original src vertex. ---
    vector<string> file_src_labels(n_files), file_dst_labels(n_files);
    for (size_t i = 0; i < n_files; i++) {
        auto &f = csv_edge_files[i];
        GraphSIMDCSVFileParser probe;
        vector<int64_t> si, di;
        probe.InitCSVFile(std::get<1>(f).c_str(), GraphComponentType::EDGE, '|');
        probe.GetDstColumnInfo(si, file_src_labels[i]); // Reverse
        probe.GetSrcColumnInfo(di, file_dst_labels[i]); // Reverse
    }

    // --- Group files by (reversed) src_vertex_label = original dst_vertex_label.
    // Each file has its own adj_list_buffers and writes to a unique edge-type
    // partition, so all files can run in parallel. ---
    int hw  = static_cast<int>(std::thread::hardware_concurrency());
    int n_t = std::max(1, std::min(static_cast<int>(n_files), hw / 2));
    std::mutex catalog_mu;

    // --- Parallel for over individual backward edge files ---
    #pragma omp parallel for schedule(dynamic,1) num_threads(n_t)
    for (int fi = 0; fi < static_cast<int>(n_files); fi++) {
        unordered_map<ExtentID, ExtentAdjBuffer> adj_list_buffers;

        SCOPED_TIMER_SIMPLE(ReadSingleEdgeCSVFile, spdlog::level::info, spdlog::level::debug);

            auto &edge_file = csv_edge_files[fi];
            string &edge_type      = std::get<0>(edge_file);
            string &edge_file_path = std::get<1>(edge_file);
            OptionalFileSize &file_size = std::get<2>(edge_file);
            string src_vertex_label = file_src_labels[fi];
            string dst_vertex_label = file_dst_labels[fi];

            spdlog::info("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Start to load {} with edge type {}",
                         edge_file_path, edge_type);

            {
                std::lock_guard<std::mutex> lk(catalog_mu);
                if (isSkippedLabel(bulkload_ctx, edge_type)) {
                    spdlog::info("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Edge Catalog Info for {} is already exists",
                                 edge_type);
                    continue;
                }
            }

            vector<string> key_names;
            vector<int64_t> src_column_idx;
            vector<int64_t> dst_column_idx;
            vector<LogicalType> types;
            GraphSIMDCSVFileParser reader;

            spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] InitCSVFile");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "InitCSVFile");
            file_size.has_value() ?
                reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|', file_size.value()) :
                reader.InitCSVFile(edge_file_path.c_str(), GraphComponentType::EDGE, '|');
            if (!reader.GetSchemaFromHeader(key_names, types)) { throw InvalidInputException("Invalid Schema Information"); }
            reader.GetDstColumnInfo(src_column_idx, src_vertex_label); // Reverse
            reader.GetSrcColumnInfo(dst_column_idx, dst_vertex_label); // Reverse
            if (src_column_idx.size() == 0 || dst_column_idx.size() == 0) {
                throw InvalidInputException("Invalid Edge File Format");
            }
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitCSVFile");

            spdlog::trace("Src vertex label = {} (idxs = [{}])", src_vertex_label, join_vector(src_column_idx));
            spdlog::trace("Dst vertex label = {} (idxs = [{}])", dst_vertex_label, join_vector(dst_column_idx));

            spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] InitLIDToPIDMap");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "InitLIDToPIDMap");
            auto src_idx_it = bulkload_ctx.lid_to_pid_map_index.find(src_vertex_label);
            if (src_idx_it == bulkload_ctx.lid_to_pid_map_index.end()) throw InvalidInputException("Corresponding src vertex file was not loaded");
            FlatHashMap<LidPair, idx_t, LidPairHash> &src_lid_to_pid_map_instance = bulkload_ctx.lid_to_pid_map[src_idx_it->second].second;

            auto dst_idx_it = bulkload_ctx.lid_to_pid_map_index.find(dst_vertex_label);
            if (dst_idx_it == bulkload_ctx.lid_to_pid_map_index.end()) throw InvalidInputException("Corresponding dst vertex file was not loaded");
            FlatHashMap<LidPair, idx_t, LidPairHash> &dst_lid_to_pid_map_instance = bulkload_ctx.lid_to_pid_map[dst_idx_it->second].second;
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitLIDToPIDMap");

            spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] InitLIDPairToEPIDMap");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "InitLIDPairToEPIDMap");
            auto edge_idx_it = bulkload_ctx.lid_pair_to_epid_map_index.find(edge_type);
            if (edge_idx_it == bulkload_ctx.lid_pair_to_epid_map_index.end()) throw InvalidInputException("[Error] Lid Pair to EPid Map does not exists");
            FlatHashMap<LidPair, idx_t, LidPairHash> &lid_pair_to_epid_map_instance = bulkload_ctx.lid_pair_to_epid_map[edge_idx_it->second].second;
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitLIDPairToEPIDMap");

            DataChunk data;
            data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

            spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] CreateEdgeCatalogInfos");
            PartitionCatalogEntry *partition_cat;
            PropertySchemaCatalogEntry *property_schema_cat;
            {
                std::lock_guard<std::mutex> lk(catalog_mu);
                CreateEdgeCatalogInfos(bulkload_ctx, edge_type, key_names, types, src_vertex_label, dst_vertex_label,
                    partition_cat, property_schema_cat, LogicalType::BACKWARD_ADJLIST, dst_column_idx.size());
            }

            // adj_list_buffers is fresh (declared per-file in the outer loop)

            LidPair prev_id {0, 0};
            idx_t cur_src_pid = 0, prev_src_pid = 0;
            ExtentID cur_vertex_extentID;
            ExtentID cur_vertex_localextentID;
            idx_t vertex_seqno;
            bool is_first_tuple_processed = false;
            PartitionID cur_part_id = 0;

            spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Start to read and create edge extents");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "ReadCSVFile and CreateEdgeExtents");
            while (true) {
                SCOPED_TIMER_SIMPLE(ReadCSVFileAndCreateEdgeExtents, spdlog::level::debug, spdlog::level::trace);

                SUBTIMER_START(ReadCSVFileAndCreateEdgeExtents, "ReadCSVFile");
                spdlog::trace("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] ReadCSVFile");
                bool eof = reader.ReadCSVFile(key_names, types, data);
                SUBTIMER_STOP(ReadCSVFileAndCreateEdgeExtents, "ReadCSVFile");
                if (eof) break;

                vector<idx_t *> src_key_columns, dst_key_columns;
                src_key_columns.resize(src_column_idx.size());
                dst_key_columns.resize(dst_column_idx.size());
                for (size_t i = 0; i < src_key_columns.size(); i++) src_key_columns[i] = (idx_t *)data.data[src_column_idx[i]].GetData();
                for (size_t i = 0; i < dst_key_columns.size(); i++) dst_key_columns[i] = (idx_t *)data.data[dst_column_idx[i]].GetData();

                idx_t src_seqno = 0;
                idx_t begin_idx = 0, end_idx;
                idx_t max_seqno = data.size();
                LidPair src_key{0, 0}, dst_key{0, 0};

                bool bwd_chunk_has_valid_src = true;

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
                SUBTIMER_START(ReadSingleEdgeCSVFile, "FillBwdAdjListBuffer");
                if (src_column_idx.size() == 1) {
                    while (src_seqno < max_seqno) {
                        src_key.first = src_key_columns[0][src_seqno];
                        const idx_t *sp = src_lid_to_pid_map_instance.get_ptr(src_key);
                        if (!sp) { src_seqno++; continue; }
                        cur_src_pid = *sp;

                        if (src_key == prev_id) {
                            src_seqno++;
                        } else {
                            end_idx = src_seqno;
                            FillBwdAdjListBuffer(bulkload_ctx.input_options.load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
                                                 dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
                                                 lid_pair_to_epid_map_instance, adj_list_buffers);
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

                        if (src_key == prev_id) {
                            src_seqno++;
                        } else {
                            end_idx = src_seqno;
                            FillBwdAdjListBuffer(bulkload_ctx.input_options.load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
                                                 dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
                                                 lid_pair_to_epid_map_instance, adj_list_buffers);
                            prev_id = src_key;
                            prev_src_pid = cur_src_pid;
                            begin_idx = src_seqno;
                            src_seqno++;
                        }
                    }
                } else {
                    throw InvalidInputException("Do not support # of compound keys >= 3 currently");
                }
                SUBTIMER_STOP(ReadSingleEdgeCSVFile, "FillBwdAdjListBuffer");

                SUBTIMER_START(ReadSingleEdgeCSVFile, "FillBwdAdjListBuffer for Remaining");
                spdlog::trace("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] FillBwdAdjListBuffer for Remaining");
                end_idx = src_seqno;
                FillBwdAdjListBuffer(bulkload_ctx.input_options.load_backward_edge, begin_idx, end_idx, src_seqno, cur_src_pid, vertex_seqno,
                                     dst_column_idx, dst_key_columns, dst_lid_to_pid_map_instance,
                                     lid_pair_to_epid_map_instance, adj_list_buffers);
                SUBTIMER_STOP(ReadSingleEdgeCSVFile, "FillBwdAdjListBuffer for Remaining");
                } // bwd_chunk_has_valid_src
            }
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "ReadCSVFile and CreateEdgeExtents");

            spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] AppendFlatAdjListChunk (Phase 2)");
            SUBTIMER_START(ReadSingleEdgeCSVFile, "AppendFlatAdjListChunk");
            PartitionCatalogEntry *src_part_cat_entry;
            {
                std::lock_guard<std::mutex> lk(catalog_mu);
                vector<idx_t> src_part_oids = bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { src_vertex_label }, GraphComponentType::VERTEX);
                src_part_cat_entry = (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, src_part_oids[0]);
            }
            AppendFlatAdjListChunk(bulkload_ctx, LogicalType::BACKWARD_ADJLIST, cur_part_id,
                src_part_cat_entry->GetLocalExtentID(), adj_list_buffers);
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "AppendFlatAdjListChunk");
            ChunkCacheManager::ccm->ThrottleIfNeeded();
    }
    spdlog::info("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Load CSV Edge Files Done");
}

static void ReadBwdEdgeFilesAndCreateEdgeExtents(BulkloadContext &bulkload_ctx) {
    if (!bulkload_ctx.input_options.load_backward_edge) {
        spdlog::info("[ReadBwdEdgeFilesAndCreateEdgeExtents] Skip loading backward edges");
        return;
    }

    vector<LabeledFile> json_edge_files;
    vector<LabeledFile> csv_edge_files;
    SeperateFilesByExtension(bulkload_ctx.input_options.edge_files, json_edge_files, csv_edge_files);
    spdlog::info("[ReadEdgeFilesAndCreateEdgeExtents] {} JSON Edge Files and {} CSV Edge Files", json_edge_files.size(), csv_edge_files.size());

    if (json_edge_files.size() > 0) {
        throw NotImplementedException("JSON Edge File is not supported yet");
    }

    ReadBwdEdgeCSVFilesAndCreateEdgeExtents(csv_edge_files, bulkload_ctx);
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

    ChunkCacheManager::ccm = new ChunkCacheManager(opts_.output_dir.c_str(), opts_.standalone);
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

void BulkloadPipeline::LoadForwardEdges() {
    ReadFwdEdgeFilesAndCreateEdgeExtents(*ctx_);
}

void BulkloadPipeline::LoadBackwardEdges() {
    ReadBwdEdgeFilesAndCreateEdgeExtents(*ctx_);
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

        SUBTIMER_START(Bulkload, "Edge Fwd Files");
        LoadForwardEdges();
        SUBTIMER_STOP(Bulkload, "Edge Fwd Files");

        SUBTIMER_START(Bulkload, "Edge Bwd Files");
        LoadBackwardEdges();
        SUBTIMER_STOP(Bulkload, "Edge Bwd Files");
    } catch (const std::system_error& e) {
        spdlog::error("[BulkloadPipeline::Run] Caught system_error with code [{}] meaning [{}]", e.code().value(), e.what());
    }

    spdlog::info("[BulkloadPipeline] Post Processing");
    RunPostProcessing();
}

} // namespace duckdb
