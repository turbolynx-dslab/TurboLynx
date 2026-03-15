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
    bool load_backward_edge = bulkload_ctx.input_options.load_backward_edge;

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

            if (load_backward_edge) local_epid_map.reserve(approximated_num_rows);
            FlatHashMap<LidPair, idx_t, LidPairHash> *epid_map_ptr =
                load_backward_edge ? &local_epid_map : nullptr;

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
                            FillAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
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
                            FillAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
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
                FillAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, cur_src_pid, vertex_seqno,
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
        if (!load_backward_edge) continue;

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
                            FillBwdAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
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
                            FillBwdAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, prev_src_pid, vertex_seqno,
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
                FillBwdAdjListBuffer(load_backward_edge, begin_idx, end_idx, src_seqno, cur_src_pid, vertex_seqno,
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

void BulkloadPipeline::LoadEdges() {
    ReadEdgeFilesInterleaved(*ctx_);
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
