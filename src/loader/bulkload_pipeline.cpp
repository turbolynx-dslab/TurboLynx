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
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"

#include <filesystem>

namespace fs = std::filesystem;

namespace duckdb {

// ---------------------------------------------------------------------------
// BulkloadContext — internal detail, not exposed in the header
// ---------------------------------------------------------------------------

struct BulkloadContext {
    BulkloadOptions input_options;
    std::shared_ptr<ClientContext> client;
    Catalog &catalog;
    GraphCatalogEntry *graph_cat;
    ExtentManager ext_mng;
    vector<
        std::pair<string, unordered_map<LidPair, idx_t, LidPairHash>>>
        lid_to_pid_map;  // For Forward & Backward AdjList
    vector<
        std::pair<string, unordered_map<LidPair, idx_t, LidPairHash>>>
        lid_pair_to_epid_map;  // For Backward AdjList
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


static void ClearAdjListBuffers(
    unordered_map<ExtentID, std::pair<std::pair<uint64_t, uint64_t>,
                                      vector<vector<idx_t>>>> &adj_list_buffers)
{
    vector<unordered_map<ExtentID, std::pair<std::pair<uint64_t, uint64_t>,
                                             vector<vector<idx_t>>>>::iterator>
        iterators;
    iterators.reserve(adj_list_buffers.size());
    for (auto it = adj_list_buffers.begin(); it != adj_list_buffers.end(); ++it) {
        iterators.push_back(it);
    }

    #pragma omp parallel for num_threads(32)
    for (size_t i = 0; i < iterators.size(); i++) {
        auto &buffers = iterators[i]->second.second;
        for (auto &inner_vector : buffers) {
            inner_vector.clear();
        }
        iterators[i]->second.first.first = 0;
        iterators[i]->second.first.second = 0;
    }
}

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

static void AppendAdjListChunk(BulkloadContext& bulkload_ctx, LogicalType edge_direction_type, PartitionID part_id, ExtentID max_extent_id,
    unordered_map<ExtentID, std::pair<std::pair<uint64_t, uint64_t>, vector<vector<idx_t>>>> &adj_list_buffers) {
    vector<vector<idx_t>> empty_adj_list;

    spdlog::trace("[AppendAdjListChunk] edge_direction_type: {}, part_id: {}, max_extent_id: {}", edge_direction_type.ToString(), part_id, max_extent_id);

    for (auto idx = 0; idx < max_extent_id; idx++) {
        ExtentID cur_vertex_localextentID = idx;
        auto iter = adj_list_buffers.find(cur_vertex_localextentID);
        vector<vector<idx_t>> &adj_list_buffer =
            iter == adj_list_buffers.end() ?
            empty_adj_list : iter->second.second;
        size_t num_adj_list =
            iter == adj_list_buffers.end() ?
            0 : iter->second.first.first + 1;
        size_t adj_len_total =
            iter == adj_list_buffers.end() ?
            0 : 2 * iter->second.first.second;

        if (adj_len_total == 0) num_adj_list = 0;

        DataChunk adj_list_chunk;
        vector<LogicalType> adj_list_chunk_types = { edge_direction_type };
        vector<data_ptr_t> adj_list_datas(1);

        vector<idx_t> tmp_adj_list_buffer;
        const size_t slot_for_num_adj = 1;

        tmp_adj_list_buffer.resize(slot_for_num_adj + num_adj_list + adj_len_total);
        tmp_adj_list_buffer[0] = num_adj_list;

        size_t offset = num_adj_list;
        for (size_t i = 0; i < num_adj_list; i++) {
            for (size_t j = 0; j < adj_list_buffer[i].size(); j++) {
                tmp_adj_list_buffer[slot_for_num_adj + offset + j] = adj_list_buffer[i][j];
            }
            offset += adj_list_buffer[i].size();
            tmp_adj_list_buffer[i + slot_for_num_adj] = offset;
        }

        adj_list_datas[0] = (data_ptr_t) tmp_adj_list_buffer.data();
        adj_list_chunk.Initialize(adj_list_chunk_types, adj_list_datas, STORAGE_STANDARD_VECTOR_SIZE);

        ExtentID cur_vertex_extentID = cur_vertex_localextentID | (((uint32_t)part_id) << 16);
        bulkload_ctx.ext_mng.AppendChunkToExistingExtent(*(bulkload_ctx.client.get()), adj_list_chunk, cur_vertex_extentID);
        adj_list_chunk.Destroy();
    }
}

static inline void FillAdjListBuffer(bool load_backward_edge, idx_t &begin_idx, idx_t &end_idx, idx_t &src_seqno, idx_t cur_src_pid,
                   idx_t &vertex_seqno, std::vector<int64_t> &dst_column_idx, vector<idx_t *> dst_key_columns,
                   unordered_map<LidPair, idx_t, LidPairHash> &dst_lid_to_pid_map_instance,
                   unordered_map<LidPair, idx_t, LidPairHash> *lid_pair_to_epid_map_instance,
                   unordered_map<ExtentID, std::pair<std::pair<uint64_t, uint64_t>, vector<vector<idx_t>>>> &adj_list_buffers,
                   idx_t epid_base, idx_t src_lid = 0) {
    idx_t cur_src_seqno = GET_SEQNO_FROM_PHYSICAL_ID(cur_src_pid);

    ExtentID cur_vertex_extentID = static_cast<ExtentID>(cur_src_pid >> 32);
    ExtentID cur_vertex_localextentID = cur_vertex_extentID & 0xFFFF;
    vector<vector<idx_t>> *adj_list_buffer;

    if (adj_list_buffers.find(cur_vertex_localextentID) == adj_list_buffers.end()) {
        vector<vector<idx_t>> empty_adj_list;
        empty_adj_list.resize(STORAGE_STANDARD_VECTOR_SIZE);
        adj_list_buffers[cur_vertex_localextentID] =
            std::make_pair<std::pair<uint64_t, uint64_t>,
                           vector<vector<idx_t>>>(
                std::make_pair<uint64_t, uint64_t>(0, 0),
                std::move(empty_adj_list));
    }
    auto &adj_list_buffer_extent = adj_list_buffers[cur_vertex_localextentID];
    adj_list_buffer = &(adj_list_buffer_extent.second);

    if (adj_list_buffer_extent.first.first < cur_src_seqno) {
        adj_list_buffer_extent.first.first = cur_src_seqno;
    }
    if (end_idx > begin_idx) {
        adj_list_buffer_extent.first.second += (end_idx - begin_idx);
    }

    idx_t dst_seqno, cur_dst_pid;
    LidPair dst_key{0, 0};
    LidPair pid_pair{cur_src_pid, 0};
    end_idx = src_seqno;
    if (load_backward_edge) {
        if (dst_column_idx.size() == 1) {
            for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
                dst_key.first = dst_key_columns[0][dst_seqno];
                cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
                dst_key_columns[0][dst_seqno] = cur_dst_pid;
                (*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
                (*adj_list_buffer)[cur_src_seqno].push_back(epid_base + dst_seqno);
                pid_pair.second = cur_dst_pid;
                lid_pair_to_epid_map_instance->emplace(pid_pair, epid_base + dst_seqno);
            }
        } else if (dst_column_idx.size() == 2) {
            for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
                dst_key.first = dst_key_columns[0][dst_seqno];
                dst_key.second = dst_key_columns[1][dst_seqno];
                cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
                dst_key_columns[0][dst_seqno] = cur_dst_pid;
                (*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
                (*adj_list_buffer)[cur_src_seqno].push_back(epid_base + dst_seqno);
                pid_pair.second = cur_dst_pid;
                lid_pair_to_epid_map_instance->emplace(pid_pair, epid_base + dst_seqno);
            }
        }
    } else {
        if (dst_column_idx.size() == 1) {
            for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
                dst_key.first = dst_key_columns[0][dst_seqno];
                cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
                dst_key_columns[0][dst_seqno] = cur_dst_pid;
                (*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
                (*adj_list_buffer)[cur_src_seqno].push_back(epid_base + dst_seqno);
            }
        } else if (dst_column_idx.size() == 2) {
            for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
                dst_key.first = dst_key_columns[0][dst_seqno];
                dst_key.second = dst_key_columns[1][dst_seqno];
                cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
                dst_key_columns[0][dst_seqno] = cur_dst_pid;
                (*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
                (*adj_list_buffer)[cur_src_seqno].push_back(epid_base + dst_seqno);
            }
        }
    }
}

static inline void FillBwdAdjListBuffer(bool load_backward_edge, idx_t &begin_idx, idx_t &end_idx, idx_t &src_seqno, idx_t cur_src_pid,
                   idx_t &vertex_seqno, std::vector<int64_t> &dst_column_idx, vector<idx_t *> dst_key_columns,
                   unordered_map<LidPair, idx_t, LidPairHash> &dst_lid_to_pid_map_instance,
                   unordered_map<LidPair, idx_t, LidPairHash> &lid_pair_to_epid_map_instance,
                   unordered_map<ExtentID, std::pair<std::pair<uint64_t, uint64_t>, vector<vector<idx_t>>>> &adj_list_buffers) {
    idx_t cur_src_seqno = GET_SEQNO_FROM_PHYSICAL_ID(cur_src_pid);

    ExtentID cur_vertex_extentID = static_cast<ExtentID>(cur_src_pid >> 32);
    ExtentID cur_vertex_localextentID = cur_vertex_extentID & 0xFFFF;
    vector<vector<idx_t>> *adj_list_buffer;
    if (adj_list_buffers.find(cur_vertex_localextentID) == adj_list_buffers.end()) {
        vector<vector<idx_t>> empty_adj_list;
        empty_adj_list.resize(STORAGE_STANDARD_VECTOR_SIZE);
        adj_list_buffers[cur_vertex_localextentID] =
            std::make_pair<std::pair<uint64_t, uint64_t>,
                           vector<vector<idx_t>>>(
                std::make_pair<uint64_t, uint64_t>(0, 0),
                std::move(empty_adj_list));
    }
    auto &adj_list_buffer_extent = adj_list_buffers[cur_vertex_localextentID];
    adj_list_buffer = &(adj_list_buffer_extent.second);

    if (adj_list_buffer_extent.first.first < cur_src_seqno) {
        adj_list_buffer_extent.first.first = cur_src_seqno;
    }
    if (end_idx > begin_idx) {
        adj_list_buffer_extent.first.second += (end_idx - begin_idx);
    }

    idx_t dst_seqno, cur_dst_pid, peid;
    LidPair dst_key{0, 0};
    LidPair pid_pair {0, cur_src_pid};
    end_idx = src_seqno;

    if (dst_column_idx.size() == 1) {
        for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
            dst_key.first = dst_key_columns[0][dst_seqno];
            cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
            pid_pair.first = cur_dst_pid;
            peid = lid_pair_to_epid_map_instance.at(pid_pair);
            (*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
            (*adj_list_buffer)[cur_src_seqno].push_back(peid);
        }
    } else if (dst_column_idx.size() == 2) {
        for (dst_seqno = begin_idx; dst_seqno < end_idx; dst_seqno++) {
            dst_key.first = dst_key_columns[0][dst_seqno];
            dst_key.second = dst_key_columns[1][dst_seqno];
            cur_dst_pid = dst_lid_to_pid_map_instance.at(dst_key);
            pid_pair.first = cur_dst_pid;
            peid = lid_pair_to_epid_map_instance.at(pid_pair);
            (*adj_list_buffer)[cur_src_seqno].push_back(cur_dst_pid);
            (*adj_list_buffer)[cur_src_seqno].push_back(peid);
        }
    }
}

// ---------------------------------------------------------------------------
// LID-to-PID map helpers
// ---------------------------------------------------------------------------

static void PopulateLidToPidMap(BulkloadContext &bulkload_ctx, std::string &label_name, std::string &query, size_t num_id_columns) {
    spdlog::trace("[PopulateLidToPidMap] Query: {}", query);
    s62_prepared_statement* prep_stmt = s62_prepare(const_cast<char*>(query.c_str()));
    s62_resultset_wrapper *resultset_wrapper;
    s62_num_rows rows = s62_execute(prep_stmt, &resultset_wrapper);
    spdlog::trace("[PopulateLidToPidMap] Query returned {} rows", rows);

    if (num_id_columns > 2) throw InvalidInputException("Do not support # of compound keys >= 3 currently");

    auto& lid_pid_map = bulkload_ctx.lid_to_pid_map.emplace_back(label_name, unordered_map<LidPair, idx_t, LidPairHash>()).second;
    lid_pid_map.reserve(rows);

    while (s62_fetch_next(resultset_wrapper) != S62_END_OF_RESULT) {
        uint64_t pid = s62_get_id(resultset_wrapper, 0);
        uint64_t id1 = s62_get_uint64(resultset_wrapper, 1);
        uint64_t id2 = (num_id_columns == 2) ? s62_get_uint64(resultset_wrapper, 2) : 0;
        lid_pid_map.emplace(LidPair(id1, id2), pid);
    }
}

static void PopulateLidToPidMap(unordered_map<LidPair, idx_t, LidPairHash> *lid_to_pid_map_instance, const vector<idx_t> &key_column_idxs, DataChunk &data, ExtentID new_eid) {
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
        unordered_map<LidPair, idx_t, LidPairHash> *lid_to_pid_map_instance;
        if (bulkload_ctx.input_options.load_edge) {
            bulkload_ctx.lid_to_pid_map.emplace_back(vertex_labelset, unordered_map<LidPair, idx_t, LidPairHash>());
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
    }
    spdlog::debug("[ReadVertexCSVFileAndCreateVertexExtents] Flush Dirty Segments and Delete From Cache");
    ChunkCacheManager::ccm->FlushDirtySegmentsAndDeleteFromcache(false);
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
        if (bulkload_ctx.input_options.load_edge) reader.SetLidToPidMap(&bulkload_ctx.lid_to_pid_map);
        reader.InitJsonFile(vertex_file_path.c_str());
        SUBTIMER_STOP(ReadSingleVertexJSONFile, "GraphSIMDJSONFileParser Init");

        SUBTIMER_START(ReadSingleVertexJSONFile, "GraphSIMDJSONFileParser LoadJSON");
        DataChunk data;
        vector<string> label_set;
        ParseLabelSet(vertex_labelset, label_set);
        reader.LoadJson(vertex_labelset, label_set, bulkload_ctx.graph_cat, GraphComponentType::VERTEX);
        SUBTIMER_STOP(ReadSingleVertexJSONFile, "GraphSIMDJSONFileParser LoadJSON");

        spdlog::info("[ReadVertexJSONFileAndCreateVertexExtents] Load {} Done", vertex_file_path);
    }
    spdlog::debug("[ReadVertexJSONFileAndCreateVertexExtents] Flush Dirty Segments and Delete From Cache");
    ChunkCacheManager::ccm->FlushDirtySegmentsAndDeleteFromcache(false);
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
    s62_connect_with_client_context(&bulkload_ctx.client);
    auto state = s62_is_connected();
    if (state != S62_CONNECTED) {
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

        auto src_it = std::find_if(bulkload_ctx.lid_to_pid_map.begin(), bulkload_ctx.lid_to_pid_map.end(),
            [&src_vertex_label](const std::pair<string, unordered_map<LidPair, idx_t, LidPairHash>> &element) {
                return element.first.find(src_vertex_label) != string::npos;
            });

        spdlog::debug("[ReconstructIDMappings] Reconstruct Src Vertex Label = {}", src_vertex_label);
        SUBTIMER_START(ReconstructIDMappingForFile, "Reconstruct Src Vertex Label");
        if (src_it == bulkload_ctx.lid_to_pid_map.end()) {
            auto id_col_names = ObtainIdColumnNames(bulkload_ctx, src_vertex_label);
            std::string src_lid_pid_query = ConstructLidPidRetrievalQuery(bulkload_ctx, src_vertex_label, id_col_names);
            PopulateLidToPidMap(bulkload_ctx, src_vertex_label, src_lid_pid_query, id_col_names.size());
        }
        else {
            spdlog::trace("[ReconstructIDMappings] Mapping for Src Vertex Label = {} is found, skipped", src_vertex_label);
        }
        SUBTIMER_STOP(ReconstructIDMappingForFile, "Reconstruct Src Vertex Label");

        auto dst_it = std::find_if(bulkload_ctx.lid_to_pid_map.begin(), bulkload_ctx.lid_to_pid_map.end(),
            [&dst_vertex_label](const std::pair<string, unordered_map<LidPair, idx_t, LidPairHash>> &element) {
                return element.first.find(dst_vertex_label) != string::npos;
            });

        spdlog::debug("[ReconstructIDMappings] Reconstruct Dst Vertex Label = {}", dst_vertex_label);
        SUBTIMER_START(ReconstructIDMappingForFile, "Reconstruct Dst Vertex Label");
        if (dst_it == bulkload_ctx.lid_to_pid_map.end()) {
            auto id_col_names = ObtainIdColumnNames(bulkload_ctx, dst_vertex_label);
            std::string dst_lid_pid_query = ConstructLidPidRetrievalQuery(bulkload_ctx, dst_vertex_label, id_col_names);
            PopulateLidToPidMap(bulkload_ctx, dst_vertex_label, dst_lid_pid_query, id_col_names.size());
        }
        else {
            spdlog::trace("[ReconstructIDMappings] Mapping for Dst Vertex Label = {} is found, skipped", dst_vertex_label);
        }
        SUBTIMER_STOP(ReconstructIDMappingForFile, "Reconstruct Dst Vertex Label");
    }
    s62_disconnect();
    spdlog::info("[ReconstructIDMappings] Reconstruct ID mappings for edge files done");
}

// ---------------------------------------------------------------------------
// Forward edge loading
// ---------------------------------------------------------------------------

// For multi-threaded version, please see 6fdb44c724faf1a3bd4218a32c54e5daf6c8aeae
static void ReadFwdEdgeCSVFilesAndCreateEdgeExtents(vector<LabeledFile> &csv_edge_files, BulkloadContext &bulkload_ctx) {
    SCOPED_TIMER_SIMPLE(ReadFwdEdgeCSVFilesAndCreateEdgeExtents, spdlog::level::info, spdlog::level::debug);
    unordered_map<ExtentID, std::pair<std::pair<uint64_t, uint64_t>, vector<vector<idx_t>>>> adj_list_buffers;
    spdlog::info("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Start to load {} CSV Edge Files", csv_edge_files.size());
    for (auto &edge_file: csv_edge_files) {
        SCOPED_TIMER_SIMPLE(ReadSingleEdgeCSVFile, spdlog::level::info, spdlog::level::debug);

        string &edge_type = std::get<0>(edge_file);
        string &edge_file_path = std::get<1>(edge_file);
        OptionalFileSize &file_size = std::get<2>(edge_file);

        spdlog::info("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Start to load {} with edge type {}", edge_file_path, edge_type);

        if (IsEdgeCatalogInfoExist(bulkload_ctx, edge_type)) {
            spdlog::info("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Edge Catalog Info for {} is already exists", edge_type);
            bulkload_ctx.skipped_labels.push_back(edge_type);
            continue;
        }

        GraphSIMDCSVFileParser reader;
        string src_vertex_label;
        string dst_vertex_label;
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
        auto src_it = std::find_if(bulkload_ctx.lid_to_pid_map.begin(), bulkload_ctx.lid_to_pid_map.end(),
            [&src_vertex_label](const std::pair<string, unordered_map<LidPair, idx_t, LidPairHash>> &element) {
                return element.first.find(src_vertex_label) != string::npos;
            });
        if (src_it == bulkload_ctx.lid_to_pid_map.end()) throw InvalidInputException("Corresponding src vertex file was not loaded");
        unordered_map<LidPair, idx_t, LidPairHash> &src_lid_to_pid_map_instance = src_it->second;

        auto dst_it = std::find_if(bulkload_ctx.lid_to_pid_map.begin(), bulkload_ctx.lid_to_pid_map.end(),
            [&dst_vertex_label](const std::pair<string, unordered_map<LidPair, idx_t, LidPairHash>> &element) {
                return element.first.find(dst_vertex_label) != string::npos;
            });
        if (dst_it == bulkload_ctx.lid_to_pid_map.end()) throw InvalidInputException("Corresponding dst vertex file was not loaded");
        unordered_map<LidPair, idx_t, LidPairHash> &dst_lid_to_pid_map_instance = dst_it->second;
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitLIDToPIDMap");

        spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] InitLIDPairToEPIDMap");
        SUBTIMER_START(ReadSingleEdgeCSVFile, "InitLIDPairToEPIDMap");
        unordered_map<LidPair, idx_t, LidPairHash> *lid_pair_to_epid_map_instance;
        if (bulkload_ctx.input_options.load_backward_edge) {
            bulkload_ctx.lid_pair_to_epid_map.emplace_back(std::get<0>(edge_file), unordered_map<LidPair, idx_t, LidPairHash>());
            lid_pair_to_epid_map_instance = &bulkload_ctx.lid_pair_to_epid_map.back().second;
            lid_pair_to_epid_map_instance->reserve(approximated_num_rows);
        }
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitLIDPairToEPIDMap");

        DataChunk data;
        data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

        spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] CreateEdgeCatalogInfos");
        PartitionCatalogEntry *partition_cat;
        PropertySchemaCatalogEntry *property_schema_cat;

        CreateEdgeCatalogInfos(bulkload_ctx, edge_type, key_names, types, src_vertex_label, dst_vertex_label,
            partition_cat, property_schema_cat, LogicalType::FORWARD_ADJLIST, src_column_idx.size());

        spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] ClearAdjListBuffers");
        SUBTIMER_START(ReadSingleEdgeCSVFile, "ClearAdjListBuffers");
        ClearAdjListBuffers(adj_list_buffers);
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "ClearAdjListBuffers");

        LidPair prev_id {0, 0};
        idx_t cur_src_pid, prev_src_pid;
        ExtentID cur_vertex_extentID;
        ExtentID cur_vertex_localextentID;
        idx_t vertex_seqno;
        bool is_first_tuple_processed = false;
        PartitionID cur_part_id;

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

            if (!is_first_tuple_processed) {
                if (src_column_idx.size() == 1) {
                    prev_id.first = src_key_columns[0][src_seqno];
                } else if (src_column_idx.size() == 2) {
                    prev_id.first = src_key_columns[0][src_seqno];
                    prev_id.second = src_key_columns[1][src_seqno];
                }
                prev_src_pid = src_lid_to_pid_map_instance.at(prev_id);
                cur_vertex_extentID = static_cast<ExtentID>(prev_src_pid >> 32);
                cur_vertex_localextentID = cur_vertex_extentID & 0xFFFF;
                cur_part_id = (PartitionID)(cur_vertex_extentID >> 16);
                is_first_tuple_processed = true;
            }

            SUBTIMER_START(ReadSingleEdgeCSVFile, "FillAdjListBuffer");
            bool load_backward_edge = bulkload_ctx.input_options.load_backward_edge;
            if (src_column_idx.size() == 1) {
                while (src_seqno < max_seqno) {
                    src_key.first = src_key_columns[0][src_seqno];
                    cur_src_pid = src_lid_to_pid_map_instance.at(src_key);
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
                    cur_src_pid = src_lid_to_pid_map_instance.at(src_key);
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

            SUBTIMER_START(ReadSingleEdgeCSVFile, "CreateExtent and AddExtent");
            spdlog::trace("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] CreateExtent and AddExtent");
            bulkload_ctx.ext_mng.CreateExtent(*(bulkload_ctx.client.get()), data, *partition_cat, *property_schema_cat, new_eid);
            property_schema_cat->AddExtent(new_eid, data.size());
            SUBTIMER_STOP(ReadSingleEdgeCSVFile, "CreateExtent and AddExtent");
        }
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "ReadCSVFile and CreateEdgeExtents");

        spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Process Remaining AdjList");
        SUBTIMER_START(ReadSingleEdgeCSVFile, "Remaining AppendAdjListChunk");
        vector<idx_t> src_part_oids = bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { src_vertex_label }, GraphComponentType::VERTEX);
        PartitionCatalogEntry *src_part_cat_entry =
            (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, src_part_oids[0]);
        AppendAdjListChunk(bulkload_ctx, LogicalType::FORWARD_ADJLIST, cur_part_id, src_part_cat_entry->GetLocalExtentID(), adj_list_buffers);
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "Remaining AppendAdjListChunk");
    }
    spdlog::debug("[ReadFwdEdgeCSVFilesAndCreateEdgeExtents] Flush Dirty Segments and Delete From Cache");
    ChunkCacheManager::ccm->FlushDirtySegmentsAndDeleteFromcache(false);
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
    unordered_map<ExtentID, std::pair<std::pair<uint64_t, uint64_t>, vector<vector<idx_t>>>> adj_list_buffers;
    spdlog::info("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Start to load {} CSV Edge Files", csv_edge_files.size());
    for (auto &edge_file: csv_edge_files) {
        SCOPED_TIMER_SIMPLE(ReadSingleEdgeCSVFile, spdlog::level::info, spdlog::level::debug);

        string &edge_type = std::get<0>(edge_file);
        string &edge_file_path = std::get<1>(edge_file);
        OptionalFileSize &file_size = std::get<2>(edge_file);

        spdlog::info("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Start to load {} with edge type {}", edge_file_path, edge_type);

        if (isSkippedLabel(bulkload_ctx, edge_type)) {
            spdlog::info("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Edge Catalog Info for {} is already exists", edge_type);
            continue;
        }

        string src_vertex_label;
        string dst_vertex_label;
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
        auto src_it = std::find_if(bulkload_ctx.lid_to_pid_map.begin(), bulkload_ctx.lid_to_pid_map.end(),
            [&src_vertex_label](const std::pair<string, unordered_map<LidPair, idx_t, LidPairHash>> &element) {
                return element.first.find(src_vertex_label) != string::npos;
            });
        if (src_it == bulkload_ctx.lid_to_pid_map.end()) throw InvalidInputException("Corresponding src vertex file was not loaded");
        unordered_map<LidPair, idx_t, LidPairHash> &src_lid_to_pid_map_instance = src_it->second;

        auto dst_it = std::find_if(bulkload_ctx.lid_to_pid_map.begin(), bulkload_ctx.lid_to_pid_map.end(),
            [&dst_vertex_label](const std::pair<string, unordered_map<LidPair, idx_t, LidPairHash>> &element) {
                return element.first.find(dst_vertex_label) != string::npos;
            });
        if (dst_it == bulkload_ctx.lid_to_pid_map.end()) throw InvalidInputException("Corresponding dst vertex file was not loaded");
        unordered_map<LidPair, idx_t, LidPairHash> &dst_lid_to_pid_map_instance = dst_it->second;
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitLIDToPIDMap");

        spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] InitLIDPairToEPIDMap");
        SUBTIMER_START(ReadSingleEdgeCSVFile, "InitLIDPairToEPIDMap");
        auto edge_it = std::find_if(bulkload_ctx.lid_pair_to_epid_map.begin(), bulkload_ctx.lid_pair_to_epid_map.end(),
            [&edge_type](const std::pair<string, unordered_map<LidPair, idx_t, LidPairHash>> &element) { return element.first == edge_type; });
        if (edge_it == bulkload_ctx.lid_pair_to_epid_map.end()) throw InvalidInputException("[Error] Lid Pair to EPid Map does not exists");
        unordered_map<LidPair, idx_t, LidPairHash> &lid_pair_to_epid_map_instance = edge_it->second;
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "InitLIDPairToEPIDMap");

        DataChunk data;
        data.Initialize(types, STORAGE_STANDARD_VECTOR_SIZE);

        spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] CreateEdgeCatalogInfos");
        PartitionCatalogEntry *partition_cat;
        PropertySchemaCatalogEntry *property_schema_cat;
        CreateEdgeCatalogInfos(bulkload_ctx, edge_type, key_names, types, src_vertex_label, dst_vertex_label,
            partition_cat, property_schema_cat, LogicalType::BACKWARD_ADJLIST, dst_column_idx.size());

        spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] ClearAdjListBuffers");
        SUBTIMER_START(ReadSingleEdgeCSVFile, "ClearAdjListBuffers");
        ClearAdjListBuffers(adj_list_buffers);
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "ClearAdjListBuffers");

        LidPair prev_id {0, 0};
        idx_t cur_src_pid, prev_src_pid;
        ExtentID cur_vertex_extentID;
        ExtentID cur_vertex_localextentID;
        idx_t vertex_seqno;
        bool is_first_tuple_processed = false;
        PartitionID cur_part_id;

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

            if (!is_first_tuple_processed) {
                if (src_column_idx.size() == 1) {
                    prev_id.first = src_key_columns[0][src_seqno];
                } else if (src_column_idx.size() == 2) {
                    prev_id.first = src_key_columns[0][src_seqno];
                    prev_id.second = src_key_columns[1][src_seqno];
                }
                prev_src_pid = src_lid_to_pid_map_instance.at(prev_id);
                cur_vertex_extentID = static_cast<ExtentID>(prev_src_pid >> 32);
                cur_vertex_localextentID = cur_vertex_extentID & 0xFFFF;
                cur_part_id = (PartitionID)(cur_vertex_extentID >> 16);
                is_first_tuple_processed = true;
            }

            SUBTIMER_START(ReadSingleEdgeCSVFile, "FillBwdAdjListBuffer");
            if (src_column_idx.size() == 1) {
                while (src_seqno < max_seqno) {
                    src_key.first = src_key_columns[0][src_seqno];
                    cur_src_pid = src_lid_to_pid_map_instance.at(src_key);

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
                    cur_src_pid = src_lid_to_pid_map_instance.at(src_key);

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
        }
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "ReadCSVFile and CreateEdgeExtents");

        spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Process Remaining AdjList");
        SUBTIMER_START(ReadSingleEdgeCSVFile, "Remaining AppendAdjListChunk");
        vector<idx_t> src_part_oids = bulkload_ctx.graph_cat->LookupPartition(*(bulkload_ctx.client.get()), { src_vertex_label }, GraphComponentType::VERTEX);
        PartitionCatalogEntry *src_part_cat_entry =
            (PartitionCatalogEntry *)bulkload_ctx.catalog.GetEntry(*(bulkload_ctx.client.get()), DEFAULT_SCHEMA, src_part_oids[0]);
        AppendAdjListChunk(bulkload_ctx, LogicalType::BACKWARD_ADJLIST, cur_part_id, src_part_cat_entry->GetLocalExtentID(), adj_list_buffers);
        SUBTIMER_STOP(ReadSingleEdgeCSVFile, "Remaining AppendAdjListChunk");
    }
    spdlog::debug("[ReadBwdEdgesCSVFileAndCreateEdgeExtents] Flush Dirty Segments and Delete From Cache");
    ChunkCacheManager::ccm->FlushDirtySegmentsAndDeleteFromcache(false);
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
