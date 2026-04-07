#include <memory>
#include <string>
#include <ctime>
#include <regex>
#include "spdlog/spdlog.h"

// antlr4 headers must come before ORCA (c.h defines TRUE/FALSE macros)
#include "CypherLexer.h"
#include "CypherParser.h"
#include "BaseErrorListener.h"
#include "parser/cypher_transformer.hpp"
#include "binder/binder.hpp"
#include "binder/query/updating_clause/bound_create_clause.hpp"
#include "binder/query/updating_clause/bound_set_clause.hpp"
#include "binder/query/updating_clause/bound_delete_clause.hpp"
#include "storage/extent/adjlist_iterator.hpp"
#include "storage/extent/extent_manager.hpp"
#include "storage/cache/disk_aio/TypeDef.hpp"
#include "catalog/catalog_entry/graph_catalog_entry.hpp"
#include "storage/delta_store.hpp"

// Replaces ANTLR's default stderr printer with an exception throw.
namespace {
class ThrowingErrorListener : public antlr4::BaseErrorListener {
public:
    void syntaxError(antlr4::Recognizer*, antlr4::Token*, size_t line, size_t col,
                     const std::string& msg, std::exception_ptr) override {
        throw std::runtime_error(
            "Syntax error at " + std::to_string(line) + ":" +
            std::to_string(col) + " — " + msg);
    }
};
} // anonymous namespace

#include "main/capi/capi_internal.hpp"
#include "main/client_config.hpp"
#include "main/database.hpp"
#include "common/disk_aio_init.hpp"
#include "storage/cache/chunk_cache_manager.h"
#include "catalog/catalog_wrapper.hpp"
#include "planner/planner.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "optimizer/orca/gpopt/tbgppdbwrappers.hpp"
#include "gpopt/mdcache/CMDCache.h"
#include "common/types/decimal.hpp"
#include "parser/parsed_data/create_schema_info.hpp"
#include "parser/parsed_data/create_graph_info.hpp"
#include "catalog/catalog_entry/list.hpp"
#include "common/types/decimal.hpp"

using namespace duckdb;
using namespace antlr4;
using namespace gpopt;

// ---------------------------------------------------------------------------
// Per-connection handle
// ---------------------------------------------------------------------------

struct ConnectionHandle {
    std::unique_ptr<DuckDB>              database;
    std::shared_ptr<ClientContext>       client;
    std::unique_ptr<turbolynx::Planner>        planner;
    std::unique_ptr<DiskAioFactory>      disk_aio_factory;
    bool                                 owns_database = true; // false when connected via client_context
    // Mutation support: when last compiled query is a CREATE-only mutation,
    // bypass ORCA and execute directly against DeltaStore.
    std::unique_ptr<duckdb::BoundRegularQuery> last_bound_mutation;
    bool                                 is_mutation_query = false;
    // For MATCH+SET: SET items extracted at compile time, applied after ORCA execution
    std::vector<duckdb::BoundSetItem>    pending_set_items;
    // For MATCH+DELETE: flag extracted at compile time
    bool                                 pending_delete = false;
    bool                                 pending_detach_delete = false;
};

static std::mutex                                          g_conn_lock;
static std::map<int64_t, std::unique_ptr<ConnectionHandle>> g_connections;
static std::atomic<int64_t>                                g_next_conn_id{0};

// Error Handling (global, last-call semantics — caller checks immediately)
static std::mutex    g_err_lock;
static turbolynx_error_code last_error_code = TURBOLYNX_NO_ERROR;
static std::string   last_error_message;

static void set_error(turbolynx_error_code code, const std::string &msg) {
    std::lock_guard<std::mutex> lk(g_err_lock);
    last_error_code    = code;
    last_error_message = msg;
}

static ConnectionHandle* get_handle(int64_t conn_id) {
    std::lock_guard<std::mutex> lk(g_conn_lock);
    auto it = g_connections.find(conn_id);
    if (it == g_connections.end()) return nullptr;
    return it->second.get();
}

// Message Constants
static const std::string INVALID_METADATA_MSG = "Invalid metadata";
static const std::string UNSUPPORTED_OPERATION_MSG = "Unsupported operation";
static const std::string INVALID_LABEL_MSG = "Invalid label";
static const std::string INVALID_METADATA_TYPE_MSG = "Invalid metadata type";
static const std::string INVALID_NUMBER_OF_PROPERTIES_MSG = "Invalid number of properties";
static const std::string INVALID_PROPERTY_MSG = "Invalid property";
static const std::string INVALID_PLAN_MSG = "Invalid plan";
static const std::string INVALID_PARAMETER = "Invalid parameter";
static const std::string INVALID_PREPARED_STATEMENT_MSG = "Invalid prepared statement";
static const std::string INVALID_RESULT_SET_MSG = "Invalid result set";

// Default values
turbolynx_resultset empty_result_set = {0, NULL, NULL};

static void initialize_planner(ConnectionHandle &h) {
    if (!h.planner) {
        auto planner_config = turbolynx::PlannerConfig();
        planner_config.JOIN_ORDER_TYPE = turbolynx::PlannerConfig::JoinOrderType::JOIN_ORDER_EXHAUSTIVE_SEARCH;
        planner_config.DEBUG_PRINT = true;
        planner_config.DISABLE_MERGE_JOIN = true;
        h.planner = std::make_unique<turbolynx::Planner>(planner_config, turbolynx::MDProviderType::TBGPP, h.client.get());
    }
}

int64_t turbolynx_connect(const char *dbname) {
    try {
        auto h = std::make_unique<ConnectionHandle>();
        h->disk_aio_factory.reset(duckdb::InitializeDiskAio(dbname));
        h->database    = std::make_unique<DuckDB>(dbname);
        ChunkCacheManager::ccm = new ChunkCacheManager(dbname);
        h->client      = std::make_shared<ClientContext>(h->database->instance->shared_from_this());
        h->owns_database = true;
        duckdb::SetClientWrapper(h->client, make_shared<CatalogWrapper>(h->database->instance->GetCatalogWrapper()));
        // WAL: replay existing log to restore DeltaStore, then open writer for new mutations
        duckdb::WALReader::Replay(string(dbname), h->database->instance->delta_store);
        h->database->instance->wal_writer = std::make_unique<duckdb::WALWriter>(string(dbname));
        initialize_planner(*h);

        int64_t id = g_next_conn_id++;
        {
            std::lock_guard<std::mutex> lk(g_conn_lock);
            g_connections[id] = std::move(h);
        }
        g_connections[id]->database->instance->connection_manager.Register(g_connections[id]->client);
        std::cout << "Database Connected (conn_id=" << id << ")" << std::endl;
        return id;
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        set_error(TURBOLYNX_ERROR_CONNECTION_FAILED, e.what());
        return -1;
    }
}

int64_t turbolynx_connect_with_client_context(void *client_context) {
    if (!client_context) {
        set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, INVALID_PARAMETER);
        return -1;
    }
    auto h = std::make_unique<ConnectionHandle>();
    h->client       = *reinterpret_cast<std::shared_ptr<ClientContext>*>(client_context);
    h->owns_database = false;
    duckdb::SetClientWrapper(h->client, make_shared<CatalogWrapper>(h->client->db->GetCatalogWrapper()));
    initialize_planner(*h);

    int64_t id = g_next_conn_id++;
    {
        std::lock_guard<std::mutex> lk(g_conn_lock);
        g_connections[id] = std::move(h);
    }
    return id;
}

void turbolynx_clear_delta(int64_t conn_id) {
    std::lock_guard<std::mutex> lk(g_conn_lock);
    auto it = g_connections.find(conn_id);
    if (it == g_connections.end()) return;
    it->second->database->instance->delta_store.Clear();
    if (it->second->database->instance->wal_writer)
        it->second->database->instance->wal_writer->Truncate();
}

void turbolynx_checkpoint_ctx(duckdb::ClientContext &context) {
    auto &ds = context.db->delta_store;
    auto &catalog = context.db->GetCatalog();

    idx_t flushed_rows = 0;

    // Write CHECKPOINT_BEGIN marker to WAL before modifying disk state
    if (context.db->wal_writer && ds.HasInsertData()) {
        context.db->wal_writer->LogCheckpointBegin();
    }

    // ── Phase 1: Flush InsertBuffers to disk extents ──
    if (ds.HasInsertData()) {
        // Collect all in-memory extent IDs
        std::vector<std::pair<uint32_t, duckdb::InsertBuffer*>> buffers;
        // Iterate insert_buffers_ (exposed via DeltaStore API)
        for (auto &[eid, buf] : ds.insert_buffers_exposed()) {
            if (!buf.Empty()) buffers.push_back({(uint32_t)eid, &buf});
        }

        for (auto &[inmem_eid, buf] : buffers) {
            uint16_t partition_id = (uint16_t)(inmem_eid >> 16);
            auto &schema_keys = buf->GetSchemaKeys();
            auto &rows = buf->GetRows();
            if (rows.empty()) continue;

            // Find the partition catalog entry
            duckdb::PartitionCatalogEntry *part_cat = nullptr;
            auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
                context, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH, true);
            if (!gcat) continue;

            // Find the vertex partition with matching partition_id
            for (auto vp_oid : *gcat->GetVertexPartitionOids()) {
                auto *vp = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, vp_oid, true);
                if (vp && vp->GetPartitionID() == partition_id) { part_cat = vp; break; }
            }
            if (!part_cat) continue;

            // Find matching PropertySchema (exact key match)
            duckdb::PropertySchemaCatalogEntry *match_ps = nullptr;
            auto *ps_ids = part_cat->GetPropertySchemaIDs();
            if (ps_ids) {
                for (auto ps_oid : *ps_ids) {
                    auto *ps = (duckdb::PropertySchemaCatalogEntry *)catalog.GetEntry(
                        context, DEFAULT_SCHEMA, ps_oid, true);
                    if (!ps) continue;
                    auto *keys = ps->GetKeys();
                    if (!keys) continue;
                    // Check exact match (InsertBuffer keys == PropertySchema keys)
                    if (keys->size() == schema_keys.size()) {
                        bool match = true;
                        for (idx_t i = 0; i < keys->size(); i++) {
                            if ((*keys)[i] != schema_keys[i]) { match = false; break; }
                        }
                        if (match) { match_ps = ps; break; }
                    }
                }
            }

            // Build DataChunk from InsertBuffer rows.
            // NOTE: Extents do NOT store _id/VID. VID is computed at scan time
            // from ExtentID + row offset. The DataChunk has only property columns,
            // matching the PropertySchema's types (GetTypesWithCopy()).

            idx_t row_count = rows.size();
            // Allocate new (non-in-memory) ExtentID
            duckdb::ExtentID new_eid = part_cat->GetNewExtentID();

            // Use the target PropertySchema's types for the DataChunk
            auto *target_ps = match_ps;
            if (!target_ps && ps_ids && !ps_ids->empty()) {
                target_ps = (duckdb::PropertySchemaCatalogEntry *)catalog.GetEntry(
                    context, DEFAULT_SCHEMA, (*ps_ids)[0], true);
            }
            if (!target_ps) continue;

            auto col_types = target_ps->GetTypesWithCopy();
            auto *ps_keys = target_ps->GetKeys();

            duckdb::DataChunk chunk;
            chunk.Initialize(col_types);

            // Fill DataChunk: map InsertBuffer keys to PropertySchema columns
            for (idx_t r = 0; r < row_count; r++) {
                for (idx_t c = 0; c < col_types.size(); c++) {
                    if (ps_keys && c < ps_keys->size()) {
                        int bi = buf->FindKeyIndex((*ps_keys)[c]);
                        if (bi >= 0 && (idx_t)bi < rows[r].size()) {
                            try { chunk.SetValue(c, r, rows[r][bi]); }
                            catch (...) { chunk.SetValue(c, r, duckdb::Value()); }
                        } else {
                            chunk.SetValue(c, r, duckdb::Value());
                        }
                    } else {
                        chunk.SetValue(c, r, duckdb::Value());
                    }
                }
            }
            chunk.SetCardinality(row_count);

            // Write extent to disk via ExtentManager
            duckdb::ExtentManager ext_mng;
            spdlog::info("[CHECKPOINT] Creating extent eid=0x{:08X} rows={} partition={}", new_eid, row_count, partition_id);
            try {
            ext_mng.CreateExtent(context, chunk, *part_cat, *target_ps, new_eid);
            target_ps->AddExtent(new_eid, row_count);
            } catch (const std::exception &e) {
                spdlog::error("[CHECKPOINT] CreateExtent EXCEPTION: {}", e.what());
            } catch (...) {
                spdlog::error("[CHECKPOINT] CreateExtent UNKNOWN exception");
            }
            // Flush newly-created segments to store.db
            for (auto &[cdf, handler] : ChunkCacheManager::ccm->file_handlers) {
                if ((cdf >> 32) == (duckdb::ChunkDefinitionID)new_eid && handler) {
                    ChunkCacheManager::ccm->UnswizzleFlushSwizzle(cdf, handler, false);
                }
            }

            flushed_rows += row_count;
        }
    }

    // ── Phase 2: Save catalog (persist new extents) — POINT OF NO RETURN ──
    catalog.SaveCatalog();

    // Write CHECKPOINT_END marker — catalog is committed, INSERTs are on disk
    if (context.db->wal_writer && flushed_rows > 0) {
        context.db->wal_writer->LogCheckpointEnd();
    }

    // ── Phase 3: Flush dirty segments to store.db + persist metadata ──
    ChunkCacheManager::ccm->FlushDirtySegmentsAndDeleteFromcache(false);
    ChunkCacheManager::ccm->FlushMetaInfo(DiskAioParameters::WORKSPACE.c_str());

    // ── Phase 4: Clear INSERT deltas, re-write WAL for remaining SET/DELETE ──
    bool has_updates = ds.HasPropertyUpdates();
    bool has_deletes = ds.HasDeletedUserIds();

    // Clear only INSERT data (flushed to disk). Keep SET/DELETE deltas.
    ds.ClearInsertData();

    // Truncate WAL, then re-write remaining SET/DELETE entries
    if (context.db->wal_writer) {
        context.db->wal_writer->Truncate();

        auto &wal = *context.db->wal_writer;

        // Re-write UPDATE_PROP entries
        for (auto &[uid, props] : ds.GetAllPropertyUpdates()) {
            for (auto &[key, val] : props) {
                wal.LogUpdateProp(uid, key, val);
            }
        }

        // Re-write DELETE_NODE entries
        for (auto &[eid, mask] : ds.GetAllDeleteMasks()) {
            for (auto off : mask.GetDeleted()) {
                // user_id=0 for extent-based deletes; user_id deletes re-written below
                wal.LogDeleteNode((uint32_t)eid, (uint32_t)off, 0);
            }
        }
        // Re-write user-id based deletes (with eid=0, off=0 — replay uses uid)
        for (auto uid : ds.GetAllDeletedUserIds()) {
            wal.LogDeleteNode(0, 0, uid);
        }

        wal.Flush();
    }

    spdlog::info("[CHECKPOINT] Compaction complete: flushed {} rows, preserved {} updates, {} deletes",
                 flushed_rows, has_updates ? ds.GetAllPropertyUpdates().size() : 0,
                 has_deletes ? ds.GetAllDeletedUserIds().size() : 0);
}

void turbolynx_checkpoint(int64_t conn_id) {
    std::lock_guard<std::mutex> lk(g_conn_lock);
    auto it = g_connections.find(conn_id);
    if (it == g_connections.end()) return;
    turbolynx_checkpoint_ctx(*it->second->client);
}

void turbolynx_disconnect(int64_t conn_id) {
    std::unique_ptr<ConnectionHandle> h;
    {
        std::lock_guard<std::mutex> lk(g_conn_lock);
        auto it = g_connections.find(conn_id);
        if (it == g_connections.end()) return;
        h = std::move(it->second);
        g_connections.erase(it);
    }
    if (h->owns_database) {
        duckdb::ReleaseClientWrapper();
        h->client.reset();
        delete ChunkCacheManager::ccm;
        ChunkCacheManager::ccm = nullptr;
        // database / disk_aio_factory cleaned up via unique_ptr destructors
    }
    std::cout << "Database Disconnected (conn_id=" << conn_id << ")" << std::endl;
}

int64_t turbolynx_connect_readonly(const char *dbname) {
    try {
        auto h = std::make_unique<ConnectionHandle>();
        h->disk_aio_factory.reset(duckdb::InitializeDiskAio(dbname));
        h->database    = std::make_unique<DuckDB>(dbname);
        ChunkCacheManager::ccm = new ChunkCacheManager(dbname, false, /*read_only=*/true);
        h->client      = std::make_shared<ClientContext>(h->database->instance->shared_from_this());
        h->owns_database = true;
        duckdb::SetClientWrapper(h->client, make_shared<CatalogWrapper>(h->database->instance->GetCatalogWrapper()));
        initialize_planner(*h);

        int64_t id = g_next_conn_id++;
        {
            std::lock_guard<std::mutex> lk(g_conn_lock);
            g_connections[id] = std::move(h);
        }
        g_connections[id]->database->instance->connection_manager.Register(g_connections[id]->client);
        std::cout << "Database Connected read-only (conn_id=" << id << ")" << std::endl;
        return id;
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        set_error(TURBOLYNX_ERROR_CONNECTION_FAILED, e.what());
        return -1;
    }
}

int turbolynx_reopen(int64_t conn_id) {
    std::unique_lock<std::mutex> lk(g_conn_lock);
    auto it = g_connections.find(conn_id);
    if (it == g_connections.end()) return -1;
    auto* h = it->second.get();
    lk.unlock();

    // Read catalog_version file and compare with current in-memory version
    Catalog& catalog = h->client->db->GetCatalog();
    std::string cat_path = catalog.GetCatalogPath();
    if (cat_path.empty()) return 0;

    std::string version_path = cat_path + "/catalog_version";
    std::ifstream ifs(version_path);
    if (!ifs.is_open()) return 0;

    std::string line;
    std::getline(ifs, line);
    if (line.empty()) return 0;

    idx_t disk_version = (idx_t)std::stoull(line);
    idx_t mem_version  = catalog.GetCatalogVersion();

    return (disk_version != mem_version) ? 1 : 0;
}

turbolynx_conn_state turbolynx_is_connected(int64_t conn_id) {
    std::lock_guard<std::mutex> lk(g_conn_lock);
    return g_connections.count(conn_id) ? TURBOLYNX_CONNECTED : TURBOLYNX_NOT_CONNECTED;
}

turbolynx_error_code turbolynx_get_last_error(char **errmsg) {
    *errmsg = (char*)last_error_message.c_str();
    return last_error_code;
}

turbolynx_version turbolynx_get_version() {
    return "0.0.1";
}

inline static GraphCatalogEntry* turbolynx_get_graph_catalog_entry(ConnectionHandle* h) {
    Catalog& catalog = h->client->db->GetCatalog();
	return (GraphCatalogEntry*) catalog.GetEntry(*h->client.get(), CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH);
}

inline static PartitionCatalogEntry* turbolynx_get_vertex_partition_catalog_entry(ConnectionHandle* h, string label) {
	Catalog& catalog = h->client->db->GetCatalog();
	return (PartitionCatalogEntry*) catalog.GetEntry(*h->client.get(), CatalogType::PARTITION_ENTRY,
								DEFAULT_SCHEMA, DEFAULT_VERTEX_PARTITION_PREFIX + label);
}

inline static PartitionCatalogEntry* turbolynx_get_edge_partition_catalog_entry(ConnectionHandle* h, string type) {
	Catalog& catalog = h->client->db->GetCatalog();
	return (PartitionCatalogEntry*) catalog.GetEntry(*h->client.get(), CatalogType::PARTITION_ENTRY,
								DEFAULT_SCHEMA, DEFAULT_EDGE_PARTITION_PREFIX + type);
}

turbolynx_num_metadata turbolynx_get_metadata_from_catalog(int64_t conn_id, turbolynx_label_name label, bool like_flag, bool filter_flag, turbolynx_metadata **_metadata) {
	auto* h = get_handle(conn_id);
	if (!h) { set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, INVALID_PARAMETER); return TURBOLYNX_ERROR; }
	if(label != NULL && !like_flag && !filter_flag) {
        last_error_message = UNSUPPORTED_OPERATION_MSG;
		last_error_code = TURBOLYNX_ERROR_UNSUPPORTED_OPERATION;
		return last_error_code;
	}

	// Get nodes metadata
	auto graph_cat = turbolynx_get_graph_catalog_entry(h);

	// Get labels and types
	vector<string> labels;
	idx_t_vector *vertex_partitions = graph_cat->GetVertexPartitionOids();
	for (int i = 0; i < vertex_partitions->size(); i++) {
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)h->client->db->GetCatalog().GetEntry(
                *h->client.get(), DEFAULT_SCHEMA, vertex_partitions->at(i));
		labels.push_back(part_cat->GetName().substr(6));
    }

	vector<string> types;
	idx_t_vector *edge_partitions = graph_cat->GetEdgePartitionOids();
	for (int i = 0; i < edge_partitions->size(); i++) {
        PartitionCatalogEntry *part_cat =
            (PartitionCatalogEntry *)h->client->db->GetCatalog().GetEntry(
                *h->client.get(), DEFAULT_SCHEMA, edge_partitions->at(i));
		types.push_back(part_cat->GetName().substr(6));
    }

	// Create turbolynx_metadata linked list with labels
	turbolynx_metadata *metadata = NULL;
	turbolynx_metadata *prev = NULL;
	for(int i = 0; i < labels.size(); i++) {
		turbolynx_metadata *new_metadata = (turbolynx_metadata*)malloc(sizeof(turbolynx_metadata));
		new_metadata->label_name = strdup(labels[i].c_str());
		new_metadata->type = TURBOLYNX_METADATA_TYPE::TURBOLYNX_NODE;
		new_metadata->next = NULL;
		if(prev == NULL) {
			metadata = new_metadata;
		} else {
			prev->next = new_metadata;
		}
		prev = new_metadata;
	}

	// Concat types
	for(int i = 0; i < types.size(); i++) {
		turbolynx_metadata *new_metadata = (turbolynx_metadata*)malloc(sizeof(turbolynx_metadata));
		new_metadata->label_name = strdup(types[i].c_str());
		new_metadata->type = TURBOLYNX_METADATA_TYPE::TURBOLYNX_EDGE;
		new_metadata->next = NULL;
		if(prev == NULL) {
			metadata = new_metadata;
		} else {
			prev->next = new_metadata;
		}
		prev = new_metadata;
	}

	*_metadata = metadata;
	return labels.size() + types.size();
}

turbolynx_state turbolynx_close_metadata(turbolynx_metadata *metadata) {
	if (metadata == NULL) {
		last_error_message = INVALID_METADATA_MSG.c_str();
		last_error_code = TURBOLYNX_ERROR_INVALID_METADATA;
		return TURBOLYNX_ERROR;
	}

	turbolynx_metadata *next;
	while(metadata != NULL) {
		next = metadata->next;
		free(metadata->label_name);
		free(metadata);
		metadata = next;
	}
	return TURBOLYNX_SUCCESS;
}

static void turbolynx_extract_width_scale_from_uint16(turbolynx_property* property, uint16_t width_scale) {
	uint8_t width = width_scale >> 8;
	uint8_t scale = width_scale & 0xFF;
	property->precision = width;
	property->scale = scale;
}

static void turbolynx_extract_width_scale_from_type(turbolynx_property* property, LogicalType property_logical_type) {
	uint8_t width, scale;
	if (property_logical_type.GetDecimalProperties(width, scale)) {
		property->precision = width;
		property->scale = scale;
	} else {
		property->precision = 0;
		property->scale = 0;
	}
}

turbolynx_num_properties turbolynx_get_property_from_catalog(int64_t conn_id, turbolynx_label_name label, turbolynx_metadata_type type, turbolynx_property** _property) {
	auto* h = get_handle(conn_id);
	if (!h) { set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, INVALID_PARAMETER); return TURBOLYNX_ERROR; }
	if (label == NULL) {
		last_error_message = INVALID_LABEL_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_LABEL;
		return TURBOLYNX_ERROR;
	}

	auto graph_cat_entry = turbolynx_get_graph_catalog_entry(h);
	vector<idx_t> partition_indexes;
	turbolynx_property *property = NULL;
	turbolynx_property *prev = NULL;
	PartitionCatalogEntry *partition_cat_entry = NULL;

	if (type == TURBOLYNX_METADATA_TYPE::TURBOLYNX_NODE) {
		partition_cat_entry = turbolynx_get_vertex_partition_catalog_entry(h, string(label));
	} else if (type == TURBOLYNX_METADATA_TYPE::TURBOLYNX_EDGE) {
		partition_cat_entry = turbolynx_get_edge_partition_catalog_entry(h, string(label));
	} else {
		last_error_message = INVALID_METADATA_TYPE_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_METADATA_TYPE;
		return TURBOLYNX_ERROR;
	}

	auto num_properties = partition_cat_entry->global_property_key_names.size();
	auto num_types = partition_cat_entry->global_property_typesid.size();

	if (num_properties != num_types) {
		last_error_message = INVALID_NUMBER_OF_PROPERTIES_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_NUMBER_OF_PROPERTIES;
		return TURBOLYNX_ERROR;
	}

	for (turbolynx_property_order i = 0; i < num_properties; i++) {
		auto property_name = partition_cat_entry->global_property_key_names[i];
		auto property_logical_type_id = partition_cat_entry->global_property_typesid[i];
		auto property_logical_type = LogicalType(property_logical_type_id);
		auto property_turbolynx_type = ConvertCPPTypeToC(property_logical_type);
		auto property_sql_type = property_logical_type.ToString();

		turbolynx_property *new_property = (turbolynx_property*)malloc(sizeof(turbolynx_property));
		new_property->label_name = label;
		new_property->label_type = type;
		new_property->order = i;
		new_property->property_name = strdup(property_name.c_str());
		new_property->property_type = property_turbolynx_type;
		new_property->property_sql_type = strdup(property_sql_type.c_str());

		if (property_logical_type_id == LogicalTypeId::DECIMAL) {
			turbolynx_extract_width_scale_from_uint16(new_property, partition_cat_entry->extra_typeinfo_vec[i]);
		} else {
			turbolynx_extract_width_scale_from_type(new_property, property_logical_type);
		}

		new_property->next = NULL;
		if (prev == NULL) {
			property = new_property;
		} else {
			prev->next = new_property;
		}
		prev = new_property;
	}

	*_property = property;
	return num_properties;
}

turbolynx_state turbolynx_close_property(turbolynx_property *property) {
	if (property == NULL) {
		last_error_message = INVALID_PROPERTY_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_PROPERTY;
		return TURBOLYNX_ERROR;
	}

	turbolynx_property *next;
	while(property != NULL) {
		next = property->next;
		free(property->property_name);
		free(property->property_sql_type);
		free(property);
		property = next;
	}
	return TURBOLYNX_SUCCESS;
}

// ─── Catalog JSON dump ───────────────────────────────────────────────────────
static std::string esc_json(const std::string& s) {
	std::string out;
	for (char c : s) {
		if (c == '"') out += "\\\"";
		else if (c == '\\') out += "\\\\";
		else if (c == '\n') out += "\\n";
		else out += c;
	}
	return out;
}

static std::string short_uri(const std::string& uri) {
	auto pos = uri.rfind('/');
	std::string s = (pos != std::string::npos) ? uri.substr(pos + 1) : uri;
	auto hash = s.rfind('#');
	if (hash != std::string::npos) s = s.substr(hash + 1);
	return s;
}

char* turbolynx_dump_catalog_json(int64_t conn_id) {
	auto* h = get_handle(conn_id);
	if (!h) return nullptr;

	auto& catalog = h->client->db->GetCatalog();
	auto* graph_cat = turbolynx_get_graph_catalog_entry(h);

	std::string json;
	json += "{\n";

	// ── Vertex Partitions ─────────────────────────────────────────────
	auto* vp_oids = graph_cat->GetVertexPartitionOids();
	json += "  \"vertexPartitions\": [\n";
	uint64_t total_nodes = 0;
	for (size_t vi = 0; vi < vp_oids->size(); vi++) {
		if (vi) json += ",\n";
		auto* part = (PartitionCatalogEntry*)catalog.GetEntry(
			*h->client.get(), DEFAULT_SCHEMA, vp_oids->at(vi));
		std::string label = part->GetName();
		if (label.size() > 6 && label.substr(0, 6) == "vtxpt_") label = label.substr(6);

		json += "    {\"label\": \"" + esc_json(label) + "\", ";
		json += "\"numColumns\": " + std::to_string(part->global_property_key_names.size()) + ", ";

		// Graphlets (property schemas)
		auto* ps_oids = part->GetPropertySchemaIDs();
		json += "\"graphlets\": [";
		uint64_t part_total = 0;
		for (size_t pi = 0; pi < ps_oids->size(); pi++) {
			if (pi) json += ", ";
			auto* ps = (PropertySchemaCatalogEntry*)catalog.GetEntry(
				*h->client.get(), DEFAULT_SCHEMA, ps_oids->at(pi));
			auto* keys = ps->GetKeys();
			auto* types = ps->GetTypes();
			uint64_t nrows = ps->GetNumberOfRowsApproximately();
			uint64_t nextents = ps->GetNumberOfExtents();
			part_total += nrows;

			json += "{\"id\": " + std::to_string(pi);
			json += ", \"rows\": " + std::to_string(nrows);
			json += ", \"extents\": " + std::to_string(nextents);
			json += ", \"cols\": " + std::to_string(keys ? keys->size() : 0);
			json += ", \"schema\": [";
			if (keys) {
				for (size_t k = 0; k < keys->size(); k++) {
					if (k) json += ", ";
					std::string tname = types ? LogicalType(types->at(k)).ToString() : "?";
					json += "{\"n\": \"" + esc_json(short_uri(keys->at(k))) + "\", \"t\": \"" + esc_json(tname) + "\"}";
				}
			}
			json += "]}";
		}
		total_nodes += part_total;
		json += "], \"totalRows\": " + std::to_string(part_total);
		json += ", \"numGraphlets\": " + std::to_string(ps_oids->size()) + "}";
	}
	json += "\n  ],\n";

	// ── Edge Partitions ───────────────────────────────────────────────
	auto* ep_oids = graph_cat->GetEdgePartitionOids();
	json += "  \"edgePartitions\": [\n";
	uint64_t total_edges = 0;
	for (size_t ei = 0; ei < ep_oids->size(); ei++) {
		if (ei) json += ",\n";
		auto* part = (PartitionCatalogEntry*)catalog.GetEntry(
			*h->client.get(), DEFAULT_SCHEMA, ep_oids->at(ei));
		std::string type_name = part->GetName();
		if (type_name.size() > 6 && type_name.substr(0, 6) == "edgpt_") type_name = type_name.substr(6);

		auto* ps_oids = part->GetPropertySchemaIDs();

		json += "    {\"type\": \"" + esc_json(type_name) + "\", ";
		json += "\"short\": \"" + esc_json(short_uri(type_name)) + "\", ";

		json += "\"graphlets\": [";
		uint64_t edge_total = 0;
		for (size_t pi = 0; pi < ps_oids->size(); pi++) {
			if (pi) json += ", ";
			auto* ps = (PropertySchemaCatalogEntry*)catalog.GetEntry(
				*h->client.get(), DEFAULT_SCHEMA, ps_oids->at(pi));
			uint64_t nrows = ps->GetNumberOfRowsApproximately();
			edge_total += nrows;
			json += "{\"id\": " + std::to_string(pi) + ", \"rows\": " + std::to_string(nrows) + "}";
		}
		total_edges += edge_total;
		json += "], \"totalRows\": " + std::to_string(edge_total);
		json += ", \"numGraphlets\": " + std::to_string(ps_oids->size()) + "}";
	}
	json += "\n  ],\n";

	// ── Summary ───────────────────────────────────────────────────────
	json += "  \"summary\": {";
	json += "\"totalNodes\": " + std::to_string(total_nodes);
	json += ", \"totalEdges\": " + std::to_string(total_edges);
	json += ", \"vertexPartitions\": " + std::to_string(vp_oids->size());
	json += ", \"edgePartitions\": " + std::to_string(ep_oids->size());
	json += "}\n}\n";

	return strdup(json.c_str());
}

// Rewrite DETACH DELETE → DELETE and return true if DETACH was present
static string rewriteDetachDelete(const string &query, bool &is_detach) {
    std::regex detach_re(R"(\bDETACH\s+DELETE\b)", std::regex::icase);
    is_detach = std::regex_search(query, detach_re);
    if (is_detach) {
        return std::regex_replace(query, detach_re, "DELETE");
    }
    return query;
}

// Rewrite REMOVE n.prop → SET n.prop = NULL (syntactic sugar, avoids ANTLR grammar change)
static string rewriteRemoveToSetNull(const string &query) {
    // Match: REMOVE <var>.<prop> [, <var>.<prop>]*
    // Replace with: SET <var>.<prop> = NULL [, <var>.<prop> = NULL]*
    std::regex remove_re(R"(\bREMOVE\s+)", std::regex::icase);
    std::smatch m;
    string q = query;
    if (std::regex_search(q, m, remove_re)) {
        // Find the REMOVE keyword and replace with SET, then append = NULL to each property
        string prefix = m.prefix().str();
        string rest = m.suffix().str();
        // Split rest by comma until next keyword (RETURN, WITH, DELETE, SET, CREATE, MATCH, or end)
        std::regex item_re(R"((\w+\.\w+))");
        string set_clause = "SET ";
        auto begin = std::sregex_iterator(rest.begin(), rest.end(), item_re);
        auto end = std::sregex_iterator();
        bool first = true;
        size_t last_pos = 0;
        for (auto it = begin; it != end; ++it) {
            if (!first) set_clause += ", ";
            set_clause += it->str() + " = NULL";
            last_pos = it->position() + it->length();
            first = true; // only one REMOVE clause typically
            break; // handle one item at a time
        }
        // Handle multiple REMOVE items separated by commas
        string items_str = rest;
        // Find where the REMOVE items end (next keyword or end of string)
        std::regex end_re(R"(\s+(?:RETURN|WITH|DELETE|SET|CREATE|MATCH|$))", std::regex::icase);
        std::smatch end_m;
        string items_part;
        if (std::regex_search(rest, end_m, end_re)) {
            items_part = rest.substr(0, end_m.position());
            string after = rest.substr(end_m.position());
            // Split items by comma
            set_clause = "SET ";
            std::regex prop_re(R"((\w+\.\w+))");
            auto pbegin = std::sregex_iterator(items_part.begin(), items_part.end(), prop_re);
            first = true;
            for (auto it = pbegin; it != std::sregex_iterator(); ++it) {
                if (!first) set_clause += ", ";
                set_clause += it->str() + " = NULL";
                first = false;
            }
            return prefix + set_clause + after;
        } else {
            // No following keyword — items go to end
            set_clause = "SET ";
            std::regex prop_re(R"((\w+\.\w+))");
            auto pbegin = std::sregex_iterator(items_str.begin(), items_str.end(), prop_re);
            first = true;
            for (auto it = pbegin; it != std::sregex_iterator(); ++it) {
                if (!first) set_clause += ", ";
                set_clause += it->str() + " = NULL";
                first = false;
            }
            return prefix + set_clause;
        }
    }
    return query;
}

static void turbolynx_compile_query(ConnectionHandle* h, string query) {
    // Guard: unsupported SET n:Label (multi-label) before ANTLR parsing
    {
        std::regex set_label_re(R"(\bSET\s+\w+\s*:\s*\w+)", std::regex::icase);
        if (std::regex_search(query, set_label_re)) {
            throw std::runtime_error(
                "Unsupported: SET <variable>:<Label> (adding/changing labels is not yet supported).");
        }
    }

    // Rewrite REMOVE → SET NULL before ANTLR parsing
    query = rewriteRemoveToSetNull(query);

    // Guard against empty/whitespace-only input before ANTLR
    {
        bool has_content = false;
        for (char c : query) {
            if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
                has_content = true;
                break;
            }
        }
        if (!has_content)
            throw std::runtime_error("Empty query");
    }

    auto inputStream = ANTLRInputStream(query);
    ThrowingErrorListener error_listener;

    CypherLexer cypherLexer(&inputStream);
    cypherLexer.removeErrorListeners();
    cypherLexer.addErrorListener(&error_listener);

    auto tokens = CommonTokenStream(&cypherLexer);
    tokens.fill();

    CypherParser cypherParser(&tokens);
    cypherParser.removeErrorListeners();
    cypherParser.addErrorListener(&error_listener);

    auto* cypher_ctx = cypherParser.oC_Cypher();
    if (!cypher_ctx)
        throw std::runtime_error("Invalid query — no parse tree produced");

    duckdb::CypherTransformer transformer(*cypher_ctx);
    auto statement = transformer.transform();
    if (!statement)
        throw std::runtime_error("Transformer returned null — check Cypher syntax");
    duckdb::Binder binder(h->client.get());
    auto boundQuery = binder.Bind(*statement);

    // Check if this is a mutation-only query (CREATE without RETURN).
    // If so, bypass ORCA and store the bound query for direct execution.
    bool is_mutation = false;
    bool has_updating = false;
    if (boundQuery->GetNumSingleQueries() == 1) {
        auto* sq = boundQuery->GetSingleQuery(0);
        bool has_reading = false;
        bool has_projection = false;
        for (idx_t i = 0; i < sq->GetNumQueryParts(); i++) {
            auto* qp = sq->GetQueryPart(i);
            if (qp->HasUpdatingClause()) has_updating = true;
            if (qp->HasReadingClause()) has_reading = true;
            if (qp->HasProjectionBody()) has_projection = true;
        }
        // CREATE-only: has updating clause, no reading, no projection (no RETURN)
        if (has_updating && !has_reading && !has_projection) {
            is_mutation = true;
        }
    }

    h->is_mutation_query = is_mutation;
    // Extract SET items before handing boundQuery to ORCA (which may consume it)
    h->pending_set_items.clear();
    h->pending_delete = false;
    // Note: pending_detach_delete is set in prepare, not here
    if (has_updating && !is_mutation) {
        auto* sq_tmp = boundQuery->GetSingleQuery(0);
        for (idx_t pi = 0; pi < sq_tmp->GetNumQueryParts(); pi++) {
            auto* qp = sq_tmp->GetQueryPart(pi);
            for (idx_t ui = 0; ui < qp->GetNumUpdatingClauses(); ui++) {
                auto* uc = qp->GetUpdatingClause(ui);
                if (uc->GetClauseType() == duckdb::BoundUpdatingClauseType::SET) {
                    auto* sc = static_cast<const duckdb::BoundSetClause*>(uc);
                    for (auto& item : sc->GetItems()) {
                        h->pending_set_items.push_back(item);
                    }
                }
                else if (uc->GetClauseType() == duckdb::BoundUpdatingClauseType::DELETE_CLAUSE) {
                    h->pending_delete = true;
                }
            }
            // Remove updating clauses so ORCA doesn't see them
            qp->ClearUpdatingClauses();
        }
    }
    if (is_mutation) {
        h->last_bound_mutation = std::move(boundQuery);
        spdlog::info("[turbolynx_compile_query] Mutation query detected — bypassing ORCA");
    } else {
        h->last_bound_mutation.reset();
        h->planner->execute(boundQuery.get());
    }
}

static void turbolynx_get_label_name_type_from_ccolref(ConnectionHandle* h, OID col_oid, turbolynx_property *new_property) {
	if (col_oid != GPDB_UNKNOWN) {
		PropertySchemaCatalogEntry *ps_cat_entry = (PropertySchemaCatalogEntry *)h->client->db->GetCatalog().GetEntry(*h->client.get(), DEFAULT_SCHEMA, col_oid);
		D_ASSERT(ps_cat_entry != NULL);
		idx_t partition_oid = ps_cat_entry->partition_oid;
		auto graph_cat = turbolynx_get_graph_catalog_entry(h);

		string label = graph_cat->GetLabelFromVertexPartitionIndex(*(h->client.get()), partition_oid);
		if (!label.empty()) {
			new_property->label_name = strdup(label.c_str());
			new_property->label_type = TURBOLYNX_METADATA_TYPE::TURBOLYNX_NODE;
			return;
		}

		string type = graph_cat->GetTypeFromEdgePartitionIndex(*(h->client.get()), partition_oid);
		if (!type.empty()) {
			new_property->label_name = strdup(type.c_str());
			new_property->label_type = TURBOLYNX_METADATA_TYPE::TURBOLYNX_EDGE;
			return;
		}
	}

	new_property->label_name = NULL;
	new_property->label_type = TURBOLYNX_METADATA_TYPE::TURBOLYNX_OTHER;
	return;
}

static void turbolynx_extract_query_metadata(ConnectionHandle* h, turbolynx_prepared_statement* prepared_statement) {
    auto executors = h->planner->genPipelineExecutors();
	 if (executors.size() == 0) {
		last_error_message = INVALID_PLAN_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_PLAN;
		return;
    }
    else {
		auto col_names = h->planner->getQueryOutputColNames();
		auto col_types = executors.back()->pipeline->GetSink()->GetTypes();
		auto col_oids = h->planner->getQueryOutputOIDs();


		turbolynx_property *property = NULL;
		turbolynx_property *prev = NULL;
		
		for (turbolynx_property_order i = 0; i < col_names.size(); i++) {
			turbolynx_property *new_property = (turbolynx_property*)malloc(sizeof(turbolynx_property));

			auto property_name = col_names[i];
			auto property_logical_type = col_types[i];
			auto property_turbolynx_type = ConvertCPPTypeToC(property_logical_type);
			auto property_sql_type = property_logical_type.ToString();

			new_property->order = i;
			new_property->property_name = strdup(property_name.c_str());
			new_property->property_type = property_turbolynx_type;
			new_property->property_sql_type = strdup(property_sql_type.c_str());

			turbolynx_get_label_name_type_from_ccolref(h, col_oids[i], new_property);
			turbolynx_extract_width_scale_from_type(new_property, property_logical_type);

			new_property->next = NULL;
			if (prev == NULL) {
				property = new_property;
			} else {
				prev->next = new_property;
			}
			prev = new_property;
		}

		prepared_statement->num_properties = col_names.size();
		prepared_statement->property = property;
		prepared_statement->plan = strdup(generatePostgresStylePlan(executors).c_str());
    }
}

// Check if query is a MATCH+CREATE edge pattern:
// MATCH (a:L1 {k:v}), (b:L2 {k:v}) CREATE (a)-[:TYPE]->(b)
static bool isMatchCreateEdge(const string &query) {
    std::regex re(R"(\bMATCH\b.*,.*\bCREATE\b.*-\[)", std::regex::icase);
    return std::regex_search(query, re);
}

// Execute MATCH+CREATE edge by decomposition:
// 1. Run MATCH for each node → get VIDs
// 2. Create edge in AdjListDelta
static turbolynx_num_rows executeMatchCreateEdge(int64_t conn_id, const string &query,
                                                  turbolynx_resultset_wrapper** result_set_wrp) {
    auto* h = get_handle(conn_id);
    if (!h) return TURBOLYNX_ERROR;

    // Parse: MATCH (a:L1 {k1:v1}), (b:L2 {k2:v2}) CREATE (a)-[:TYPE]->(b)
    std::regex re(
        R"(\bMATCH\s*\(\s*(\w+)\s*:\s*(\w+)\s*\{([^}]*)\}\s*\)\s*,\s*\(\s*(\w+)\s*:\s*(\w+)\s*\{([^}]*)\}\s*\)\s*CREATE\s*\(\s*\w+\s*\)\s*-\[\s*:\s*(\w+)\s*\]\s*->\s*\(\s*\w+\s*\))",
        std::regex::icase);
    std::smatch m;
    if (!std::regex_search(query, m, re)) {
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, "Cannot parse MATCH+CREATE edge pattern");
        return TURBOLYNX_ERROR;
    }

    string var_a = m[1], label_a = m[2], props_a = m[3];
    string var_b = m[4], label_b = m[5], props_b = m[6];
    string edge_type = m[7];

    // Extract first property key:value from each node for MATCH
    auto extractFirstProp = [](const string &props) -> std::pair<string,string> {
        std::regex p(R"((\w+)\s*:\s*('[^']*'|"[^"]*"|\d+))");
        std::smatch pm;
        if (std::regex_search(props, pm, p)) return {pm[1], pm[2]};
        return {"", ""};
    };
    auto [key_a, val_a] = extractFirstProp(props_a);
    auto [key_b, val_b] = extractFirstProp(props_b);

    if (key_a.empty() || key_b.empty()) {
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, "MATCH+CREATE edge: nodes need properties for matching");
        return TURBOLYNX_ERROR;
    }

    // Step 1: Get VID of node a
    string match_a = "MATCH (" + var_a + ":" + label_a + " {" + key_a + ": " + val_a + "}) RETURN " + var_a + "." + key_a;
    auto* prep_a = turbolynx_prepare(conn_id, const_cast<char*>(match_a.c_str()));
    if (!prep_a) return TURBOLYNX_ERROR;
    turbolynx_resultset_wrapper* res_a = nullptr;
    turbolynx_execute(conn_id, prep_a, &res_a);

    uint64_t vid_a = 0;
    bool found_a = false;
    if (res_a && res_a->result_set && res_a->result_set->result) {
        auto *vec = reinterpret_cast<duckdb::Vector*>(res_a->result_set->result->__internal_data);
        if (vec && res_a->num_total_rows > 0) {
            vid_a = ((uint64_t*)vec->GetData())[0];
            found_a = true;
        }
    }
    if (res_a) turbolynx_close_resultset(res_a);
    turbolynx_close_prepared_statement(prep_a);

    // Step 2: Get VID of node b
    string match_b = "MATCH (" + var_b + ":" + label_b + " {" + key_b + ": " + val_b + "}) RETURN " + var_b + "." + key_b;
    auto* prep_b = turbolynx_prepare(conn_id, const_cast<char*>(match_b.c_str()));
    if (!prep_b) return TURBOLYNX_ERROR;
    turbolynx_resultset_wrapper* res_b = nullptr;
    turbolynx_execute(conn_id, prep_b, &res_b);

    uint64_t vid_b = 0;
    bool found_b = false;
    if (res_b && res_b->result_set && res_b->result_set->result) {
        auto *vec = reinterpret_cast<duckdb::Vector*>(res_b->result_set->result->__internal_data);
        if (vec && res_b->num_total_rows > 0) {
            vid_b = ((uint64_t*)vec->GetData())[0];
            found_b = true;
        }
    }
    if (res_b) turbolynx_close_resultset(res_b);
    turbolynx_close_prepared_statement(prep_b);

    // Step 3: Create edge if both nodes found
    if (found_a && found_b) {
        auto &delta_store = h->database->instance->delta_store;
        auto &catalog = h->database->instance->GetCatalog();

        // Find edge partition for the edge type
        auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
            *h->client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH, true);
        if (gcat) {
            for (auto ep_oid : *gcat->GetEdgePartitionOids()) {
                auto *ep = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
                    *h->client, DEFAULT_SCHEMA, ep_oid, true);
                if (!ep) continue;
                // Check if this edge partition matches the edge type
                // For simplicity, use the first edge partition found
                uint16_t ep_id = ep->GetPartitionID();
                static uint64_t s_edge_counter = 1000000;
                uint64_t edge_id = ((uint64_t)ep_id << 48) | (++s_edge_counter);

                delta_store.GetAdjListDelta(ep_id).InsertEdge(vid_a, vid_b, edge_id);
                delta_store.GetAdjListDelta(ep_id).InsertEdge(vid_b, vid_a, edge_id);

                if (h->database->instance->wal_writer)
                    h->database->instance->wal_writer->LogInsertEdge(ep_id, vid_a, vid_b, edge_id);

                spdlog::info("[MATCH+CREATE EDGE] type={} src=0x{:016X} dst=0x{:016X} eid=0x{:016X}",
                             edge_type, vid_a, vid_b, edge_id);
                break;
            }
        }
    }

    *result_set_wrp = nullptr;
    return 0;
}

// Check if query is UNWIND [...] AS x CREATE (...)
static bool isUnwindCreate(const string &query) {
    std::regex re(R"(^\s*UNWIND\s+.+\s+AS\s+\w+\s+CREATE\s+)", std::regex::icase);
    return std::regex_search(query, re);
}

// Execute UNWIND+CREATE by two-phase: run UNWIND AS RETURN, then CREATE per row.
static turbolynx_num_rows executeUnwindCreate(int64_t conn_id, const string &query,
                                               turbolynx_resultset_wrapper** result_set_wrp) {
    // Parse: UNWIND <list_expr> AS <var> CREATE (<node_var>:<label> {<props>})
    std::regex re(R"(\bUNWIND\s+(.+?)\s+AS\s+(\w+)\s+CREATE\s+\(\s*(\w+)\s*:\s*(\w+)\s*\{([^}]*)\}\s*\))",
                  std::regex::icase);
    std::smatch m;
    if (!std::regex_search(query, m, re)) {
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, "Cannot parse UNWIND+CREATE pattern");
        return TURBOLYNX_ERROR;
    }

    string list_expr = m[1].str();
    string unwind_var = m[2].str();
    // string node_var = m[3].str(); // unused
    string label = m[4].str();
    string props_str = m[5].str();

    // Check for empty list — skip execution entirely
    {
        string trimmed = list_expr;
        // Remove whitespace
        trimmed.erase(std::remove_if(trimmed.begin(), trimmed.end(), ::isspace), trimmed.end());
        if (trimmed == "[]") {
            *result_set_wrp = nullptr;
            return 0;
        }
    }

    // Phase 1: Execute UNWIND ... AS x RETURN x
    string unwind_q = "UNWIND " + list_expr + " AS " + unwind_var + " RETURN " + unwind_var;
    auto* unwind_prep = turbolynx_prepare(conn_id, const_cast<char*>(unwind_q.c_str()));
    if (!unwind_prep) return TURBOLYNX_ERROR;
    turbolynx_resultset_wrapper* unwind_result = nullptr;
    auto unwind_rows = turbolynx_execute(conn_id, unwind_prep, &unwind_result);
    if (unwind_rows == TURBOLYNX_ERROR) {
        turbolynx_close_prepared_statement(unwind_prep);
        return TURBOLYNX_ERROR;
    }

    // Collect unwind values via direct vector access
    std::vector<string> values;
    if (unwind_result && unwind_result->result_set && unwind_result->result_set->result) {
        auto *res = unwind_result->result_set->result;
        duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(res->__internal_data);
        auto type_id = vec->GetType().id();
        for (turbolynx_num_rows i = 0; i < unwind_rows; i++) {
            if (type_id == duckdb::LogicalTypeId::BIGINT) {
                values.push_back(std::to_string(((int64_t*)vec->GetData())[i]));
            } else if (type_id == duckdb::LogicalTypeId::UBIGINT) {
                values.push_back(std::to_string(((uint64_t*)vec->GetData())[i]));
            } else if (type_id == duckdb::LogicalTypeId::INTEGER) {
                values.push_back(std::to_string(((int32_t*)vec->GetData())[i]));
            } else if (type_id == duckdb::LogicalTypeId::DOUBLE) {
                values.push_back(std::to_string(((double*)vec->GetData())[i]));
            } else if (type_id == duckdb::LogicalTypeId::VARCHAR) {
                auto sv = ((duckdb::string_t*)vec->GetData())[i];
                values.push_back("'" + string(sv.GetDataUnsafe(), sv.GetSize()) + "'");
            } else {
                values.push_back(std::to_string(((int64_t*)vec->GetData())[i]));
            }
        }
    }
    turbolynx_close_resultset(unwind_result);
    turbolynx_close_prepared_statement(unwind_prep);

    if (values.empty()) {
        *result_set_wrp = nullptr;
        return 0;
    }

    // Phase 2: For each unwind value, substitute into CREATE and execute
    turbolynx_num_rows total_created = 0;
    for (auto &val : values) {
        // Replace unwind_var references in props_str with the actual value
        string resolved_props = props_str;
        // Replace standalone variable reference (e.g., "id: x" → "id: 42")
        std::regex var_re("\\b" + unwind_var + "\\b");
        resolved_props = std::regex_replace(resolved_props, var_re, val);

        string create_q = "CREATE (n:" + label + " {" + resolved_props + "})";
        auto* create_prep = turbolynx_prepare(conn_id, const_cast<char*>(create_q.c_str()));
        if (!create_prep) {
            spdlog::error("[UNWIND+CREATE] Failed to prepare: {}", create_q);
            continue;
        }
        turbolynx_resultset_wrapper* create_result = nullptr;
        turbolynx_execute(conn_id, create_prep, &create_result);
        if (create_result) turbolynx_close_resultset(create_result);
        turbolynx_close_prepared_statement(create_prep);
        total_created++;
    }

    spdlog::info("[UNWIND+CREATE] Created {} nodes of label {}", total_created, label);
    *result_set_wrp = nullptr;
    return 0;
}

// Check if query is a MERGE and handle it by decomposing into MATCH + CREATE
static bool isMergeQuery(const string &query) {
    std::regex merge_re(R"(^\s*MERGE\s+)", std::regex::icase);
    return std::regex_search(query, merge_re);
}

// Session config statement (Memgraph-style):
//   SET DATABASE SETTING 'parallel.threads' TO '4';
// Returns N if matched, -1 otherwise.
static int64_t parseSetThreadsStmt(const string &query) {
    std::regex re(
        R"(^\s*SET\s+DATABASE\s+SETTING\s+'parallel\.threads'\s+TO\s+'(\d+)'\s*;?\s*$)",
        std::regex::icase);
    std::smatch m;
    if (std::regex_match(query, m, re)) {
        try { return std::stoll(m[1].str()); } catch (...) { return -1; }
    }
    return -1;
}

// Execute a MERGE query: MERGE (n:Label {key: val, ...})
// Decomposed into: MATCH check → conditional CREATE
static turbolynx_num_rows executeMerge(int64_t conn_id, const string &query,
                                        turbolynx_resultset_wrapper** result_set_wrp) {
    // Parse: MERGE (var:Label {key1: val1, key2: val2, ...})
    std::regex merge_re(R"(\bMERGE\s*\(\s*(\w+)\s*:\s*(\w+)\s*\{([^}]*)\}\s*\))", std::regex::icase);
    std::smatch m;
    if (!std::regex_search(query, m, merge_re)) {
        // Fallback: simple MERGE (var:Label) without properties
        std::regex simple_re(R"(\bMERGE\s*\(\s*(\w+)\s*:\s*(\w+)\s*\))", std::regex::icase);
        if (std::regex_search(query, m, simple_re)) {
            string label = m[2].str();
            // No filter properties — just CREATE if label is empty (unusual case)
            string create_q = "CREATE (n:" + label + ")";
            auto* prep = turbolynx_prepare(conn_id, const_cast<char*>(create_q.c_str()));
            if (!prep) return TURBOLYNX_ERROR;
            auto result = turbolynx_execute(conn_id, prep, result_set_wrp);
            turbolynx_close_prepared_statement(prep);
            return result;
        }
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, "Cannot parse MERGE query");
        return TURBOLYNX_ERROR;
    }

    string var = m[1].str();
    string label = m[2].str();
    string props_str = m[3].str();

    // Extract the FIRST property as the match key (e.g., id: 999)
    std::regex prop_re(R"((\w+)\s*:\s*('[^']*'|"[^"]*"|\d+))");
    auto prop_begin = std::sregex_iterator(props_str.begin(), props_str.end(), prop_re);
    string match_key, match_val;
    if (prop_begin != std::sregex_iterator()) {
        match_key = (*prop_begin)[1].str();
        match_val = (*prop_begin)[2].str();
    }

    if (match_key.empty()) {
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, "MERGE requires at least one property for matching");
        return TURBOLYNX_ERROR;
    }

    // Step 1: MATCH check
    string match_q = "MATCH (" + var + ":" + label + " {" + match_key + ": " + match_val + "}) RETURN count(" + var + ") AS cnt";
    auto* match_prep = turbolynx_prepare(conn_id, const_cast<char*>(match_q.c_str()));
    if (!match_prep) return TURBOLYNX_ERROR;
    turbolynx_resultset_wrapper* match_result = nullptr;
    auto match_rows = turbolynx_execute(conn_id, match_prep, &match_result);

    int64_t cnt = 0;
    if (match_result && match_result->result_set && match_result->result_set->result) {
        auto *res = match_result->result_set->result;
        duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(res->__internal_data);
        spdlog::info("[MERGE] result col0 type={}", vec->GetType().ToString());
        // Read count value directly
        if (vec->GetType().id() == duckdb::LogicalTypeId::BIGINT) {
            cnt = ((int64_t*)vec->GetData())[0];
        } else if (vec->GetType().id() == duckdb::LogicalTypeId::UBIGINT) {
            cnt = (int64_t)((uint64_t*)vec->GetData())[0];
        } else {
            cnt = turbolynx_get_int64(match_result, 0);
        }
    }
    spdlog::info("[MERGE] match_q='{}' cnt={} rows={}", match_q, cnt, match_rows);
    if (match_result) turbolynx_close_resultset(match_result);
    turbolynx_close_prepared_statement(match_prep);

    // Step 2: CREATE if not exists
    if (cnt == 0) {
        string create_q = "CREATE (" + var + ":" + label + " {" + props_str + "})";
        auto* create_prep = turbolynx_prepare(conn_id, const_cast<char*>(create_q.c_str()));
        if (!create_prep) return TURBOLYNX_ERROR;
        auto result = turbolynx_execute(conn_id, create_prep, result_set_wrp);
        turbolynx_close_prepared_statement(create_prep);
        return result;
    }

    // Node exists — no-op
    *result_set_wrp = nullptr;
    return 0;
}

turbolynx_prepared_statement* turbolynx_prepare(int64_t conn_id, turbolynx_query query) {
	auto* h = get_handle(conn_id);
	if (!h) { set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, INVALID_PARAMETER); return nullptr; }
	try {
		auto prep_stmt = (turbolynx_prepared_statement*)malloc(sizeof(turbolynx_prepared_statement));
		// Session config: PRAGMA threads = N / SET parallel_threads = N
		// Apply immediately, return a no-op prepared statement marker.
		{
			int64_t n = parseSetThreadsStmt(string(query));
			if (n >= 0) {
				duckdb::ClientConfig::GetConfig(*h->client).maximum_threads = (idx_t)n;
				prep_stmt->query = query;
				prep_stmt->__internal_prepared_statement = (void*)0x3;  // marker: SET threads (no-op execute)
				prep_stmt->num_properties = 0;
				prep_stmt->property = nullptr;
				prep_stmt->plan = strdup("SET parallel_threads (config)");
				return prep_stmt;
			}
		}
		// Handle MERGE at prepare time — store as special marker
		if (isMergeQuery(string(query))) {
			prep_stmt->query = query;
			prep_stmt->__internal_prepared_statement = nullptr;  // marker: MERGE query
			prep_stmt->num_properties = 0;
			prep_stmt->property = nullptr;
			prep_stmt->plan = strdup("MERGE (rewrite)");
			return prep_stmt;
		}
		// Handle UNWIND+CREATE — store as special marker
		if (isUnwindCreate(string(query))) {
			prep_stmt->query = query;
			prep_stmt->__internal_prepared_statement = (void*)0x2;  // marker: UNWIND+CREATE
			prep_stmt->num_properties = 0;
			prep_stmt->property = nullptr;
			prep_stmt->plan = strdup("UNWIND+CREATE (rewrite)");
			return prep_stmt;
		}
		// Handle MATCH+CREATE edge — store as special marker (similar to MERGE)
		if (isMatchCreateEdge(string(query))) {
			prep_stmt->query = query;
			prep_stmt->__internal_prepared_statement = (void*)0x1;  // marker: MATCH+CREATE edge
			prep_stmt->num_properties = 0;
			prep_stmt->property = nullptr;
			prep_stmt->plan = strdup("MATCH+CREATE edge (rewrite)");
			return prep_stmt;
		}
		bool is_detach = false;
		string rewritten = rewriteDetachDelete(string(query), is_detach);
		rewritten = rewriteRemoveToSetNull(rewritten);
		h->pending_detach_delete = is_detach;
		prep_stmt->query = query;
		prep_stmt->__internal_prepared_statement = reinterpret_cast<void*>(new CypherPreparedStatement(rewritten));
		turbolynx_compile_query(h, rewritten);
		if (h->is_mutation_query) {
			// Mutation queries have no output columns
			prep_stmt->num_properties = 0;
			prep_stmt->property = nullptr;
			prep_stmt->plan = strdup("CREATE (mutation)");
		} else {
			turbolynx_extract_query_metadata(h, prep_stmt);
		}
		return prep_stmt;
	} catch (const std::exception& e) {
		spdlog::error("[turbolynx_prepare] exception: {}", e.what());
		set_error(TURBOLYNX_ERROR_INVALID_STATEMENT, e.what());
		return nullptr;
	} catch (...) {
		spdlog::error("[turbolynx_prepare] unknown exception");
		set_error(TURBOLYNX_ERROR_INVALID_STATEMENT, "Unknown compilation error");
		return nullptr;
	}
}

turbolynx_state turbolynx_close_prepared_statement(turbolynx_prepared_statement* prepared_statement) {
	if (prepared_statement == NULL) {
		last_error_message = INVALID_PREPARED_STATEMENT_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_STATEMENT;
		return TURBOLYNX_ERROR;
	}

	auto *raw_ptr = prepared_statement->__internal_prepared_statement;
	// Skip deletion for special markers (nullptr=MERGE, 0x1=MATCH+CREATE edge, 0x2=UNWIND+CREATE, 0x3=SET threads)
	if (raw_ptr != nullptr && raw_ptr != (void*)0x1 && raw_ptr != (void*)0x2 && raw_ptr != (void*)0x3) {
		auto cypher_stmt = reinterpret_cast<CypherPreparedStatement *>(raw_ptr);
		delete cypher_stmt;
	}

	turbolynx_close_property(prepared_statement->property);
	free(prepared_statement);
	return TURBOLYNX_SUCCESS;
}

turbolynx_state turbolynx_bind_value(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_value val) {
	auto value = reinterpret_cast<Value *>(val);
	auto cypher_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);
	if (!cypher_stmt) {
        last_error_message = INVALID_PREPARED_STATEMENT_MSG;
        last_error_code = TURBOLYNX_ERROR_INVALID_STATEMENT;
		return TURBOLYNX_ERROR;
	}
	if (!cypher_stmt->bindValue(param_idx - 1, *value)) {
        last_error_message = "Can not bind to parameter number " + std::to_string(param_idx) + ", statement only has " + std::to_string(cypher_stmt->getNumParams()) + " parameter(s)";
        last_error_code = TURBOLYNX_ERROR_INVALID_PARAMETER_INDEX;
		return TURBOLYNX_ERROR;
	}
	return TURBOLYNX_SUCCESS;
}

turbolynx_state turbolynx_bind_boolean(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, bool val) {
	auto value = Value::BOOLEAN(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_int8(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, int8_t val) {
	auto value = Value::TINYINT(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_int16(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, int16_t val) {
	auto value = Value::SMALLINT(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_int32(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, int32_t val) {
	auto value = Value::INTEGER(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_int64(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, int64_t val) {
	auto value = Value::BIGINT(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

static hugeint_t turbolynx_internal_hugeint(turbolynx_hugeint val) {
	hugeint_t internal;
	internal.lower = val.lower;
	internal.upper = val.upper;
	return internal;
}

turbolynx_state turbolynx_bind_hugeint(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_hugeint val) {
	auto value = Value::HUGEINT(turbolynx_internal_hugeint(val));
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_uint8(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, uint8_t val) {
	auto value = Value::UTINYINT(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_uint16(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, uint16_t val) {
	auto value = Value::USMALLINT(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_uint32(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, uint32_t val) {
	auto value = Value::UINTEGER(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_uint64(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, uint64_t val) {
	auto value = Value::UBIGINT(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_float(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, float val) {
	auto value = Value::FLOAT(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_double(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, double val) {
	auto value = Value::DOUBLE(val);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_date(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_date val) {
	auto value = Value::DATE(duckdb::date_t(val.days));
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_date_string(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, const char *val) {
    std::tm time = {};
    if (strptime(val, "%Y-%m-%d", &time) == nullptr) {
        last_error_message = INVALID_PARAMETER;
        last_error_code = TURBOLYNX_ERROR_INVALID_PARAMETER;
		return TURBOLYNX_ERROR;
    }

    std::tm epoch = {};
    epoch.tm_year = 70;  // 1970
    epoch.tm_mon = 0;    // January
    epoch.tm_mday = 1;   // 1st day

    auto epoch_time = std::mktime(&epoch);
    auto input_time = std::mktime(&time);

    if (epoch_time == -1 || input_time == -1) {
        last_error_message = INVALID_PARAMETER;
        last_error_code = TURBOLYNX_ERROR_INVALID_PARAMETER;
		return TURBOLYNX_ERROR;
    }

	auto value = Value::DATE(duckdb::date_t(static_cast<int64_t>(std::difftime(input_time, epoch_time) / (60 * 60 * 24))));
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_time(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_time val) {
	auto value = Value::TIME(duckdb::dtime_t(val.micros));
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_timestamp(turbolynx_prepared_statement* prepared_statement, idx_t param_idx,
                                   turbolynx_timestamp val) {
	auto value = Value::TIMESTAMP(duckdb::timestamp_t(val.micros));
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

turbolynx_state turbolynx_bind_varchar(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, const char *val) {
	try {
		auto value = Value(val);
		return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
	} catch (...) {
		return TURBOLYNX_ERROR;
	}
}

turbolynx_state turbolynx_bind_varchar_length(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, const char *val,
                                        idx_t length) {
	try {
		auto value = Value(std::string(val, length));
		return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
	} catch (...) {
		return TURBOLYNX_ERROR;
	}
}

turbolynx_state turbolynx_bind_decimal(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_decimal val) {
	auto hugeint_val = turbolynx_internal_hugeint(val.value);
	if (val.width > duckdb::Decimal::MAX_WIDTH_INT64) {
		auto value = Value::DECIMAL(hugeint_val, val.width, val.scale);
		return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
	}
	auto value = hugeint_val.lower;
	auto duck_val = Value::DECIMAL((int64_t)value, val.width, val.scale);
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&duck_val);
}

turbolynx_state turbolynx_bind_null(turbolynx_prepared_statement* prepared_statement, idx_t param_idx) {
	auto value = Value();
	return turbolynx_bind_value(prepared_statement, param_idx, (turbolynx_value)&value);
}

static void turbolynx_register_resultset(turbolynx_prepared_statement* prepared_statement, turbolynx_resultset_wrapper** _results_set_wrp) {
	auto cypher_prep_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);

	// Create linked list of turbolynx_resultset
	turbolynx_resultset *result_set = NULL;
	turbolynx_resultset *prev_result_set = NULL;

	for (auto data_chunk: cypher_prep_stmt->queryResults) {
		turbolynx_resultset *new_result_set = (turbolynx_resultset*)malloc(sizeof(turbolynx_resultset));
		new_result_set->num_properties = prepared_statement->num_properties;
		new_result_set->next = NULL;

		// Create linked list of turbolynx_result and register to result
		{
			turbolynx_result *result = NULL;
			turbolynx_result *prev_result = NULL;
			turbolynx_property *property = prepared_statement->property;

			for (int i = 0; i < prepared_statement->num_properties; i++) {
				turbolynx_result *new_result = (turbolynx_result*)malloc(sizeof(turbolynx_result));
				new_result->data_type = property->property_type;
				new_result->data_sql_type = property->property_sql_type;
				new_result->num_rows = data_chunk->size();
				new_result->__internal_data = (void*)(&data_chunk->data[i]);
				new_result->next = NULL;

				if (!result) {
					result = new_result;
				} else {
					prev_result->next = new_result;
				}
				prev_result = new_result;
				property = property->next;
			}
			new_result_set->result = result;
		}

		if (!result_set) {
			result_set = new_result_set;
		} else {
			prev_result_set->next = new_result_set;
		}
		prev_result_set = new_result_set;
	}

	if (result_set == NULL) result_set = &empty_result_set;
	turbolynx_resultset_wrapper *result_set_wrp = (turbolynx_resultset_wrapper*)malloc(sizeof(turbolynx_resultset_wrapper));
	result_set_wrp->result_set = result_set;
	result_set_wrp->cursor = 0;
	result_set_wrp->num_total_rows = cypher_prep_stmt->getNumRows();
	*_results_set_wrp = result_set_wrp;
}

// Auto compaction: check if in-memory delta exceeds threshold and trigger checkpoint.
static idx_t g_auto_compact_row_threshold = 10000;
static idx_t g_auto_compact_extent_threshold = 128;

static void maybeAutoCompact(ConnectionHandle* h) {
    if (g_auto_compact_row_threshold == 0) return; // disabled
    auto &ds = h->database->instance->delta_store;
    idx_t total_rows = ds.GetTotalInMemoryRows();
    idx_t extent_count = ds.GetInMemoryExtentCount();
    if (total_rows >= g_auto_compact_row_threshold || extent_count >= g_auto_compact_extent_threshold) {
        spdlog::info("[AUTO-COMPACT] Triggered: {} in-memory rows, {} extents (thresholds: {}/{})",
                     total_rows, extent_count, g_auto_compact_row_threshold, g_auto_compact_extent_threshold);
        try {
            turbolynx_checkpoint_ctx(*h->client);
        } catch (const std::exception &e) {
            spdlog::warn("[AUTO-COMPACT] Failed: {}", e.what());
        }
    }
}

void turbolynx_set_auto_compact_threshold(idx_t row_threshold, idx_t extent_threshold) {
    g_auto_compact_row_threshold = row_threshold;
    g_auto_compact_extent_threshold = extent_threshold;
}

void turbolynx_set_max_threads(int64_t conn_id, size_t max_threads) {
    auto h = get_handle(conn_id);
    if (h == NULL || h->client == NULL) return;
    duckdb::ClientConfig::GetConfig(*h->client).maximum_threads = (idx_t)max_threads;
}

// Execute a CREATE mutation directly against DeltaStore, bypassing ORCA.
static turbolynx_num_rows turbolynx_execute_mutation(ConnectionHandle* h,
                                                      turbolynx_prepared_statement* prepared_statement,
                                                      turbolynx_resultset_wrapper** result_set_wrp) {
    auto& bound_query = h->last_bound_mutation;
    if (!bound_query || bound_query->GetNumSingleQueries() == 0) {
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, "No bound mutation query");
        return TURBOLYNX_ERROR;
    }

    auto& delta_store = h->database->instance->delta_store;
    auto* sq = bound_query->GetSingleQuery(0);

    for (idx_t pi = 0; pi < sq->GetNumQueryParts(); pi++) {
        auto* qp = sq->GetQueryPart(pi);
        for (idx_t ui = 0; ui < qp->GetNumUpdatingClauses(); ui++) {
            auto* uc = qp->GetUpdatingClause(ui);
            if (uc->GetClauseType() == duckdb::BoundUpdatingClauseType::CREATE) {
                auto* create = static_cast<const duckdb::BoundCreateClause*>(uc);
                for (auto& node_info : create->GetNodes()) {
                    // Determine partition OID for insert buffer
                    duckdb::idx_t part_oid = 0;
                    if (!node_info.partition_ids.empty()) {
                        part_oid = node_info.partition_ids[0];
                    }
                    // Look up the logical PartitionID (uint16_t) from the catalog
                    auto& catalog = h->database->instance->GetCatalog();
                    auto* part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
                        *h->client.get(), DEFAULT_SCHEMA, part_oid);
                    uint16_t logical_pid = part_cat ? part_cat->GetPartitionID() : (uint16_t)(part_oid & 0xFFFF);
                    // Allocate (or reuse) an in-memory ExtentID for this partition.
                    // All rows for the same partition go into the same in-memory extent.
                    auto inmem_eids = delta_store.GetInMemoryExtentIDs(logical_pid);
                    uint32_t inmem_eid;
                    if (inmem_eids.empty()) {
                        inmem_eid = delta_store.AllocateInMemoryExtentID(logical_pid);
                    } else {
                        inmem_eid = inmem_eids[0]; // reuse first in-memory extent
                    }
                    // Build row of Values with property key names
                    duckdb::vector<std::string> keys;
                    duckdb::vector<duckdb::Value> row;
                    for (auto& [key, val] : node_info.properties) {
                        keys.push_back(key);
                        row.push_back(val);
                    }
                    // WAL: log before applying
                    if (h->database->instance->wal_writer)
                        h->database->instance->wal_writer->LogInsertNode(logical_pid, inmem_eid, keys, row);
                    delta_store.GetInsertBuffer(inmem_eid).AppendRow(std::move(keys), std::move(row));
                    spdlog::info("[CREATE] Inserted node label='{}' with {} properties into in-memory extent 0x{:08X} (partition {}, oid {})",
                                 node_info.label, node_info.properties.size(), inmem_eid, logical_pid, part_oid);
                }

                // Process edges: record in AdjListDelta (forward + backward)
                for (auto& edge_info : create->GetEdges()) {
                    if (edge_info.edge_partition_ids.empty()) {
                        spdlog::warn("[CREATE] Edge type '{}' has no partition — skipping", edge_info.type);
                        continue;
                    }
                    // For now: use src/dst VIDs of 0 as placeholders for newly created nodes.
                    // Full VID resolution (via index lookup) is deferred to MATCH+CREATE support.
                    // The edge is still recorded so that the infrastructure is exercised.
                    idx_t edge_part_oid = edge_info.edge_partition_ids[0];
                    auto& catalog = h->database->instance->GetCatalog();
                    auto* edge_part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
                        *h->client.get(), DEFAULT_SCHEMA, edge_part_oid);
                    if (!edge_part_cat) {
                        spdlog::warn("[CREATE] Edge partition oid {} not found", edge_part_oid);
                        continue;
                    }
                    uint16_t edge_logical_pid = edge_part_cat->GetPartitionID();

                    // Synthetic edge ID: [edge_partition_id:16][counter:48]
                    static uint64_t s_edge_counter = 0;
                    uint64_t edge_id = ((uint64_t)edge_logical_pid << 48) | (++s_edge_counter);

                    // For src/dst VIDs: look up by 'id' property from the bound nodes.
                    // In the pure CREATE pattern (a)-[:T]->(b), both nodes are newly created
                    // and don't have real VIDs yet. Use placeholder VIDs = 0 for now.
                    uint64_t src_vid = edge_info.src_vid;
                    uint64_t dst_vid = edge_info.dst_vid;

                    // WAL: log edge before applying
                    if (h->database->instance->wal_writer)
                        h->database->instance->wal_writer->LogInsertEdge(edge_logical_pid, src_vid, dst_vid, edge_id);
                    // Record in forward direction (src → dst)
                    delta_store.GetAdjListDelta(edge_logical_pid).InsertEdge(src_vid, dst_vid, edge_id);
                    // Record in backward direction (dst → src) using same edge_id
                    delta_store.GetAdjListDelta(edge_logical_pid).InsertEdge(dst_vid, src_vid, edge_id);

                    spdlog::info("[CREATE] Inserted edge type='{}' partition={} edge_id=0x{:016X} src=0x{:016X} dst=0x{:016X}",
                                 edge_info.type, edge_logical_pid, edge_id, src_vid, dst_vid);
                }
            }
        }
    }

    // Auto compaction check after mutation
    maybeAutoCompact(h);

    // Return 0 rows — mutation has no result set
    *result_set_wrp = nullptr;
    return 0;
}

turbolynx_num_rows turbolynx_execute(int64_t conn_id, turbolynx_prepared_statement* prepared_statement, turbolynx_resultset_wrapper** result_set_wrp) {
	auto* h = get_handle(conn_id);
	if (!h) { set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, INVALID_PARAMETER); return TURBOLYNX_ERROR; }
	try {
	// MERGE queries are handled by decomposition (prepare set __internal = nullptr)
	if (prepared_statement->__internal_prepared_statement == nullptr) {
		return executeMerge(conn_id, string(prepared_statement->query), result_set_wrp);
	}
	// MATCH+CREATE edge queries (prepare set __internal = 0x1)
	if (prepared_statement->__internal_prepared_statement == (void*)0x1) {
		return executeMatchCreateEdge(conn_id, string(prepared_statement->query), result_set_wrp);
	}
	// UNWIND+CREATE queries (prepare set __internal = 0x2)
	if (prepared_statement->__internal_prepared_statement == (void*)0x2) {
		return executeUnwindCreate(conn_id, string(prepared_statement->query), result_set_wrp);
	}
	// SET parallel_threads / PRAGMA threads (prepare set __internal = 0x3)
	// Config already applied at prepare time — return empty result.
	if (prepared_statement->__internal_prepared_statement == (void*)0x3) {
		*result_set_wrp = (turbolynx_resultset_wrapper*)malloc(sizeof(turbolynx_resultset_wrapper));
		(*result_set_wrp)->result_set = &empty_result_set;
		return 0;
	}
	auto cypher_prep_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);
	turbolynx_compile_query(h, cypher_prep_stmt->getBoundQuery());

	// Handle mutation queries (CREATE, etc.) — bypass ORCA pipeline
	if (h->is_mutation_query) {
		return turbolynx_execute_mutation(h, prepared_statement, result_set_wrp);
	}

	auto executors = h->planner->genPipelineExecutors();
    if (executors.size() == 0) {
		last_error_message = INVALID_PLAN_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_PLAN;
		return TURBOLYNX_ERROR;
    }
    else {
        for( auto exec : executors ) {
			std::cout << exec->pipeline->toString() << std::endl;
			exec->ExecutePipeline();
		}
		// After pipeline execution: apply SET mutations if present
		auto &query_results = *(executors.back()->context->query_results);
		if (!h->pending_set_items.empty()) {
			// Guard: check that all SET property keys exist in the catalog schema.
			// Schema evolution (adding new properties) is not yet supported.
			{
				auto &catalog = h->database->instance->GetCatalog();
				auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
					*h->client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH, true);
				if (gcat) {
					// Collect all known property keys from vertex partitions
					std::unordered_set<std::string> known_keys;
					for (auto vp_oid : *gcat->GetVertexPartitionOids()) {
						auto *vp = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
							*h->client, DEFAULT_SCHEMA, vp_oid, true);
						if (!vp) continue;
						auto *key_names = vp->GetUniversalPropertyKeyNames();
						if (key_names) {
							for (auto &key : *key_names) {
								known_keys.insert(key);
							}
						}
					}
					// Filter out SET items for unknown properties.
					// If value is NULL (from REMOVE rewrite), silently skip.
					// Otherwise, throw an error for schema evolution.
					std::vector<duckdb::BoundSetItem> valid_items;
					for (auto &item : h->pending_set_items) {
						if (known_keys.find(item.property_key) == known_keys.end()) {
							if (item.value.IsNull()) {
								continue; // REMOVE non-existent property — no-op
							}
							throw std::runtime_error(
								"Unsupported: SET with new property '" + item.property_key +
								"' (schema evolution not yet supported). Only existing properties can be updated.");
						}
						valid_items.push_back(item);
					}
					h->pending_set_items = std::move(valid_items);
				}
			}
			auto &delta_store = h->database->instance->delta_store;
			for (auto &chunk : query_results) {
				if (chunk->ColumnCount() == 0 || chunk->size() == 0) continue;
				// Find the 'id' column (UBIGINT or BIGINT) for user-id based updates.
				// Also find the _id column (ID type) for VID-based updates.
				idx_t id_col = duckdb::DConstants::INVALID_INDEX;
				idx_t vid_col = duckdb::DConstants::INVALID_INDEX;
				for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
					auto tid = chunk->data[c].GetType().id();
					if (tid == duckdb::LogicalTypeId::ID) vid_col = c;
					else if (tid == duckdb::LogicalTypeId::UBIGINT || tid == duckdb::LogicalTypeId::BIGINT) {
						if (id_col == duckdb::DConstants::INVALID_INDEX) id_col = c;
					}
				}
				for (idx_t row = 0; row < chunk->size(); row++) {
					// Store by user id (for merge when VID not in output)
					if (id_col != duckdb::DConstants::INVALID_INDEX) {
						uint64_t user_id = ((uint64_t *)chunk->data[id_col].GetData())[row];
						for (auto &item : h->pending_set_items) {
							delta_store.SetPropertyByUserId(user_id, item.property_key, item.value);
							// WAL
							if (h->database->instance->wal_writer)
								h->database->instance->wal_writer->LogUpdateProp(user_id, item.property_key, item.value);
						}
						spdlog::info("[SET] user_id={} props={}", user_id, h->pending_set_items.size());
					}
					// Note: VID-based SetByName removed — user_id based updates only.
					// The VID-based path caused type mismatch in mergeUpdateSegment.
				}
			}
			h->pending_set_items.clear();
		}

		// Apply DELETE mutations if present
		if (h->pending_delete) {
			auto &delta_store = h->database->instance->delta_store;
			for (auto &chunk : query_results) {
				if (chunk->ColumnCount() == 0 || chunk->size() == 0) continue;
				// Find VID column (ID type) for DeleteMask
				idx_t vid_col = duckdb::DConstants::INVALID_INDEX;
				for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
					if (chunk->data[c].GetType().id() == duckdb::LogicalTypeId::ID) { vid_col = c; break; }
				}
				if (vid_col == duckdb::DConstants::INVALID_INDEX) continue;
				// Also find user 'id' column for user-id based delete
				idx_t uid_col = duckdb::DConstants::INVALID_INDEX;
				for (idx_t c = 0; c < chunk->ColumnCount(); c++) {
					auto tid = chunk->data[c].GetType().id();
					if (c != vid_col && (tid == duckdb::LogicalTypeId::UBIGINT || tid == duckdb::LogicalTypeId::BIGINT)) {
						uid_col = c; break;
					}
				}
				auto *vid_data = (uint64_t *)chunk->data[vid_col].GetData();
				for (idx_t row = 0; row < chunk->size(); row++) {
					uint64_t vid = vid_data[row];
					uint32_t extent_id = (uint32_t)(vid >> 32);
					uint32_t row_offset = (uint32_t)(vid & 0xFFFFFFFF);

					// Plain DELETE: check edge constraint (Neo4j semantics)
					if (!h->pending_detach_delete) {
						auto &catalog = h->database->instance->GetCatalog();
						uint16_t part_id = (uint16_t)(extent_id >> 16);
						auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
							*h->client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH, true);
						// Helper: check if a neighbor node is deleted (via DeleteMask)
						auto is_neighbor_deleted = [&](uint64_t neighbor_vid) -> bool {
							uint32_t n_eid = (uint32_t)(neighbor_vid >> 32);
							uint32_t n_off = (uint32_t)(neighbor_vid & 0xFFFFFFFF);
							return delta_store.GetDeleteMask(n_eid).IsDeleted(n_off);
						};
						if (gcat) {
							for (auto ep_oid : *gcat->GetEdgePartitionOids()) {
								auto *ep = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
									*h->client, DEFAULT_SCHEMA, ep_oid, true);
								if (!ep) continue;
								uint16_t ep_id = ep->GetPartitionID();
								auto &adj_delta = delta_store.GetAdjListDelta(ep_id);

								// Check forward edges (src→dst): p[0]=dst_vid, p[1]=edge_id
								auto *src_part = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
									*h->client, DEFAULT_SCHEMA, ep->GetSrcPartOid(), true);
								if (src_part && src_part->GetPartitionID() == part_id && !duckdb::IsInMemoryExtent(extent_id)) {
									uint64_t *s = nullptr, *e = nullptr;
									duckdb::AdjacencyListIterator fwd_iter;
									auto *idx_ids = ep->GetAdjIndexOidVec();
									if (idx_ids && !idx_ids->empty()) {
										auto *idx_cat = (duckdb::IndexCatalogEntry *)catalog.GetEntry(
											*h->client, DEFAULT_SCHEMA, (*idx_ids)[0], true);
										if (idx_cat) {
											fwd_iter.Initialize(*h->client, idx_cat->GetAdjColIdx(), extent_id, true);
											fwd_iter.getAdjListPtr(vid, extent_id, &s, &e, true);
											for (uint64_t *p = s; p && p < e; p += 2) {
												if (!adj_delta.IsEdgeDeleted(vid, p[1]) && !is_neighbor_deleted(p[0])) {
													throw std::runtime_error(
														"Cannot delete node with existing relationships. Use DETACH DELETE instead.");
												}
											}
										}
									}
								}
								// Check backward edges (dst→src): p[0]=src_vid, p[1]=edge_id
								auto *dst_part = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
									*h->client, DEFAULT_SCHEMA, ep->GetDstPartOid(), true);
								if (dst_part && dst_part->GetPartitionID() == part_id && !duckdb::IsInMemoryExtent(extent_id)) {
									uint64_t *s = nullptr, *e = nullptr;
									duckdb::AdjacencyListIterator bwd_iter;
									auto *idx_ids = ep->GetAdjIndexOidVec();
									if (idx_ids && idx_ids->size() > 1) {
										auto *idx_cat = (duckdb::IndexCatalogEntry *)catalog.GetEntry(
											*h->client, DEFAULT_SCHEMA, (*idx_ids)[1], true);
										if (idx_cat) {
											bwd_iter.Initialize(*h->client, idx_cat->GetAdjColIdx(), extent_id, false);
											bwd_iter.getAdjListPtr(vid, extent_id, &s, &e, true);
											for (uint64_t *p = s; p && p < e; p += 2) {
												if (!adj_delta.IsEdgeDeleted(vid, p[1]) && !is_neighbor_deleted(p[0])) {
													throw std::runtime_error(
														"Cannot delete node with existing relationships. Use DETACH DELETE instead.");
												}
											}
										}
									}
								}
								// Check delta-inserted edges (both directions)
								auto *inserted = adj_delta.GetInserted(vid);
								if (inserted && !inserted->empty()) {
									for (auto &ee : *inserted) {
										if (!adj_delta.IsEdgeDeleted(vid, ee.edge_id) && !is_neighbor_deleted(ee.dst_vid)) {
											throw std::runtime_error(
												"Cannot delete node with existing relationships. Use DETACH DELETE instead.");
										}
									}
								}
							}
						}
					}

					// DETACH DELETE: cascade-delete all incident edges
					if (h->pending_detach_delete && !duckdb::IsInMemoryExtent(extent_id)) {
						auto &catalog = h->database->instance->GetCatalog();
						// Find vertex partition from extent
						uint16_t part_id = (uint16_t)(extent_id >> 16);
						// Get all edge partitions from graph catalog
						auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
							*h->client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH, true);
						if (gcat) {
							for (auto ep_oid : *gcat->GetEdgePartitionOids()) {
								auto *ep = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
									*h->client, DEFAULT_SCHEMA, ep_oid, true);
								if (!ep) continue;
								uint16_t ep_id = ep->GetPartitionID();
								// Check forward (src→dst): src_part matches our partition
								auto *src_part = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
									*h->client, DEFAULT_SCHEMA, ep->GetSrcPartOid(), true);
								if (src_part && src_part->GetPartitionID() == part_id) {
									// Get adj list for this VID in forward direction
									uint64_t *s = nullptr, *e = nullptr;
									duckdb::AdjacencyListIterator fwd_iter;
									auto *idx_ids = ep->GetAdjIndexOidVec();
									if (idx_ids && !idx_ids->empty()) {
										auto *idx_cat = (duckdb::IndexCatalogEntry *)catalog.GetEntry(
											*h->client, DEFAULT_SCHEMA, (*idx_ids)[0], true);
										if (idx_cat) {
											fwd_iter.Initialize(*h->client, idx_cat->GetAdjColIdx(), extent_id, true);
											fwd_iter.getAdjListPtr(vid, extent_id, &s, &e, true);
											for (uint64_t *p = s; p && p < e; p += 2) {
												delta_store.GetAdjListDelta(ep_id).DeleteEdge(vid, p[1]);
											}
										}
									}
								}
								// Check backward (dst→src): dst_part matches our partition
								auto *dst_part = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
									*h->client, DEFAULT_SCHEMA, ep->GetDstPartOid(), true);
								if (dst_part && dst_part->GetPartitionID() == part_id) {
									uint64_t *s = nullptr, *e = nullptr;
									duckdb::AdjacencyListIterator bwd_iter;
									auto *idx_ids = ep->GetAdjIndexOidVec();
									if (idx_ids && idx_ids->size() > 1) {
										auto *idx_cat = (duckdb::IndexCatalogEntry *)catalog.GetEntry(
											*h->client, DEFAULT_SCHEMA, (*idx_ids)[1], true);
										if (idx_cat) {
											bwd_iter.Initialize(*h->client, idx_cat->GetAdjColIdx(), extent_id, false);
											bwd_iter.getAdjListPtr(vid, extent_id, &s, &e, true);
											for (uint64_t *p = s; p && p < e; p += 2) {
												delta_store.GetAdjListDelta(ep_id).DeleteEdge(vid, p[1]);
											}
										}
									}
								}
							}
						}
					}

					// Record delete by VID (for base extent rows)
					delta_store.GetDeleteMask(extent_id).Delete(row_offset);
					// Record delete by user id (for in-memory + any scan that uses user id)
					uint64_t del_uid = 0;
					if (uid_col != duckdb::DConstants::INVALID_INDEX) {
						del_uid = ((uint64_t *)chunk->data[uid_col].GetData())[row];
						delta_store.DeleteByUserId(del_uid);
					}
					// WAL
					if (h->database->instance->wal_writer)
						h->database->instance->wal_writer->LogDeleteNode(extent_id, row_offset, del_uid);
					spdlog::info("[{}] vid=0x{:016X} extent=0x{:08X} offset={}",
								 h->pending_detach_delete ? "DETACH DELETE" : "DELETE",
								 vid, extent_id, row_offset);
				}
			}
			h->pending_delete = false;
		}

		// Auto compaction check after SET/DELETE mutations
		if (!h->pending_set_items.empty() || h->pending_delete) {
			// Items already cleared above, but check delta state
		}
		maybeAutoCompact(h);

		cypher_prep_stmt->copyResults(query_results);
		turbolynx_register_resultset(prepared_statement, result_set_wrp);
		if (prepared_statement->plan != NULL) free(prepared_statement->plan);
		prepared_statement->plan = strdup(generatePostgresStylePlan(executors, true).c_str());
    	return cypher_prep_stmt->getNumRows();
    }
	} catch (const std::exception &e) {
		spdlog::error("[turbolynx_execute] exception: {}", e.what());
		set_error(TURBOLYNX_ERROR_INVALID_PLAN, e.what());
		return TURBOLYNX_ERROR;
	} catch (...) {
		spdlog::error("[turbolynx_execute] unknown exception");
		set_error(TURBOLYNX_ERROR_INVALID_PLAN, "Unknown error during query execution");
		return TURBOLYNX_ERROR;
	}
}

turbolynx_state turbolynx_close_resultset(turbolynx_resultset_wrapper* result_set_wrp) {
	if (result_set_wrp == NULL || result_set_wrp->result_set == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return TURBOLYNX_ERROR;
	}

	turbolynx_resultset *next_result_set = NULL;
	turbolynx_resultset *result_set = result_set_wrp->result_set;
	while (result_set != NULL && result_set != &empty_result_set) {
		next_result_set = result_set->next;
		turbolynx_result *next_result = NULL;
		turbolynx_result *result = result_set->result;
		while (result != NULL) {
			auto next_result = result->next;
			free(result);
			result = next_result;
		}
		free(result_set);
		result_set = next_result_set;
	}
	free(result_set_wrp);

	return TURBOLYNX_SUCCESS;
}

turbolynx_fetch_state turbolynx_fetch_next(turbolynx_resultset_wrapper* result_set_wrp) {
	if (result_set_wrp == NULL || result_set_wrp->result_set == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return TURBOLYNX_ERROR_RESULT;
	}

	if (result_set_wrp->cursor >= result_set_wrp->num_total_rows) {
		return TURBOLYNX_END_OF_RESULT;
	}
	else {
		result_set_wrp->cursor++;
		if (result_set_wrp->cursor <= result_set_wrp->num_total_rows) {
			return TURBOLYNX_MORE_RESULT;
		} else {
			return TURBOLYNX_END_OF_RESULT;
		}
	}
}

static turbolynx_result* turbolynx_move_to_cursored_result(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx, size_t& local_cursor) {
	auto cursor = result_set_wrp->cursor - 1;
	size_t acc_rows = 0;

	turbolynx_resultset *result_set = result_set_wrp->result_set;
	while (result_set != NULL) {
		auto num_rows = result_set->result->num_rows;
		if (cursor < acc_rows + num_rows) {
			break;
		}
		acc_rows += num_rows;
		result_set = result_set->next;
	}
	local_cursor = cursor - acc_rows;

	if (result_set == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return NULL;
	}

	auto result = result_set->result;
	for (int i = 0; i < col_idx; i++) {
		result = result->next;
	}

	if (result == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_COLUMN_INDEX;
		return NULL;
	}

	return result;
}

template <typename T, duckdb::LogicalTypeId TYPE_ID>
T turbolynx_get_value(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	if (result_set_wrp == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return T();
	}

    size_t local_cursor;
    auto result = turbolynx_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
    if (result == NULL) { return T(); }

	duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);

    if (vec->GetType().id() != TYPE_ID && vec->GetType().InternalType() != duckdb::LogicalType(TYPE_ID).InternalType()) {
        last_error_message = INVALID_RESULT_SET_MSG;
        last_error_code = TURBOLYNX_ERROR_INVALID_COLUMN_TYPE;
        return T();
    }
    else {
        return vec->GetValue(local_cursor).GetValue<T>();
    }
}

template <>
string turbolynx_get_value<string, duckdb::LogicalTypeId::VARCHAR>(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	if (result_set_wrp == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return string();
	}

    size_t local_cursor;
    auto result = turbolynx_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
    if (result == NULL) { return string(); }

	duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);

    if (vec->GetType().id() != duckdb::LogicalTypeId::VARCHAR) {
        last_error_message = INVALID_RESULT_SET_MSG;
        last_error_code = TURBOLYNX_ERROR_INVALID_COLUMN_TYPE;
        return string();
    }
    else {
		auto &validity = duckdb::FlatVector::Validity(*vec);
		if (!validity.RowIsValid(local_cursor)) {
			return string();  // NULL value
		}
		return ((string_t*)vec->GetData())[local_cursor].GetString();
    }
}

template <>
int16_t turbolynx_get_value<int16_t, duckdb::LogicalTypeId::DECIMAL>(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	if (result_set_wrp == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return 0;
	}

    size_t local_cursor;
    auto result = turbolynx_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
    if (result == NULL) { return 0; }

	duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);

    if (vec->GetType().id() != duckdb::LogicalTypeId::DECIMAL) {
        last_error_message = INVALID_RESULT_SET_MSG;
        last_error_code = TURBOLYNX_ERROR_INVALID_COLUMN_TYPE;
        return 0;
    }
    else {
		return SmallIntValue::Get(((int16_t*)vec->GetData())[local_cursor]);
    }
}

template <>
int32_t turbolynx_get_value<int32_t, duckdb::LogicalTypeId::DECIMAL>(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	if (result_set_wrp == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return 0;
	}

    size_t local_cursor;
    auto result = turbolynx_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
    if (result == NULL) { return 0; }

	duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);

    if (vec->GetType().id() != duckdb::LogicalTypeId::DECIMAL) {
        last_error_message = INVALID_RESULT_SET_MSG;
        last_error_code = TURBOLYNX_ERROR_INVALID_COLUMN_TYPE;
        return 0;
    }
    else {
		return IntegerValue::Get(((int32_t*)vec->GetData())[local_cursor]);
    }
}

template <>
int64_t turbolynx_get_value<int64_t, duckdb::LogicalTypeId::DECIMAL>(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	if (result_set_wrp == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return 0;
	}

    size_t local_cursor;
    auto result = turbolynx_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
    if (result == NULL) { return 0; }

	duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);

    if (vec->GetType().id() != duckdb::LogicalTypeId::DECIMAL) {
        last_error_message = INVALID_RESULT_SET_MSG;
        last_error_code = TURBOLYNX_ERROR_INVALID_COLUMN_TYPE;
        return 0;
    }
    else {
		return BigIntValue::Get(((int64_t*)vec->GetData())[local_cursor]);
    }
}

template <>
hugeint_t turbolynx_get_value<hugeint_t, duckdb::LogicalTypeId::DECIMAL>(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	if (result_set_wrp == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return hugeint_t();
	}

    size_t local_cursor;
    auto result = turbolynx_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
    if (result == NULL) { return hugeint_t(); }

	duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);

    if (vec->GetType().id() != duckdb::LogicalTypeId::DECIMAL) {
        last_error_message = INVALID_RESULT_SET_MSG;
        last_error_code = TURBOLYNX_ERROR_INVALID_COLUMN_TYPE;
        return 0;
    }
    else {
		return HugeIntValue::Get(Value::HUGEINT(((hugeint_t*)vec->GetData())[local_cursor]));
    }
}

bool turbolynx_get_bool(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
    return turbolynx_get_value<bool, duckdb::LogicalTypeId::BOOLEAN>(result_set_wrp, col_idx);
}

int8_t turbolynx_get_int8(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
    return turbolynx_get_value<int8_t, duckdb::LogicalTypeId::TINYINT>(result_set_wrp, col_idx);
}

int16_t turbolynx_get_int16(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return turbolynx_get_value<int16_t, duckdb::LogicalTypeId::SMALLINT>(result_set_wrp, col_idx);
}

int32_t turbolynx_get_int32(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return turbolynx_get_value<int32_t, duckdb::LogicalTypeId::INTEGER>(result_set_wrp, col_idx);
}

int64_t turbolynx_get_int64(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	// Special case: DATE is stored as int32 days-since-epoch.
	// When the caller treats it as int64, convert to milliseconds (days * 86400000).
	if (result_set_wrp != NULL) {
		size_t local_cursor;
		auto result = turbolynx_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
		if (result != NULL) {
			duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);
			if (vec->GetType().id() == duckdb::LogicalTypeId::DATE) {
				duckdb::date_t d = turbolynx_get_value<duckdb::date_t, duckdb::LogicalTypeId::DATE>(result_set_wrp, col_idx);
				return (int64_t)d.days * 86400000LL;
			}
		}
	}
	return turbolynx_get_value<int64_t, duckdb::LogicalTypeId::BIGINT>(result_set_wrp, col_idx);
}

turbolynx_hugeint turbolynx_get_hugeint(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	hugeint_t hugeint = turbolynx_get_value<hugeint_t, duckdb::LogicalTypeId::HUGEINT>(result_set_wrp, col_idx);
	turbolynx_hugeint result;
	result.lower = hugeint.lower;
	result.upper = hugeint.upper;
	return result;
}

uint8_t turbolynx_get_uint8(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return turbolynx_get_value<uint8_t, duckdb::LogicalTypeId::UTINYINT>(result_set_wrp, col_idx);
}

uint16_t turbolynx_get_uint16(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return turbolynx_get_value<uint16_t, duckdb::LogicalTypeId::USMALLINT>(result_set_wrp, col_idx);
}

uint32_t turbolynx_get_uint32(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return turbolynx_get_value<uint32_t, duckdb::LogicalTypeId::UINTEGER>(result_set_wrp, col_idx);
}

uint64_t turbolynx_get_uint64(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return turbolynx_get_value<uint64_t, duckdb::LogicalTypeId::UBIGINT>(result_set_wrp, col_idx);
}

float turbolynx_get_float(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return turbolynx_get_value<float, duckdb::LogicalTypeId::FLOAT>(result_set_wrp, col_idx);
}

double turbolynx_get_double(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return turbolynx_get_value<double, duckdb::LogicalTypeId::DOUBLE>(result_set_wrp, col_idx);
}

turbolynx_date turbolynx_get_date(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	duckdb::date_t date = turbolynx_get_value<duckdb::date_t, duckdb::LogicalTypeId::DATE>(result_set_wrp, col_idx);
	turbolynx_date result;
	result.days = date.days;
	return result;
}

turbolynx_time turbolynx_get_time(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	duckdb::dtime_t time = turbolynx_get_value<duckdb::dtime_t, duckdb::LogicalTypeId::TIME>(result_set_wrp, col_idx);
	turbolynx_time result;
	result.micros = time.micros;
	return result;
}

turbolynx_timestamp turbolynx_get_timestamp(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	duckdb::timestamp_t timestamp = turbolynx_get_value<duckdb::timestamp_t, duckdb::LogicalTypeId::TIMESTAMP>(result_set_wrp, col_idx);
	turbolynx_timestamp result;
	result.micros = timestamp.value;
	return result;
}

turbolynx_string turbolynx_get_varchar(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	string str = turbolynx_get_value<string, duckdb::LogicalTypeId::VARCHAR>(result_set_wrp, col_idx);
	turbolynx_string result;
	result.size = str.length();
	result.data = (char*)malloc(result.size + 1);
    strcpy(result.data, str.c_str());
	return result;
}

turbolynx_decimal turbolynx_get_decimal(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	// Decimal needs special handling
	auto default_value = turbolynx_decimal{0,0,{0,0}};
	if (result_set_wrp == NULL) {
		last_error_message = INVALID_RESULT_SET_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_RESULT_SET;
		return default_value;
	}

    size_t local_cursor;
    auto result = turbolynx_move_to_cursored_result(result_set_wrp, col_idx, local_cursor);
    if (result == NULL) { return default_value; }
	duckdb::Vector* vec = reinterpret_cast<duckdb::Vector*>(result->__internal_data);
	auto data_type = vec->GetType();
	auto width = duckdb::DecimalType::GetWidth(data_type);
	auto scale = duckdb::DecimalType::GetScale(data_type);
	switch (data_type.InternalType()) {
		case duckdb::PhysicalType::INT16:
			return turbolynx_decimal{width,scale,{turbolynx_get_value<int16_t, duckdb::LogicalTypeId::DECIMAL>(result_set_wrp, col_idx),0}};
		case duckdb::PhysicalType::INT32:
			return turbolynx_decimal{width,scale,{turbolynx_get_value<int32_t, duckdb::LogicalTypeId::DECIMAL>(result_set_wrp, col_idx),0}};
		case duckdb::PhysicalType::INT64:
			return turbolynx_decimal{width,scale,{turbolynx_get_value<int64_t, duckdb::LogicalTypeId::DECIMAL>(result_set_wrp, col_idx),0}};
		case duckdb::PhysicalType::INT128:
		{
			auto int128_val = turbolynx_get_value<hugeint_t, duckdb::LogicalTypeId::DECIMAL>(result_set_wrp, col_idx);
			return turbolynx_decimal{width,scale,{int128_val.lower,int128_val.upper}};
			break;
		}
		default:
			return default_value;
	}
}

uint64_t turbolynx_get_id(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx) {
	return turbolynx_get_value<uint64_t, duckdb::LogicalTypeId::ID>(result_set_wrp, col_idx);
}

turbolynx_string turbolynx_decimal_to_string(turbolynx_decimal val) {
	auto type_ = duckdb::LogicalType::DECIMAL(val.width, val.scale);
	auto internal_type = type_.InternalType();
	auto scale = DecimalType::GetScale(type_);

	string str;
	if (internal_type == PhysicalType::INT16) {
		str = Decimal::ToString((int16_t)val.value.lower, scale);
	} else if (internal_type == PhysicalType::INT32) {
		str = Decimal::ToString((int32_t)val.value.lower, scale);
	} else if (internal_type == PhysicalType::INT64) {
		str = Decimal::ToString((int64_t)val.value.lower, scale);
	} else {
		D_ASSERT(internal_type == PhysicalType::INT128);
		auto hugeint_val = hugeint_t();
		hugeint_val.lower = val.value.lower;
		hugeint_val.upper = val.value.upper;
		str = Decimal::ToString(hugeint_val, scale);
	}

	turbolynx_string result;
	result.size = str.length();
	result.data = (char*)malloc(result.size + 1);
	strcpy(result.data, str.c_str());
	return result;
}