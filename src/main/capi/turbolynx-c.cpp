#include <memory>
#include <string>
#include <ctime>
#include "spdlog/spdlog.h"

// antlr4 headers must come before ORCA (c.h defines TRUE/FALSE macros)
#include "CypherLexer.h"
#include "CypherParser.h"
#include "BaseErrorListener.h"
#include "parser/cypher_transformer.hpp"
#include "binder/binder.hpp"

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

static void turbolynx_compile_query(ConnectionHandle* h, string query) {
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

    h->planner->execute(boundQuery.get());
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

turbolynx_prepared_statement* turbolynx_prepare(int64_t conn_id, turbolynx_query query) {
	auto* h = get_handle(conn_id);
	if (!h) { set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, INVALID_PARAMETER); return nullptr; }
	try {
		auto prep_stmt = (turbolynx_prepared_statement*)malloc(sizeof(turbolynx_prepared_statement));
		prep_stmt->query = query;
		prep_stmt->__internal_prepared_statement = reinterpret_cast<void*>(new CypherPreparedStatement(string(query)));
		turbolynx_compile_query(h, string(query));
		turbolynx_extract_query_metadata(h, prep_stmt);
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

	auto cypher_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);
	if (!cypher_stmt) {
		last_error_message = INVALID_PREPARED_STATEMENT_MSG;
		last_error_code = TURBOLYNX_ERROR_INVALID_STATEMENT;
		return TURBOLYNX_ERROR;
	}
	delete cypher_stmt;

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

turbolynx_num_rows turbolynx_execute(int64_t conn_id, turbolynx_prepared_statement* prepared_statement, turbolynx_resultset_wrapper** result_set_wrp) {
	auto* h = get_handle(conn_id);
	if (!h) { set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, INVALID_PARAMETER); return TURBOLYNX_ERROR; }
	try {
	auto cypher_prep_stmt = reinterpret_cast<CypherPreparedStatement *>(prepared_statement->__internal_prepared_statement);
	turbolynx_compile_query(h, cypher_prep_stmt->getBoundQuery());
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
		cypher_prep_stmt->copyResults(*(executors.back()->context->query_results));
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