#include <memory>
#include <string>
#include <ctime>
#include <cctype>
#include <cstring>
#include <functional>
#include <set>
#include <sstream>
#include <regex>
#include <unordered_map>
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
#include "catalog/catalog_entry/extent_catalog_entry.hpp"
#include "storage/delta_store.hpp"
#include "storage/wal.hpp"

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
#include "parser/parsed_data/create_partition_info.hpp"
#include "parser/parsed_data/create_property_schema_info.hpp"
#include "parser/parsed_data/create_index_info.hpp"
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
    std::shared_ptr<duckdb::ClientContext> client;
    std::unique_ptr<turbolynx::Planner>        planner;
    DiskAioFactory*                      disk_aio_factory = nullptr;
    int64_t                              registered_connection_id = -1;
    bool                                 owns_disk_aio = false;
    bool                                 owns_database = true; // false when connected via client_context
    // Mutation support: when last compiled query is a CREATE-only mutation,
    // bypass ORCA and execute directly against DeltaStore.
    std::unique_ptr<duckdb::BoundRegularQuery> last_bound_mutation;
    bool                                 is_mutation_query = false;
    bool                                 has_query_projection = false;
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
static const std::string LABEL_ADD_PREFIX = "__tl_add_label__";
static const std::string LABEL_REMOVE_PREFIX = "__tl_remove_label__";

// Default values
turbolynx_resultset empty_result_set = {0, NULL, NULL};

static std::string TrimCopy(const std::string &input) {
    auto begin = input.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return "";
    }
    auto end = input.find_last_not_of(" \t\r\n");
    return input.substr(begin, end - begin + 1);
}

static std::string ToLowerCopy(std::string input) {
    std::transform(input.begin(), input.end(), input.begin(),
                   [](unsigned char ch) { return (char)std::tolower(ch); });
    return input;
}

static std::string NormalizeQueryForPrepare(std::string query) {
    query = TrimCopy(query);
    while (!query.empty() && query.back() == ';') {
        query.pop_back();
        query = TrimCopy(query);
    }
    return query;
}

static std::vector<std::string> SplitTopLevelCommaList(const std::string &input) {
    std::vector<std::string> items;
    std::string current;
    bool in_single_quote = false;
    bool in_double_quote = false;
    int paren_depth = 0;
    int bracket_depth = 0;
    int brace_depth = 0;

    for (char ch : input) {
        if (ch == '\'' && !in_double_quote) {
            in_single_quote = !in_single_quote;
        } else if (ch == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
        } else if (!in_single_quote && !in_double_quote) {
            if (ch == '(') {
                paren_depth++;
            } else if (ch == ')') {
                paren_depth--;
            } else if (ch == '[') {
                bracket_depth++;
            } else if (ch == ']') {
                bracket_depth--;
            } else if (ch == '{') {
                brace_depth++;
            } else if (ch == '}') {
                brace_depth--;
            } else if (ch == ',' && paren_depth == 0 && bracket_depth == 0 &&
                       brace_depth == 0) {
                items.push_back(TrimCopy(current));
                current.clear();
                continue;
            }
        }
        current.push_back(ch);
    }
    if (!current.empty()) {
        items.push_back(TrimCopy(current));
    }
    return items;
}

static bool ParseLabelMutationItem(const std::string &item, std::string &variable,
                                   std::vector<std::string> &labels) {
    std::regex re(R"(^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z_][A-Za-z0-9_]*(?:\s*:\s*[A-Za-z_][A-Za-z0-9_]*)*)$)");
    std::smatch match;
    if (!std::regex_match(item, match, re)) {
        return false;
    }
    variable = match[1].str();
    labels.clear();
    std::stringstream ss(match[2].str());
    std::string label;
    while (std::getline(ss, label, ':')) {
        label = TrimCopy(label);
        if (!label.empty()) {
            labels.push_back(label);
        }
    }
    return !labels.empty();
}

static size_t FindClauseBoundary(const std::string &query, size_t search_from) {
    static const std::vector<std::string> keywords = {
        " return ", " with ", " delete ", " detach delete ", " create ",
        " merge ", " unwind ", " where ", " set ", " remove ", " match "
    };
    auto lower = ToLowerCopy(query);
    size_t boundary = std::string::npos;
    for (auto &keyword : keywords) {
        auto pos = lower.find(keyword, search_from);
        if (pos != std::string::npos) {
            boundary = std::min(boundary, pos);
        }
    }
    auto semicolon_pos = query.find(';', search_from);
    if (semicolon_pos != std::string::npos) {
        boundary = std::min(boundary, semicolon_pos);
    }
    return boundary;
}

static std::string RewriteSetOrRemoveClauseItems(const std::string &clause_body,
                                                 bool remove_clause) {
    auto items = SplitTopLevelCommaList(clause_body);
    std::vector<std::string> rewritten_items;
    rewritten_items.reserve(items.size());
    for (auto &item : items) {
        if (item.empty()) {
            continue;
        }
        std::string variable;
        std::vector<std::string> labels;
        if (ParseLabelMutationItem(item, variable, labels)) {
            for (auto &label : labels) {
                rewritten_items.push_back(
                    variable + "." +
                    (remove_clause ? LABEL_REMOVE_PREFIX : LABEL_ADD_PREFIX) +
                    label + " = true");
            }
            continue;
        }
        if (remove_clause) {
            rewritten_items.push_back(item + " = NULL");
        } else {
            rewritten_items.push_back(item);
        }
    }
    std::string rewritten;
    for (idx_t i = 0; i < rewritten_items.size(); i++) {
        if (i > 0) {
            rewritten += ", ";
        }
        rewritten += rewritten_items[i];
    }
    return rewritten;
}

static std::string RewriteClauseByKeyword(const std::string &query,
                                          const std::string &keyword,
                                          bool remove_clause) {
    auto lower = ToLowerCopy(query);
    std::string result;
    size_t cursor = 0;
    size_t keyword_pos = 0;
    while ((keyword_pos = lower.find(keyword, cursor)) != std::string::npos) {
        size_t clause_body_begin = keyword_pos + keyword.size();
        size_t clause_body_end = FindClauseBoundary(query, clause_body_begin);
        result.append(query.substr(cursor, keyword_pos - cursor));
        result.append(remove_clause ? "SET " : query.substr(keyword_pos, keyword.size()));
        auto clause_body = query.substr(
            clause_body_begin,
            clause_body_end == std::string::npos ? std::string::npos
                                                 : clause_body_end - clause_body_begin);
        result.append(RewriteSetOrRemoveClauseItems(clause_body, remove_clause));
        if (clause_body_end == std::string::npos) {
            cursor = query.size();
            break;
        }
        cursor = clause_body_end;
    }
    result.append(query.substr(cursor));
    return result;
}

static std::string RewriteSetLabelItems(const std::string &query) {
    return RewriteClauseByKeyword(query, "set ", false);
}

static std::vector<std::string> SplitLabelSetString(const std::string &labelset) {
    std::vector<std::string> labels;
    std::stringstream ss(labelset);
    std::string label;
    while (std::getline(ss, label, ':')) {
        label = TrimCopy(label);
        if (!label.empty()) {
            labels.push_back(label);
        }
    }
    return labels;
}

static std::vector<std::string> NormalizeLabelSet(std::vector<std::string> labels) {
    std::sort(labels.begin(), labels.end());
    labels.erase(std::unique(labels.begin(), labels.end()), labels.end());
    return labels;
}

static std::string JoinLabelSet(const std::vector<std::string> &labels) {
    std::string joined;
    for (idx_t i = 0; i < labels.size(); i++) {
        if (i > 0) {
            joined += ":";
        }
        joined += labels[i];
    }
    return joined;
}

static void initialize_planner(ConnectionHandle &h) {
    if (!h.planner) {
        turbolynx::PlannerConfig planner_config;  // uses sensible defaults
        h.planner = std::make_unique<turbolynx::Planner>(planner_config, turbolynx::MDProviderType::TBGPP, h.client.get());
    }
}

static void RefreshCatalogAndPlanner(ConnectionHandle &h) {
    h.client->db->GetCatalogWrapper().ClearSchemaCache();
    duckdb::SetClientWrapper(
        h.client, make_shared<CatalogWrapper>(*h.client->db));
    h.planner.reset();
    initialize_planner(h);
}

static idx_t FindIdColumn(const DataChunk &chunk) {
    for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
        if (chunk.data[c].GetType().id() == duckdb::LogicalTypeId::ID) {
            return c;
        }
    }
    return duckdb::DConstants::INVALID_INDEX;
}

static PropertySchemaCatalogEntry *FindPropertySchemaByExtent(ConnectionHandle *h, uint32_t extent_id) {
    auto &catalog = h->database->instance->GetCatalog();
    auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
        *h->client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA, DEFAULT_GRAPH, true);
    if (!gcat) {
        return nullptr;
    }
    for (auto vp_oid : *gcat->GetVertexPartitionOids()) {
        auto *vp = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
            *h->client, DEFAULT_SCHEMA, vp_oid, true);
        if (!vp) {
            continue;
        }
        auto *ps_ids = vp->GetPropertySchemaIDs();
        if (!ps_ids) {
            continue;
        }
        for (auto ps_oid : *ps_ids) {
            auto *ps = (duckdb::PropertySchemaCatalogEntry *)catalog.GetEntry(
                *h->client, DEFAULT_SCHEMA, ps_oid, true);
            if (!ps) {
                continue;
            }
            for (auto eid : ps->extent_ids) {
                if (eid == extent_id) {
                    return ps;
                }
            }
        }
    }
    return nullptr;
}

static bool SnapshotDeltaRow(const duckdb::DeltaStore &ds, uint64_t pid,
                             vector<string> &keys, vector<duckdb::Value> &values) {
    const duckdb::InsertBuffer *buf = nullptr;
    idx_t row_idx = 0;
    if (!ds.TryGetDeltaRow(pid, buf, row_idx) || !buf || !buf->IsValid(row_idx)) {
        return false;
    }
    keys = buf->GetSchemaKeys();
    values = buf->GetRow(row_idx);
    return true;
}

static bool SnapshotBaseRowFromChunk(ConnectionHandle *h, const DataChunk &chunk, idx_t row,
                                     uint32_t extent_id, vector<string> &keys,
                                     vector<duckdb::Value> &values) {
    auto *ps = FindPropertySchemaByExtent(h, extent_id);
    if (!ps) {
        return false;
    }

    auto *ps_keys = ps->GetKeys();
    if (!ps_keys) {
        return false;
    }

    keys = *ps_keys;
    values.clear();
    values.reserve(keys.size());

    idx_t next_col = 0;
    if (next_col < chunk.ColumnCount() &&
        chunk.data[next_col].GetType().id() == duckdb::LogicalTypeId::ID) {
        // Whole-node readback returns the internal _id first, followed by
        // user-visible properties in property-schema order. Do not skip
        // user properties that also use the ID logical type, such as `id`.
        next_col++;
    }
    for (idx_t key_idx = 0; key_idx < keys.size(); key_idx++) {
        if (next_col < chunk.ColumnCount()) {
            values.push_back(chunk.GetValue(next_col, row));
            next_col++;
        } else {
            values.push_back(duckdb::Value());
        }
    }
    return true;
}

static bool SnapshotCurrentNodeRecord(ConnectionHandle *h, const DataChunk &chunk, idx_t row,
                                      uint64_t logical_id, vector<string> &keys,
                                      vector<duckdb::Value> &values) {
    auto &ds = h->database->instance->delta_store;
    auto current_pid = ds.ResolvePid(logical_id);
    bool has_current_pid = current_pid != 0 ||
                           (logical_id == 0 && !ds.IsLogicalIdDeleted(logical_id));
    if (!has_current_pid) {
        return false;
    }
    if (ds.ResolveIsDelta(logical_id) || duckdb::IsInMemoryExtent((uint32_t)(current_pid >> 32))) {
        return SnapshotDeltaRow(ds, current_pid, keys, values);
    }
    return SnapshotBaseRowFromChunk(h, chunk, row, (uint32_t)(current_pid >> 32), keys, values);
}

static void ApplySetItemsToSnapshot(vector<string> &keys, vector<duckdb::Value> &values,
                                    const std::vector<duckdb::BoundSetItem> &items) {
    for (auto &item : items) {
        auto it = std::find(keys.begin(), keys.end(), item.property_key);
        if (item.value.IsNull()) {
            if (it == keys.end()) {
                continue;
            }
            auto idx = std::distance(keys.begin(), it);
            keys.erase(it);
            values.erase(values.begin() + idx);
            continue;
        }

        if (it == keys.end()) {
            keys.push_back(item.property_key);
            values.push_back(item.value);
            continue;
        }
        auto idx = std::distance(keys.begin(), it);
        values[idx] = item.value;
    }
}

static bool IsLabelAddItem(const duckdb::BoundSetItem &item) {
    return item.property_key.rfind(LABEL_ADD_PREFIX, 0) == 0;
}

static bool IsLabelRemoveItem(const duckdb::BoundSetItem &item) {
    return item.property_key.rfind(LABEL_REMOVE_PREFIX, 0) == 0;
}

static std::string DecodeSyntheticLabelKey(const std::string &property_key) {
    if (property_key.rfind(LABEL_ADD_PREFIX, 0) == 0) {
        return property_key.substr(LABEL_ADD_PREFIX.size());
    }
    if (property_key.rfind(LABEL_REMOVE_PREFIX, 0) == 0) {
        return property_key.substr(LABEL_REMOVE_PREFIX.size());
    }
    return "";
}

static std::vector<duckdb::BoundSetItem> FilterPropertySetItems(
    const std::vector<duckdb::BoundSetItem> &items) {
    std::vector<duckdb::BoundSetItem> filtered;
    filtered.reserve(items.size());
    for (auto &item : items) {
        if (IsLabelAddItem(item) || IsLabelRemoveItem(item)) {
            continue;
        }
        filtered.push_back(item);
    }
    return filtered;
}

static std::vector<std::string> ApplyLabelSetItemsToLabels(
    std::vector<std::string> labels,
    const std::vector<duckdb::BoundSetItem> &items) {
    auto normalized = NormalizeLabelSet(std::move(labels));
    std::set<std::string> label_set(normalized.begin(), normalized.end());
    for (auto &item : items) {
        auto label = DecodeSyntheticLabelKey(item.property_key);
        if (label.empty()) {
            continue;
        }
        if (IsLabelAddItem(item)) {
            label_set.insert(label);
        } else if (IsLabelRemoveItem(item)) {
            label_set.erase(label);
        }
    }
    return NormalizeLabelSet(std::vector<std::string>(label_set.begin(),
                                                      label_set.end()));
}

static void InvalidateCurrentNodeVersion(duckdb::DeltaStore &ds, uint64_t logical_id) {
    auto current_pid = ds.ResolvePid(logical_id);
    bool has_current_pid = current_pid != 0 ||
                           (logical_id == 0 && !ds.IsLogicalIdDeleted(logical_id));
    if (!has_current_pid) {
        return;
    }
    uint32_t extent_id = (uint32_t)(current_pid >> 32);
    uint32_t row_offset = (uint32_t)(current_pid & 0xFFFFFFFFull);
    if (ds.ResolveIsDelta(logical_id) || duckdb::IsInMemoryExtent(extent_id)) {
        ds.InvalidateDeltaRow(current_pid);
        return;
    }
    ds.GetDeleteMask(extent_id).Delete(row_offset);
}

static duckdb::PartitionCatalogEntry *FindPartitionCatalogByLogicalId(
    duckdb::ClientContext &context, uint16_t partition_id) {
    auto &catalog = context.db->GetCatalog();
    auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
        context, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
        DEFAULT_GRAPH, true);
    if (!gcat) {
        return nullptr;
    }

    auto find_in = [&](auto *partition_oids)
        -> duckdb::PartitionCatalogEntry * {
        if (!partition_oids) {
            return nullptr;
        }
        for (auto part_oid : *partition_oids) {
            auto *part = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
                context, DEFAULT_SCHEMA, part_oid, true);
            if (part && part->GetPartitionID() == partition_id) {
                return part;
            }
        }
        return nullptr;
    };

    if (auto *part = find_in(gcat->GetVertexPartitionOids())) {
        return part;
    }
    return find_in(gcat->GetEdgePartitionOids());
}

static std::string StripVertexPartitionPrefix(const std::string &partition_name) {
    if (partition_name.rfind(DEFAULT_VERTEX_PARTITION_PREFIX, 0) == 0) {
        return partition_name.substr(std::strlen(DEFAULT_VERTEX_PARTITION_PREFIX));
    }
    return partition_name;
}

static std::vector<std::string>
GetVertexPartitionLabels(duckdb::PartitionCatalogEntry *part_cat) {
    if (!part_cat) {
        return {};
    }
    return NormalizeLabelSet(
        SplitLabelSetString(StripVertexPartitionPrefix(part_cat->GetName())));
}

static bool LabelsExactlyMatchPartition(duckdb::PartitionCatalogEntry *part_cat,
                                        const std::vector<std::string> &labels) {
    return GetVertexPartitionLabels(part_cat) == NormalizeLabelSet(labels);
}

static duckdb::PartitionCatalogEntry *FindExactVertexPartitionByLabels(
    duckdb::ClientContext &context, const std::vector<std::string> &labels) {
    auto normalized = NormalizeLabelSet(labels);
    auto &catalog = context.db->GetCatalog();
    auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
        context, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
        DEFAULT_GRAPH, true);
    if (!gcat) {
        return nullptr;
    }
    for (auto part_oid : *gcat->GetVertexPartitionOids()) {
        auto *part_cat = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
            context, DEFAULT_SCHEMA, part_oid, true);
        if (part_cat && LabelsExactlyMatchPartition(part_cat, normalized)) {
            return part_cat;
        }
    }
    return nullptr;
}

static uint16_t EncodeExtraTypeInfo(const duckdb::LogicalType &type) {
    if (type.id() != duckdb::LogicalTypeId::DECIMAL) {
        return 0;
    }
    uint16_t width_scale = duckdb::DecimalType::GetWidth(type);
    width_scale = (uint16_t)((width_scale << 8) | duckdb::DecimalType::GetScale(type));
    return width_scale;
}

static duckdb::LogicalType ResolvePropertyTypeForValue(
    duckdb::PartitionCatalogEntry *part_cat, const std::string &key,
    const duckdb::Value &value) {
    if (!value.IsNull()) {
        return value.type();
    }
    if (part_cat) {
        auto it = part_cat->global_property_key_names.begin();
        auto found = std::find(it, part_cat->global_property_key_names.end(), key);
        if (found != part_cat->global_property_key_names.end()) {
            idx_t idx = std::distance(part_cat->global_property_key_names.begin(), found);
            if (idx < part_cat->global_property_typesid.size()) {
                return duckdb::LogicalType(part_cat->global_property_typesid[idx]);
            }
        }
    }
    return duckdb::LogicalType::ANY;
}

static void WidenGraphPropertyTypeIfNeeded(duckdb::GraphCatalogEntry *gcat,
                                           duckdb::PropertyKeyID key_id,
                                           const duckdb::LogicalType &type) {
    if (!gcat) {
        return;
    }
    auto target_type = (idx_t)type.id();
    auto it = gcat->propertykey_to_typeid_map.find(key_id);
    if (it == gcat->propertykey_to_typeid_map.end()) {
        gcat->propertykey_to_typeid_map[key_id] = target_type;
        return;
    }
    if (it->second != target_type && it->second != (idx_t)duckdb::LogicalTypeId::ANY) {
        it->second = (idx_t)duckdb::LogicalTypeId::ANY;
    }
}

static void EnsurePartitionGlobalProperty(duckdb::PartitionCatalogEntry *part_cat,
                                          const std::string &key,
                                          duckdb::PropertyKeyID key_id,
                                          const duckdb::LogicalType &type) {
    if (!part_cat) {
        return;
    }
    auto existing = part_cat->global_property_key_to_location.find(key_id);
    if (existing == part_cat->global_property_key_to_location.end()) {
        idx_t next_idx = part_cat->global_property_key_names.size();
        part_cat->global_property_key_to_location[key_id] = next_idx;
        part_cat->global_property_key_names.push_back(key);
        part_cat->global_property_key_ids.push_back(key_id);
        part_cat->global_property_typesid.push_back(type.id());
        part_cat->extra_typeinfo_vec.push_back(EncodeExtraTypeInfo(type));
        part_cat->min_max_array.resize(part_cat->global_property_typesid.size());
        part_cat->welford_array.resize(part_cat->global_property_typesid.size());
        part_cat->num_columns++;
        return;
    }
    auto idx = existing->second;
    if (idx < part_cat->global_property_typesid.size() &&
        part_cat->global_property_typesid[idx] != type.id() &&
        part_cat->global_property_typesid[idx] != duckdb::LogicalTypeId::ANY) {
        part_cat->global_property_typesid[idx] = duckdb::LogicalTypeId::ANY;
        if (idx < part_cat->extra_typeinfo_vec.size()) {
            part_cat->extra_typeinfo_vec[idx] = 0;
        }
    }
}

static duckdb::PropertySchemaCatalogEntry *FindExactNodePropertySchema(
    duckdb::ClientContext &context, duckdb::PartitionCatalogEntry *part_cat,
    const std::vector<std::string> &keys) {
    if (!part_cat) {
        return nullptr;
    }
    auto &catalog = context.db->GetCatalog();
    auto *ps_ids = part_cat->GetPropertySchemaIDs();
    if (!ps_ids) {
        return nullptr;
    }
    for (auto ps_oid : *ps_ids) {
        auto *ps = (duckdb::PropertySchemaCatalogEntry *)catalog.GetEntry(
            context, DEFAULT_SCHEMA, ps_oid, true);
        if (!ps) {
            continue;
        }
        auto *ps_keys = ps->GetKeys();
        if (ps_keys && *ps_keys == keys) {
            return ps;
        }
    }
    return nullptr;
}

static void CopyEdgeConnectionsForPartition(
    duckdb::ClientContext &context, duckdb::GraphCatalogEntry *gcat,
    duckdb::PartitionCatalogEntry *source_part,
    duckdb::PartitionCatalogEntry *target_part) {
    if (!gcat || !source_part || !target_part) {
        return;
    }
    auto &catalog = context.db->GetCatalog();
    for (auto ep_oid : *gcat->GetEdgePartitionOids()) {
        auto *edge_part = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
            context, DEFAULT_SCHEMA, ep_oid, true);
        if (!edge_part) {
            continue;
        }
        if (edge_part->GetSrcPartOid() == source_part->GetOid() ||
            edge_part->GetDstPartOid() == source_part->GetOid()) {
            gcat->AddEdgeConnectionInfo(context, target_part->GetOid(),
                                        edge_part->GetOid());
        }
    }
}

// Seed partition-level histogram/min-max/Welford arrays on a freshly created
// label-evolved partition. Without this, ORCA sees num_rows == 0 and NDV == 0
// for the new relation, marks is_dummy_stats=true, and collapses plan subtrees
// while leaving dangling scalar references behind — manifesting as SEGVs deep
// in the executor. Copying from the source partition is an overestimate but
// keeps the optimizer coherent until the real histogram is rebuilt.
static void CopyPartitionStatsFromSource(
    duckdb::PartitionCatalogEntry *target_part,
    duckdb::PartitionCatalogEntry *source_part) {
    if (!target_part || !source_part) {
        return;
    }
    target_part->offset_infos = source_part->offset_infos;
    target_part->boundary_values = source_part->boundary_values;
    target_part->num_groups_for_each_column =
        source_part->num_groups_for_each_column;
    target_part->multipliers_for_each_column =
        source_part->multipliers_for_each_column;
    target_part->group_info_for_each_table =
        source_part->group_info_for_each_table;

    // SetSchema already sized min_max_array / welford_array to num columns.
    // Copy when sizes agree; otherwise leave the zero-initialized defaults.
    if (source_part->min_max_array.size() == target_part->min_max_array.size()) {
        target_part->min_max_array = source_part->min_max_array;
    }
    if (source_part->welford_array.size() == target_part->welford_array.size()) {
        target_part->welford_array = source_part->welford_array;
    }
}

// Copy NDVs and seed a nonzero approximate row count on the new PropertySchema
// so ORCA's RetrieveRelStats sees num_rows > 0 (preventing relation_empty=true)
// and GetNDV returns real values instead of the is_dummy_stats fallback.
static void SeedPropertySchemaStatsFromSource(
    duckdb::ClientContext &context,
    duckdb::PartitionCatalogEntry *source_part,
    duckdb::PropertySchemaCatalogEntry *target_ps) {
    if (!source_part || !target_ps) {
        return;
    }
    auto &catalog = context.db->GetCatalog();
    auto *source_ps_oids = source_part->GetPropertySchemaIDs();
    duckdb::PropertySchemaCatalogEntry *representative = nullptr;
    if (source_ps_oids) {
        for (auto ps_oid : *source_ps_oids) {
            auto *ps = (duckdb::PropertySchemaCatalogEntry *)catalog.GetEntry(
                context, DEFAULT_SCHEMA, ps_oid, true);
            if (!ps) {
                continue;
            }
            auto *ndvs = ps->GetNDVs();
            if (ndvs && !ndvs->empty()) {
                representative = ps;
                break;
            }
            if (!representative) {
                representative = ps;
            }
        }
    }
    if (representative) {
        auto *src_ndvs = representative->GetNDVs();
        auto *dst_ndvs = target_ps->GetNDVs();
        if (src_ndvs && dst_ndvs) {
            *dst_ndvs = *src_ndvs;
        }
        target_ps->offset_infos = representative->offset_infos;
        target_ps->frequency_values = representative->frequency_values;
    }
    // Ensure GetNumberOfRowsApproximately() returns > 0 even before the first
    // extent is allocated, so ORCA treats the new partition as non-empty.
    target_ps->SetNumberOfLastExtentNumTuples(1);
}

static duckdb::PartitionCatalogEntry *EnsureVertexPartitionForLabels(
    ConnectionHandle *h, const std::vector<std::string> &labels,
    duckdb::PartitionCatalogEntry *source_part) {
    auto normalized = NormalizeLabelSet(labels);
    if (auto *existing = FindExactVertexPartitionByLabels(*h->client, normalized)) {
        return existing;
    }

    if (!source_part) {
        throw std::runtime_error("Cannot create label-set partition without a source partition");
    }

    auto &catalog = h->database->instance->GetCatalog();
    auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
        *h->client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
        DEFAULT_GRAPH, true);
    if (!gcat) {
        throw std::runtime_error("Graph catalog not found");
    }

    auto labelset_name = JoinLabelSet(normalized);
    auto partition_name = std::string(DEFAULT_VERTEX_PARTITION_PREFIX) + labelset_name;
    auto property_schema_name =
        std::string(DEFAULT_VERTEX_PROPERTYSCHEMA_PREFIX) + labelset_name;

    duckdb::CreatePartitionInfo partition_info(DEFAULT_SCHEMA, partition_name.c_str());
    auto *part_cat = (duckdb::PartitionCatalogEntry *)catalog.CreatePartition(
        *h->client, &partition_info);
    auto new_pid = gcat->GetNewPartitionID();
    gcat->AddVertexPartition(*h->client, new_pid, part_cat->GetOid(),
                             const_cast<std::vector<std::string>&>(normalized));

    duckdb::CreatePropertySchemaInfo propertyschema_info(
        DEFAULT_SCHEMA, property_schema_name.c_str(), new_pid, part_cat->GetOid());
    auto *property_schema_cat =
        (duckdb::PropertySchemaCatalogEntry *)catalog.CreatePropertySchema(
            *h->client, &propertyschema_info);
    duckdb::CreateIndexInfo idx_info(
        DEFAULT_SCHEMA, labelset_name + "_id", duckdb::IndexType::PHYSICAL_ID,
        part_cat->GetOid(), property_schema_cat->GetOid(), 0, {-1});
    auto *index_cat = (duckdb::IndexCatalogEntry *)catalog.CreateIndex(
        *h->client, &idx_info);

    auto key_names = source_part->global_property_key_names;
    auto types = source_part->GetTypes();
    auto key_ids = source_part->global_property_key_ids;
    part_cat->AddPropertySchema(*h->client, property_schema_cat->GetOid(), key_ids);
    part_cat->SetSchema(*h->client, key_names, types, key_ids);
    auto key_column_idxs = source_part->id_key_column_idxs;
    part_cat->SetIdKeyColumnIdxs(key_column_idxs);
    part_cat->SetPhysicalIDIndex(index_cat->GetOid());
    part_cat->SetPartitionID(new_pid);

    property_schema_cat->SetSchema(*h->client, key_names, types, key_ids);
    property_schema_cat->SetPhysicalIDIndex(index_cat->GetOid());
    CopyEdgeConnectionsForPartition(*h->client, gcat, source_part, part_cat);
    CopyPartitionStatsFromSource(part_cat, source_part);
    SeedPropertySchemaStatsFromSource(*h->client, source_part, property_schema_cat);
    return part_cat;
}

static duckdb::PropertySchemaCatalogEntry *EnsureExactNodePropertySchema(
    ConnectionHandle *h, duckdb::PartitionCatalogEntry *part_cat,
    const std::vector<std::string> &keys, const std::vector<duckdb::Value> &values) {
    if (!part_cat) {
        return nullptr;
    }
    if (auto *existing = FindExactNodePropertySchema(*h->client, part_cat, keys)) {
        return existing;
    }

    auto &catalog = h->database->instance->GetCatalog();
    auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
        *h->client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
        DEFAULT_GRAPH, true);
    if (!gcat) {
        return nullptr;
    }

    auto mutable_keys = keys;
    std::vector<duckdb::LogicalType> types;
    types.reserve(keys.size());
    for (idx_t i = 0; i < keys.size(); i++) {
        auto value = i < values.size() ? values[i] : duckdb::Value();
        types.push_back(ResolvePropertyTypeForValue(part_cat, keys[i], value));
    }

    std::vector<duckdb::PropertyKeyID> key_ids;
    gcat->GetPropertyKeyIDs(*h->client, mutable_keys, types, key_ids);
    for (idx_t i = 0; i < key_ids.size(); i++) {
        WidenGraphPropertyTypeIfNeeded(gcat, key_ids[i], types[i]);
        EnsurePartitionGlobalProperty(part_cat, mutable_keys[i], key_ids[i], types[i]);
    }

    auto schema_name = part_cat->GetName() + DEFAULT_TEMPORAL_INFIX +
                       std::to_string(part_cat->GetNewTemporalID());
    duckdb::CreatePropertySchemaInfo propertyschema_info(
        DEFAULT_SCHEMA, schema_name.c_str(), part_cat->GetPartitionID(),
        part_cat->GetOid());
    auto *property_schema_cat =
        (duckdb::PropertySchemaCatalogEntry *)catalog.CreatePropertySchema(
            *h->client, &propertyschema_info);
    duckdb::CreateIndexInfo idx_info(
        DEFAULT_SCHEMA, schema_name + "_id", duckdb::IndexType::PHYSICAL_ID,
        part_cat->GetOid(), property_schema_cat->GetOid(), 0, {-1});
    auto *index_cat = (duckdb::IndexCatalogEntry *)catalog.CreateIndex(
        *h->client, &idx_info);
    part_cat->AddPropertySchema(*h->client, property_schema_cat->GetOid(), key_ids);
    property_schema_cat->SetSchema(*h->client, mutable_keys, types, key_ids);
    property_schema_cat->SetPhysicalIDIndex(index_cat->GetOid());
    SeedPropertySchemaStatsFromSource(*h->client, part_cat, property_schema_cat);
    return property_schema_cat;
}

static duckdb::PropertySchemaCatalogEntry *FindInsertPropertySchema(
    duckdb::ClientContext &context, duckdb::PartitionCatalogEntry *part_cat,
    const std::vector<std::string> &required_keys) {
    if (!part_cat) {
        return nullptr;
    }

    auto &catalog = context.db->GetCatalog();
    auto *ps_ids = part_cat->GetPropertySchemaIDs();
    duckdb::PropertySchemaCatalogEntry *fallback = nullptr;
    duckdb::PropertySchemaCatalogEntry *best_match = nullptr;
    idx_t best_match_width = 0;
    if (!ps_ids) {
        return nullptr;
    }

    for (auto ps_oid : *ps_ids) {
        auto *ps = (duckdb::PropertySchemaCatalogEntry *)catalog.GetEntry(
            context, DEFAULT_SCHEMA, ps_oid, true);
        if (!ps) {
            continue;
        }
        if (!fallback || (!fallback->is_fake && fallback->GetName().find(DEFAULT_TEMPORAL_INFIX) == std::string::npos &&
                          (ps->is_fake || ps->GetName().find(DEFAULT_TEMPORAL_INFIX) != std::string::npos))) {
            fallback = ps;
        }
        auto *keys = ps->GetKeys();
        if (!keys) {
            continue;
        }
        bool has_all_keys = true;
        for (auto &required_key : required_keys) {
            if (std::find(keys->begin(), keys->end(), required_key) ==
                keys->end()) {
                has_all_keys = false;
                break;
            }
        }
        if (has_all_keys) {
            bool is_temporal = ps->is_fake ||
                               ps->GetName().find(DEFAULT_TEMPORAL_INFIX) != std::string::npos;
            if (!best_match || keys->size() > best_match_width ||
                (keys->size() == best_match_width &&
                 best_match &&
                 (best_match->is_fake ||
                  best_match->GetName().find(DEFAULT_TEMPORAL_INFIX) != std::string::npos) &&
                 !is_temporal)) {
                best_match = ps;
                best_match_width = keys->size();
            }
        }
    }

    if (best_match) {
        return best_match;
    }
    return fallback;
}

static bool BuildEdgeDeltaRow(
    ConnectionHandle *h, duckdb::PartitionCatalogEntry *edge_part_cat,
    uint64_t src_logical_id, uint64_t dst_logical_id,
    const std::vector<std::pair<std::string, duckdb::Value>> &properties,
    std::vector<std::string> &keys, std::vector<duckdb::Value> &values) {
    std::vector<std::string> required_keys = {"_sid", "_tid"};
    required_keys.reserve(properties.size() + 2);
    for (auto &[key, _] : properties) {
        required_keys.push_back(key);
    }

    auto *ps = FindInsertPropertySchema(*h->client, edge_part_cat, required_keys);
    if (!ps) {
        return false;
    }

    auto *ps_keys = ps->GetKeys();
    if (!ps_keys) {
        return false;
    }

    keys = *ps_keys;
    values.assign(keys.size(), duckdb::Value());
    for (idx_t i = 0; i < keys.size(); i++) {
        if (keys[i] == "_sid") {
            values[i] = duckdb::Value::UBIGINT(src_logical_id);
            continue;
        }
        if (keys[i] == "_tid") {
            values[i] = duckdb::Value::UBIGINT(dst_logical_id);
            continue;
        }

        auto prop_it = std::find_if(
            properties.begin(), properties.end(),
            [&](const auto &entry) { return entry.first == keys[i]; });
        if (prop_it != properties.end()) {
            values[i] = prop_it->second;
        }
    }

    for (auto &[key, _] : properties) {
        if (std::find(keys.begin(), keys.end(), key) == keys.end()) {
            return false;
        }
    }

    return true;
}

static uint64_t AppendDeltaRow(ConnectionHandle *h, uint16_t logical_pid,
                               vector<string> keys,
                               vector<duckdb::Value> values,
                               uint64_t logical_id) {
    auto &ds = h->database->instance->delta_store;
    auto inmem_eid = ds.GetOrAllocateInMemoryExtentID(logical_pid, keys);
    ds.AppendInsertRow(inmem_eid, std::move(keys), std::move(values), logical_id);
    return logical_id;
}

static uint64_t AppendNodeDeltaRow(ConnectionHandle *h, uint16_t logical_pid,
                                   vector<string> keys, vector<duckdb::Value> values,
                                   uint64_t logical_id = 0) {
    auto &ds = h->database->instance->delta_store;
    if (logical_id == 0) {
        logical_id = ds.AllocateNodeLogicalId();
    }
    return AppendDeltaRow(h, logical_pid, std::move(keys), std::move(values),
                          logical_id);
}

static bool ExtentHasAdjacencyChunk(ConnectionHandle *h, ExtentID extent_id, idx_t adj_col) {
    auto &catalog = h->database->instance->GetCatalog();
    auto *ext_cat = (duckdb::ExtentCatalogEntry *)catalog.GetEntry(
        *h->client, duckdb::CatalogType::EXTENT_ENTRY, DEFAULT_SCHEMA,
        DEFAULT_EXTENT_PREFIX + std::to_string(extent_id), true);
    return ext_cat && adj_col < ext_cat->adjlist_chunks.size();
}

static void ForEachIncidentBaseEdge(
    ConnectionHandle *h, uint64_t logical_id,
    const std::function<void(uint16_t, uint64_t, uint64_t)> &fn) {
    auto &delta_store = h->database->instance->delta_store;
    if (delta_store.IsLogicalIdDeleted(logical_id)) {
        return;
    }
    uint64_t adjacency_pid = delta_store.ResolveAdjacencyPid(logical_id);
    uint32_t extent_id = (uint32_t)(adjacency_pid >> 32);
    if (duckdb::IsInMemoryExtent(extent_id)) {
        return;
    }

    auto &catalog = h->database->instance->GetCatalog();
    uint16_t part_id = (uint16_t)(extent_id >> 16);
    auto *gcat = (duckdb::GraphCatalogEntry *)catalog.GetEntry(
        *h->client, duckdb::CatalogType::GRAPH_ENTRY, DEFAULT_SCHEMA,
        DEFAULT_GRAPH, true);
    if (!gcat) {
        return;
    }

    for (auto ep_oid : *gcat->GetEdgePartitionOids()) {
        auto *ep = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
            *h->client, DEFAULT_SCHEMA, ep_oid, true);
        if (!ep) {
            continue;
        }
        auto *src_part = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
            *h->client, DEFAULT_SCHEMA, ep->GetSrcPartOid(), true);
        auto *dst_part = (duckdb::PartitionCatalogEntry *)catalog.GetEntry(
            *h->client, DEFAULT_SCHEMA, ep->GetDstPartOid(), true);
        auto *idx_ids = ep->GetAdjIndexOidVec();
        if (!idx_ids) {
            continue;
        }

        for (auto idx_oid : *idx_ids) {
            auto *idx_cat = (duckdb::IndexCatalogEntry *)catalog.GetEntry(
                *h->client, DEFAULT_SCHEMA, idx_oid, true);
            if (!idx_cat) {
                continue;
            }
            bool is_fwd = idx_cat->GetIndexType() == duckdb::IndexType::FORWARD_CSR;
            bool is_bwd = idx_cat->GetIndexType() == duckdb::IndexType::BACKWARD_CSR;
            if (!is_fwd && !is_bwd) {
                continue;
            }
            if (is_fwd && !(src_part && src_part->GetPartitionID() == part_id)) {
                continue;
            }
            if (is_bwd && !(dst_part && dst_part->GetPartitionID() == part_id)) {
                continue;
            }
            if (!ExtentHasAdjacencyChunk(h, extent_id, idx_cat->GetAdjColIdx())) {
                continue;
            }

            duckdb::AdjacencyListIterator iter;
            iter.Initialize(*h->client, idx_cat->GetAdjColIdx(), extent_id, is_fwd);
            uint64_t *start = nullptr;
            uint64_t *end = nullptr;
            iter.getAdjListPtr(adjacency_pid, extent_id, &start, &end, true);
            for (uint64_t *p = start; p && p < end; p += 2) {
                fn(ep->GetPartitionID(), p[0], p[1]);
            }
        }
    }
}

static void ForEachLiveIncidentDeltaEdge(
    ConnectionHandle *h, uint64_t logical_id,
    const std::function<void(uint16_t, uint64_t, uint64_t)> &fn) {
    auto &delta_store = h->database->instance->delta_store;
    for (auto &[part_id, adj_delta] : delta_store.adj_deltas_exposed()) {
        auto *inserted = adj_delta.GetInserted(logical_id);
        if (!inserted) {
            continue;
        }
        for (auto &entry : *inserted) {
            if (adj_delta.IsEdgeDeleted(logical_id, entry.edge_id)) {
                continue;
            }
            fn((uint16_t)part_id, entry.dst_vid, entry.edge_id);
        }
    }
}

static void LogAndApplyDeleteEdgeBidirectional(duckdb::WALWriter *wal,
                                               duckdb::DeltaStore &delta_store,
                                               uint16_t edge_partition_id,
                                               uint64_t src_vid,
                                               uint64_t dst_vid,
                                               uint64_t edge_id) {
    if (wal) {
        wal->LogDeleteEdge(edge_partition_id, src_vid, edge_id);
        wal->LogDeleteEdge(edge_partition_id, dst_vid, edge_id);
    }
    delta_store.GetAdjListDelta(edge_partition_id).DeleteEdge(src_vid, edge_id);
    delta_store.GetAdjListDelta(edge_partition_id).DeleteEdge(dst_vid, edge_id);
}

static bool ApplyPendingSetMutations(
    ConnectionHandle *h,
    const std::vector<std::shared_ptr<duckdb::DataChunk>> &query_results) {
    if (h->pending_set_items.empty()) {
        return false;
    }

    auto &delta_store = h->database->instance->delta_store;
    auto *wal = h->database->instance->wal_writer.get();
    auto property_items = FilterPropertySetItems(h->pending_set_items);
    bool catalog_changed = false;
    for (auto &chunk : query_results) {
        if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
            continue;
        }
        idx_t vid_col = FindIdColumn(*chunk);
        if (vid_col == duckdb::DConstants::INVALID_INDEX) {
            continue;
        }
        auto *vid_data = (uint64_t *)chunk->data[vid_col].GetData();
        for (idx_t row = 0; row < chunk->size(); row++) {
            uint64_t logical_id = vid_data[row];
            vector<string> keys;
            vector<duckdb::Value> values;
            if (!SnapshotCurrentNodeRecord(h, *chunk, row, logical_id, keys, values)) {
                continue;
            }
            auto current_pid = delta_store.ResolvePid(logical_id);
            bool has_current_pid = current_pid != 0 ||
                                   (logical_id == 0 &&
                                    !delta_store.IsLogicalIdDeleted(logical_id));
            if (!has_current_pid) {
                continue;
            }
            uint16_t logical_pid = (uint16_t)(((uint32_t)(current_pid >> 32)) >> 16);
            auto *current_part = FindPartitionCatalogByLogicalId(*h->client, logical_pid);
            auto current_labels = GetVertexPartitionLabels(current_part);
            ApplySetItemsToSnapshot(keys, values, property_items);
            auto target_labels =
                ApplyLabelSetItemsToLabels(current_labels, h->pending_set_items);
            auto *target_part = current_part;
            if (target_labels != current_labels) {
                target_part = EnsureVertexPartitionForLabels(h, target_labels,
                                                            current_part);
                catalog_changed = true;
            }
            if (!target_part) {
                continue;
            }
            auto *existing_target_ps =
                FindExactNodePropertySchema(*h->client, target_part, keys);
            auto *target_ps =
                EnsureExactNodePropertySchema(h, target_part, keys, values);
            if (target_ps &&
                (!existing_target_ps ||
                 existing_target_ps->GetOid() != target_ps->GetOid())) {
                catalog_changed = true;
            }
            uint16_t target_logical_pid = target_part->GetPartitionID();
            if (wal) {
                wal->LogUpdateNodeV2(target_logical_pid, logical_id, keys, values);
            }
            delta_store.PreserveAdjacencyPidOnUpdate(logical_id);
            InvalidateCurrentNodeVersion(delta_store, logical_id);
            AppendNodeDeltaRow(h, target_logical_pid, std::move(keys), std::move(values),
                               logical_id);
        }
    }

    h->pending_set_items.clear();
    if (catalog_changed) {
        h->database->instance->GetCatalog().SaveCatalog();
        RefreshCatalogAndPlanner(*h);
    }
    return true;
}

static bool ApplyPendingDeleteMutations(
    ConnectionHandle *h,
    const std::vector<std::shared_ptr<duckdb::DataChunk>> &query_results) {
    if (!h->pending_delete) {
        return false;
    }

    auto &delta_store = h->database->instance->delta_store;
    auto *wal = h->database->instance->wal_writer.get();
    bool detach_delete = h->pending_detach_delete;
    for (auto &chunk : query_results) {
        if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
            continue;
        }
        idx_t vid_col = FindIdColumn(*chunk);
        if (vid_col == duckdb::DConstants::INVALID_INDEX) {
            continue;
        }
        auto *vid_data = (uint64_t *)chunk->data[vid_col].GetData();
        for (idx_t row = 0; row < chunk->size(); row++) {
            uint64_t logical_id = vid_data[row];
            auto current_pid = delta_store.ResolvePid(logical_id);
            bool has_current_pid = current_pid != 0 ||
                                   (logical_id == 0 &&
                                    !delta_store.IsLogicalIdDeleted(logical_id));
            if (!has_current_pid) {
                continue;
            }
            uint32_t extent_id = (uint32_t)(current_pid >> 32);
            uint32_t row_offset = (uint32_t)(current_pid & 0xFFFFFFFFull);
            std::unordered_set<uint64_t> deleted_edge_ids;
            auto cascade_delete_edge =
                [&](uint16_t edge_partition_id, uint64_t neighbor_vid,
                    uint64_t edge_id) {
                    if (!deleted_edge_ids.insert(edge_id).second) {
                        return;
                    }
                    LogAndApplyDeleteEdgeBidirectional(
                        wal, delta_store, edge_partition_id, logical_id,
                        neighbor_vid, edge_id);
                };

            ForEachIncidentBaseEdge(
                h, logical_id,
                [&](uint16_t edge_partition_id, uint64_t neighbor_vid,
                    uint64_t edge_id) {
                    cascade_delete_edge(edge_partition_id, neighbor_vid, edge_id);
                });
            ForEachLiveIncidentDeltaEdge(
                h, logical_id,
                [&](uint16_t edge_partition_id, uint64_t neighbor_vid,
                    uint64_t edge_id) {
                    cascade_delete_edge(edge_partition_id, neighbor_vid, edge_id);
                });

            if (wal) {
                wal->LogDeleteNodeV2(logical_id);
            }
            InvalidateCurrentNodeVersion(delta_store, logical_id);
            delta_store.InvalidateLogicalId(logical_id);
            spdlog::info("[{}] vid=0x{:016X} extent=0x{:08X} offset={}",
                         detach_delete ? "DETACH DELETE" : "DELETE",
                         logical_id, extent_id, row_offset);
        }
    }

    h->pending_delete = false;
    h->pending_detach_delete = false;
    return true;
}

int64_t turbolynx_connect(const char *dbname) {
    try {
        auto h = std::make_unique<ConnectionHandle>();
        bool was_null = (DiskAioFactory::GetPtr() == NULL);
        h->disk_aio_factory = duckdb::InitializeDiskAio(dbname);
        h->owns_disk_aio = was_null;
        h->database    = std::make_unique<DuckDB>(dbname);
        ChunkCacheManager::ccm = new ChunkCacheManager(dbname);
        h->client      = std::make_shared<duckdb::ClientContext>(h->database->instance->shared_from_this());
        h->owns_database = true;
        duckdb::SetClientWrapper(h->client, make_shared<CatalogWrapper>(*h->database->instance));
        duckdb::LoadLogicalMappings(string(dbname), h->database->instance->delta_store);
        // WAL: replay existing log to restore DeltaStore, then open writer for new mutations
        duckdb::WALReader::Replay(string(dbname), h->database->instance->delta_store);
        h->database->instance->wal_writer = std::make_unique<duckdb::WALWriter>(string(dbname));
        initialize_planner(*h);

        int64_t id = g_next_conn_id++;
        {
            std::lock_guard<std::mutex> lk(g_conn_lock);
            g_connections[id] = std::move(h);
        }
        g_connections[id]->registered_connection_id =
            g_connections[id]->database->instance->connection_manager.Register(
                g_connections[id]->client);
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
    h->client       = *reinterpret_cast<std::shared_ptr<duckdb::ClientContext>*>(client_context);
    h->owns_database = false;
    duckdb::SetClientWrapper(h->client, make_shared<CatalogWrapper>(*h->client->db));
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
    duckdb::PersistLogicalMappings(DiskAioParameters::WORKSPACE,
                                   it->second->database->instance->delta_store);
    if (it->second->database->instance->wal_writer)
        it->second->database->instance->wal_writer->Truncate();
}

void turbolynx_checkpoint_ctx(duckdb::ClientContext &context) {
    auto &ds = context.db->delta_store;
    auto &catalog = context.db->GetCatalog();

    idx_t flushed_rows = 0;
    struct PendingCheckpointFlush {
        duckdb::PropertySchemaCatalogEntry *property_schema = nullptr;
        duckdb::ExtentID extent_id = 0;
        idx_t row_count = 0;
        std::vector<std::pair<uint64_t, uint64_t>> logical_mappings;
    };
    std::vector<PendingCheckpointFlush> pending_flushes;

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
            vector<idx_t> live_rows;
            live_rows.reserve(buf->LiveSize());
            for (idx_t row_idx = 0; row_idx < buf->Size(); row_idx++) {
                if (buf->IsValid(row_idx)) {
                    live_rows.push_back(row_idx);
                }
            }
            if (live_rows.empty()) continue;

            // Find the partition catalog entry.
            duckdb::PartitionCatalogEntry *part_cat =
                FindPartitionCatalogByLogicalId(context, partition_id);
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

            idx_t row_count = live_rows.size();
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
                auto src_row_idx = live_rows[r];
                for (idx_t c = 0; c < col_types.size(); c++) {
                    if (ps_keys && c < ps_keys->size()) {
                        int bi = buf->FindKeyIndex((*ps_keys)[c]);
                        if (bi >= 0 && (idx_t)bi < rows[src_row_idx].size()) {
                            try { chunk.SetValue(c, r, rows[src_row_idx][bi]); }
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
            } catch (const std::exception &e) {
                throw std::runtime_error(
                    "checkpoint flush failed for extent 0x" +
                    std::to_string(new_eid) + ": " + e.what());
            } catch (...) {
                throw std::runtime_error(
                    "checkpoint flush failed for extent 0x" +
                    std::to_string(new_eid));
            }
            // Flush newly-created segments to store.db
            for (auto &[cdf, handler] : ChunkCacheManager::ccm->file_handlers) {
                if ((cdf >> 32) == (duckdb::ChunkDefinitionID)new_eid && handler) {
                    ChunkCacheManager::ccm->UnswizzleFlushSwizzle(cdf, handler, false);
                }
            }

            PendingCheckpointFlush flush_info;
            flush_info.property_schema = target_ps;
            flush_info.extent_id = new_eid;
            flush_info.row_count = row_count;
            flush_info.logical_mappings.reserve(row_count);
            for (idx_t r = 0; r < row_count; r++) {
                auto logical_id = buf->GetLogicalId(live_rows[r]);
                if (logical_id == 0) {
                    continue;
                }
                flush_info.logical_mappings.emplace_back(
                    logical_id, duckdb::MakePhysicalId(new_eid, (uint32_t)r));
            }
            pending_flushes.push_back(std::move(flush_info));
        }
    }

    for (auto &flush_info : pending_flushes) {
        D_ASSERT(flush_info.property_schema != nullptr);
        flush_info.property_schema->AddExtent(flush_info.extent_id,
                                              flush_info.row_count);
        for (auto &[logical_id, pid] : flush_info.logical_mappings) {
            ds.UpsertLogicalMapping(logical_id, pid, false);
        }
        flushed_rows += flush_info.row_count;
    }

    // ── Phase 2: Save catalog + logical ID mappings — POINT OF NO RETURN ──
    catalog.SaveCatalog();
    duckdb::PersistLogicalMappings(DiskAioParameters::WORKSPACE, ds);

    // Write CHECKPOINT_END marker — catalog is committed, INSERTs are on disk
    if (context.db->wal_writer && flushed_rows > 0) {
        context.db->wal_writer->LogCheckpointEnd();
    }

    // ── Phase 3: Flush dirty segments to store.db + persist metadata ──
    ChunkCacheManager::ccm->FlushDirtySegmentsAndDeleteFromcache(false);
    ChunkCacheManager::ccm->FlushMetaInfo(DiskAioParameters::WORKSPACE.c_str());

    // ── Phase 4: Clear flushed INSERT rows, then re-write remaining WAL state ──
    ds.ClearInsertData();

    idx_t delete_mask_entries = 0;
    idx_t inserted_edge_entries = 0;
    idx_t deleted_edge_entries = 0;

    // Truncate WAL, then re-write remaining delete masks + CSR delta state.
    if (context.db->wal_writer) {
        context.db->wal_writer->Truncate();

        auto &wal = *context.db->wal_writer;

        for (auto &[eid, mask] : ds.GetAllDeleteMasks()) {
            for (auto off : mask.GetDeleted()) {
                wal.LogDeleteNode((uint32_t)eid, (uint32_t)off, 0);
                delete_mask_entries++;
            }
        }

        std::unordered_set<uint64_t> seen_edge_ids;
        for (auto &[epid, adj] : ds.adj_deltas_exposed()) {
            for (auto &[src_vid, entries] : adj.GetAllInserted()) {
                for (auto &entry : entries) {
                    if (!seen_edge_ids.insert(entry.edge_id).second) {
                        continue;
                    }
                    wal.LogInsertEdge((uint16_t)epid, src_vid, entry.dst_vid,
                                      entry.edge_id);
                    inserted_edge_entries++;
                }
            }
            for (auto &[src_vid, deleted] : adj.GetAllDeleted()) {
                for (auto edge_id : deleted) {
                    wal.LogDeleteEdge((uint16_t)epid, src_vid, edge_id);
                    deleted_edge_entries++;
                }
            }
        }

        wal.Flush();
    }

    spdlog::info(
        "[CHECKPOINT] Complete: flushed {} rows, rewrote {} row deletes, {} edge inserts, {} edge deletes",
        flushed_rows, delete_mask_entries, inserted_edge_entries,
        deleted_edge_entries);
}

void turbolynx_checkpoint(int64_t conn_id) {
    std::lock_guard<std::mutex> lk(g_conn_lock);
    auto it = g_connections.find(conn_id);
    if (it == g_connections.end()) return;
    try {
        turbolynx_checkpoint_ctx(*it->second->client);
    } catch (const std::exception &e) {
        spdlog::error("[CHECKPOINT] Failed: {}", e.what());
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, e.what());
    } catch (...) {
        spdlog::error("[CHECKPOINT] Failed: unknown exception");
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, "Checkpoint failed");
    }
}

// ---------------------------------------------------------------------------
// C++ accessor functions (for shell integration)
// ---------------------------------------------------------------------------

duckdb::ClientContext* turbolynx_get_client_context(int64_t conn_id) {
    auto* h = get_handle(conn_id);
    return h ? h->client.get() : nullptr;
}

turbolynx::Planner* turbolynx_get_planner(int64_t conn_id) {
    auto* h = get_handle(conn_id);
    return h ? h->planner.get() : nullptr;
}

// turbolynx_execute_raw defined at end of file (after all helpers)

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
        if (h->registered_connection_id >= 0 && h->database &&
            h->database->instance) {
            h->database->instance->connection_manager.Unregister(
                h->registered_connection_id);
            h->registered_connection_id = -1;
        }
        h->planner.reset();
        h->last_bound_mutation.reset();
        duckdb::ReleaseClientWrapper();
        h->client.reset();
        delete ChunkCacheManager::ccm;
        ChunkCacheManager::ccm = nullptr;
        if (h->owns_disk_aio && h->disk_aio_factory) {
            delete h->disk_aio_factory;
            h->disk_aio_factory = nullptr;
        }
    }
    std::cout << "Database Disconnected (conn_id=" << conn_id << ")" << std::endl;
}

int64_t turbolynx_connect_readonly(const char *dbname) {
    try {
        auto h = std::make_unique<ConnectionHandle>();
        bool was_null = (DiskAioFactory::GetPtr() == NULL);
        h->disk_aio_factory = duckdb::InitializeDiskAio(dbname);
        h->owns_disk_aio = was_null;
        h->database    = std::make_unique<DuckDB>(dbname);
        ChunkCacheManager::ccm = new ChunkCacheManager(dbname, /*read_only=*/true);
        h->client      = std::make_shared<duckdb::ClientContext>(h->database->instance->shared_from_this());
        h->owns_database = true;
        duckdb::SetClientWrapper(h->client, make_shared<CatalogWrapper>(*h->database->instance));
        duckdb::LoadLogicalMappings(string(dbname), h->database->instance->delta_store);
        initialize_planner(*h);

        int64_t id = g_next_conn_id++;
        {
            std::lock_guard<std::mutex> lk(g_conn_lock);
            g_connections[id] = std::move(h);
        }
        g_connections[id]->registered_connection_id =
            g_connections[id]->database->instance->connection_manager.Register(
                g_connections[id]->client);
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
    return RewriteClauseByKeyword(query, "remove ", true);
}

static void turbolynx_compile_query(ConnectionHandle* h, string query) {
    query = RewriteSetLabelItems(query);
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
    if (!cypher_ctx || cypherParser.getNumberOfSyntaxErrors() > 0)
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
    bool has_projection = false;
    if (boundQuery->GetNumSingleQueries() == 1) {
        auto* sq = boundQuery->GetSingleQuery(0);
        bool has_reading = false;
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
        // MATCH without RETURN is invalid Cypher
        if (has_reading && !has_projection && !has_updating) {
            throw std::runtime_error("Query must end with a RETURN clause");
        }
    }

    h->is_mutation_query = is_mutation;
    h->has_query_projection = has_projection;
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

static vector<idx_t> BuildVisibleColumnMapping(
    const vector<string> &visible_col_names, duckdb::Schema &actual_schema,
    idx_t actual_col_count) {
    vector<idx_t> mapping;
    if (visible_col_names.empty()) {
        return mapping;
    }

    auto actual_names = actual_schema.getStoredColumnNames();
    vector<bool> used(actual_col_count, false);
    if (actual_names.size() == actual_col_count && !actual_names.empty()) {
        for (auto &visible_name : visible_col_names) {
            idx_t found = DConstants::INVALID_INDEX;
            for (idx_t i = 0; i < actual_col_count; i++) {
                if (!used[i] && actual_names[i] == visible_name) {
                    found = i;
                    used[i] = true;
                    break;
                }
            }
            if (found == DConstants::INVALID_INDEX) {
                mapping.clear();
                break;
            }
            mapping.push_back(found);
        }
        if (mapping.size() == visible_col_names.size()) {
            return mapping;
        }
    }

    idx_t fallback_count =
        std::min<idx_t>((idx_t)visible_col_names.size(), actual_col_count);
    for (idx_t i = 0; i < fallback_count; i++) {
        mapping.push_back(i);
    }
    return mapping;
}

static vector<duckdb::LogicalType> GetActualResultTypes(
    const vector<std::shared_ptr<duckdb::DataChunk>> &chunks,
    duckdb::Schema &actual_schema) {
    idx_t actual_col_count = 0;
    for (auto &chunk : chunks) {
        if (chunk && chunk->ColumnCount() > 0) {
            actual_col_count = chunk->ColumnCount();
            break;
        }
    }

    vector<duckdb::LogicalType> actual_types;
    if (actual_col_count > 0) {
        actual_types.resize(actual_col_count);
        for (idx_t c = 0; c < actual_col_count; c++) {
            actual_types[c] = duckdb::LogicalType::SQLNULL;
        }
        for (auto &chunk : chunks) {
            if (!chunk || chunk->ColumnCount() != actual_col_count) {
                continue;
            }
            bool resolved_all = true;
            for (idx_t c = 0; c < actual_col_count; c++) {
                if (actual_types[c].id() != duckdb::LogicalTypeId::SQLNULL &&
                    actual_types[c].id() != duckdb::LogicalTypeId::INVALID &&
                    actual_types[c].id() != duckdb::LogicalTypeId::UNKNOWN &&
                    actual_types[c].id() != duckdb::LogicalTypeId::ANY) {
                    continue;
                }
                auto type = chunk->data[c].GetType();
                if (chunk->size() > 0 &&
                    (type.id() == duckdb::LogicalTypeId::SQLNULL ||
                     type.id() == duckdb::LogicalTypeId::INVALID ||
                     type.id() == duckdb::LogicalTypeId::UNKNOWN ||
                     type.id() == duckdb::LogicalTypeId::ANY)) {
                    for (idx_t r = 0; r < chunk->size(); r++) {
                        auto value = chunk->GetValue(c, r);
                        if (!value.IsNull()) {
                            type = value.type();
                            break;
                        }
                    }
                }
                if (type.id() == duckdb::LogicalTypeId::SQLNULL ||
                    type.id() == duckdb::LogicalTypeId::INVALID ||
                    type.id() == duckdb::LogicalTypeId::UNKNOWN ||
                    type.id() == duckdb::LogicalTypeId::ANY) {
                    resolved_all = false;
                    continue;
                }
                actual_types[c] = type;
            }
            if (resolved_all) {
                break;
            }
        }
        auto schema_types = actual_schema.getStoredTypes();
        for (idx_t c = 0; c < actual_types.size(); c++) {
            if (actual_types[c].id() != duckdb::LogicalTypeId::SQLNULL &&
                actual_types[c].id() != duckdb::LogicalTypeId::INVALID &&
                actual_types[c].id() != duckdb::LogicalTypeId::UNKNOWN &&
                actual_types[c].id() != duckdb::LogicalTypeId::ANY) {
                continue;
            }
            if (c < schema_types.size() &&
                schema_types[c].id() != duckdb::LogicalTypeId::SQLNULL &&
                schema_types[c].id() != duckdb::LogicalTypeId::INVALID &&
                schema_types[c].id() != duckdb::LogicalTypeId::UNKNOWN &&
                schema_types[c].id() != duckdb::LogicalTypeId::ANY) {
                actual_types[c] = schema_types[c];
            }
        }
    } else {
        actual_types = actual_schema.getStoredTypes();
    }
    return actual_types;
}

static void PopulatePreparedStatementResultMetadata(
    turbolynx_prepared_statement *prepared_statement,
    CypherPreparedStatement *cypher_prep_stmt,
    const vector<string> &visible_col_names, duckdb::Schema &actual_schema,
    const vector<std::shared_ptr<duckdb::DataChunk>> &chunks) {
    auto actual_types = GetActualResultTypes(chunks, actual_schema);
    auto visible_mapping =
        BuildVisibleColumnMapping(visible_col_names, actual_schema,
                                  actual_types.size());
    cypher_prep_stmt->visibleColumnMapping = visible_mapping;

    if (prepared_statement->property) {
        turbolynx_close_property(prepared_statement->property);
        prepared_statement->property = NULL;
    }

    turbolynx_property *property = NULL;
    turbolynx_property *prev = NULL;
    for (idx_t i = 0; i < visible_col_names.size(); i++) {
        idx_t mapped_idx =
            i < visible_mapping.size() ? visible_mapping[i] : (idx_t)i;
        if (mapped_idx >= actual_types.size()) {
            mapped_idx = (idx_t)i;
        }
        auto logical_type =
            mapped_idx < actual_types.size() ? actual_types[mapped_idx]
                                             : duckdb::LogicalType::SQLNULL;
        auto property_turbolynx_type = ConvertCPPTypeToC(logical_type);
        auto property_sql_type = logical_type.ToString();

        turbolynx_property *new_property =
            (turbolynx_property *)malloc(sizeof(turbolynx_property));
        auto property_name = visible_col_names[i];
        new_property->label_name = strdup("");
        new_property->label_type = TURBOLYNX_OTHER;
        new_property->order = i;
        new_property->property_name = strdup(property_name.c_str());
        new_property->property_type = property_turbolynx_type;
        new_property->property_sql_type = strdup(property_sql_type.c_str());
        new_property->precision = 0;
        new_property->scale = 0;
        new_property->next = NULL;

        if (property == NULL) {
            property = new_property;
        } else {
            prev->next = new_property;
        }
        prev = new_property;
    }

    prepared_statement->num_properties = visible_col_names.size();
    prepared_statement->property = property;
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
        auto &sink_schema = executors.back()->pipeline->GetSink()->schema;
		auto col_types = executors.back()->pipeline->GetSink()->GetTypes();
		auto col_oids = h->planner->getQueryOutputOIDs();
        auto cypher_prep_stmt = reinterpret_cast<CypherPreparedStatement *>(
            prepared_statement->__internal_prepared_statement);
        auto visible_mapping =
            BuildVisibleColumnMapping(col_names, sink_schema, col_types.size());
        cypher_prep_stmt->visibleColumnMapping = visible_mapping;
        idx_t visible_count =
            std::min<idx_t>(col_names.size(),
                            std::min<idx_t>(col_types.size(), col_oids.size()));
        if (visible_count == 0) {
            prepared_statement->num_properties = 0;
            prepared_statement->property = NULL;
            prepared_statement->plan = strdup(generatePostgresStylePlan(executors).c_str());
            return;
        }

		turbolynx_property *property = NULL;
		turbolynx_property *prev = NULL;
		
		for (turbolynx_property_order i = 0; i < visible_count; i++) {
			turbolynx_property *new_property = (turbolynx_property*)malloc(sizeof(turbolynx_property));

			auto property_name = col_names[i];
            idx_t mapped_idx =
                i < visible_mapping.size() ? visible_mapping[i] : (idx_t)i;
            if (mapped_idx >= col_types.size()) {
                mapped_idx = (idx_t)i;
            }
			auto property_logical_type = col_types[mapped_idx];
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

		prepared_statement->num_properties = visible_count;
		prepared_statement->property = property;
		prepared_statement->plan = strdup(generatePostgresStylePlan(executors).c_str());
    }
}

static bool EnsureImplicitMutationReadbackProjection(
    ConnectionHandle *h, CypherPreparedStatement *cypher_stmt) {
    if (!h || !cypher_stmt || h->is_mutation_query || h->has_query_projection ||
        h->pending_set_items.empty()) {
        return false;
    }

    vector<string> return_vars;
    for (auto &item : h->pending_set_items) {
        if (item.variable_name.empty()) {
            continue;
        }
        if (std::find(return_vars.begin(), return_vars.end(),
                      item.variable_name) == return_vars.end()) {
            return_vars.push_back(item.variable_name);
        }
    }
    if (return_vars.empty()) {
        return false;
    }

    cypher_stmt->originalQuery += " RETURN ";
    for (idx_t i = 0; i < return_vars.size(); i++) {
        if (i > 0) {
            cypher_stmt->originalQuery += ", ";
        }
        cypher_stmt->originalQuery += return_vars[i];
    }

    turbolynx_compile_query(h, cypher_stmt->getBoundQuery());
    return true;
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

    auto lookup_node_id = [&](const string &var_name, const string &label,
                              const string &props, uint64_t &logical_id) -> bool {
        string match_query = "MATCH (" + var_name + ":" + label + " {" + props +
                             "}) RETURN id(" + var_name + ")";
        auto *prep = turbolynx_prepare(conn_id, const_cast<char *>(match_query.c_str()));
        if (!prep) {
            return false;
        }

        turbolynx_resultset_wrapper *res = nullptr;
        auto exec_result = turbolynx_execute(conn_id, prep, &res);
        bool found = false;
        if (exec_result != TURBOLYNX_ERROR && res &&
            turbolynx_fetch_next(res) != TURBOLYNX_END_OF_RESULT) {
            logical_id = turbolynx_get_id(res, 0);
            logical_id = h->database->instance->delta_store.ResolveLogicalId(logical_id);
            found = true;
        }

        if (res) {
            turbolynx_close_resultset(res);
        }
        turbolynx_close_prepared_statement(prep);
        return found;
    };

    uint64_t logical_id_a = 0;
    uint64_t logical_id_b = 0;
    bool found_a = lookup_node_id(var_a, label_a, props_a, logical_id_a);
    bool found_b = lookup_node_id(var_b, label_b, props_b, logical_id_b);

    // Step 3: Create edge if both nodes found
    if (found_a && found_b) {
        auto &delta_store = h->database->instance->delta_store;
        uint64_t current_pid_a = delta_store.ResolvePid(logical_id_a);
        uint64_t current_pid_b = delta_store.ResolvePid(logical_id_b);
        bool has_current_pid_a =
            current_pid_a != 0 ||
            (logical_id_a == 0 && !delta_store.IsLogicalIdDeleted(logical_id_a));
        bool has_current_pid_b =
            current_pid_b != 0 ||
            (logical_id_b == 0 && !delta_store.IsLogicalIdDeleted(logical_id_b));
        if (!has_current_pid_a || !has_current_pid_b) {
            set_error(TURBOLYNX_ERROR_INVALID_PARAMETER,
                      "MATCH+CREATE edge resolved a deleted node");
            return TURBOLYNX_ERROR;
        }
        auto &catalog = h->database->instance->GetCatalog();
        auto *gcat = turbolynx_get_graph_catalog_entry(h);
        auto normalize_name = [](std::string name) {
            std::transform(name.begin(), name.end(), name.begin(),
                           [](unsigned char ch) { return (char)std::tolower(ch); });
            return name;
        };
        auto wanted = normalize_name(edge_type);
        PartitionCatalogEntry *edge_part = nullptr;
        if (gcat) {
            for (auto ep_oid : *gcat->GetEdgePartitionOids()) {
                auto *candidate = (PartitionCatalogEntry *)catalog.GetEntry(
                    *h->client, DEFAULT_SCHEMA, ep_oid, true);
                if (!candidate) {
                    continue;
                }
                auto part_name = candidate->GetName();
                auto stripped = part_name;
                if (stripped.rfind(DEFAULT_EDGE_PARTITION_PREFIX, 0) == 0) {
                    stripped = stripped.substr(strlen(DEFAULT_EDGE_PARTITION_PREFIX));
                }
                auto base_name = stripped;
                auto at_pos = base_name.find('@');
                if (at_pos != std::string::npos) {
                    base_name = base_name.substr(0, at_pos);
                }
                if (normalize_name(stripped) == wanted ||
                    normalize_name(base_name) == wanted ||
                    normalize_name(short_uri(stripped)) == wanted ||
                    normalize_name(short_uri(part_name)) == wanted) {
                    edge_part = candidate;
                    break;
                }
            }
        }
        if (!edge_part) {
            set_error(TURBOLYNX_ERROR_INVALID_METADATA, "Unknown edge type: " + edge_type);
            return TURBOLYNX_ERROR;
        }

        uint16_t edge_partition_id = edge_part->GetPartitionID();
        uint64_t edge_id = delta_store.AllocateEdgeId(edge_partition_id);
        vector<string> edge_keys;
        vector<duckdb::Value> edge_values;
        if (!BuildEdgeDeltaRow(h, edge_part, logical_id_a, logical_id_b, {},
                               edge_keys, edge_values)) {
            set_error(TURBOLYNX_ERROR_INVALID_METADATA,
                      "Cannot build edge record for type: " + edge_type);
            return TURBOLYNX_ERROR;
        }
        AppendDeltaRow(h, edge_partition_id, std::move(edge_keys),
                       std::move(edge_values), edge_id);
        duckdb::LogAndApplyInsertEdge(h->database->instance->wal_writer.get(),
                                      delta_store, edge_partition_id,
                                      logical_id_a, logical_id_b, edge_id);

        spdlog::info(
            "[MATCH+CREATE EDGE] type={} src_lid=0x{:016X} dst_lid=0x{:016X} "
            "eid=0x{:016X} (current src=0x{:016X}, current dst=0x{:016X})",
            edge_type, logical_id_a, logical_id_b, edge_id, current_pid_a,
            current_pid_b);
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

// Session config statement: PRAGMA threads = N
// Returns N if matched, -1 otherwise.
static int64_t parseSetThreadsStmt(const string &query) {
    std::regex re(R"(^\s*PRAGMA\s+threads\s*=\s*(\d+)\s*;?\s*$)",
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
    if (!query) { set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, INVALID_PARAMETER); return nullptr; }
	try {
		auto prep_stmt = (turbolynx_prepared_statement*)malloc(sizeof(turbolynx_prepared_statement));
		// Own the query text: caller may free/mutate their buffer after prepare.
		char* owned_query = strdup(query);
		auto normalized_query = NormalizeQueryForPrepare(string(owned_query));
		// Session config: PRAGMA threads = N / SET parallel_threads = N
		// Apply immediately, return a no-op prepared statement marker.
		{
			int64_t n = parseSetThreadsStmt(normalized_query);
			if (n >= 0) {
				duckdb::ClientConfig::GetConfig(*h->client).maximum_threads = (idx_t)n;
				prep_stmt->query = owned_query;
				prep_stmt->__internal_prepared_statement = (void*)0x3;  // marker: SET threads (no-op execute)
				prep_stmt->num_properties = 0;
				prep_stmt->property = nullptr;
				prep_stmt->plan = strdup("SET parallel_threads (config)");
				return prep_stmt;
			}
		}
		// Handle MERGE at prepare time — store as special marker
		if (isMergeQuery(normalized_query)) {
			prep_stmt->query = owned_query;
			prep_stmt->__internal_prepared_statement = nullptr;  // marker: MERGE query
			prep_stmt->num_properties = 0;
			prep_stmt->property = nullptr;
			prep_stmt->plan = strdup("MERGE (rewrite)");
			return prep_stmt;
		}
		// Handle UNWIND+CREATE — store as special marker
		if (isUnwindCreate(normalized_query)) {
			prep_stmt->query = owned_query;
			prep_stmt->__internal_prepared_statement = (void*)0x2;  // marker: UNWIND+CREATE
			prep_stmt->num_properties = 0;
			prep_stmt->property = nullptr;
			prep_stmt->plan = strdup("UNWIND+CREATE (rewrite)");
			return prep_stmt;
		}
		// Handle MATCH+CREATE edge — store as special marker (similar to MERGE)
		if (isMatchCreateEdge(normalized_query)) {
			prep_stmt->query = owned_query;
			prep_stmt->__internal_prepared_statement = (void*)0x1;  // marker: MATCH+CREATE edge
			prep_stmt->num_properties = 0;
			prep_stmt->property = nullptr;
			prep_stmt->plan = strdup("MATCH+CREATE edge (rewrite)");
			return prep_stmt;
		}
		bool is_detach = false;
		string rewritten = rewriteDetachDelete(normalized_query, is_detach);
		rewritten = RewriteSetLabelItems(rewritten);
		rewritten = rewriteRemoveToSetNull(rewritten);
		h->pending_detach_delete = is_detach;
		prep_stmt->query = owned_query;
		auto* cypher_stmt = new CypherPreparedStatement(rewritten);
		prep_stmt->__internal_prepared_statement = reinterpret_cast<void*>(cypher_stmt);
		if (cypher_stmt->getNumParams() > 0) {
			// Parameterized query — defer compilation to execute time
			// (binder can't resolve $param placeholders without bound values)
			prep_stmt->num_properties = 0;
			prep_stmt->property = nullptr;
			prep_stmt->plan = strdup("PREPARED (parameterized)");
		} else {
			turbolynx_compile_query(h, rewritten);
            EnsureImplicitMutationReadbackProjection(h, cypher_stmt);
			if (h->is_mutation_query || !h->has_query_projection) {
				// Mutation queries without RETURN have no output columns
				prep_stmt->num_properties = 0;
				prep_stmt->property = nullptr;
				prep_stmt->plan = strdup("MUTATION (no return)");
			} else {
				turbolynx_extract_query_metadata(h, prep_stmt);
			}
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
	free(prepared_statement->query);
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
    auto &visible_mapping = cypher_prep_stmt->visibleColumnMapping;

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
                idx_t physical_col_idx =
                    i < visible_mapping.size() ? visible_mapping[i] : (idx_t)i;
                if (physical_col_idx >= data_chunk->ColumnCount()) {
                    physical_col_idx = (idx_t)i;
                }
					turbolynx_result *new_result = (turbolynx_result*)malloc(sizeof(turbolynx_result));
					new_result->data_type = property->property_type;
					new_result->data_sql_type = property->property_sql_type;
					new_result->num_rows = data_chunk->size();
					new_result->__internal_data = (void*)(&data_chunk->data[physical_col_idx]);
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

void turbolynx_set_auto_compact_threshold(size_t row_threshold, size_t extent_threshold) {
    g_auto_compact_row_threshold = static_cast<idx_t>(row_threshold);
    g_auto_compact_extent_threshold = static_cast<idx_t>(extent_threshold);
}

void turbolynx_set_max_threads(int64_t conn_id, size_t max_threads) {
    auto h = get_handle(conn_id);
    if (h == NULL || h->client == NULL) return;
    duckdb::ClientConfig::GetConfig(*h->client).maximum_threads = (idx_t)max_threads;
}

void turbolynx_interrupt(int64_t conn_id) {
    auto* h = get_handle(conn_id);
    if (!h || !h->client) return;
    h->client->Interrupt();
}

int64_t turbolynx_query_progress(int64_t conn_id) {
    auto* h = get_handle(conn_id);
    if (!h || !h->client) return -1;
    if (!h->client->is_executing.load(std::memory_order_relaxed)) return -1;
    return h->client->rows_processed.load(std::memory_order_relaxed);
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
    std::unordered_map<std::string, uint64_t> created_node_lids;
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
                    // Build row of Values with property key names
                    duckdb::vector<std::string> keys;
                    duckdb::vector<duckdb::Value> row;
                    for (auto& [key, val] : node_info.properties) {
                        keys.push_back(key);
                        row.push_back(val);
                    }
                    uint32_t inmem_eid = delta_store.GetOrAllocateInMemoryExtentID(logical_pid, keys);
                    uint64_t logical_id = delta_store.AllocateNodeLogicalId();
                    if (h->database->instance->wal_writer) {
                        h->database->instance->wal_writer->LogInsertNodeV2(
                            logical_pid, logical_id, keys, row);
                    }
                    AppendNodeDeltaRow(h, logical_pid, std::move(keys), std::move(row),
                                       logical_id);
                    created_node_lids[node_info.variable_name] = logical_id;
                    spdlog::info("[CREATE] Inserted node label='{}' with {} properties into in-memory extent 0x{:08X} (partition {}, oid {})",
                                 node_info.label, node_info.properties.size(), inmem_eid, logical_pid, part_oid);
                }

                // Process edges: record both Graphlet Delta and CSR Delta.
                for (auto& edge_info : create->GetEdges()) {
                    if (edge_info.edge_partition_ids.empty()) {
                        spdlog::warn("[CREATE] Edge type '{}' has no partition — skipping", edge_info.type);
                        continue;
                    }
                    idx_t edge_part_oid = edge_info.edge_partition_ids[0];
                    auto& catalog = h->database->instance->GetCatalog();
                    auto* edge_part_cat = (PartitionCatalogEntry*)catalog.GetEntry(
                        *h->client.get(), DEFAULT_SCHEMA, edge_part_oid);
                    if (!edge_part_cat) {
                        spdlog::warn("[CREATE] Edge partition oid {} not found", edge_part_oid);
                        continue;
                    }
                    uint16_t edge_logical_pid = edge_part_cat->GetPartitionID();
                    uint64_t edge_id = delta_store.AllocateEdgeId(edge_logical_pid);
                    uint64_t src_vid = edge_info.src_vid;
                    uint64_t dst_vid = edge_info.dst_vid;
                    auto src_it = created_node_lids.find(edge_info.src_variable_name);
                    auto dst_it = created_node_lids.find(edge_info.dst_variable_name);
                    if (src_it != created_node_lids.end()) {
                        src_vid = src_it->second;
                    }
                    if (dst_it != created_node_lids.end()) {
                        dst_vid = dst_it->second;
                    }
                    bool src_missing =
                        src_vid == 0 && created_node_lids.find(edge_info.src_variable_name) ==
                                            created_node_lids.end();
                    bool dst_missing =
                        dst_vid == 0 && created_node_lids.find(edge_info.dst_variable_name) ==
                                            created_node_lids.end();
                    if (src_missing || dst_missing) {
                        throw std::runtime_error(
                            "CREATE edge could not resolve endpoint logical IDs");
                    }

                    duckdb::vector<std::string> edge_keys;
                    duckdb::vector<duckdb::Value> edge_row;
                    if (!BuildEdgeDeltaRow(h, edge_part_cat, src_vid, dst_vid,
                                           edge_info.properties, edge_keys,
                                           edge_row)) {
                        throw std::runtime_error(
                            "CREATE edge could not build edge record for type '" +
                            edge_info.type + "'");
                    }
                    AppendDeltaRow(h, edge_logical_pid, std::move(edge_keys),
                                   std::move(edge_row), edge_id);

                    duckdb::LogAndApplyInsertEdge(h->database->instance->wal_writer.get(),
                                                   delta_store, edge_logical_pid,
                                                   src_vid, dst_vid, edge_id);

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
        std::vector<std::shared_ptr<duckdb::DataChunk>> chunks;
        duckdb::Schema schema;
        std::vector<std::string> col_names;
        bool is_mutation = false;

        spdlog::debug("[ExecuteCAPI] before raw query={}",
                     prepared_statement && prepared_statement->query
                         ? prepared_statement->query
                         : "<null>");
        auto result = turbolynx_execute_raw(conn_id, prepared_statement, chunks,
                                            schema, col_names, is_mutation);
        spdlog::debug("[ExecuteCAPI] after raw result={} mutation={} chunks={} cols={}",
                     result, is_mutation, chunks.size(), col_names.size());
        if (result < 0) {
            return TURBOLYNX_ERROR;
        }

        if (is_mutation) {
            *result_set_wrp = nullptr;
            return result;
        }

        auto cypher_prep_stmt = reinterpret_cast<CypherPreparedStatement *>(
            prepared_statement->__internal_prepared_statement);
        if (!cypher_prep_stmt) {
            last_error_message = INVALID_PREPARED_STATEMENT_MSG;
            last_error_code = TURBOLYNX_ERROR_INVALID_STATEMENT;
            return TURBOLYNX_ERROR;
        }

        spdlog::debug("[ExecuteCAPI] before metadata");
        PopulatePreparedStatementResultMetadata(prepared_statement,
                                               cypher_prep_stmt, col_names,
                                               schema, chunks);
        spdlog::debug("[ExecuteCAPI] before copyResults");
        cypher_prep_stmt->copyResults(chunks);
        spdlog::debug("[ExecuteCAPI] before register_resultset");
        turbolynx_register_resultset(prepared_statement, result_set_wrp);
        spdlog::debug("[ExecuteCAPI] after register_resultset rows={}",
                     cypher_prep_stmt->getNumRows());
        return cypher_prep_stmt->getNumRows();
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
    auto value = vec->GetValue(local_cursor);

    if (value.IsNull()) {
        return T();
    }
    if (value.type().id() != TYPE_ID &&
        value.type().InternalType() != duckdb::LogicalType(TYPE_ID).InternalType()) {
        last_error_message = INVALID_RESULT_SET_MSG;
        last_error_code = TURBOLYNX_ERROR_INVALID_COLUMN_TYPE;
        return T();
    }
    else {
        return value.GetValue<T>();
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
    auto value = vec->GetValue(local_cursor);
    if (value.IsNull()) {
        return string();
    }
    if (value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
        last_error_message = INVALID_RESULT_SET_MSG;
        last_error_code = TURBOLYNX_ERROR_INVALID_COLUMN_TYPE;
        return string();
    }
    return value.GetValue<string>();
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
			return turbolynx_decimal{width,scale,{static_cast<uint64_t>(turbolynx_get_value<int16_t, duckdb::LogicalTypeId::DECIMAL>(result_set_wrp, col_idx)),0}};
		case duckdb::PhysicalType::INT32:
			return turbolynx_decimal{width,scale,{static_cast<uint64_t>(turbolynx_get_value<int32_t, duckdb::LogicalTypeId::DECIMAL>(result_set_wrp, col_idx)),0}};
		case duckdb::PhysicalType::INT64:
			return turbolynx_decimal{width,scale,{static_cast<uint64_t>(turbolynx_get_value<int64_t, duckdb::LogicalTypeId::DECIMAL>(result_set_wrp, col_idx)),0}};
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

// ---------------------------------------------------------------------------
// turbolynx_execute_raw — execute and return raw DataChunks for shell rendering
// ---------------------------------------------------------------------------

int64_t turbolynx_execute_raw(int64_t conn_id,
                               turbolynx_prepared_statement* prepared_statement,
                               std::vector<std::shared_ptr<duckdb::DataChunk>>& out_chunks,
                               duckdb::Schema& out_schema,
                               std::vector<std::string>& out_col_names,
                               bool& out_is_mutation) {
    auto* h = get_handle(conn_id);
    if (!h) { set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, INVALID_PARAMETER); return -1; }
    out_is_mutation = false;
    out_chunks.clear();
    out_col_names.clear();

    try {
        // Reset interrupt flag and mark as executing
        h->client->ResetInterrupt();
        h->client->is_executing = true;

        // Special markers from turbolynx_prepare
        if (prepared_statement->__internal_prepared_statement == nullptr) {
            turbolynx_resultset_wrapper* wrp = nullptr;
            executeMerge(conn_id, string(prepared_statement->query), &wrp);
            if (wrp) turbolynx_close_resultset(wrp);
            out_is_mutation = true;
            return 0;
        }
        if (prepared_statement->__internal_prepared_statement == (void*)0x1) {
            turbolynx_resultset_wrapper* wrp = nullptr;
            executeMatchCreateEdge(conn_id, string(prepared_statement->query), &wrp);
            if (wrp) turbolynx_close_resultset(wrp);
            out_is_mutation = true;
            return 0;
        }
        if (prepared_statement->__internal_prepared_statement == (void*)0x2) {
            turbolynx_resultset_wrapper* wrp = nullptr;
            executeUnwindCreate(conn_id, string(prepared_statement->query), &wrp);
            if (wrp) turbolynx_close_resultset(wrp);
            out_is_mutation = true;
            return 0;
        }
        if (prepared_statement->__internal_prepared_statement == (void*)0x3) {
            out_is_mutation = true;
            return 0;
        }

        auto cypher_prep_stmt = reinterpret_cast<CypherPreparedStatement*>(
            prepared_statement->__internal_prepared_statement);
        std::string bound_query;
        std::string bind_error;
        if (!cypher_prep_stmt->tryGetBoundQuery(bound_query, bind_error)) {
            set_error(TURBOLYNX_ERROR_INVALID_PARAMETER, bind_error);
            return -1;
        }
        turbolynx_compile_query(h, bound_query);
        EnsureImplicitMutationReadbackProjection(h, cypher_prep_stmt);

        if (h->is_mutation_query) {
            turbolynx_resultset_wrapper* wrp = nullptr;
            turbolynx_execute_mutation(h, prepared_statement, &wrp);
            out_is_mutation = true;
            return 0;
        }

        // Read query
        auto executors = h->planner->genPipelineExecutors();
        if (executors.empty()) {
            set_error(TURBOLYNX_ERROR_INVALID_PLAN, INVALID_PLAN_MSG);
            return -1;
        }
        for (auto exec : executors) {
            spdlog::debug("[ExecuteCAPI] run pipeline={} source={} sink={}",
                         exec->pipeline->GetPipelineId(),
                         exec->pipeline->GetSource()->ToString(),
                         exec->pipeline->GetSink()->ToString());
            exec->ExecutePipeline();
            spdlog::debug("[ExecuteCAPI] done pipeline={}",
                         exec->pipeline->GetPipelineId());
        }

        auto& query_results = *(executors.back()->context->query_results);
        auto& schema = executors.back()->pipeline->GetSink()->schema;
        out_col_names = h->planner->getQueryOutputColNames();

        if (!h->pending_set_items.empty()) {
            ApplyPendingSetMutations(h, query_results);
            out_is_mutation = true;
        }
        if (h->pending_delete) {
            ApplyPendingDeleteMutations(h, query_results);
            out_is_mutation = true;
        }

        maybeAutoCompact(h);

	        // Copy schema and chunks to output
	        out_schema = schema;
	        int64_t total_rows = 0;
	        for (auto& chunk : query_results) {
	            if (chunk) { total_rows += chunk->size(); out_chunks.push_back(chunk); }
	        }
	        for (auto* e : executors) delete e;
	        h->client->is_executing = false;
	        return total_rows;
    } catch (const std::exception& e) {
        h->client->is_executing = false;
        spdlog::error("[turbolynx_execute_raw] {}", e.what());
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, e.what());
        return -1;
    } catch (...) {
        h->client->is_executing = false;
        spdlog::error("[turbolynx_execute_raw] unknown exception");
        set_error(TURBOLYNX_ERROR_INVALID_PLAN, "Unknown error");
        return -1;
    }
}
