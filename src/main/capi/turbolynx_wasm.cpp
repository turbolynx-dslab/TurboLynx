// turbolynx_wasm.cpp — WASM entry points for TurboLynx
//
// Provides Emscripten-exported C functions for browser/Node.js usage.
// All functions are synchronous (runs in Web Worker).
//
// NOTE: We use int32_t for conn_id (not int64_t) because Emscripten ccall
// passes JavaScript Number as i32 — int64_t would be split into two i32s.

#ifdef TURBOLYNX_WASM
#ifdef __EMSCRIPTEN__
#include <emscripten.h>
#define WASM_EXPORT EMSCRIPTEN_KEEPALIVE
#else
#define WASM_EXPORT
#endif

#include <string>
#include <vector>
#include <cstdlib>
#include <cstring>

#include "main/capi/turbolynx.h"

// Helper: JSON-escape a C string and append to dst
static void json_escape_append(std::string &dst, const char *s) {
    if (!s) return;
    for (const char *p = s; *p; p++) {
        switch (*p) {
            case '"':  dst += "\\\""; break;
            case '\\': dst += "\\\\"; break;
            case '\n': dst += "\\n"; break;
            case '\r': dst += "\\r"; break;
            case '\t': dst += "\\t"; break;
            default:   dst += *p; break;
        }
    }
}

// Helper: build a JSON error response
static char* make_error_json(const char* msg) {
    std::string json = "{\"error\":\"";
    json_escape_append(json, msg ? msg : "unknown error");
    json += "\"}";
    char* result = (char*)malloc(json.size() + 1);
    strcpy(result, json.c_str());
    return result;
}

extern "C" {

WASM_EXPORT
const char* turbolynx_wasm_get_version() {
    static const char version[] = "0.0.1-wasm-alpha";
    return version;
}

WASM_EXPORT
int32_t turbolynx_wasm_open(const char* workspace_path) {
    return (int32_t)turbolynx_connect_readonly(workspace_path);
}

WASM_EXPORT
char* turbolynx_wasm_query(int32_t conn_id32, const char* cypher) {
  try {
    int64_t conn_id = (int64_t)conn_id32;

    // Prepare the query
    auto* stmt = turbolynx_prepare(conn_id, (turbolynx_query)cypher);
    if (!stmt) {
        char* errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        char* r = make_error_json(errmsg ? errmsg : "prepare failed");
        return r;
    }

    // Execute
    turbolynx_resultset_wrapper* rs = nullptr;
    turbolynx_execute(conn_id, stmt, &rs);

    if (!rs) {
        char* errmsg = nullptr;
        turbolynx_get_last_error(&errmsg);
        char* r = make_error_json(errmsg ? errmsg : "execute failed");
        turbolynx_close_prepared_statement(stmt);
        return r;
    }

    // Column info comes from the prepared statement's property list
    int col_count = (int)stmt->num_properties;
    std::string json = "{\"columns\":[";
    {
        turbolynx_property* p = stmt->property;
        for (int i = 0; i < col_count && p; i++, p = p->next) {
            if (i > 0) json += ",";
            json += "\"";
            json_escape_append(json, p->property_name);
            json += "\"";
        }
    }
    json += "],\"types\":[";
    {
        turbolynx_property* p = stmt->property;
        for (int i = 0; i < col_count && p; i++, p = p->next) {
            if (i > 0) json += ",";
            json += "\"";
            json += std::to_string((int)p->property_type);
            json += "\"";
        }
    }
    json += "],\"rows\":[";

    // Build a type array for fast lookup during row iteration
    std::vector<turbolynx_type> col_types(col_count);
    {
        turbolynx_property* p = stmt->property;
        for (int i = 0; i < col_count && p; i++, p = p->next) {
            col_types[i] = p->property_type;
        }
    }

    // Fetch rows
    int row_idx = 0;
    while (turbolynx_fetch_next(rs) == TURBOLYNX_MORE_RESULT) {
        if (row_idx > 0) json += ",";
        json += "[";
        for (int col = 0; col < col_count; col++) {
            if (col > 0) json += ",";
            turbolynx_type t = col_types[col];
            switch (t) {
                case TURBOLYNX_TYPE_VARCHAR: {
                    turbolynx_string s = turbolynx_get_varchar(rs, col);
                    json += "\"";
                    if (s.data) json_escape_append(json, s.data);
                    json += "\"";
                    break;
                }
                case TURBOLYNX_TYPE_BIGINT:
                    json += std::to_string(turbolynx_get_int64(rs, col));
                    break;
                case TURBOLYNX_TYPE_INTEGER:
                    json += std::to_string(turbolynx_get_int32(rs, col));
                    break;
                case TURBOLYNX_TYPE_SMALLINT:
                    json += std::to_string(turbolynx_get_int16(rs, col));
                    break;
                case TURBOLYNX_TYPE_TINYINT:
                    json += std::to_string(turbolynx_get_int8(rs, col));
                    break;
                case TURBOLYNX_TYPE_UBIGINT:
                    json += std::to_string(turbolynx_get_uint64(rs, col));
                    break;
                case TURBOLYNX_TYPE_UINTEGER:
                    json += std::to_string(turbolynx_get_uint32(rs, col));
                    break;
                case TURBOLYNX_TYPE_USMALLINT:
                    json += std::to_string(turbolynx_get_uint16(rs, col));
                    break;
                case TURBOLYNX_TYPE_UTINYINT:
                    json += std::to_string(turbolynx_get_uint8(rs, col));
                    break;
                case TURBOLYNX_TYPE_FLOAT:
                    json += std::to_string(turbolynx_get_float(rs, col));
                    break;
                case TURBOLYNX_TYPE_DOUBLE:
                    json += std::to_string(turbolynx_get_double(rs, col));
                    break;
                case TURBOLYNX_TYPE_BOOLEAN:
                    json += turbolynx_get_bool(rs, col) ? "true" : "false";
                    break;
                case TURBOLYNX_TYPE_ID:
                    json += std::to_string(turbolynx_get_id(rs, col));
                    break;
                default: {
                    turbolynx_string s = turbolynx_get_varchar(rs, col);
                    json += "\"";
                    if (s.data) json_escape_append(json, s.data);
                    json += "\"";
                    break;
                }
            }
        }
        json += "]";
        row_idx++;
    }
    json += "],\"rowCount\":";
    json += std::to_string(row_idx);
    json += "}";

    turbolynx_close_resultset(rs);
    turbolynx_close_prepared_statement(stmt);

    char* result = (char*)malloc(json.size() + 1);
    strcpy(result, json.c_str());
    return result;
  } catch (const std::exception& e) {
    return make_error_json(e.what());
  } catch (...) {
    return make_error_json("Unknown C++ exception in WASM query");
  }
}

WASM_EXPORT
char* turbolynx_wasm_get_labels(int32_t conn_id32) {
    int64_t conn_id = (int64_t)conn_id32;
    turbolynx_metadata* meta = nullptr;
    // filter_flag=true returns all labels without any label filter
    turbolynx_get_metadata_from_catalog(conn_id, (turbolynx_label_name)"", false, true, &meta);

    std::string json = "[";
    bool first = true;
    for (turbolynx_metadata* m = meta; m; m = m->next) {
        if (!first) json += ",";
        json += "{\"name\":\"";
        json_escape_append(json, m->label_name);
        json += "\",\"type\":";
        json += (m->type == TURBOLYNX_NODE ? "\"node\"" : "\"edge\"");
        json += "}";
        first = false;
    }
    json += "]";

    if (meta) turbolynx_close_metadata(meta);

    char* result = (char*)malloc(json.size() + 1);
    strcpy(result, json.c_str());
    return result;
}

WASM_EXPORT
char* turbolynx_wasm_get_schema(int32_t conn_id32, const char* label, int is_edge) {
    int64_t conn_id = (int64_t)conn_id32;
    turbolynx_metadata_type ttype = is_edge ? TURBOLYNX_EDGE : TURBOLYNX_NODE;
    turbolynx_property* prop = nullptr;
    turbolynx_get_property_from_catalog(conn_id, (turbolynx_label_name)label, ttype, &prop);

    std::string json = "{";
    bool first = true;
    for (turbolynx_property* p = prop; p; p = p->next) {
        if (!first) json += ",";
        json += "\"";
        json_escape_append(json, p->property_name);
        json += "\":\"";
        if (p->property_sql_type) {
            json_escape_append(json, p->property_sql_type);
        } else {
            json += std::to_string((int)p->property_type);
        }
        json += "\"";
        first = false;
    }
    json += "}";

    if (prop) turbolynx_close_property(prop);

    char* result = (char*)malloc(json.size() + 1);
    strcpy(result, json.c_str());
    return result;
}

WASM_EXPORT
void turbolynx_wasm_close(int32_t conn_id32) {
    turbolynx_disconnect((int64_t)conn_id32);
}

WASM_EXPORT
void turbolynx_wasm_free(void* ptr) {
    free(ptr);
}

}  // extern "C"

#else
// Non-WASM build: empty file
#endif
