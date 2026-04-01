#pragma once


#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

//===--------------------------------------------------------------------===//
// Type Information
//===--------------------------------------------------------------------===//
typedef uint64_t idx_t;
typedef const char* turbolynx_version;
typedef char* turbolynx_query;
typedef char* turbolynx_plan;
typedef char* turbolynx_label_name;
typedef char* turbolynx_property_name;
typedef char* turbolynx_sql_type;
typedef int turbolynx_property_order;
typedef int turbolynx_precision;
typedef int turbolynx_scale;
typedef size_t turbolynx_num_metadata;
typedef size_t turbolynx_num_properties;
typedef size_t turbolynx_num_rows;
typedef size_t turbolynx_cursor;

typedef enum TURBOLYNX_METADATA_TYPE {
	TURBOLYNX_NODE = 0,
	TURBOLYNX_EDGE = 1,
	TURBOLYNX_OTHER = 2,
} turbolynx_metadata_type;

typedef enum TURBOLYNX_TYPE {
	TURBOLYNX_TYPE_INVALID = 0,
	// bool
	TURBOLYNX_TYPE_BOOLEAN, // 1
	// int8_t
	TURBOLYNX_TYPE_TINYINT, // 2
	// int16_t
	TURBOLYNX_TYPE_SMALLINT, // 3
	// int32_t
	TURBOLYNX_TYPE_INTEGER, // 4
	// int64_t
	TURBOLYNX_TYPE_BIGINT, // 5
	// uint8_t
	TURBOLYNX_TYPE_UTINYINT, // 6
	// uint16_t
	TURBOLYNX_TYPE_USMALLINT, // 7
	// uint32_t
	TURBOLYNX_TYPE_UINTEGER, // 8
	// uint64_t
	TURBOLYNX_TYPE_UBIGINT, // 9
	// float
	TURBOLYNX_TYPE_FLOAT, // 10
	// double
	TURBOLYNX_TYPE_DOUBLE, // 11
	// TURBOLYNX_timestamp, in microseconds
	TURBOLYNX_TYPE_TIMESTAMP, // 12
	// TURBOLYNX_date
	TURBOLYNX_TYPE_DATE, // 13
	// TURBOLYNX_time
	TURBOLYNX_TYPE_TIME, // 14
	// TURBOLYNX_interval
	TURBOLYNX_TYPE_INTERVAL, // 15
	// TURBOLYNX_hugeint
	TURBOLYNX_TYPE_HUGEINT, // 16
	// const char*
	TURBOLYNX_TYPE_VARCHAR, // 17
	// TURBOLYNX_blob
	TURBOLYNX_TYPE_BLOB, // 18
	// decimal
	TURBOLYNX_TYPE_DECIMAL, // 19
	// TURBOLYNX_timestamp, in seconds
	TURBOLYNX_TYPE_TIMESTAMP_S, // 20
	// TURBOLYNX_timestamp, in milliseconds
	TURBOLYNX_TYPE_TIMESTAMP_MS, // 21
	// TURBOLYNX_timestamp, in nanoseconds
	TURBOLYNX_TYPE_TIMESTAMP_NS, // 22
	// enum type, only useful as logical type
	TURBOLYNX_TYPE_ENUM, // 23
	// list type, only useful as logical type
	TURBOLYNX_TYPE_LIST, // 24
	// struct type, only useful as logical type
	TURBOLYNX_TYPE_STRUCT, // 25
	// map type, only useful as logical type
	TURBOLYNX_TYPE_MAP, // 26
	// TURBOLYNX_hugeint
	TURBOLYNX_TYPE_UUID, // 27
	// union type, only useful as logical type
	TURBOLYNX_TYPE_UNION, // 28
	// TURBOLYNX_bit
	TURBOLYNX_TYPE_BIT, // 29
	// TURBOLYNX_ID
	TURBOLYNX_TYPE_ID, // 30
} turbolynx_type;

//! Days are stored as days since 1970-01-01
//! Use the duckdb_from_date/duckdb_to_date function to extract individual information
typedef struct {
	int32_t days;
} turbolynx_date;

typedef struct {
	int32_t year;
	int8_t month;
	int8_t day;
} turbolynx_date_struct;

//! Time is stored as microseconds since 00:00:00
//! Use the duckdb_from_time/duckdb_to_time function to extract individual information
typedef struct {
	int64_t micros;
} turbolynx_time;

typedef struct {
	int8_t hour;
	int8_t min;
	int8_t sec;
	int32_t micros;
} turbolynx_time_struct;

//! Timestamps are stored as microseconds since 1970-01-01
//! Use the duckdb_from_timestamp/duckdb_to_timestamp function to extract individual information
typedef struct {
	int64_t micros;
} turbolynx_timestamp;

typedef struct {
	turbolynx_date_struct date;
	turbolynx_time_struct time;
} turbolynx_timestamp_struct;

typedef struct {
	int32_t months;
	int32_t days;
	int64_t micros;
} turbolynx_interval;

typedef struct {
	uint64_t lower;
	int64_t upper;
} turbolynx_hugeint;

typedef struct {
	uint8_t width;
	uint8_t scale;

	turbolynx_hugeint value;
} turbolynx_decimal;

typedef struct {
	char *data;
	idx_t size;
} turbolynx_string;

/*
    The internal data representation of a VARCHAR/BLOB column
*/
typedef struct {
	union {
		struct {
			uint32_t length;
			char prefix[4];
			char *ptr;
		} pointer;
		struct {
			uint32_t length;
			char inlined[12];
		} inlined;
	} value;
} turbolynx_string_t;

typedef struct {
	uint64_t offset;
	uint64_t length;
} turbolynx_list_entry;

typedef struct _turbolynx_property {
	turbolynx_label_name label_name;
	turbolynx_metadata_type label_type;
	turbolynx_property_order order;
	turbolynx_property_name property_name;
	turbolynx_type property_type;
	turbolynx_sql_type property_sql_type;
	turbolynx_precision precision;
	turbolynx_scale scale;
	struct _turbolynx_property *next;
} turbolynx_property;

typedef struct _turbolynx_prepared_statement {
	turbolynx_query query;
	turbolynx_plan plan;
	turbolynx_num_properties num_properties;
	turbolynx_property *property;
	void *__internal_prepared_statement;
} turbolynx_prepared_statement;

typedef struct _turbolynx_value {
	void *__val;
} * turbolynx_value;

typedef struct _turbolynx_result {
	turbolynx_type data_type;
	turbolynx_sql_type data_sql_type;
	turbolynx_num_rows num_rows;
	void* __internal_data;
	struct _turbolynx_result *next;
} turbolynx_result;

typedef struct _turbolynx_resultset {
	turbolynx_num_properties num_properties;
	turbolynx_result *result;
	struct _turbolynx_resultset *next;
} turbolynx_resultset;

typedef struct _turbolynx_resultset_wrapper {
	turbolynx_num_rows num_total_rows;
	turbolynx_cursor cursor;
	struct _turbolynx_resultset *result_set;
} turbolynx_resultset_wrapper;

typedef struct _turbolynx_metadata {
	turbolynx_label_name label_name;
	turbolynx_metadata_type type;
	struct _turbolynx_metadata *next;
} turbolynx_metadata;

typedef enum {
    TURBOLYNX_SUCCESS = 0,
    TURBOLYNX_ERROR = -1,
} turbolynx_state;

typedef enum {
    TURBOLYNX_CONNECTED = 0,
    TURBOLYNX_NOT_CONNECTED = 1,
} turbolynx_conn_state;

typedef enum {
	TURBOLYNX_MORE_RESULT = 1,
	TURBOLYNX_END_OF_RESULT = 0,
	TURBOLYNX_ERROR_RESULT = -1,
} turbolynx_fetch_state;

typedef enum {
    TURBOLYNX_NO_ERROR = 0,
    TURBOLYNX_ERROR_CONNECTION_FAILED = -1,
	TURBOLYNX_ERROR_INVALID_STATEMENT = -2,
	TURBOLYNX_ERROR_INVALID_PARAMETER_INDEX = -3,
	TURBOLYNX_ERROR_UNSUPPORTED_OPERATION = -4,
	TURBOLYNX_ERROR_INVALID_METADATA = -5,
	TURBOLYNX_ERROR_INVALID_LABEL = -6,
	TURBOLYNX_ERROR_INVALID_PROPERTY = -7,
	TURBOLYNX_ERROR_INVALID_NUMBER_OF_PROPERTIES = -8,
	TURBOLYNX_ERROR_INVALID_PREPARED_STATEMENT = -9,
	TURBOLYNX_ERROR_INVALID_METADATA_TYPE = -10,
	TURBOLYNX_ERROR_INVALID_PLAN = -11,
	TURBOLYNX_ERROR_INVALID_RESULT_SET = -12,
	TURBOLYNX_ERROR_INVALID_COLUMN_INDEX = -13,
	TURBOLYNX_ERROR_INVALID_COLUMN_TYPE = -14,
	TURBOLYNX_ERROR_INVALID_CURSOR = -15,
	TURBOLYNX_ERROR_INVALID_PARAMETER = -16
} turbolynx_error_code;

typedef enum {
	DUCKDB_PENDING_RESULT_READY = 0,
	DUCKDB_PENDING_RESULT_NOT_READY = 1,
	DUCKDB_PENDING_ERROR = 2,
	DUCKDB_PENDING_NO_TASKS_AVAILABLE = 3
} duckdb_pending_state;

//===--------------------------------------------------------------------===//
// Open/Connect
//===--------------------------------------------------------------------===//

// Returns conn_id (>= 0) on success, -1 on failure.
int64_t turbolynx_connect(const char *dbname);

// Open an existing database in read-only mode.
// Multiple readers can coexist; fails if a writer holds an exclusive lock.
// Returns conn_id (>= 0) on success, -1 on failure.
int64_t turbolynx_connect_readonly(const char *dbname);

// Connect using an existing ClientContext. Returns conn_id (>= 0) or -1.
int64_t turbolynx_connect_with_client_context(void *client_context);

void turbolynx_disconnect(int64_t conn_id);

// Clear all in-memory delta data (INSERT/UPDATE/DELETE buffers).
// Used for test isolation — resets DeltaStore to clean state.
void turbolynx_clear_delta(int64_t conn_id);

// Compact: flush DeltaStore to base extents, truncate WAL.
void turbolynx_checkpoint(int64_t conn_id);

// Set auto compaction thresholds. Set row_threshold=0 to disable.
void turbolynx_set_auto_compact_threshold(size_t row_threshold, size_t extent_threshold);


// Check if the catalog has been updated since this connection was opened.
// Returns 1 if catalog version changed (caller should reconnect), 0 if up-to-date, -1 on error.
int turbolynx_reopen(int64_t conn_id);

turbolynx_conn_state turbolynx_is_connected(int64_t conn_id);

turbolynx_error_code turbolynx_get_last_error(char **errmsg);

turbolynx_version turbolynx_get_version();

//===--------------------------------------------------------------------===//
// Metadata
//===--------------------------------------------------------------------===//

turbolynx_num_metadata turbolynx_get_metadata_from_catalog(int64_t conn_id, turbolynx_label_name label, bool like_flag, bool filter_flag, turbolynx_metadata **metadata);

turbolynx_state turbolynx_close_metadata(turbolynx_metadata *metadata);

turbolynx_num_properties turbolynx_get_property_from_catalog(int64_t conn_id, turbolynx_label_name label, turbolynx_metadata_type type, turbolynx_property** property);

turbolynx_state turbolynx_close_property(turbolynx_property *property);

// Dump full catalog (partitions + graphlets) as JSON string.
// Caller must free() the returned string.
char* turbolynx_dump_catalog_json(int64_t conn_id);

//===--------------------------------------------------------------------===//
// turbolynx_query
//===--------------------------------------------------------------------===//

turbolynx_prepared_statement* turbolynx_prepare(int64_t conn_id, turbolynx_query query);

turbolynx_state turbolynx_close_prepared_statement(turbolynx_prepared_statement* prepared_statement);

turbolynx_state turbolynx_bind_value(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_value val);

turbolynx_state turbolynx_bind_boolean(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, bool val);

turbolynx_state turbolynx_bind_int8(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, int8_t val);

turbolynx_state turbolynx_bind_int16(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, int16_t val);

turbolynx_state turbolynx_bind_int32(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, int32_t val);

turbolynx_state turbolynx_bind_int64(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, int64_t val);

turbolynx_state turbolynx_bind_hugeint(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_hugeint val);

turbolynx_state turbolynx_bind_uint8(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, uint8_t val);

turbolynx_state turbolynx_bind_uint16(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, uint16_t val);

turbolynx_state turbolynx_bind_uint32(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, uint32_t val);

turbolynx_state turbolynx_bind_uint64(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, uint64_t val);

turbolynx_state turbolynx_bind_float(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, float val);

turbolynx_state turbolynx_bind_double(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, double val);

//! turbolynx_date stores days, which represents the days since 1970-01-01
//! For example, if you want to input 1970-01-15, then 
//! turbolynx_date date_value = {14}
//! turbolynx_bind_date(prep_stmt, 1, date_value)
turbolynx_state turbolynx_bind_date(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_date val);

//! string version of turbolynx_bind_date
//! For example, if you want to input 1970-01-15, then
//! turbolynx_bind_date(prep_stmt, 1, "1970-01-15")
turbolynx_state turbolynx_bind_date_string(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, const char *val);

//! turbolynx_time stores micros, which is a microseconds since 00:00:00
//! For example, if you want to input 06:02:03, then
//! turbolynx_time time_value = {21,723,000}
//! turbolynx_bind_time(prep_stmt, 1, time_value)
turbolynx_state turbolynx_bind_time(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_time val);

//! turbolynx_timestamp stores micros, which is a microseconds since 1970-01-01
//! For example, if you want to input 2021-08-03 11:59:44.123456, then
//! turbolynx_timestamp timestamp_value = {1627991984123456}
//! turbolynx_bind_timestamp(prep_stmt, 1, timestamp_value)
turbolynx_state turbolynx_bind_timestamp(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_timestamp val);

//! turbolynx_bind_varchar binds a VARCHAR value to a parameter
//! For example, if you want to input "hello", then
//! turbolynx_bind_varchar(prep_stmt, 1, "hello")
turbolynx_state turbolynx_bind_varchar(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, const char *val);

//! turbolynx_bind_varchar_length binds a VARCHAR value to a parameter with a specified length
//! For example, if you want to input "hello", then
//! turbolynx_bind_varchar_length(prep_stmt, 1, "hello", 5)
turbolynx_state turbolynx_bind_varchar_length(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, const char *val, idx_t length);

//! turbolynx_decimal stores a decimal value, which represents with a width and scale
//! For example, if you want to input 1000.10, then
//! turbolynx_hugeint hint_value = {100010, 0};
//! turbolynx_decimal dec_value = {6, 2, hint_value};
//! turbolynx_bind_decimal(prep_stmt, 1, dec_value)
//! For more example, please see LogicalType::GetDecimalProperties(uint8_t &width, uint8_t &scale)
turbolynx_state turbolynx_bind_decimal(turbolynx_prepared_statement* prepared_statement, idx_t param_idx, turbolynx_decimal val);

//! turbolynx_bind_null binds a NULL value to a parameter
turbolynx_state turbolynx_bind_null(turbolynx_prepared_statement* prepared_statement, idx_t param_idx);

//===--------------------------------------------------------------------===//
// turbolynx_execute
//===--------------------------------------------------------------------===//

turbolynx_num_rows turbolynx_execute(int64_t conn_id, turbolynx_prepared_statement* prep_query, turbolynx_resultset_wrapper** result_set_wrp);

turbolynx_state turbolynx_close_resultset(turbolynx_resultset_wrapper* result_set_wrp);

turbolynx_fetch_state turbolynx_fetch_next(turbolynx_resultset_wrapper* result_set_wrp);

bool turbolynx_get_bool(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

int8_t turbolynx_get_int8(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

int16_t turbolynx_get_int16(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

int32_t turbolynx_get_int32(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

int64_t turbolynx_get_int64(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

turbolynx_hugeint turbolynx_get_hugeint(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

uint8_t turbolynx_get_uint8(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

uint16_t turbolynx_get_uint16(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

uint32_t turbolynx_get_uint32(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

uint64_t turbolynx_get_uint64(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

float turbolynx_get_float(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

double turbolynx_get_double(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

turbolynx_date turbolynx_get_date(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

turbolynx_time turbolynx_get_time(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

turbolynx_timestamp turbolynx_get_timestamp(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

turbolynx_string turbolynx_get_varchar(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

turbolynx_decimal turbolynx_get_decimal(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

uint64_t turbolynx_get_id(turbolynx_resultset_wrapper* result_set_wrp, idx_t col_idx);

turbolynx_string turbolynx_decimal_to_string(turbolynx_decimal val);

#ifdef __cplusplus
}

// C++ only: checkpoint via ClientContext (for shell and direct callers).
namespace duckdb { class ClientContext; }
void turbolynx_checkpoint_ctx(duckdb::ClientContext &context);

#endif