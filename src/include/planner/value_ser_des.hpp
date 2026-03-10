#pragma once

#include "gpos/memory/CMemoryPool.h"
#include "catalog/catalog.hpp"

#include "common/assert.hpp"
#include "common/types.hpp"
#include "common/types/value.hpp"
#include "common/types/string_type.hpp"

#include <cstdlib>


namespace turbolynx {

class DatumSerDes {

public:
	// datum mp -> duckdb value
		// TODO api should not copy return value
	static duckdb::Value DeserializeOrcaByteArrayIntoDuckDBValue(uint32_t type_id, int32_t type_modifier, const void* mem_ptr, uint64_t length) {
		duckdb::LogicalTypeId duckdb_type = (duckdb::LogicalTypeId) ((type_id - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES);

		// TODO deallocation is responsible here
		switch (duckdb_type) {
			case duckdb::LogicalTypeId::INTEGER: {
				D_ASSERT(length == 4 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::INTEGER);	// TODO to BIGINT
				} else {
					int32_t value = *((int32_t*)mem_ptr);
					return duckdb::Value::INTEGER((int32_t)value);	// TODO to BIGINT
				}
			}
			case duckdb::LogicalTypeId::BIGINT: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					// return duckdb::Value(duckdb::LogicalType::UBIGINT);	// TODO to BIGINT
					return duckdb::Value(duckdb::LogicalType::BIGINT);	// TODO to BIGINT
				} else {
					int64_t value = *((int64_t*)mem_ptr);
					// return duckdb::Value::UBIGINT((uint64_t)value);	// TODO to BIGINT
					return duckdb::Value::BIGINT((uint64_t)value);	// TODO to BIGINT
				}
			}
			case duckdb::LogicalTypeId::UINTEGER: {
				D_ASSERT(length == 4 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::UINTEGER);
				} else {
					uint32_t value = *((uint32_t*)mem_ptr);
					return duckdb::Value::UINTEGER((uint32_t)value);
				}
			}
			case duckdb::LogicalTypeId::UBIGINT: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::UBIGINT);
				} else {
					uint64_t value = *((uint64_t*)mem_ptr);
					return duckdb::Value::UBIGINT(value);
				}
			}
			case duckdb::LogicalTypeId::DOUBLE: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::DOUBLE);
				} else {
					double value = *((double*)mem_ptr);
					return duckdb::Value::DOUBLE(value);
				}
			}
			case duckdb::LogicalTypeId::HUGEINT: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::HUGEINT);
				} else {
					uint64_t value = *((int64_t*)mem_ptr);
					return duckdb::Value::HUGEINT(value);
				}
			}
			case duckdb::LogicalTypeId::VARCHAR: {
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::VARCHAR);
				} else {
					string str_value((char*)mem_ptr);
					duckdb::string_t value(str_value);
					return duckdb::Value(value);
				}
			}
			case duckdb::LogicalTypeId::ID: {
				D_ASSERT(length == 8 || length == 0);
				if (length == 0) {
					return duckdb::Value(duckdb::LogicalType::ID);
				} else {
					uint64_t value = *((uint64_t*)mem_ptr);
					return duckdb::Value::ID(value);
				}
			}
			case duckdb::LogicalTypeId::BOOLEAN: {
				int8_t value = *((int8_t*)mem_ptr);
				return duckdb::Value::BOOLEAN(value);
			}
			case duckdb::LogicalTypeId::DATE: {
				int32_t value = *((int32_t *)mem_ptr);
				return duckdb::Value::DATE(duckdb::date_t(value));
			}
			case duckdb::LogicalTypeId::DECIMAL: { // TODO decimal temporary
				int64_t value = *((int64_t *)mem_ptr);
				return duckdb::Value::DECIMAL(value, 12, 2);
			}
			case duckdb::LogicalTypeId::PATH: {
				return duckdb::Value::LIST(duckdb::LogicalTypeId::UBIGINT, {});
			}
			case duckdb::LogicalTypeId::LIST: {
				if (length == 0) {
					D_ASSERT(type_modifier >= std::numeric_limits<uint8_t>::min() && type_modifier <= std::numeric_limits<uint8_t>::max());
					duckdb::LogicalTypeId child_type_id = (duckdb::LogicalTypeId)type_modifier;
					return duckdb::Value::LIST(child_type_id, {});
				} else {
					D_ASSERT(type_modifier >= std::numeric_limits<uint8_t>::min() && type_modifier <= std::numeric_limits<uint8_t>::max());
					duckdb::LogicalTypeId child_type_id = (duckdb::LogicalTypeId)type_modifier;
					char *str_ptr = (char *)mem_ptr;
					switch(child_type_id) {
					case duckdb::LogicalTypeId::VARCHAR: {
						uint32_t num_strings = *((uint32_t *)str_ptr);
						vector<duckdb::Value> list_values;
						uint32_t accm_bytes = sizeof(uint32_t);
						for (auto i = 0; i < num_strings; i++) {
							uint32_t str_size = *((uint32_t*)(str_ptr + accm_bytes));
							accm_bytes += sizeof(uint32_t);
							string str_value((char*)(str_ptr + accm_bytes), str_size);
							duckdb::string_t value(str_value);
							list_values.push_back(duckdb::Value(value));
							accm_bytes += str_size;
						}
						return duckdb::Value::LIST(duckdb::LogicalTypeId::VARCHAR, list_values);
					}
					default:
						D_ASSERT(false);
					}
				}
			}
			default:
				D_ASSERT(false);
		}
		

		
	}


};


}