//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query/updating_clause/updating_clause.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

enum class UpdatingClauseType : uint8_t { SET = 0, DELETE_CLAUSE = 1, INSERT = 2, MERGE = 3 };

class UpdatingClause {
public:
    explicit UpdatingClause(UpdatingClauseType type) : clause_type(type) {}
    virtual ~UpdatingClause() = default;

    UpdatingClauseType GetClauseType() const { return clause_type; }

private:
    UpdatingClauseType clause_type;
};

} // namespace turbolynx
