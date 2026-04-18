//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query/reading_clause/reading_clause.hpp
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

enum class CypherClauseType : uint8_t { MATCH = 0, UNWIND = 1 };

class ReadingClause {
public:
    explicit ReadingClause(CypherClauseType type) : clause_type(type) {}
    virtual ~ReadingClause() = default;

    CypherClauseType GetClauseType() const { return clause_type; }

private:
    CypherClauseType clause_type;
};

} // namespace turbolynx
