#pragma once

#include <cstdint>

namespace duckdb {

enum class BoundUpdatingClauseType : uint8_t { SET = 0, DELETE_CLAUSE = 1, CREATE = 2, MERGE = 3 };

class BoundUpdatingClause {
public:
    explicit BoundUpdatingClause(BoundUpdatingClauseType clause_type)
        : clause_type(clause_type) {}
    virtual ~BoundUpdatingClause() = default;

    BoundUpdatingClauseType GetClauseType() const { return clause_type; }

private:
    BoundUpdatingClauseType clause_type;
};

} // namespace duckdb
