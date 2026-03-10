#pragma once

#include "parser/query/reading_clause/reading_clause.hpp"
#include "parser/query/updating_clause/updating_clause.hpp"
#include "parser/query/return_with_clause/with_clause.hpp"
#include <vector>
#include <memory>

namespace duckdb {

// A query part is delimited by WITH clauses.
// Structure: [MATCH/UNWIND]* [SET/DELETE/INSERT]* WITH ...
class QueryPart {
public:
    explicit QueryPart(unique_ptr<WithClause> with_clause)
        : with_clause(std::move(with_clause)) {}

    void AddReadingClause(unique_ptr<ReadingClause> clause) {
        reading_clauses.push_back(std::move(clause));
    }
    void AddUpdatingClause(unique_ptr<UpdatingClause> clause) {
        updating_clauses.push_back(std::move(clause));
    }

    idx_t GetNumReadingClauses()  const { return reading_clauses.size(); }
    idx_t GetNumUpdatingClauses() const { return updating_clauses.size(); }

    ReadingClause*  GetReadingClause(idx_t i)  const { return reading_clauses[i].get(); }
    UpdatingClause* GetUpdatingClause(idx_t i) const { return updating_clauses[i].get(); }

    WithClause* GetWithClause() const { return with_clause.get(); }

private:
    vector<unique_ptr<ReadingClause>>  reading_clauses;
    vector<unique_ptr<UpdatingClause>> updating_clauses;
    unique_ptr<WithClause>             with_clause;
};

} // namespace duckdb
