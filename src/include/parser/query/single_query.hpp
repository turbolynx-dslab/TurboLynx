#pragma once

#include "parser/query/query_part.hpp"
#include "parser/query/return_with_clause/return_clause.hpp"
#include <vector>
#include <memory>

namespace duckdb {

// A single linear Cypher query (no UNION).
// Structure: QueryPart* [MATCH/UNWIND]* [SET/DELETE]* RETURN
class SingleQuery {
public:
    SingleQuery() = default;

    void AddQueryPart(unique_ptr<QueryPart> part) {
        query_parts.push_back(std::move(part));
    }
    void AddReadingClause(unique_ptr<ReadingClause> clause) {
        reading_clauses.push_back(std::move(clause));
    }
    void AddUpdatingClause(unique_ptr<UpdatingClause> clause) {
        updating_clauses.push_back(std::move(clause));
    }
    void SetReturnClause(unique_ptr<ReturnClause> ret) {
        return_clause = std::move(ret);
    }

    idx_t GetNumQueryParts()      const { return query_parts.size(); }
    idx_t GetNumReadingClauses()  const { return reading_clauses.size(); }
    idx_t GetNumUpdatingClauses() const { return updating_clauses.size(); }

    QueryPart*      GetQueryPart(idx_t i)      const { return query_parts[i].get(); }
    ReadingClause*  GetReadingClause(idx_t i)  const { return reading_clauses[i].get(); }
    UpdatingClause* GetUpdatingClause(idx_t i) const { return updating_clauses[i].get(); }

    bool            HasReturnClause() const { return return_clause != nullptr; }
    ReturnClause*   GetReturnClause() const { return return_clause.get(); }

private:
    vector<unique_ptr<QueryPart>>      query_parts;
    vector<unique_ptr<ReadingClause>>  reading_clauses;
    vector<unique_ptr<UpdatingClause>> updating_clauses;
    unique_ptr<ReturnClause>           return_clause;
};

} // namespace duckdb
