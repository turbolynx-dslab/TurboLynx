#pragma once

#include "binder/query/reading_clause/bound_reading_clause.hpp"
#include "binder/query/updating_clause/bound_updating_clause.hpp"
#include "binder/query/return_with_clause/bound_projection_body.hpp"
#include "binder/expression/bound_expression.hpp"

namespace duckdb {

// A normalized query part is delimited by WITH clauses (or the final RETURN).
// Structure: [MATCH/UNWIND]* [SET/DELETE]* WITH/RETURN ...
class NormalizedQueryPart {
public:
    NormalizedQueryPart() = default;

    // ---- Reading clauses ----
    void AddReadingClause(unique_ptr<BoundReadingClause> clause) {
        reading_clauses.push_back(std::move(clause));
    }
    bool HasReadingClause()         const { return !reading_clauses.empty(); }
    idx_t GetNumReadingClauses()    const { return reading_clauses.size(); }
    BoundReadingClause* GetReadingClause(idx_t i) const { return reading_clauses[i].get(); }

    // ---- Updating clauses ----
    void AddUpdatingClause(unique_ptr<BoundUpdatingClause> clause) {
        updating_clauses.push_back(std::move(clause));
    }
    bool HasUpdatingClause()        const { return !updating_clauses.empty(); }
    idx_t GetNumUpdatingClauses()   const { return updating_clauses.size(); }
    BoundUpdatingClause* GetUpdatingClause(idx_t i) const { return updating_clauses[i].get(); }
    void ClearUpdatingClauses() { updating_clauses.clear(); }

    // ---- Projection body (WITH or RETURN) ----
    void SetProjectionBody(unique_ptr<BoundProjectionBody> body) {
        projection_body = std::move(body);
    }
    bool HasProjectionBody()        const { return projection_body != nullptr; }
    BoundProjectionBody* GetProjectionBody() const { return projection_body.get(); }

    // ---- WHERE predicate on WITH/RETURN ----
    void SetProjectionBodyPredicate(shared_ptr<BoundExpression> pred) {
        projection_body_predicate = std::move(pred);
    }
    bool HasProjectionBodyPredicate() const { return projection_body_predicate != nullptr; }
    BoundExpression* GetProjectionBodyPredicate() const { return projection_body_predicate.get(); }
    shared_ptr<BoundExpression> GetProjectionBodyPredicateShared() const { return projection_body_predicate; }

private:
    vector<unique_ptr<BoundReadingClause>>  reading_clauses;
    vector<unique_ptr<BoundUpdatingClause>> updating_clauses;
    unique_ptr<BoundProjectionBody>         projection_body;
    shared_ptr<BoundExpression>             projection_body_predicate;
};

} // namespace duckdb
