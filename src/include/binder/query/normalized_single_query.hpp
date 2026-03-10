#pragma once

#include "binder/query/normalized_query_part.hpp"

namespace duckdb {

class NormalizedSingleQuery {
public:
    NormalizedSingleQuery() = default;

    void AppendQueryPart(unique_ptr<NormalizedQueryPart> part) {
        query_parts.push_back(std::move(part));
    }

    idx_t GetNumQueryParts() const { return query_parts.size(); }
    NormalizedQueryPart* GetQueryPart(idx_t i) const { return query_parts[i].get(); }

    bool IsReadOnly() const {
        for (auto& p : query_parts) {
            if (p->HasUpdatingClause()) return false;
        }
        return true;
    }

private:
    vector<unique_ptr<NormalizedQueryPart>> query_parts;
};

} // namespace duckdb
