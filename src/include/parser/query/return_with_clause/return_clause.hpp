#pragma once

#include "parser/query/return_with_clause/projection_body.hpp"
#include <memory>

namespace duckdb {

class ReturnClause {
public:
    explicit ReturnClause(unique_ptr<ProjectionBody> body) : body(std::move(body)) {}
    virtual ~ReturnClause() = default;

    ProjectionBody* GetBody() const { return body.get(); }

private:
    unique_ptr<ProjectionBody> body;
};

} // namespace duckdb
