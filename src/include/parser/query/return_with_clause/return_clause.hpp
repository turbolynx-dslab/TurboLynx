//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query/return_with_clause/return_clause.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/query/return_with_clause/projection_body.hpp"
#include <memory>

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

class ReturnClause {
public:
    explicit ReturnClause(unique_ptr<ProjectionBody> body) : body(std::move(body)) {}
    virtual ~ReturnClause() = default;

    ProjectionBody* GetBody() const { return body.get(); }

private:
    unique_ptr<ProjectionBody> body;
};

} // namespace turbolynx
