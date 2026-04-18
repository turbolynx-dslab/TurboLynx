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

enum class BoundClauseType : uint8_t { MATCH = 0, UNWIND = 1 };

class BoundReadingClause {
public:
    explicit BoundReadingClause(BoundClauseType clause_type) : clause_type(clause_type) {}
    virtual ~BoundReadingClause() = default;

    BoundClauseType GetClauseType() const { return clause_type; }

private:
    BoundClauseType clause_type;
};

} // namespace turbolynx
