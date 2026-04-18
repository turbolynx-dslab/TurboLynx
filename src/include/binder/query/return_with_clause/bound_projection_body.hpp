#pragma once

#include "binder/expression/bound_expression.hpp"

namespace duckdb {
}
namespace turbolynx {
}
namespace duckdb {
	using namespace turbolynx;
}
namespace turbolynx {
using namespace duckdb;

struct BoundOrderByItem {
    shared_ptr<BoundExpression> expr;
    bool                        ascending = true;
};

class BoundProjectionBody {
public:
    explicit BoundProjectionBody(bool is_distinct,
                                  bound_expression_vector projections)
        : is_distinct(is_distinct), projections(std::move(projections)),
          skip_number(UINT64_MAX), limit_number(UINT64_MAX) {}

    // Copyable (used by WITH clause normalization)
    BoundProjectionBody(const BoundProjectionBody&) = default;

    bool                           IsDistinct()       const { return is_distinct; }
    const bound_expression_vector& GetProjections()   const { return projections; }

    void SetOrderBy(vector<BoundOrderByItem> items) { order_by = std::move(items); }
    bool HasOrderBy() const { return !order_by.empty(); }
    const vector<BoundOrderByItem>& GetOrderBy() const { return order_by; }

    void SetSkipNumber(uint64_t n) { skip_number = n; }
    bool HasSkip() const { return skip_number != UINT64_MAX; }
    uint64_t GetSkipNumber() const { return skip_number; }

    void SetLimitNumber(uint64_t n) { limit_number = n; }
    bool HasLimit() const { return limit_number != UINT64_MAX; }
    uint64_t GetLimitNumber() const { return limit_number; }

    bool HasAggregation() const;

private:
    bool                    is_distinct;
    bound_expression_vector projections;
    vector<BoundOrderByItem> order_by;
    uint64_t                skip_number;
    uint64_t                limit_number;
};

} // namespace turbolynx
