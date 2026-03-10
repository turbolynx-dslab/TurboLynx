#pragma once

#include "common/types/value.hpp"
#include "common/types.hpp"
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// ---- Expression types ----
enum class BoundExpressionType : uint8_t {
    LITERAL        = 0,
    VARIABLE       = 1,   // whole node or rel reference
    PROPERTY       = 2,   // n.prop
    FUNCTION       = 3,   // scalar/cast/list
    AGG_FUNCTION   = 4,   // COUNT, SUM, ...
    COMPARISON     = 5,   // =, <>, <, >, <=, >=
    BOOL_OP        = 6,   // AND, OR, NOT
    CASE           = 7,   // CASE WHEN ... END
    NULL_OP        = 8,   // IS NULL / IS NOT NULL
    EXISTENTIAL    = 9,   // EXISTS { ... }
    PARAMETER      = 10,  // $param
    PATH           = 11,  // shortest path expression
    LIST_COMP      = 12,  // [x IN list WHERE ... | expr]
    ID_IN_COLL     = 13,  // id(x) IN list
};

class BoundExpression {
public:
    explicit BoundExpression(BoundExpressionType type, LogicalType data_type, string unique_name)
        : expr_type(type), data_type(std::move(data_type)), unique_name(std::move(unique_name)) {}
    virtual ~BoundExpression() = default;

    BoundExpressionType GetExprType()    const { return expr_type; }
    const LogicalType&  GetDataType()    const { return data_type; }
    const string&       GetUniqueName()  const { return unique_name; }

    bool HasAlias() const { return !alias.empty(); }
    const string& GetAlias() const { return alias; }
    void SetAlias(string a) { alias = std::move(a); }

    virtual shared_ptr<BoundExpression> Copy() const = 0;

protected:
    BoundExpressionType expr_type;
    LogicalType         data_type;
    string              unique_name;
    string              alias;
};

using bound_expression_vector = vector<shared_ptr<BoundExpression>>;

} // namespace duckdb
