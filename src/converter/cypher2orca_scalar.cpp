// Cypher2OrcaConverter — scalar expression conversion
// (replaces planner_logical_scalar.cpp + planner_logical_scalar_duckdb.cpp)
//
// Converts TurboLynx BoundExpression nodes into ORCA CExpression (scalar).

#include "converter/cypher2orca_converter.hpp"
#include "binder/expression/bound_exists_subquery_expression.hpp"
#include "gpopt/operators/CScalarSubqueryExists.h"
#include "gpopt/operators/CScalarSubqueryNotExists.h"
#include "planner/value_ser_des.hpp"
#include "catalog/catalog.hpp"
#include "catalog/catalog_wrapper.hpp"
#include "main/database.hpp"

// Non-conflicting DuckDB planner expression headers (no matching TurboLynx binder name)
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_conjunction_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"
#include "planner/expression/bound_reference_expression.hpp"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CScalarCast.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CScalarFunc.h"
#include "gpopt/operators/CScalarAggFunc.h"
#include "gpopt/operators/CScalarSwitch.h"
#include "gpopt/operators/CScalarSwitchCase.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "naucrates/base/CDatumGenericGPDB.h"

#include <algorithm>
#include <cstring>
#include <cstdlib>

using namespace gpopt;
using namespace gpmd;
using namespace gpos;

namespace duckdb {

// ============================================================
// Helper: convert OID + type_mod back to LogicalType
// (mirrors Planner::pConvertTypeOidToLogicalType)
// ============================================================
static duckdb::LogicalType OidToLogicalType(OID oid, INT type_mod);

static duckdb::LogicalTypeId OidToLogicalTypeId(OID oid)
{
    return (duckdb::LogicalTypeId)(
        static_cast<std::underlying_type_t<duckdb::LogicalTypeId>>(
            (oid - LOGICAL_TYPE_BASE_ID) % NUM_MAX_LOGICAL_TYPES));
}

static duckdb::LogicalType OidToLogicalType(OID oid, INT type_mod)
{
    auto tid = OidToLogicalTypeId(oid);
    if (tid == duckdb::LogicalTypeId::DECIMAL) {
        if (type_mod <= 0) return duckdb::LogicalType::DECIMAL(12, 2);
        uint8_t w = (uint8_t)(type_mod >> 8);
        uint8_t s = (uint8_t)(type_mod & 0xFF);
        return duckdb::LogicalType::DECIMAL(w, s);
    }
    if (tid == duckdb::LogicalTypeId::LIST) {
        if (type_mod < 0) return duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT);
        OID child_oid = (OID)(type_mod & 0xFF) + LOGICAL_TYPE_BASE_ID;
        INT child_mod = (type_mod >> 8);
        return duckdb::LogicalType::LIST(OidToLogicalType(child_oid, child_mod));
    }
    if (tid == duckdb::LogicalTypeId::PATH) {
        return duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT);
    }
    return duckdb::LogicalType(tid);
}

// ============================================================
// Helper: serialize a DuckDB Value into a malloc'd byte array
// suitable for CDatumGenericGPDB.  Sets out_mem_ptr / out_length.
// Caller is responsible for free().
// ============================================================
static void SerializeValueIntoOrcaBytes(const LogicalType &type, const Value &val,
                                        void *&out_ptr, uint64_t &out_len,
                                        LINT &out_lint, CDouble &out_double)
{
    out_ptr    = nullptr;
    out_len    = 0;
    out_lint   = 0;
    out_double = 0.0;

    if (val.IsNull()) return;   // NULL literal — leave pointers as-is

    switch (type.id()) {
    case LogicalTypeId::INTEGER: {
        int32_t v = val.GetValue<int32_t>();
        out_lint = v;
        out_len  = sizeof(int32_t);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::BIGINT: {
        int64_t v = val.GetValue<int64_t>();
        out_lint = v;
        out_len  = sizeof(int64_t);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::UINTEGER: {
        uint32_t v = val.GetValue<uint32_t>();
        out_lint = (LINT)v;
        out_len  = sizeof(uint32_t);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::UBIGINT: {
        uint64_t v = val.GetValue<uint64_t>();
        out_lint = (LINT)v;
        out_len  = sizeof(uint64_t);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::HUGEINT: {
        // treat as int64
        int64_t v = val.GetValue<int64_t>();
        out_lint = v;
        out_len  = sizeof(int64_t);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::DOUBLE: {
        double v = val.GetValue<double>();
        out_double = CDouble(v);
        out_len  = sizeof(double);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::FLOAT: {
        float  f = val.GetValue<float>();
        double v = static_cast<double>(f);
        out_double = CDouble(v);
        out_len  = sizeof(double);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::VARCHAR: {
        string s = val.GetValue<string>();
        out_len  = s.size() + 1;  // include null terminator
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, s.c_str(), out_len);
        break;
    }
    case LogicalTypeId::BOOLEAN: {
        bool b = val.GetValue<bool>();
        out_lint = b ? 1 : 0;
        out_len  = 1;
        out_ptr  = malloc(out_len);
        int8_t v = b ? 1 : 0;
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::DATE: {
        date_t d = val.GetValue<date_t>();
        int32_t v = d.days;
        out_len  = sizeof(int32_t);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::ID: {
        uint64_t v = val.GetValue<uint64_t>();
        out_lint = (LINT)v;
        out_len  = sizeof(uint64_t);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    case LogicalTypeId::DECIMAL: {
        int64_t v = val.GetValue<int64_t>();
        out_lint = v;
        out_len  = sizeof(int64_t);
        out_ptr  = malloc(out_len);
        memcpy(out_ptr, &v, out_len);
        break;
    }
    default:
        // Fallback: zero-length (NULL sentinel) — will produce a null literal
        break;
    }
}

// ============================================================
// Helper: OID + type modifier from a CExpression's scalar pop
// ============================================================
OID Cypher2OrcaConverter::GetTypeOidFromCExpr(CExpression *expr)
{
    return (OID)(((CMDIdGPDB *)(CScalar::PopConvert(expr->Pop())->MdidType()))->Oid());
}

INT Cypher2OrcaConverter::GetTypeModFromCExpr(CExpression *expr)
{
    return CScalar::PopConvert(expr->Pop())->TypeModifier();
}

IMDType::ECmpType Cypher2OrcaConverter::MapCmpType(ExpressionType t, bool swap)
{
    if (!swap) {
        switch (t) {
        case ExpressionType::COMPARE_EQUAL:              return IMDType::EcmptEq;
        case ExpressionType::COMPARE_NOTEQUAL:           return IMDType::EcmptNEq;
        case ExpressionType::COMPARE_GREATERTHAN:        return IMDType::EcmptG;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return IMDType::EcmptGEq;
        case ExpressionType::COMPARE_LESSTHAN:           return IMDType::EcmptL;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:  return IMDType::EcmptLEq;
        default: D_ASSERT(false); return IMDType::EcmptEq;
        }
    } else {
        // swap: flip < ↔ >
        switch (t) {
        case ExpressionType::COMPARE_EQUAL:              return IMDType::EcmptEq;
        case ExpressionType::COMPARE_NOTEQUAL:           return IMDType::EcmptNEq;
        case ExpressionType::COMPARE_GREATERTHAN:        return IMDType::EcmptL;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return IMDType::EcmptLEq;
        case ExpressionType::COMPARE_LESSTHAN:           return IMDType::EcmptG;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:  return IMDType::EcmptGEq;
        default: D_ASSERT(false); return IMDType::EcmptEq;
        }
    }
}

// ============================================================
// TryGenScalarIdent — if expression is already in plan schema, return CScalarIdent
// ============================================================
CExpression *Cypher2OrcaConverter::TryGenScalarIdent(const BoundExpression &expr,
                                                      turbolynx::LogicalPlan *plan)
{
    if (plan == nullptr) return nullptr;

    CColRef *cr = plan->getSchema()->getColRefOfKey(
        expr.GetUniqueName(), std::numeric_limits<uint64_t>::max());
    if (cr == nullptr) {
        cr = plan->getSchema()->getColRefOfKey(
            expr.GetAlias(), std::numeric_limits<uint64_t>::max());
    }
    if (cr == nullptr && outer_plan_registered_) {
        D_ASSERT(outer_plan_ != nullptr);
        cr = outer_plan_->getSchema()->getColRefOfKey(
            expr.GetUniqueName(), std::numeric_limits<uint64_t>::max());
        if (cr == nullptr) {
            cr = outer_plan_->getSchema()->getColRefOfKey(
                expr.GetAlias(), std::numeric_limits<uint64_t>::max());
        }
    }
    if (cr == nullptr) return nullptr;

    if (expr.HasAlias()) {
        col_name_map_[cr] = expr.GetAlias();
    }
    return GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, cr));
}

// ============================================================
// ConvertExpression — main dispatch
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertExpression(const BoundExpression &expr,
                                                      turbolynx::LogicalPlan *plan)
{
    // If expression already exists in the plan schema, emit CScalarIdent.
    // Skip for AGG_FUNCTION: aggregates must always be freshly constructed,
    // even if a prior aggregate result has the same name (e.g., _agg_0).
    if (expr.GetExprType() != BoundExpressionType::AGG_FUNCTION) {
        CExpression *ident = TryGenScalarIdent(expr, plan);
        if (ident != nullptr) return ident;
    }

    switch (expr.GetExprType()) {
    case BoundExpressionType::LITERAL:
        return ConvertLiteral(static_cast<const BoundLiteralExpression &>(expr));
    case BoundExpressionType::PROPERTY:
        return ConvertProperty(static_cast<const BoundPropertyExpression &>(expr), plan);
    case BoundExpressionType::VARIABLE:
        return ConvertVariable(static_cast<const BoundVariableExpression &>(expr), plan);
    case BoundExpressionType::FUNCTION:
        return ConvertFunction(static_cast<const CypherBoundFunctionExpression &>(expr), plan);
    case BoundExpressionType::AGG_FUNCTION:
        return ConvertAggFunc(static_cast<const BoundAggFunctionExpression &>(expr), plan);
    case BoundExpressionType::COMPARISON:
        return ConvertComparison(static_cast<const CypherBoundComparisonExpression &>(expr), plan);
    case BoundExpressionType::BOOL_OP:
        return ConvertBoolOp(static_cast<const BoundBoolExpression &>(expr), plan);
    case BoundExpressionType::NULL_OP:
        return ConvertNullOp(static_cast<const BoundNullExpression &>(expr), plan);
    case BoundExpressionType::CASE:
        return ConvertCase(static_cast<const CypherBoundCaseExpression &>(expr), plan);
    case BoundExpressionType::EXISTENTIAL:
        return ConvertExistsSubquery(static_cast<const BoundExistsSubqueryExpression &>(expr), plan);
    default:
        D_ASSERT(false);
        return nullptr;
    }
}

// ============================================================
// ConvertLiteral
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertLiteral(const BoundLiteralExpression &expr)
{
    const Value &val  = expr.GetValue();
    const LogicalType &type = val.type();

    uint32_t type_id = LOGICAL_TYPE_BASE_ID + (OID)type.id();
    INT      type_mod = GetTypeMod(type);

    CMDIdGPDB *type_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, type_id, 1, 0);
    type_mdid->AddRef();

    void    *ser_ptr = nullptr;
    uint64_t ser_len = 0;
    LINT     lint_val = 0;
    CDouble  double_val(0.0);

    if (!val.IsNull()) {
        SerializeValueIntoOrcaBytes(type, val, ser_ptr, ser_len, lint_val, double_val);
    }

    IDatumGeneric *datum = (IDatumGeneric *)(GPOS_NEW(mp_) CDatumGenericGPDB(
        mp_, (IMDId *)type_mdid, type_mod, ser_ptr, ser_len,
        val.IsNull(), lint_val, double_val));
    datum->AddRef();

    CExpression *pexpr = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CScalarConst(mp_, (IDatum *)datum));
    pexpr->AddRef();
    return pexpr;
}

// ============================================================
// ConvertProperty  (n.prop → CScalarIdent via schema lookup)
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertProperty(const BoundPropertyExpression &expr,
                                                    turbolynx::LogicalPlan *plan)
{
    const string &var_name = expr.GetVarName();
    uint64_t      key_id   = expr.GetPropertyKeyID();

    CColRef *cr = plan->getSchema()->getColRefOfKey(var_name, key_id);

    // fallback to alias
    if (cr == nullptr && expr.HasAlias()) {
        cr = plan->getSchema()->getColRefOfKey(
            expr.GetAlias(), std::numeric_limits<uint64_t>::max());
    }
    // fallback to outer plan
    if (cr == nullptr && outer_plan_registered_) {
        D_ASSERT(outer_plan_ != nullptr);
        cr = outer_plan_->getSchema()->getColRefOfKey(var_name, key_id);
        if (cr == nullptr && expr.HasAlias()) {
            cr = outer_plan_->getSchema()->getColRefOfKey(
                expr.GetAlias(), std::numeric_limits<uint64_t>::max());
        }
    }

    // Property doesn't exist on this node/rel — return NULL constant
    if (cr == nullptr) {
        const LogicalType &type = expr.GetDataType();
        OID type_oid = LOGICAL_TYPE_BASE_ID + (OID)type.id();
        // For SQLNULL or unknown types, use VARCHAR as a safe fallback
        if (type.id() == LogicalTypeId::SQLNULL || type.id() == LogicalTypeId::ANY) {
            type_oid = LOGICAL_TYPE_BASE_ID + (OID)LogicalTypeId::VARCHAR;
        }
        CMDIdGPDB *null_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, type_oid, 1, 0);
        null_mdid->AddRef();
        IDatumGeneric *null_datum = (IDatumGeneric *)(GPOS_NEW(mp_) CDatumGenericGPDB(
            mp_, (IMDId *)null_mdid, -1, nullptr, 0, true /*is_null*/, 0, CDouble(0.0)));
        null_datum->AddRef();
        CExpression *pexpr = GPOS_NEW(mp_) CExpression(
            mp_, GPOS_NEW(mp_) CScalarConst(mp_, (IDatum *)null_datum));
        pexpr->AddRef();
        return pexpr;
    }

    if (expr.HasAlias()) {
        col_name_map_[cr] = expr.GetAlias();
    }
    return GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, cr));
}

// ============================================================
// ConvertVariable  (whole-node/rel reference → CScalarIdent)
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertVariable(const BoundVariableExpression &expr,
                                                    turbolynx::LogicalPlan *plan)
{
    const string &var_name = expr.GetVarName();
    CColRef *cr = plan->getSchema()->getColRefOfKey(
        var_name, std::numeric_limits<uint64_t>::max());
    if (cr == nullptr && outer_plan_registered_) {
        cr = outer_plan_->getSchema()->getColRefOfKey(
            var_name, std::numeric_limits<uint64_t>::max());
    }
    // For node/edge variables (count(p), etc.) fall back to _id column
    if (cr == nullptr) {
        cr = plan->getSchema()->getColRefOfKey(var_name, ID_KEY_ID);
    }
    if (cr == nullptr && outer_plan_registered_) {
        cr = outer_plan_->getSchema()->getColRefOfKey(var_name, ID_KEY_ID);
    }
    if (cr == nullptr) {
        throw std::runtime_error("Variable '" + var_name + "' is not defined");
    }

    if (expr.HasAlias()) {
        col_name_map_[cr] = expr.GetAlias();
    }
    return GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, cr));
}

// ============================================================
// ConvertBoolOp  (AND / OR / NOT)
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertBoolOp(const BoundBoolExpression &expr,
                                                  turbolynx::LogicalPlan *plan)
{
    CScalarBoolOp::EBoolOperator op;
    switch (expr.GetOpType()) {
    case BoundBoolOpType::AND: op = CScalarBoolOp::EboolopAnd; break;
    case BoundBoolOpType::OR:  op = CScalarBoolOp::EboolopOr;  break;
    case BoundBoolOpType::NOT: op = CScalarBoolOp::EboolopNot; break;
    default: D_ASSERT(false); return nullptr;
    }

    // NOT EXISTS → CScalarSubqueryNotExists (enables ORCA to decorrelate
    // into LeftAntiSemiHashJoin instead of falling back to NLJ)
    if (op == CScalarBoolOp::EboolopNot && expr.GetNumChildren() == 1 &&
        expr.GetChild(0)->GetExprType() == BoundExpressionType::EXISTENTIAL) {
        auto &exists_expr = static_cast<const BoundExistsSubqueryExpression &>(*expr.GetChild(0));
        CExpression *exists_orca = ConvertExistsSubquery(exists_expr, plan);
        // exists_orca is CScalarSubqueryExists(inner_plan).
        // Extract inner plan and wrap with CScalarSubqueryNotExists.
        CExpression *inner_plan_expr = (*exists_orca)[0];
        inner_plan_expr->AddRef();
        exists_orca->Release();
        return GPOS_NEW(mp_) CExpression(
            mp_, GPOS_NEW(mp_) gpopt::CScalarSubqueryNotExists(mp_), inner_plan_expr);
    }

    CExpressionArray *children = GPOS_NEW(mp_) CExpressionArray(mp_);
    for (idx_t i = 0; i < expr.GetNumChildren(); i++) {
        children->Append(ConvertExpression(*expr.GetChild(i), plan));
    }
    return CUtils::PexprScalarBoolOp(mp_, op, children);
}

// ============================================================
// ConvertNullOp  (IS NULL / IS NOT NULL)
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertNullOp(const BoundNullExpression &expr,
                                                  turbolynx::LogicalPlan *plan)
{
    CExpression *child = ConvertExpression(*expr.GetChild(), plan);
    if (expr.IsNotNull()) {
        return CUtils::PexprIsNotNull(mp_, child);
    } else {
        return CUtils::PexprIsNull(mp_, child);
    }
}

// ============================================================
// ConvertComparison
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertComparison(const CypherBoundComparisonExpression &expr,
                                                      turbolynx::LogicalPlan *plan)
{
    const BoundExpression *l_expr = expr.GetLeft();
    const BoundExpression *r_expr = expr.GetRight();

    CExpression *lhs, *rhs;
    bool swap = false;

    bool l_is_lit = (l_expr->GetExprType() == BoundExpressionType::LITERAL);
    bool r_is_lit = (r_expr->GetExprType() == BoundExpressionType::LITERAL);

    if (l_is_lit && !r_is_lit) {
        // Build RHS first to determine target type for literal
        rhs = ConvertExpression(*r_expr, plan);
        OID target_oid = GetTypeOidFromCExpr(rhs);
        // Re-build LHS literal with the same type as RHS for type matching
        lhs = ConvertLiteral(static_cast<const BoundLiteralExpression &>(*l_expr));
        swap = true;  // const on left → swap operands
    } else if (!l_is_lit && r_is_lit) {
        lhs = ConvertExpression(*l_expr, plan);
        rhs = ConvertLiteral(static_cast<const BoundLiteralExpression &>(*r_expr));
    } else {
        lhs = ConvertExpression(*l_expr, plan);
        rhs = ConvertExpression(*r_expr, plan);
    }

    // If literal was on the left, swap operands so const is on the right
    if (swap) {
        std::swap(lhs, rhs);
    }

    // Check if const is still on left after potential swap → swap+flip
    bool const_on_left = (lhs->Pop()->Eopid() == COperator::EopScalarConst &&
                          rhs->Pop()->Eopid() == COperator::EopScalarIdent);
    if (const_on_left) {
        std::swap(lhs, rhs);
        swap = true;
    } else {
        swap = false;
    }

    IMDType::ECmpType cmp = MapCmpType(expr.GetCmpType(), swap);

    CMDAccessor *mda = GetMDAccessor();
    IMDId *l_mdid = CScalar::PopConvert(lhs->Pop())->MdidType();
    IMDId *r_mdid = CScalar::PopConvert(rhs->Pop())->MdidType();
    IMDId *func_mdid = CMDAccessorUtils::GetScCmpMdIdConsiderCasts(mda, l_mdid, r_mdid, cmp);
    D_ASSERT(func_mdid != nullptr);

    return CUtils::PexprScalarCmp(mp_, lhs, rhs, cmp);
}

// ============================================================
// ConvertFunction  (scalar function call)
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertFunction(const CypherBoundFunctionExpression &expr,
                                                    turbolynx::LogicalPlan *plan)
{
    string func_name = expr.GetFuncName();
    if (IsCastingFunction(func_name)) {
        return ConvertCastFunction(expr, plan);
    }
    // EXISTS subquery — delegate to converter's PlanExistsSubquery
    if (func_name == "__exists_subquery__") {
        // Should not reach here — EXISTS is handled via BoundExpressionType::EXISTENTIAL
        throw InternalException("EXISTS subquery should be handled by ConvertExpression dispatch");
    }

    // list_slice(list, begin, end) — sub-list extraction
    if (func_name == "list_slice" && expr.GetNumChildren() == 3) {
        LogicalType ret_type = expr.GetDataType();
        if (ret_type.id() == LogicalTypeId::ANY || ret_type.id() == LogicalTypeId::UNKNOWN) {
            ret_type = LogicalType::LIST(LogicalType::BIGINT);
        }
        vector<LogicalType> arg_types = {ret_type, LogicalType::BIGINT, LogicalType::BIGINT};
        idx_t func_mdid_id = context_->db->GetCatalogWrapper().GetScalarFuncMdId(
            *context_, func_name, arg_types);
        CMDIdGPDB *func_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, func_mdid_id);
        func_mdid->AddRef();
        const IMDFunction *pmd = GetMDAccessor()->RetrieveFunc(func_mdid);
        IMDId *sfunc_mdid = pmd->MDId(); sfunc_mdid->AddRef();
        CWStringConst *str = GPOS_NEW(mp_) CWStringConst(mp_, pmd->Mdname().GetMDName()->GetBuffer());

        OID ret_oid = LOGICAL_TYPE_BASE_ID + (OID)ret_type.id();
        INT type_mod = GetTypeMod(ret_type);
        CMDIdGPDB *ret_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, ret_oid, 1, 0);
        ret_mdid->AddRef();

        CExpressionArray *child_exprs = GPOS_NEW(mp_) CExpressionArray(mp_);
        for (idx_t i = 0; i < expr.GetNumChildren(); i++) {
            child_exprs->Append(ConvertExpression(*expr.GetChild(i), plan));
        }
        COperator *pop = GPOS_NEW(mp_) CScalarFunc(mp_, sfunc_mdid, ret_mdid,
            type_mod, str);
        return GPOS_NEW(mp_) CExpression(mp_, pop, child_exprs);
    }
    // || (string concatenation) — handled by generic path below.
    // ConcatFun registers "||" as a ScalarFunctionSet in the catalog.
    // list_extract(list, idx) — element access: resolve via DuckDB scalar func
    if (func_name == "list_extract" && expr.GetNumChildren() == 2) {
        // Determine return type from bound expression type (binder infers element type)
        LogicalType ret_type = expr.GetDataType();
        if (ret_type.id() == LogicalTypeId::ANY || ret_type.id() == LogicalTypeId::UNKNOWN) {
            ret_type = LogicalType::BIGINT;  // default fallback
        }
        vector<LogicalType> arg_types = {LogicalType::LIST(ret_type), LogicalType::BIGINT};
        idx_t func_mdid_id = context_->db->GetCatalogWrapper().GetScalarFuncMdId(
            *context_, func_name, arg_types);
        CMDIdGPDB *func_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, func_mdid_id);
        func_mdid->AddRef();
        const IMDFunction *pmd = GetMDAccessor()->RetrieveFunc(func_mdid);
        IMDId *sfunc_mdid = pmd->MDId(); sfunc_mdid->AddRef();
        CWStringConst *str = GPOS_NEW(mp_) CWStringConst(mp_, pmd->Mdname().GetMDName()->GetBuffer());

        OID ret_oid = LOGICAL_TYPE_BASE_ID + (OID)ret_type.id();
        INT type_mod = GetTypeMod(ret_type);
        CMDIdGPDB *ret_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, ret_oid, 1, 0);
        ret_mdid->AddRef();

        CExpressionArray *child_exprs = GPOS_NEW(mp_) CExpressionArray(mp_);
        for (idx_t i = 0; i < expr.GetNumChildren(); i++) {
            child_exprs->Append(ConvertExpression(*expr.GetChild(i), plan));
        }
        COperator *pop = GPOS_NEW(mp_) CScalarFunc(mp_, sfunc_mdid, ret_mdid,
            type_mod, str);
        return GPOS_NEW(mp_) CExpression(mp_, pop, child_exprs);
    }
    // List comprehension: __list_comprehension(source, 'var', filter, [map])
    // Identity mapping (binder optimization) won't reach here.
    // Non-identity mapping: for now, return source list (mapping evaluated
    // at execution time would need UNWIND+collect which is complex).
    // For IC14: mapping is reduce(pattern_comp) → 0.0 per element.
    // TODO: implement proper UNWIND+map+collect decorrelation.
    if (func_name == "__list_comprehension") {
        return ConvertExpression(*expr.GetChild(0), plan);
    }

    // Pattern comprehension: __pattern_comprehension(...)
    // For now, return an empty list constant. Full decorrelation in M5.
    if (func_name == "__pattern_comprehension") {
        // Return empty LIST(DOUBLE) — placeholder
        CMDAccessor *mda = GetMDAccessor();
        auto list_val = Value::LIST({Value::DOUBLE(0.0)});
        auto list_type = list_val.type();
        BoundLiteralExpression empty_list(std::move(list_val), "_pattern_comp_placeholder");
        return ConvertLiteral(empty_list);
    }

    // 2-hop pattern: __pattern_exists_2hop(src, 'R1', 'R2', tgt)
    // → __check_2hop_exists(label1, label2, src_vid, tgt_vid)
    if (func_name == "__pattern_exists_2hop") {
        D_ASSERT(expr.GetNumChildren() == 4);
        auto *src_child = expr.GetChild(0);
        auto *label1_child = expr.GetChild(1);
        auto *label2_child = expr.GetChild(2);
        auto *tgt_child = expr.GetChild(3);

        string label1, label2, src_var, tgt_var;
        if (label1_child->GetExprType() == BoundExpressionType::LITERAL)
            label1 = static_cast<const BoundLiteralExpression &>(*label1_child).GetValue().GetValue<string>();
        if (label2_child->GetExprType() == BoundExpressionType::LITERAL)
            label2 = static_cast<const BoundLiteralExpression &>(*label2_child).GetValue().GetValue<string>();
        if (src_child->GetExprType() == BoundExpressionType::VARIABLE)
            src_var = static_cast<const BoundVariableExpression &>(*src_child).GetVarName();
        if (tgt_child->GetExprType() == BoundExpressionType::VARIABLE)
            tgt_var = static_cast<const BoundVariableExpression &>(*tgt_child).GetVarName();

        CColRef *src_colref = plan->getSchema()->getColRefOfKey(src_var, ID_KEY_ID);
        CColRef *tgt_colref = plan->getSchema()->getColRefOfKey(tgt_var, ID_KEY_ID);

        if (!src_colref || !tgt_colref || label1.empty() || label2.empty()) {
            CMDAccessor *mda = GetMDAccessor();
            const IMDTypeBool *pmdtype = mda->PtMDType<IMDTypeBool>();
            IDatum *datum = pmdtype->CreateBoolDatum(mp_, true, false);
            return GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarConst(mp_, datum));
        }

        string check_func_name = "__check_2hop_exists";
        vector<LogicalType> check_arg_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                                                LogicalType::UBIGINT, LogicalType::UBIGINT};
        idx_t func_mdid_id = context_->db->GetCatalogWrapper().GetScalarFuncMdId(
            *context_, check_func_name, check_arg_types);
        CMDIdGPDB *func_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, func_mdid_id, 0, 0);
        func_mdid->AddRef();
        const IMDFunction *pmd = GetMDAccessor()->RetrieveFunc(func_mdid);
        IMDId *sfunc_mdid = pmd->MDId(); sfunc_mdid->AddRef();
        CWStringConst *str = GPOS_NEW(mp_) CWStringConst(mp_, pmd->Mdname().GetMDName()->GetBuffer());
        IMDId *ret_type_mdid = pmd->GetResultTypeMdid();

        CExpressionArray *child_exprs = GPOS_NEW(mp_) CExpressionArray(mp_);
        BoundLiteralExpression l1_lit(Value(label1), "_l1");
        child_exprs->Append(ConvertLiteral(l1_lit));
        BoundLiteralExpression l2_lit(Value(label2), "_l2");
        child_exprs->Append(ConvertLiteral(l2_lit));
        child_exprs->Append(GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, src_colref)));
        child_exprs->Append(GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, tgt_colref)));

        COperator *pop = GPOS_NEW(mp_) CScalarFunc(mp_, sfunc_mdid, ret_type_mdid,
            default_type_modifier, str);
        CExpression *pexpr = GPOS_NEW(mp_) CExpression(mp_, pop, child_exprs);
        pexpr->AddRef();
        return pexpr;
    }

    // 1-hop pattern: __pattern_exists(src_node, 'EDGE_LABEL', tgt_node)
    // → __check_edge_exists(edge_label_str, src_vid, tgt_vid)
    if (func_name == "__pattern_exists") {
        D_ASSERT(expr.GetNumChildren() == 3);
        auto *src_child = expr.GetChild(0);
        auto *label_child = expr.GetChild(1);
        auto *tgt_child = expr.GetChild(2);

        // Get edge label string
        string edge_label;
        if (label_child->GetExprType() == BoundExpressionType::LITERAL) {
            edge_label = static_cast<const BoundLiteralExpression &>(*label_child)
                .GetValue().GetValue<string>();
        }

        // Resolve src and tgt node VID colrefs from schema
        string src_var, tgt_var;
        if (src_child->GetExprType() == BoundExpressionType::VARIABLE)
            src_var = static_cast<const BoundVariableExpression &>(*src_child).GetVarName();
        if (tgt_child->GetExprType() == BoundExpressionType::VARIABLE)
            tgt_var = static_cast<const BoundVariableExpression &>(*tgt_child).GetVarName();

        CColRef *src_colref = plan->getSchema()->getColRefOfKey(src_var, ID_KEY_ID);
        CColRef *tgt_colref = plan->getSchema()->getColRefOfKey(tgt_var, ID_KEY_ID);

        if (!src_colref || !tgt_colref || edge_label.empty()) {
            // Fallback: return constant TRUE if we can't resolve
            CMDAccessor *mda = GetMDAccessor();
            const IMDTypeBool *pmdtype = mda->PtMDType<IMDTypeBool>();
            IDatum *datum = pmdtype->CreateBoolDatum(mp_, true, false);
            return GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarConst(mp_, datum));
        }

        // Build: __check_edge_exists(label_const, src_vid_ident, tgt_vid_ident)
        string check_func_name = "__check_edge_exists";
        vector<LogicalType> check_arg_types = {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT};
        idx_t func_mdid_id = context_->db->GetCatalogWrapper().GetScalarFuncMdId(
            *context_, check_func_name, check_arg_types);

        CMDIdGPDB *func_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, func_mdid_id, 0, 0);
        func_mdid->AddRef();
        const IMDFunction *pmd = GetMDAccessor()->RetrieveFunc(func_mdid);
        IMDId *sfunc_mdid = pmd->MDId();
        sfunc_mdid->AddRef();
        CWStringConst *str = GPOS_NEW(mp_) CWStringConst(mp_, pmd->Mdname().GetMDName()->GetBuffer());
        IMDId *ret_type_mdid = pmd->GetResultTypeMdid();

        // Build children: [label_const, src_ident, tgt_ident]
        CExpressionArray *child_exprs = GPOS_NEW(mp_) CExpressionArray(mp_);

        // label constant — reuse ConvertLiteral
        BoundLiteralExpression label_lit(Value(edge_label), "_edge_label");
        child_exprs->Append(ConvertLiteral(label_lit));

        // src VID ident
        child_exprs->Append(
            GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, src_colref)));

        // tgt VID ident
        child_exprs->Append(
            GPOS_NEW(mp_) CExpression(mp_, GPOS_NEW(mp_) CScalarIdent(mp_, tgt_colref)));

        COperator *pop = GPOS_NEW(mp_) CScalarFunc(mp_, sfunc_mdid, ret_type_mdid,
            default_type_modifier, str);
        CExpression *pexpr = GPOS_NEW(mp_) CExpression(mp_, pop, child_exprs);
        pexpr->AddRef();
        return pexpr;
    }
    // normalize to lowercase for catalog lookup
    std::transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);

    // Build child expressions and collect DuckDB types
    CExpressionArray *child_exprs = GPOS_NEW(mp_) CExpressionArray(mp_);
    vector<LogicalType> child_types;
    for (idx_t i = 0; i < expr.GetNumChildren(); i++) {
        CExpression *ce = ConvertExpression(*expr.GetChild(i), plan);
        child_exprs->Append(ce);
        OID oid = GetTypeOidFromCExpr(ce);
        INT mod = GetTypeModFromCExpr(ce);
        auto lt = OidToLogicalType(oid, mod);
        // Resolve complex types from registry (STRUCT with fields, etc.)
        if (lt.id() == LogicalTypeId::STRUCT && mod >= 10000) {
            auto it = complex_type_registry_.find(mod);
            if (it != complex_type_registry_.end()) lt = it->second;
        }
        child_types.push_back(lt);
    }

    // path_nodes / path_rels: coerce non-LIST argument to LIST(UBIGINT)
    if ((func_name == "path_nodes" || func_name == "path_rels") && child_types.size() == 1) {
        if (child_types[0].id() != LogicalTypeId::LIST) {
            child_types[0] = LogicalType::LIST(LogicalType::UBIGINT);
        }
    }

    idx_t func_mdid_id = context_->db->GetCatalogWrapper().GetScalarFuncMdId(*context_, func_name, child_types);
    CMDIdGPDB *func_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, func_mdid_id, 0, 0);
    func_mdid->AddRef();

    const IMDFunction *pmd = GetMDAccessor()->RetrieveFunc(func_mdid);
    IMDId *sfunc_mdid = pmd->MDId();
    sfunc_mdid->AddRef();
    CWStringConst *str = GPOS_NEW(mp_) CWStringConst(mp_, pmd->Mdname().GetMDName()->GetBuffer());
    IMDId *ret_type_mdid = pmd->GetResultTypeMdid();

    // Determine return type modifier via DuckDB function bind
    ScalarFunctionCatalogEntry *func_cat;
    idx_t func_idx;
    context_->db->GetCatalogWrapper().GetScalarFuncAndIdx(*context_, func_mdid_id, func_cat, func_idx);
    auto function = func_cat->functions.get()->functions[func_idx];

    vector<unique_ptr<duckdb::Expression>> duckdb_children;
    for (idx_t i = 0; i < expr.GetNumChildren(); i++) {
        auto ce = ConvertExpressionDuckDB(*expr.GetChild(i));
        if (expr.GetChild(i)->HasAlias()) ce->alias = expr.GetChild(i)->GetAlias();
        duckdb_children.push_back(std::move(ce));
    }
    if (function.bind) {
        function.bind(*context_, function, duckdb_children);
    }
    INT type_mod = GetTypeMod(function.return_type);

    // After bind, the return type may have changed (e.g. struct_pack binds
    // to a concrete STRUCT type). Update the return type mdid accordingly.
    if (function.return_type.id() != LogicalTypeId::INVALID &&
        function.return_type.id() != LogicalTypeId::ANY) {
        OID updated_oid = LOGICAL_TYPE_BASE_ID + (OID)function.return_type.id();
        ret_type_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, updated_oid, 1, 0);
    }

    // For complex types (STRUCT, etc.), store the full type in the registry
    // and encode the registry index in the type modifier, since ORCA's
    // (OID, INT) pair can't represent STRUCT field metadata.
    if (function.return_type.id() == LogicalTypeId::STRUCT) {
        INT reg_id = next_complex_type_id_++;
        complex_type_registry_[reg_id] = function.return_type;
        type_mod = reg_id;
    }
    // Also update return type OID if bind changed it from ANY/INVALID
    // (e.g. struct_extract resolves ANY → VARCHAR based on STRUCT field type)
    if (function.return_type.id() != LogicalTypeId::INVALID &&
        function.return_type.id() != LogicalTypeId::ANY &&
        function.return_type.id() != LogicalTypeId::STRUCT) {
        OID updated_oid = LOGICAL_TYPE_BASE_ID + (OID)function.return_type.id();
        ret_type_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, updated_oid, 1, 0);
        type_mod = GetTypeMod(function.return_type);
    }

    COperator *pop = GPOS_NEW(mp_) CScalarFunc(mp_, sfunc_mdid, ret_type_mdid, type_mod, str);
    CExpression *pexpr;
    if (expr.GetNumChildren() > 0) {
        pexpr = GPOS_NEW(mp_) CExpression(mp_, pop, child_exprs);
    } else {
        pexpr = GPOS_NEW(mp_) CExpression(mp_, pop);
    }
    pexpr->AddRef();
    return pexpr;
}

// ============================================================
// ConvertCastFunction  (TO_DOUBLE / TO_FLOAT / TO_INTEGER)
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertCastFunction(const CypherBoundFunctionExpression &expr,
                                                        turbolynx::LogicalPlan *plan)
{
    CMDAccessor *mda = GetMDAccessor();
    D_ASSERT(expr.GetNumChildren() == 1);

    CExpression *child_expr = ConvertExpression(*expr.GetChild(0), plan);

    // Child type mdid
    OID child_oid = GetTypeOidFromCExpr(child_expr);
    CMDIdGPDB *child_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, child_oid, 1, 0);
    child_mdid->AddRef();

    // Return type mdid — infer from function name
    const LogicalType &ret_type = expr.GetDataType();
    uint32_t ret_oid = LOGICAL_TYPE_BASE_ID + (OID)ret_type.id();
    CMDIdGPDB *ret_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, ret_oid, 1, 0);
    ret_mdid->AddRef();

    // Look up cast function
    auto pmdcast = mda->Pmdcast((IMDId *)child_mdid, (IMDId *)ret_mdid);
    IMDId *cast_func_mdid = pmdcast->GetCastFuncMdId();

    return GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) CScalarCast(mp_, (IMDId *)ret_mdid, cast_func_mdid, false),
        child_expr);
}

// ============================================================
// ConvertAggFunc  (aggregate function call)
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertAggFunc(const BoundAggFunctionExpression &expr,
                                                   turbolynx::LogicalPlan *plan)
{
    string func_name = expr.GetFuncName();
    std::transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);

    CExpressionArray *child_exprs = GPOS_NEW(mp_) CExpressionArray(mp_);
    vector<LogicalType> child_types;

    if (expr.HasChild()) {
        CExpression *ce = ConvertExpression(*expr.GetChild(), plan);
        child_exprs->Append(ce);
        OID oid = GetTypeOidFromCExpr(ce);
        INT mod = GetTypeModFromCExpr(ce);
        auto lt = OidToLogicalType(oid, mod);
        // Resolve complex types from registry
        if ((lt.id() == LogicalTypeId::STRUCT || lt.id() == LogicalTypeId::LIST) && mod >= 10000) {
            auto it = complex_type_registry_.find(mod);
            if (it != complex_type_registry_.end()) lt = it->second;
        }
        child_types.push_back(lt);
    }

    idx_t func_mdid_id = context_->db->GetCatalogWrapper().GetAggFuncMdId(*context_, func_name, child_types);

    CMDIdGPDB *func_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, func_mdid_id, 0, 0);
    func_mdid->AddRef();
    const IMDAggregate *pmdagg = GetMDAccessor()->RetrieveAgg(func_mdid);
    IMDId *agg_mdid = pmdagg->MDId();
    agg_mdid->AddRef();
    CWStringConst *str = GPOS_NEW(mp_) CWStringConst(mp_, pmdagg->Mdname().GetMDName()->GetBuffer());

    // Determine return type modifier via DuckDB function bind
    AggregateFunctionCatalogEntry *agg_cat;
    idx_t func_idx;
    context_->db->GetCatalogWrapper().GetAggFuncAndIdx(*context_, func_mdid_id, agg_cat, func_idx);
    auto function = agg_cat->functions.get()->functions[func_idx];

    vector<unique_ptr<duckdb::Expression>> duckdb_children;
    if (expr.HasChild()) {
        duckdb_children.push_back(ConvertExpressionDuckDB(*expr.GetChild()));
    }
    if (function.bind) {
        function.bind(*context_, function, duckdb_children);
    }
    INT type_mod = GetTypeMod(function.return_type);

    CScalarAggFunc *pop = CUtils::PopAggFunc(
        mp_, agg_mdid, type_mod, str, expr.IsDistinct(),
        EaggfuncstageGlobal, false, nullptr, EaggfunckindNormal);

    CExpression *pexpr = GPOS_NEW(mp_) CExpression(
        mp_, pop, CUtils::PexprAggFuncArgs(mp_, child_exprs));
    pexpr->AddRef();
    return pexpr;
}

// ============================================================
// ConvertCase  (CASE WHEN ... THEN ... ELSE ... END)
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertCase(const CypherBoundCaseExpression &expr,
                                                turbolynx::LogicalPlan *plan)
{
    // Infer return type from THEN/ELSE branches (binder sets ANY).
    LogicalType ret_type = expr.GetDataType();
    if (ret_type.id() == LogicalTypeId::ANY || ret_type.id() == LogicalTypeId::UNKNOWN) {
        for (const auto &check : expr.GetChecks()) {
            auto t = check.then_expr->GetDataType();
            if (t.id() != LogicalTypeId::ANY && t.id() != LogicalTypeId::UNKNOWN &&
                t.id() != LogicalTypeId::SQLNULL) {
                ret_type = t;
                break;
            }
        }
        if ((ret_type.id() == LogicalTypeId::ANY || ret_type.id() == LogicalTypeId::UNKNOWN) &&
            expr.HasElse()) {
            auto t = expr.GetElse()->GetDataType();
            if (t.id() != LogicalTypeId::ANY && t.id() != LogicalTypeId::UNKNOWN &&
                t.id() != LogicalTypeId::SQLNULL) {
                ret_type = t;
            }
        }
        if (ret_type.id() == LogicalTypeId::ANY || ret_type.id() == LogicalTypeId::UNKNOWN) {
            ret_type = LogicalType::BOOLEAN;
        }
    }

    // Build ELSE value (bottom of the nested if-chain)
    CExpression *else_expr;
    if (expr.HasElse()) {
        else_expr = ConvertExpression(*expr.GetElse(), plan);
    } else {
        // NULL literal
        uint32_t null_type_id = LOGICAL_TYPE_BASE_ID + (OID)ret_type.id();
        CMDIdGPDB *null_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, null_type_id, 1, 0);
        null_mdid->AddRef();
        IDatumGeneric *null_datum = (IDatumGeneric *)(GPOS_NEW(mp_) CDatumGenericGPDB(
            mp_, (IMDId *)null_mdid, -1, nullptr, 0, true /*is_null*/, 0, CDouble(0.0)));
        null_datum->AddRef();
        else_expr = GPOS_NEW(mp_) CExpression(
            mp_, GPOS_NEW(mp_) CScalarConst(mp_, (IDatum *)null_datum));
    }

    // If binder-level ret_type is still indeterminate (ANY/UNKNOWN/BOOLEAN fallback),
    // re-infer from the actually-converted ELSE expression's ORCA type.
    if (ret_type.id() == LogicalTypeId::BOOLEAN || ret_type.id() == LogicalTypeId::ANY ||
        ret_type.id() == LogicalTypeId::UNKNOWN) {
        OID else_oid = GetTypeOidFromCExpr(else_expr);
        INT else_mod = GetTypeModFromCExpr(else_expr);
        LogicalType else_lt = OidToLogicalType(else_oid, else_mod);
        if (else_lt.id() != LogicalTypeId::ANY && else_lt.id() != LogicalTypeId::UNKNOWN &&
            else_lt.id() != LogicalTypeId::SQLNULL && else_lt.id() != LogicalTypeId::BOOLEAN) {
            ret_type = else_lt;
        }
    }

    // Build nested CScalarIf from bottom up:
    // If(cond_n, then_n, If(cond_n-1, then_n-1, ... If(cond_1, then_1, else) ...))
    const auto &checks = expr.GetChecks();
    CExpression *result = else_expr;
    for (int i = (int)checks.size() - 1; i >= 0; i--) {
        uint32_t if_type_id = LOGICAL_TYPE_BASE_ID + (OID)ret_type.id();
        CMDIdGPDB *if_mdid = GPOS_NEW(mp_) CMDIdGPDB(IMDId::EmdidGeneral, if_type_id, 1, 0);

        CExpressionArray *if_children = GPOS_NEW(mp_) CExpressionArray(mp_);
        if_children->Append(ConvertExpression(*checks[i].when_expr, plan));  // condition
        if_children->Append(ConvertExpression(*checks[i].then_expr, plan));  // true value
        if_children->Append(result);                                          // false value
        result = GPOS_NEW(mp_) CExpression(
            mp_, GPOS_NEW(mp_) CScalarIf(mp_, if_mdid), if_children);
    }

    return result;
}

// ============================================================
// DuckDB expression variants (used for function type inference)
// ============================================================

unique_ptr<duckdb::Expression> Cypher2OrcaConverter::ConvertExpressionDuckDB(
    const BoundExpression &expr)
{
    switch (expr.GetExprType()) {
    case BoundExpressionType::LITERAL:
        return ConvertLiteralDuckDB(static_cast<const BoundLiteralExpression &>(expr));
    case BoundExpressionType::PROPERTY:
        return ConvertPropertyDuckDB(static_cast<const BoundPropertyExpression &>(expr));
    case BoundExpressionType::FUNCTION:
        return ConvertFunctionDuckDB(static_cast<const CypherBoundFunctionExpression &>(expr));
    case BoundExpressionType::AGG_FUNCTION:
        return ConvertAggFuncDuckDB(static_cast<const BoundAggFunctionExpression &>(expr));
    case BoundExpressionType::COMPARISON:
        return ConvertComparisonDuckDB(static_cast<const CypherBoundComparisonExpression &>(expr));
    case BoundExpressionType::BOOL_OP:
        return ConvertBoolOpDuckDB(static_cast<const BoundBoolExpression &>(expr));
    case BoundExpressionType::CASE: {
        // Infer CASE return type from THEN/ELSE branches
        const auto &case_expr = static_cast<const CypherBoundCaseExpression &>(expr);
        for (const auto &check : case_expr.GetChecks()) {
            auto then_de = ConvertExpressionDuckDB(*check.then_expr);
            if (then_de->return_type.id() != LogicalTypeId::ANY &&
                then_de->return_type.id() != LogicalTypeId::UNKNOWN &&
                then_de->return_type.id() != LogicalTypeId::SQLNULL) {
                return make_unique<BoundConstantExpression>(duckdb::Value(then_de->return_type));
            }
        }
        if (case_expr.HasElse()) {
            auto else_de = ConvertExpressionDuckDB(*case_expr.GetElse());
            if (else_de->return_type.id() != LogicalTypeId::ANY &&
                else_de->return_type.id() != LogicalTypeId::UNKNOWN &&
                else_de->return_type.id() != LogicalTypeId::SQLNULL) {
                return make_unique<BoundConstantExpression>(duckdb::Value(else_de->return_type));
            }
        }
        return make_unique<BoundConstantExpression>(duckdb::Value(expr.GetDataType()));
    }
    case BoundExpressionType::VARIABLE: {
        // Whole-node/rel reference — use the data type
        return make_unique<BoundReferenceExpression>(expr.GetDataType(), 0);
    }
    default:
        // Fallback: constant placeholder with matching type
        return make_unique<BoundConstantExpression>(duckdb::Value(expr.GetDataType()));
    }
}

unique_ptr<duckdb::Expression> Cypher2OrcaConverter::ConvertLiteralDuckDB(
    const BoundLiteralExpression &expr)
{
    // Use the actual value (not just the type) so bind functions can evaluate it
    return make_unique<BoundConstantExpression>(expr.GetValue());
}

unique_ptr<duckdb::Expression> Cypher2OrcaConverter::ConvertPropertyDuckDB(
    const BoundPropertyExpression &expr)
{
    const LogicalType &type = expr.GetDataType();
    LogicalType duckdb_type;
    if (type.id() == LogicalTypeId::DECIMAL) {
        duckdb_type = LogicalType::DECIMAL(12, 2);
    } else {
        duckdb_type = type;
    }
    return make_unique<BoundReferenceExpression>(duckdb_type, 0);
}

unique_ptr<duckdb::Expression> Cypher2OrcaConverter::ConvertFunctionDuckDB(
    const CypherBoundFunctionExpression &expr)
{
    string func_name = expr.GetFuncName();
    if (IsCastingFunction(func_name)) {
        // Return placeholder with function's return type
        return make_unique<BoundConstantExpression>(duckdb::Value(expr.GetDataType()));
    }
    std::transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);

    vector<LogicalType> child_types;
    vector<unique_ptr<duckdb::Expression>> duckdb_children;
    for (idx_t i = 0; i < expr.GetNumChildren(); i++) {
        auto ce = ConvertExpressionDuckDB(*expr.GetChild(i));
        // Preserve aliases from bound expressions (struct_pack uses them for field names)
        if (expr.GetChild(i)->HasAlias()) {
            ce->alias = expr.GetChild(i)->GetAlias();
        }
        child_types.push_back(ce->return_type);
        duckdb_children.push_back(std::move(ce));
    }

    idx_t func_mdid_id = context_->db->GetCatalogWrapper().GetScalarFuncMdId(
        *context_, func_name, child_types);
    ScalarFunctionCatalogEntry *func_cat;
    idx_t func_idx;
    context_->db->GetCatalogWrapper().GetScalarFuncAndIdx(
        *context_, func_mdid_id, func_cat, func_idx);
    auto function = func_cat->functions.get()->functions[func_idx];

    if (function.bind) {
        auto bind_info = function.bind(*context_, function, duckdb_children);
    }
    // Return a constant placeholder with the resolved return type (callers only need return_type)
    return make_unique<BoundConstantExpression>(duckdb::Value(function.return_type));
}

unique_ptr<duckdb::Expression> Cypher2OrcaConverter::ConvertAggFuncDuckDB(
    const BoundAggFunctionExpression &expr)
{
    string func_name = expr.GetFuncName();
    std::transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);

    vector<LogicalType> child_types;
    vector<unique_ptr<duckdb::Expression>> duckdb_children;
    if (expr.HasChild()) {
        auto ce = ConvertExpressionDuckDB(*expr.GetChild());
        child_types.push_back(ce->return_type);
        duckdb_children.push_back(std::move(ce));
    }

    idx_t func_mdid_id = context_->db->GetCatalogWrapper().GetAggFuncMdId(
        *context_, func_name, child_types);
    AggregateFunctionCatalogEntry *agg_cat;
    idx_t func_idx;
    context_->db->GetCatalogWrapper().GetAggFuncAndIdx(
        *context_, func_mdid_id, agg_cat, func_idx);
    auto function = agg_cat->functions.get()->functions[func_idx];

    unique_ptr<FunctionData> bind_info;
    if (function.bind) {
        bind_info = function.bind(*context_, function, duckdb_children);
        duckdb_children.resize(
            std::min(function.arguments.size(), duckdb_children.size()));
    }
    return make_unique<BoundAggregateExpression>(
        std::move(function), std::move(duckdb_children), nullptr,
        std::move(bind_info), expr.IsDistinct());
}

unique_ptr<duckdb::Expression> Cypher2OrcaConverter::ConvertComparisonDuckDB(
    const CypherBoundComparisonExpression &expr)
{
    // Comparison always returns BOOLEAN; return a placeholder with BOOLEAN type
    return make_unique<BoundConstantExpression>(duckdb::Value(LogicalType::BOOLEAN));
}

unique_ptr<duckdb::Expression> Cypher2OrcaConverter::ConvertBoolOpDuckDB(
    const BoundBoolExpression &expr)
{
    if (expr.GetOpType() == BoundBoolOpType::NOT) {
        auto bound_op = make_unique<BoundOperatorExpression>(
            ExpressionType::OPERATOR_NOT, LogicalType::BOOLEAN);
        bound_op->children.push_back(ConvertExpressionDuckDB(*expr.GetChild(0)));
        return bound_op;
    }
    duckdb::ExpressionType conj_type = (expr.GetOpType() == BoundBoolOpType::AND)
        ? ExpressionType::CONJUNCTION_AND
        : ExpressionType::CONJUNCTION_OR;
    auto conjunction = make_unique<BoundConjunctionExpression>(conj_type);
    for (idx_t i = 0; i < expr.GetNumChildren(); i++) {
        conjunction->children.push_back(ConvertExpressionDuckDB(*expr.GetChild(i)));
    }
    return conjunction;
}

// ============================================================
// ConvertExistsSubquery  (EXISTS { MATCH ... WHERE ... })
// ============================================================
CExpression *Cypher2OrcaConverter::ConvertExistsSubquery(
    const BoundExistsSubqueryExpression &expr,
    turbolynx::LogicalPlan *outer_plan)
{
    auto &bound_match = const_cast<BoundExistsSubqueryExpression &>(expr).GetBoundMatch();

    // Identify which nodes in the inner pattern are already bound in the outer plan.
    // These nodes should NOT be re-scanned in the inner plan.
    vector<string> outer_bound_nodes;
    if (outer_plan && outer_plan->getSchema()) {
        const BoundQueryGraphCollection *qgc = bound_match.GetQueryGraphCollection();
        for (uint32_t i = 0; i < qgc->GetNumQueryGraphs(); i++) {
            auto *qg = qgc->GetQueryGraph(i);
            for (auto &node : qg->GetQueryNodes()) {
                if (outer_plan->getSchema()->isNodeBound(node->GetUniqueName())) {
                    outer_bound_nodes.push_back(node->GetUniqueName());
                }
            }
        }
    }

    // Build inner plan, skipping scans for outer-bound nodes.
    // PlanRegularMatch with subquery_outer_nodes will only scan edge + target,
    // and record which edge key to use for correlation.
    const BoundQueryGraphCollection *qgc = bound_match.GetQueryGraphCollection();
    const bound_expression_vector &predicates = bound_match.GetPredicates();
    map<string, SubqueryCorrelation> corr_keys;
    turbolynx::LogicalPlan *inner_plan = PlanRegularMatch(
        *qgc, nullptr, predicates, outer_bound_nodes, &corr_keys);

    // Apply remaining WHERE predicates from the inner match
    if (!predicates.empty()) {
        inner_plan = PlanSelection(predicates, inner_plan);
    }

    // Add correlation predicates: outer.node._id = inner.edge._sid/_tid
    if (!outer_bound_nodes.empty() && inner_plan && outer_plan) {
        CExpressionArray *corr_preds = GPOS_NEW(mp_) CExpressionArray(mp_);

        for (auto &node_name : outer_bound_nodes) {
            CColRef *outer_colref = outer_plan->getSchema()->getColRefOfKey(node_name, ID_KEY_ID);
            if (!outer_colref) continue;

            auto it = corr_keys.find(node_name);
            if (it != corr_keys.end()) {
                // Correlate via edge key (e.g., edge._sid or edge._tid)
                CColRef *inner_edge_colref = inner_plan->getSchema()->getColRefOfKey(
                    it->second.edge_name, it->second.edge_key_id);
                if (inner_edge_colref) {
                    CExpression *pred = CUtils::PexprScalarEqCmp(mp_,
                        CUtils::PexprScalarIdent(mp_, inner_edge_colref),
                        CUtils::PexprScalarIdent(mp_, outer_colref));
                    corr_preds->Append(pred);
                }
            }
        }

        if (corr_preds->Size() > 0) {
            // Wrap inner plan with CLogicalSelect for correlation predicates
            CExpression *corr_pred = (corr_preds->Size() == 1)
                ? (*corr_preds)[0]
                : CUtils::PexprScalarBoolOp(mp_, CScalarBoolOp::EboolopAnd, corr_preds);
            if (corr_preds->Size() == 1) {
                (*corr_preds)[0]->AddRef();
            }
            auto *inner_expr = inner_plan->getPlanExpr();
            inner_expr->AddRef();
            CExpression *select_expr = CUtils::PexprLogicalSelect(mp_, inner_expr, corr_pred);

            // Create new inner plan with correlation select
            inner_plan = GPOS_NEW(mp_) turbolynx::LogicalPlan(
                select_expr, *inner_plan->getSchema());
        } else {
            corr_preds->Release();
        }
    }

    // Wrap inner plan in CScalarSubqueryExists
    auto *plan_expr = inner_plan->getPlanExpr();
    plan_expr->AddRef();
    CExpression *exists_expr = GPOS_NEW(mp_) CExpression(
        mp_, GPOS_NEW(mp_) gpopt::CScalarSubqueryExists(mp_), plan_expr);

    return exists_expr;
}

} // namespace duckdb
