// Catch2 unit tests for tl_fuzz::compare and friends.
//
// These exercise the canonicalizer in isolation — no TurboLynx
// runtime, no Neo4j, no Cypher.  Rows are built by hand so the
// expected outcome is unambiguous.

#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "result_canonicalizer.hpp"

using qtest::Null;
using qtest::Row;
using qtest::Value;
using qtest::QueryResult;
using tl_fuzz::CompareMode;
using tl_fuzz::compare;

namespace {

Row make_row(std::initializer_list<Value> cells) {
    Row r;
    for (const auto& v : cells) r.cols.push_back(v);
    return r;
}

QueryResult make_result(std::initializer_list<Row> rows) {
    QueryResult q;
    for (const auto& r : rows) q.rows.push_back(r);
    return q;
}

}  // namespace

TEST_CASE("empty results compare equal", "[fuzz][canonicalizer]") {
    auto a = make_result({});
    auto b = make_result({});
    auto r = compare(a, b);
    CHECK(r.equal);
    CHECK(r.diff.empty());
}

TEST_CASE("identical single row compares equal", "[fuzz][canonicalizer]") {
    auto a = make_result({make_row({Value(int64_t{42}), Value(std::string("x"))})});
    auto b = make_result({make_row({Value(int64_t{42}), Value(std::string("x"))})});
    auto r = compare(a, b);
    CHECK(r.equal);
}

TEST_CASE("differing single row reports diff", "[fuzz][canonicalizer]") {
    auto a = make_result({make_row({Value(int64_t{42})})});
    auto b = make_result({make_row({Value(int64_t{43})})});
    auto r = compare(a, b);
    CHECK_FALSE(r.equal);
    CHECK_FALSE(r.diff.empty());
}

TEST_CASE("row count mismatch is reported", "[fuzz][canonicalizer]") {
    auto a = make_result({make_row({Value(int64_t{1})})});
    auto b = make_result({});
    auto r = compare(a, b);
    CHECK_FALSE(r.equal);
    CHECK(r.diff.find("row count mismatch") != std::string::npos);
}

TEST_CASE("reordered rows: multiset equal, ordered not equal",
          "[fuzz][canonicalizer]") {
    auto a = make_result({
        make_row({Value(int64_t{1})}),
        make_row({Value(int64_t{2})}),
        make_row({Value(int64_t{3})}),
    });
    auto b = make_result({
        make_row({Value(int64_t{3})}),
        make_row({Value(int64_t{1})}),
        make_row({Value(int64_t{2})}),
    });
    CHECK(compare(a, b, CompareMode::Multiset).equal);
    CHECK_FALSE(compare(a, b, CompareMode::Ordered).equal);
}

TEST_CASE("NULL == NULL on both sides", "[fuzz][canonicalizer]") {
    auto a = make_result({make_row({Value(Null{})})});
    auto b = make_result({make_row({Value(Null{})})});
    CHECK(compare(a, b).equal);
}

TEST_CASE("NULL != non-null value", "[fuzz][canonicalizer]") {
    auto a = make_result({make_row({Value(Null{})})});
    auto b = make_result({make_row({Value(int64_t{0})})});
    auto r = compare(a, b);
    CHECK_FALSE(r.equal);
}

TEST_CASE("row width mismatch is reported", "[fuzz][canonicalizer]") {
    auto a = make_result({make_row({Value(int64_t{1}), Value(int64_t{2})})});
    auto b = make_result({make_row({Value(int64_t{1})})});
    auto r = compare(a, b);
    CHECK_FALSE(r.equal);
}
