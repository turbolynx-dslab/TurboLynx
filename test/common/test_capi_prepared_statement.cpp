#include "main/capi/cypher_prepared_statement.hpp"
#include "main/capi/turbolynx.h"
#include "catch.hpp"
#include "test_helper.hpp"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>

using namespace duckdb;

namespace {

class ScopedConnection {
public:
    explicit ScopedConnection(const std::string& db_path) {
        conn_id = turbolynx_connect(db_path.c_str());
    }

    ~ScopedConnection() {
        if (conn_id >= 0) {
            turbolynx_disconnect(conn_id);
        }
    }

    int64_t conn_id = -1;
};

class ScopedPreparedStatement {
public:
    explicit ScopedPreparedStatement(turbolynx_prepared_statement* stmt) : stmt(stmt) {
    }

    ~ScopedPreparedStatement() {
        if (stmt) {
            turbolynx_close_prepared_statement(stmt);
        }
    }

    turbolynx_prepared_statement* get() const {
        return stmt;
    }

private:
    turbolynx_prepared_statement* stmt = nullptr;
};

class ScopedResultSet {
public:
    explicit ScopedResultSet(turbolynx_resultset_wrapper* result) : result(result) {
    }

    ~ScopedResultSet() {
        if (result) {
            turbolynx_close_resultset(result);
        }
    }

    turbolynx_resultset_wrapper* get() const {
        return result;
    }

private:
    turbolynx_resultset_wrapper* result = nullptr;
};

std::string FetchString(turbolynx_resultset_wrapper* result, idx_t col_idx) {
    auto value = turbolynx_get_varchar(result, col_idx);
    std::string out = value.data ? std::string(value.data, value.size) : std::string();
    free(value.data);
    return out;
}

std::string LastError() {
    char* err = nullptr;
    turbolynx_get_last_error(&err);
    return err ? err : "";
}

} // namespace

TEST_CASE("CypherPreparedStatement replaces only real parameter tokens", "[common][capi][prepared]") {
    CypherPreparedStatement stmt(
        "RETURN '$name' AS literal /* $name */, $name AS actual, $name AS again, $other AS other");

    REQUIRE(stmt.getNumParams() == 2);

    auto string_value = Value(std::string("A'\\B"));
    auto int_value = Value::INTEGER(7);

    REQUIRE(stmt.bindValue(0, string_value));
    REQUIRE(stmt.bindValue(1, int_value));

    std::string bound_query;
    std::string error;
    REQUIRE(stmt.tryGetBoundQuery(bound_query, error));
    CHECK(error.empty());

    CHECK(bound_query.find("'$name' AS literal") != std::string::npos);
    CHECK(bound_query.find("/* $name */") != std::string::npos);
    CHECK(bound_query.find("'A\\'\\\\B' AS actual") != std::string::npos);
    CHECK(bound_query.find("'A\\'\\\\B' AS again") != std::string::npos);
    CHECK(bound_query.find("7 AS other") != std::string::npos);
}

TEST_CASE("Prepared statements copy caller query buffers", "[common][capi][prepared]") {
    turbolynxtest::ScopedTempDir temp_dir;
    ScopedConnection conn(temp_dir.path());
    REQUIRE(conn.conn_id >= 0);

    auto* query = strdup("MERGE (n:Person {id: 1, firstName: 'Owned'})");
    REQUIRE(query != nullptr);

    ScopedPreparedStatement prep(turbolynx_prepare(conn.conn_id, query));
    REQUIRE(prep.get() != nullptr);
    REQUIRE(prep.get()->query != query);
    CHECK(std::string(prep.get()->query) == "MERGE (n:Person {id: 1, firstName: 'Owned'})");

    std::strcpy(query, "X");
    CHECK(std::string(prep.get()->query) == "MERGE (n:Person {id: 1, firstName: 'Owned'})");

    free(query);
}

TEST_CASE("Prepared varchar binding escapes quotes without touching literals", "[common][capi][prepared]") {
    turbolynxtest::ScopedTempDir temp_dir;
    ScopedConnection conn(temp_dir.path());
    REQUIRE(conn.conn_id >= 0);

    ScopedPreparedStatement prep(turbolynx_prepare(
        conn.conn_id, const_cast<char*>("RETURN '$value' AS literal, $value AS actual")));
    REQUIRE(prep.get() != nullptr);

    REQUIRE(turbolynx_bind_varchar(prep.get(), 1, "A'\\B") == TURBOLYNX_SUCCESS);

    turbolynx_resultset_wrapper* raw_result = nullptr;
    auto total_rows = turbolynx_execute(conn.conn_id, prep.get(), &raw_result);
    REQUIRE(total_rows == 1);

    ScopedResultSet result(raw_result);
    REQUIRE(result.get() != nullptr);
    REQUIRE(turbolynx_fetch_next(result.get()) == TURBOLYNX_MORE_RESULT);
    CHECK(FetchString(result.get(), 0) == "$value");
    CHECK(FetchString(result.get(), 1) == "A'\\B");
}

TEST_CASE("Prepared execution reports missing parameters as runtime errors", "[common][capi][prepared]") {
    turbolynxtest::ScopedTempDir temp_dir;
    ScopedConnection conn(temp_dir.path());
    REQUIRE(conn.conn_id >= 0);

    ScopedPreparedStatement prep(turbolynx_prepare(
        conn.conn_id, const_cast<char*>("RETURN $first AS first, $second AS second")));
    REQUIRE(prep.get() != nullptr);

    REQUIRE(turbolynx_bind_int64(prep.get(), 1, 42) == TURBOLYNX_SUCCESS);

    turbolynx_resultset_wrapper* raw_result = nullptr;
    auto total_rows = turbolynx_execute(conn.conn_id, prep.get(), &raw_result);
    CHECK(total_rows == TURBOLYNX_ERROR);
    CHECK(raw_result == nullptr);
    CHECK(LastError().find("$second") != std::string::npos);
}
