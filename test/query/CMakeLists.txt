# =============================================================================
# TurboLynx -- Query Tests (LDBC + TPC-H + OSS)
# =============================================================================
# Build:
#   cmake -DENABLE_QUERY_TESTS=ON \
#         -DTURBOLYNX_LDBC_PATH=/data/ldbc/sf1 \
#         -DTURBOLYNX_TPCH_PATH=/data/tpch/sf1 \
#         -DTURBOLYNX_OSS_PATH=/data/oss/small ..
#   ninja query_test
#
# Run all (all DBs):
#   ctest -L query --output-on-failure
#
# Run by category:
#   ./test/query/query_test "[ldbc][count]" --ldbc-path /data/ldbc/sf1
#   ./test/query/query_test "[tpch]"        --tpch-path /data/tpch/sf1
#   ./test/query/query_test "[oss]"         --oss-path  /data/oss/small
#
# Run everything at once:
#   ./test/query/query_test --ldbc-path /data/ldbc/sf1 \
#                           --tpch-path /data/tpch/sf1 \
#                           --oss-path  /data/oss/small
#
# Tags:
#   LDBC small:  [ldbc][count] [ldbc][filter] [ldbc][traversal]
#                [ldbc][robustness] [ldbc][crud] [ldbc][func]
#   LDBC E2E:    [ldbc][is]   [ldbc][ic]
#   TPC-H:       [tpch]       [tpch][stress]
#   OSS:         [oss][regression63]
#
# If no DB path flag is set, all tests are skipped (return 77 = SKIP).
# Legacy --db-path is still accepted (auto-detects schema).
# =============================================================================

add_executable(query_test
    query_test_main.cpp
    # LDBC small tests
    test_ldbc_count.cpp
    test_ldbc_filter.cpp
    test_ldbc_traversal.cpp
    test_ldbc_robustness.cpp
    test_ldbc_crud.cpp
    test_ldbc_functions.cpp
    # LDBC E2E correctness
    test_ldbc_is_correctness.cpp
    test_ldbc_ic_correctness.cpp
    # LDBC Hadoop datagen SF=0.1 smoke (local-only, opt-in via
    # --ldbc-hadoop-path; auto-skip otherwise)
    test_ldbc_hadoop_sf01.cpp
    # TPC-H
    test_tpch_correctness.cpp
    test_tpch_stress.cpp
    # OSS supply-chain regression (issue #63)
    test_oss_regression.cpp
    # types-mini (per-type passthrough / arithmetic / CASE LCT / agg)
    test_types_correctness.cpp
    # DBpedia mini — schemaless query differential vs Neo4j
    test_dbpedia_mini.cpp
)

# DBpedia mini brings in libcurl (HTTP wrapper for Neo4j) + nlohmann/json.
find_package(CURL REQUIRED)
target_include_directories(query_test PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/json/include
    ${CURL_INCLUDE_DIRS}
)
target_link_libraries(query_test PRIVATE ${CURL_LIBRARIES})

target_link_libraries(query_test PRIVATE turbolynx)

target_include_directories(query_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/helpers
    ${CMAKE_SOURCE_DIR}/third_party/catch
    ${CMAKE_SOURCE_DIR}/test/include
    ${CMAKE_SOURCE_DIR}/src/include
)

target_compile_definitions(query_test PRIVATE
    TEST_QUERY_SHELL_BIN="$<TARGET_FILE:turbolynx_bin>"
    TURBOLYNX_REPO_ROOT="${CMAKE_SOURCE_DIR}"
)

# Mini-fixture switches. When set, the LDBC and TPC-H test sources
# (and the integrity probes in query_test_main.cpp) compile with the
# expected counts for the small mini fixtures committed under
# test/data/ldbc-mini/ and test/data/tpch-mini/, instead of SF1/SF1
# values. CI sets these so the suite runs against the committed
# fixtures; leave unset for local SF1 development workflows.
option(TURBOLYNX_LDBC_FIXTURE_MINI "Compile LDBC tests for the SF0.003 mini fixture" OFF)
option(TURBOLYNX_TPCH_FIXTURE_MINI "Compile TPC-H tests for the SF0.01 mini fixture" OFF)
if(TURBOLYNX_LDBC_FIXTURE_MINI)
    target_compile_definitions(query_test PRIVATE TURBOLYNX_LDBC_FIXTURE_MINI=1)
endif()
if(TURBOLYNX_TPCH_FIXTURE_MINI)
    target_compile_definitions(query_test PRIVATE TURBOLYNX_TPCH_FIXTURE_MINI=1)
endif()

# DB paths can be set at cmake time or passed at runtime
# via --ldbc-path / --tpch-path / --oss-path.
set(TURBOLYNX_LDBC_PATH "" CACHE PATH "Path to LDBC SF1 database for query tests")
set(TURBOLYNX_TPCH_PATH "" CACHE PATH "Path to TPC-H SF1 database for query tests")
set(TURBOLYNX_OSS_PATH  "" CACHE PATH "Path to OSS supply-chain fixture database for query tests")

# CTest registration
# SKIP_RETURN_CODE 77: if db path is empty/missing, tests skip gracefully

# LDBC tests
foreach(tag count filter traversal robustness crud func is ic)
    add_test(NAME query_ldbc_${tag}
        COMMAND query_test "[ldbc][${tag}]" --ldbc-path "${TURBOLYNX_LDBC_PATH}")
    set_tests_properties(query_ldbc_${tag} PROPERTIES
        LABELS "query;ldbc"
        SKIP_RETURN_CODE 77)
endforeach()

# TPC-H tests
add_test(NAME query_tpch
    COMMAND query_test "[tpch]" --tpch-path "${TURBOLYNX_TPCH_PATH}")
set_tests_properties(query_tpch PROPERTIES
    LABELS "query;tpch"
    SKIP_RETURN_CODE 77)

# OSS supply-chain tests (sanity + issue #63 regression)
add_test(NAME query_oss
    COMMAND query_test "[oss]" --oss-path "${TURBOLYNX_OSS_PATH}")
set_tests_properties(query_oss PROPERTIES
    LABELS "query;oss"
    SKIP_RETURN_CODE 77)

# types-mini tests
set(TURBOLYNX_TYPES_PATH "" CACHE PATH "Path to types-mini fixture for query tests")
add_test(NAME query_types
    COMMAND query_test "[types]" --types-path "${TURBOLYNX_TYPES_PATH}")
set_tests_properties(query_types PROPERTIES
    LABELS "query;types"
    SKIP_RETURN_CODE 77)

# DBpedia mini differential query tests against a live Neo4j HTTP
# endpoint.  No path arg — the fixture lives in test/data/dbpedia-mini/
# and is loaded into a mkdtemp workspace inside the test.  Defaults to
# NEO4J_HTTP_URI=http://localhost:7474; tests SKIP gracefully when
# Neo4j is unreachable so Neo4j-less hosts stay green.
add_test(NAME query_dbpedia_mini
    COMMAND query_test "[query][dbpedia-mini]")
set_tests_properties(query_dbpedia_mini PROPERTIES
    LABELS              "query;dbpedia-mini"
    ENVIRONMENT         "NEO4J_HTTP_URI=http://localhost:7474"
    SKIP_RETURN_CODE    77)
