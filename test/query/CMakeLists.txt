# =============================================================================
# TurboLynx -- LDBC SF1 Query Tests
# =============================================================================
# Build:
#   cmake -DENABLE_QUERY_TESTS=ON -DTURBOLYNX_DB_PATH=/path/to/ldbc.db ..
#   ninja query_test
#
# Run all stages:
#   ctest -L query --output-on-failure
#
# Run a specific stage:
#   ctest -R query_stage1 --output-on-failure
#
# Direct invocation (from build dir):
#   ./test/query/query_test "[q1]" --db-path /path/to/ldbc.db
#   ./test/query/query_test "[q5][ic]" --db-path /path/to/ldbc.db
#
# Tags:
#   [q1] count    [q2] filter    [q3] traversal (multihop, both, mpv)
#   [q4] IS       [q5] IC        [q6] robustness
#   [both] bidirectional    [mpv] multi-partition vertex
#   [mpe] multi-partition edge
#
# If --db-path is not set, all tests are skipped (return 77 = SKIP).
# =============================================================================

add_executable(query_test
    query_test_main.cpp
    test_q1_count.cpp
    test_q2_filter.cpp
    test_q3_traversal.cpp
    test_q4_is.cpp
    test_q5_ic.cpp
    test_q6_robustness.cpp
    test_q7_crud.cpp
    test_q8_functions.cpp
    test_q9_tpch.cpp
    test_q10_bench.cpp
)

target_link_libraries(query_test PRIVATE turbolynx)

target_include_directories(query_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/helpers
    ${CMAKE_SOURCE_DIR}/third_party/catch
    ${CMAKE_SOURCE_DIR}/test/include
    ${CMAKE_SOURCE_DIR}/src/include
)

# DB path can be set at cmake time or passed at runtime via --db-path
set(TURBOLYNX_DB_PATH "" CACHE PATH "Path to LDBC SF1 TurboLynx database for query tests")

# CTest registration -- one entry per stage; label "query" makes them opt-in
# SKIP_RETURN_CODE 77: if db-path is empty/missing, tests skip gracefully

foreach(stage 1 2 3 4 5 6 7 8 9)
    add_test(NAME query_stage${stage}
        COMMAND query_test "[q${stage}]" --db-path "${TURBOLYNX_DB_PATH}")
    set_tests_properties(query_stage${stage} PROPERTIES
        LABELS "query;ldbc"
        SKIP_RETURN_CODE 77)
endforeach()
