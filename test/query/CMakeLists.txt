# =============================================================================
# TurboLynx -- Query Tests (LDBC + TPC-H)
# =============================================================================
# Build:
#   cmake -DENABLE_QUERY_TESTS=ON -DTURBOLYNX_DB_PATH=/path/to/db ..
#   ninja query_test
#
# Run all:
#   ctest -L query --output-on-failure
#
# Run by category:
#   ./test/query/query_test "[ldbc][count]"    --db-path /data/ldbc/sf1
#   ./test/query/query_test "[ldbc][is]"       --db-path /data/ldbc/sf1
#   ./test/query/query_test "[tpch]"           --db-path /data/tpch/sf1
#
# Tags:
#   LDBC small:  [ldbc][count] [ldbc][filter] [ldbc][traversal]
#                [ldbc][robustness] [ldbc][crud] [ldbc][func]
#   LDBC E2E:    [ldbc][is]   [ldbc][ic]
#   TPC-H:       [tpch]       [tpch][stress]
#
# If --db-path is not set, all tests are skipped (return 77 = SKIP).
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
    # TPC-H
    test_tpch_correctness.cpp
    test_tpch_stress.cpp
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
set(TURBOLYNX_DB_PATH "" CACHE PATH "Path to TurboLynx database for query tests")

# CTest registration
# SKIP_RETURN_CODE 77: if db-path is empty/missing, tests skip gracefully

# LDBC tests
foreach(tag count filter traversal robustness crud func is ic)
    add_test(NAME query_ldbc_${tag}
        COMMAND query_test "[ldbc][${tag}]" --db-path "${TURBOLYNX_DB_PATH}")
    set_tests_properties(query_ldbc_${tag} PROPERTIES
        LABELS "query;ldbc"
        SKIP_RETURN_CODE 77)
endforeach()

# TPC-H tests
add_test(NAME query_tpch
    COMMAND query_test "[tpch]" --db-path "${TURBOLYNX_DB_PATH}")
set_tests_properties(query_tpch PROPERTIES
    LABELS "query;tpch"
    SKIP_RETURN_CODE 77)
