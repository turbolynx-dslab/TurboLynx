# =============================================================================
# TurboLynx -- End-to-End Bulkload Tests
# =============================================================================
# Build:
#   cmake -DENABLE_BULKLOAD_TESTS=ON -DTURBOLYNX_DATA_DIR=/source-data ..
#   ninja bulkload_test
#
# Run all:
#   ctest -L bulkload --output-on-failure
#
# Run specific dataset:
#   ctest -R bulkload_ldbc_sf1 --output-on-failure
#
# Direct invocation:
#   ./test/bulkload_test "[bulkload][ldbc][sf1]" --data-dir /source-data
#   ./test/bulkload_test "[bulkload][ldbc][sf1]" --data-dir /source-data --generate
#   ./test/bulkload_test "[bulkload][ldbc][sf1]" --data-dir /source-data --download
# =============================================================================

set(DATASETS_JSON "${CMAKE_CURRENT_SOURCE_DIR}/datasets.json")
set(DOWNLOAD_SCRIPT "${CMAKE_SOURCE_DIR}/scripts/download_test_data.sh")

add_executable(bulkload_test
    bulkload_test_main.cpp
    test_ldbc.cpp
    test_tpch.cpp
    test_dbpedia.cpp
    test_staged.cpp
)

target_link_libraries(bulkload_test PRIVATE turbolynx)

target_include_directories(bulkload_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/helpers
    ${CMAKE_SOURCE_DIR}/third_party/catch
    ${CMAKE_SOURCE_DIR}/third_party/yyjson/include
    ${CMAKE_SOURCE_DIR}/test/include
    ${CMAKE_SOURCE_DIR}/src/include
)

target_compile_definitions(bulkload_test PRIVATE
    TEST_DATASETS_JSON="${DATASETS_JSON}"
    TEST_BULKLOAD_BIN="$<TARGET_FILE:bulkload>"
    TEST_DEFAULT_DATA_DIR="${TURBOLYNX_DATA_DIR}"
    TEST_DOWNLOAD_SCRIPT="${DOWNLOAD_SCRIPT}"
)

# Link yyjson (it's a C library compiled as part of the project already,
# but we need its .c file compiled here since it's header-only in include)
target_sources(bulkload_test PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/yyjson/yyjson.c
)

# CTest registration -- one entry per dataset; label "bulkload" makes them opt-in
# SKIP_RETURN_CODE 77 means: if test returns 77, treat as SKIP (not failure)

add_test(NAME bulkload_ldbc_sf1
    COMMAND bulkload_test "[bulkload][ldbc][sf1]" --data-dir "${TURBOLYNX_DATA_DIR}")
set_tests_properties(bulkload_ldbc_sf1 PROPERTIES
    LABELS "bulkload"
    SKIP_RETURN_CODE 77)

add_test(NAME bulkload_tpch_sf1
    COMMAND bulkload_test "[bulkload][tpch][sf1]" --data-dir "${TURBOLYNX_DATA_DIR}")
set_tests_properties(bulkload_tpch_sf1 PROPERTIES
    LABELS "bulkload"
    SKIP_RETURN_CODE 77)

add_test(NAME bulkload_dbpedia
    COMMAND bulkload_test "[bulkload][dbpedia]" --data-dir "${TURBOLYNX_DATA_DIR}")
set_tests_properties(bulkload_dbpedia PROPERTIES
    LABELS "bulkload"
    SKIP_RETURN_CODE 77)

# Staged isolation tests (vertices-only → add edges → full SF1)
foreach(stage 1 2 3 4 5 6)
    add_test(NAME bulkload_staged_stage${stage}
        COMMAND bulkload_test "[bulkload][staged][stage${stage}]" --data-dir "${TURBOLYNX_DATA_DIR}")
    set_tests_properties(bulkload_staged_stage${stage} PROPERTIES
        LABELS "bulkload;staged"
        SKIP_RETURN_CODE 77)
endforeach()

# Placeholder entries for future datasets (not yet implemented):
# add_test(NAME bulkload_ldbc_sf10  ...)
# add_test(NAME bulkload_ldbc_sf100 ...)
# add_test(NAME bulkload_tpch_sf10  ...)
