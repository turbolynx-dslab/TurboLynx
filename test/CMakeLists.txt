# =============================================================================
# TurboLynx Test Suite
# =============================================================================
# Structure:
#   unittest executable  — single binary containing ALL test modules
#   CTest targets        — filter by Catch2 tag per module
#
# Build:
#   cmake -GNinja -DBUILD_UNITTESTS=ON ..
#   ninja unittest
#
# Run all:
#   ctest --output-on-failure
# Run one module:
#   ctest -R catalog --output-on-failure
# Run directly (from build dir):
#   ./test/unittest "[catalog]"
#   ./test/unittest "[common]"
# =============================================================================

include_directories(../third_party/catch)
include_directories(include)

# ---- Test modules (each adds object files to ALL_OBJECT_FILES) -------------
if(${BUILD_UNITTESTS})
    add_subdirectory(common)
    add_subdirectory(catalog)
    add_subdirectory(storage)
    add_subdirectory(execution)
endif()

# ---- Single test executable (gated so a fuzz-only build doesn't drag
#      the whole Catch2 unittest binary in) -----------------------------------
if(${BUILD_UNITTESTS})
    add_executable(unittest unittest.cpp test_config.cpp ${ALL_OBJECT_FILES})
    target_link_libraries(unittest turbolynx)

    # ---- CTest integration --------------------------------------------------
    # Each module runs as a separate CTest entry for fine-grained CI control.
    # The Catch2 tag syntax "[tag]" is passed to the unittest binary.
    # Note: enable_testing() is called at root CMakeLists.txt scope.

    add_test(NAME turbolynx_common    COMMAND unittest "[common]"    --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")
    add_test(NAME turbolynx_catalog   COMMAND unittest "[catalog]"   --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")
    add_test(NAME turbolynx_storage   COMMAND unittest "[storage]"   --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")
    add_test(NAME turbolynx_execution COMMAND unittest "[execution]" --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")

    # Full suite (all modules together)
    add_test(NAME turbolynx_all       COMMAND unittest               --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")

    # Labels allow: ctest -L fast   or   ctest -L unit
    set_tests_properties(turbolynx_common    PROPERTIES LABELS "fast;unit")
    set_tests_properties(turbolynx_catalog   PROPERTIES LABELS "unit")
    set_tests_properties(turbolynx_storage   PROPERTIES LABELS "unit")
    set_tests_properties(turbolynx_execution PROPERTIES LABELS "fast;unit")
    set_tests_properties(turbolynx_all       PROPERTIES LABELS "all")
endif()

# ---- End-to-end bulkload tests (opt-in) ------------------------------------
option(ENABLE_BULKLOAD_TESTS "Build E2E bulkload test binary" ON)
set(TURBOLYNX_DATA_DIR "/source-data" CACHE PATH "Root directory for benchmark datasets")

if(${BUILD_UNITTESTS} AND ENABLE_BULKLOAD_TESTS)
    add_subdirectory(bulkload)
endif()

# ---- LDBC SF1 query tests (opt-in) -----------------------------------------
option(ENABLE_QUERY_TESTS "Build LDBC SF1 query test binary" ON)

if(${BUILD_UNITTESTS} AND ENABLE_QUERY_TESTS)
    add_subdirectory(query)
endif()

# ---- Multi-layer fuzz harness (opt-in; see test/fuzz/CMakeLists.txt) -------
# Options BUILD_FUZZ_TESTS / BUILD_FUZZ_L1 / BUILD_FUZZ_L2 / BUILD_FUZZ_ORACLE
# are declared in the root CMakeLists.txt so the global sanitizer / coverage
# flag block (CXX_EXTRA_DEBUG) can react to BUILD_FUZZ_L1 before the test
# tree is processed.

if(BUILD_FUZZ_TESTS OR BUILD_FUZZ_L1 OR BUILD_FUZZ_L2 OR BUILD_FUZZ_ORACLE)
    add_subdirectory(fuzz)
endif()
