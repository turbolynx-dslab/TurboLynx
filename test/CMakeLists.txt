# =============================================================================
# S62GDB Test Suite
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

# ---- Single test executable ------------------------------------------------
add_executable(unittest unittest.cpp test_config.cpp ${ALL_OBJECT_FILES})
target_link_libraries(unittest s62gdb)

# ---- CTest integration -----------------------------------------------------
# Each module runs as a separate CTest entry for fine-grained CI control.
# The Catch2 tag syntax "[tag]" is passed to the unittest binary.
# Note: enable_testing() is called at root CMakeLists.txt scope.

add_test(NAME s62_common    COMMAND unittest "[common]"    --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")
add_test(NAME s62_catalog   COMMAND unittest "[catalog]"   --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")
add_test(NAME s62_storage   COMMAND unittest "[storage]"   --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")
add_test(NAME s62_execution COMMAND unittest "[execution]" --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")

# Full suite (all modules together)
add_test(NAME s62_all       COMMAND unittest               --test-workspace "${CMAKE_BINARY_DIR}/test_workspace")

# Labels allow: ctest -L fast   or   ctest -L unit
set_tests_properties(s62_common    PROPERTIES LABELS "fast;unit")
set_tests_properties(s62_catalog   PROPERTIES LABELS "unit")
set_tests_properties(s62_storage   PROPERTIES LABELS "unit")
set_tests_properties(s62_execution PROPERTIES LABELS "fast;unit")
set_tests_properties(s62_all       PROPERTIES LABELS "all")
