# Shared infrastructure used by all fuzz layers.
#
# Built as a standalone OBJECT library so the L1 libFuzzer binary and
# the L2 / oracle Catch2 binaries can each link it without pulling
# unrelated test machinery.

add_library(turbolynx_fuzz_common OBJECT
    result_canonicalizer.cpp
)

# Layers will link against this via $<TARGET_OBJECTS:turbolynx_fuzz_common>.
target_include_directories(turbolynx_fuzz_common PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/test/query           # for helpers/query_runner.hpp
    ${CMAKE_SOURCE_DIR}/src/include          # for main/capi/turbolynx.h chain
    ${CMAKE_SOURCE_DIR}/third_party/catch    # for Catch2 in test binary
)

# ---- Standalone unit tests for the canonicalizer ---------------------------

add_executable(fuzz_common_test
    test_result_canonicalizer.cpp
    $<TARGET_OBJECTS:turbolynx_fuzz_common>
)
target_include_directories(fuzz_common_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/test/query
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_SOURCE_DIR}/third_party/catch
)
# turbolynx is linked to satisfy the qtest::Value variant header chain
# (it transitively pulls main/capi/turbolynx.h); the unit tests themselves
# never call into the runtime.
target_link_libraries(fuzz_common_test turbolynx)

add_test(NAME fuzz_common COMMAND fuzz_common_test)
set_tests_properties(fuzz_common PROPERTIES LABELS "fuzz;fuzz_common")
