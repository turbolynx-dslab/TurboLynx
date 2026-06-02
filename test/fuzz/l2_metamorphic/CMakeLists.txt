# L2 — metamorphic structural fuzz.
#
# Catch2 binary, no external deps (no Java, no Neo4j).  Each TEST_CASE
# is one rewriter; runs N pairs against a shared seeded workspace and
# asserts the multiset comparator from test/fuzz/common reports them
# equal.

file(GLOB L2_REWRITER_SRCS CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/rewriters/*.cpp")

add_executable(fuzz_l2
    fuzz_l2_main.cpp
    seeded_workspace.cpp
    runner.cpp
    ${L2_REWRITER_SRCS}
    $<TARGET_OBJECTS:turbolynx_fuzz_common>
)

target_include_directories(fuzz_l2 PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/test/fuzz/common
    ${CMAKE_SOURCE_DIR}/test/query
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_SOURCE_DIR}/third_party/catch
)

target_link_libraries(fuzz_l2 turbolynx)

add_test(NAME fuzz_l2 COMMAND fuzz_l2)
set_tests_properties(fuzz_l2 PROPERTIES LABELS "fuzz;fuzz_l2")
