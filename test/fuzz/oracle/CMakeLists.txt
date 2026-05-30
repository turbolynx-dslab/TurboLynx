# L3 + L4 — oracle fuzz against managed Neo4j.
#
# Uses Neo4j's transactional HTTP endpoint via libcurl + nlohmann/json
# (both already shipping in TurboLynx's third_party / system packages).
# No JNI, no bolt driver, no JVM in the harness binary itself — the
# Neo4j server is whatever the host operator has running (defaults to
# bolt://localhost:7687, override via NEO4J_HTTP_URI).
#
# Auto-spawn / lifecycle management lands in a later phase per PLAN.md;
# v1 expects an already-running Neo4j on the host (e.g. the dev box's
# system-installed instance, or a CI service container).

find_package(CURL REQUIRED)

add_executable(fuzz_oracle
    fuzz_oracle_main.cpp
    neo4j_runner.cpp
    seeded_workspace_oracle.cpp
    op_generator.cpp
    read_queries.cpp
    $<TARGET_OBJECTS:turbolynx_fuzz_common>
)

target_include_directories(fuzz_oracle PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/test/fuzz/common
    ${CMAKE_SOURCE_DIR}/test/query
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_SOURCE_DIR}/third_party/catch
    ${CMAKE_SOURCE_DIR}/third_party/json/include
    ${CURL_INCLUDE_DIRS}
)

target_link_libraries(fuzz_oracle
    turbolynx
    ${CURL_LIBRARIES}
)

add_test(NAME fuzz_oracle COMMAND fuzz_oracle)
set_tests_properties(fuzz_oracle PROPERTIES
    LABELS      "fuzz;fuzz_oracle"
    ENVIRONMENT "NEO4J_HTTP_URI=http://localhost:7474"
)

# ---- One-off audit binary --------------------------------------------------
# Hand-written reproducers for every open fuzz-found issue; prints
# TL vs Neo4j side-by-side so a reviewer can decide which findings are
# real bugs.  Not registered as a CTest entry — run manually:
#   ./test/fuzz/oracle/verify_open_issues
add_executable(verify_open_issues
    verify_open_issues.cpp
    neo4j_runner.cpp
    seeded_workspace_oracle.cpp
    $<TARGET_OBJECTS:turbolynx_fuzz_common>
)
target_include_directories(verify_open_issues PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/test/fuzz/common
    ${CMAKE_SOURCE_DIR}/test/query
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_SOURCE_DIR}/third_party/json/include
    ${CURL_INCLUDE_DIRS}
)
target_link_libraries(verify_open_issues turbolynx ${CURL_LIBRARIES})
