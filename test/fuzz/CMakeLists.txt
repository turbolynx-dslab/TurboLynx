# =============================================================================
# TurboLynx Fuzz Test Suite — Multi-Layer
# =============================================================================
# Four layers, three binaries, three independent gates:
#
#   BUILD_FUZZ_L1      L1 — crash / sanitizer fuzz (libFuzzer + ASAN/UBSAN)
#   BUILD_FUZZ_L2      L2 — metamorphic structural fuzz (no external deps)
#   BUILD_FUZZ_ORACLE  L3 + L4 — differential / stateful fuzz vs Neo4j
#                       (requires Java 17 and the Neo4j tarball)
#
#   BUILD_FUZZ_TESTS   umbrella that turns all three on
#
# All gates default to OFF.  Release / shipping builds touch none of
# this; `libturbolynx.{so,dylib}` and the Python wheel never depend on
# Java, Neo4j, libFuzzer, or any sanitizer runtime.
# =============================================================================

if(BUILD_FUZZ_TESTS)
    set(BUILD_FUZZ_L1     ON CACHE BOOL "Build L1 crash/sanitizer fuzz" FORCE)
    set(BUILD_FUZZ_L2     ON CACHE BOOL "Build L2 metamorphic fuzz"      FORCE)
    set(BUILD_FUZZ_ORACLE ON CACHE BOOL "Build L3+L4 oracle fuzz"        FORCE)
endif()

# Shared infrastructure is always built when any layer is requested.
if(BUILD_FUZZ_L1 OR BUILD_FUZZ_L2 OR BUILD_FUZZ_ORACLE)
    add_subdirectory(common)
endif()

# Layer subdirectories are added only after their CMakeLists.txt lands.
# This lets P0 (common/) build standalone while later phases fill in
# the layer dirs incrementally.

if(BUILD_FUZZ_L1 AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/l1_crash/CMakeLists.txt")
    add_subdirectory(l1_crash)
endif()

if(BUILD_FUZZ_L2 AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/l2_metamorphic/CMakeLists.txt")
    add_subdirectory(l2_metamorphic)
endif()

if(BUILD_FUZZ_ORACLE AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/oracle/CMakeLists.txt")
    add_subdirectory(oracle)
endif()
