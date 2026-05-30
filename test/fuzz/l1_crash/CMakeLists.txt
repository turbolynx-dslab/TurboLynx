# L1 — crash / sanitizer fuzz.
#
# Requires the whole library to have been compiled with
# `-fsanitize=fuzzer-no-link,address,undefined`, which the root
# CMakeLists.txt arranges when BUILD_FUZZ_L1=ON.  This subdir only adds
# the fuzz entry binary, which links libFuzzer (`-fsanitize=fuzzer`).

if(NOT (CMAKE_CXX_COMPILER_ID STREQUAL "Clang"))
    message(FATAL_ERROR
        "BUILD_FUZZ_L1 requires Clang (libFuzzer is clang-only). "
        "Re-configure with -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++.")
endif()

if(NOT ENABLE_SANITIZER)
    message(WARNING
        "BUILD_FUZZ_L1 without ENABLE_SANITIZER=ON gives weak verdicts. "
        "ASAN catches memory bugs that libFuzzer alone misses.")
endif()

add_executable(fuzz_l1 fuzz_entry.cpp)

target_include_directories(fuzz_l1 PRIVATE
    ${CMAKE_SOURCE_DIR}/src/include
)

# fuzzer + sanitizers on both compile and link.  The library itself was
# already compiled with -fsanitize=fuzzer-no-link by the root CMake block.
target_compile_options(fuzz_l1 PRIVATE
    -fsanitize=fuzzer,address
    -fno-sanitize-recover=all
)
target_link_options(fuzz_l1 PRIVATE
    -fsanitize=fuzzer,address
)

target_link_libraries(fuzz_l1 turbolynx)

# CTest entry: run the seed corpus once with a small wall-clock budget.
# Nightly CI overrides -max_total_time for longer sessions.
add_test(NAME fuzz_l1_smoke
         COMMAND fuzz_l1 -max_total_time=30
                         ${CMAKE_CURRENT_SOURCE_DIR}/corpus)
set_tests_properties(fuzz_l1_smoke PROPERTIES LABELS "fuzz;fuzz_l1")
