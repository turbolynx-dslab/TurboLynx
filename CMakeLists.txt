cmake_minimum_required(VERSION 3.16)

if(POLICY CMP0026)
  cmake_policy(SET CMP0026 NEW)
endif()

if(POLICY CMP0051)
  cmake_policy(SET CMP0051 NEW)
endif()

if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()

if(POLICY CMP0063)
  cmake_policy(SET CMP0063 NEW)
endif()

project(S62GDB)

# Include WASM profile (must be before feature flags)
include(${CMAKE_SOURCE_DIR}/cmake/wasm.cmake)

if(NOT TURBOLYNX_WASM)
  find_package(Threads REQUIRED)
  link_libraries(Threads::Threads)
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_EXTENSIONS ON)

set(CMAKE_VERBOSE_MAKEFILE OFF)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT TURBOLYNX_WASM)
  if(NOT CMAKE_C_COMPILER)
    set(CMAKE_C_COMPILER /usr/bin/gcc)
  endif()

  if(NOT CMAKE_CXX_COMPILER)
    set(CMAKE_CXX_COMPILER /usr/bin/g++)
  endif()
endif()

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
else()
  find_program(CCACHE_PROGRAM sccache)
  if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
  endif()
endif()

# Determine install paths
# default to gnu standard installation directories (lib, bin, include)
# https://cmake.org/cmake/help/latest/module/GNUInstallDirs.html
include(GNUInstallDirs)
set(INSTALL_LIB_DIR
    ${CMAKE_INSTALL_LIBDIR}
    CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR
    ${CMAKE_INSTALL_BINDIR}
    CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDE_DIR
    ${CMAKE_INSTALL_INCLUDEDIR}
    CACHE PATH "Installation directory for header files")
if(WIN32 AND NOT CYGWIN)
  set(DEF_INSTALL_CMAKE_DIR cmake)
else()
  set(DEF_INSTALL_CMAKE_DIR ${INSTALL_LIB_DIR}/cmake/S62GDB)
endif()
set(INSTALL_CMAKE_DIR
    ${DEF_INSTALL_CMAKE_DIR}
    CACHE PATH "Installation directory for CMake files")
set(S62_EXPORT_SET "S62Exports")

# Options
option(DISABLE_UNITY "Disable unity build." FALSE)
option(ENABLE_SANITIZER "Enable address sanitizer." FALSE)
option(ENABLE_UBSAN "Enable undefined behavior sanitizer." FALSE)
option(EXPLICIT_EXCEPTIONS "Explicitly enable C++ exceptions." FALSE)
option(EXPORT_DYNAMIC_SYMBOLS "Export dynamic symbols." FALSE)
option(TREAT_WARNINGS_AS_ERRORS "Treat warnings as errors" FALSE)
option(NATIVE_ARCH "Compile targeting the native architecture" TRUE)
option(ENABLE_AVX "Enable AVX instructions" TRUE)
option(ENABLE_TBB "Enable Intel TBB" TRUE)
option(ENABLE_OPENMP "Enable Open MP" TRUE)
option(ENABLE_TCMALLOC "Enable TCMalloc" TRUE)
option(BUILD_UNITTESTS "Build the C++ Unit Tests." TRUE)
option(BUILD_TOOLS "Build the S62GDB Tools" TRUE)
option(BUILD_STATIC "Build the S62GDB Static Library" FALSE)

# Settings
if(${ENABLE_SANITIZER} AND ${ENABLE_UBSAN})
  set(CXX_EXTRA_DEBUG "${CXX_EXTRA_DEBUG} -fsanitize=address,undefined -fno-sanitize=vptr -fno-sanitize-recover=all")
elseif(${ENABLE_SANITIZER})
  set(CXX_EXTRA_DEBUG "${CXX_EXTRA_DEBUG} -fsanitize=address -fno-sanitize=vptr")
elseif(${ENABLE_UBSAN})
  set(CXX_EXTRA_DEBUG "${CXX_EXTRA_DEBUG} -fsanitize=undefined -fno-sanitize-recover=all")
endif()

if(${ENABLE_TCMALLOC})
  add_definitions(-DENABLE_TCMALLOC_FLAG)
endif()

if(((${ENABLE_SANITIZER} OR ${ENABLE_UBSAN}) AND (CMAKE_BUILD_TYPE STREQUAL "Debug")))
  add_definitions(-DENABLE_SANITIZER_FLAG)
  set(ENABLE_TCMALLOC FALSE)
endif()

if(${EXPLICIT_EXCEPTIONS})
  set(CXX_EXTRA "${CXX_EXTRA} -fexceptions")
endif()

if(${EXPORT_DYNAMIC_SYMBOLS})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")
endif()

if(NOT TURBOLYNX_WASM)
  if(NATIVE_ARCH)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
  endif()

  if(ENABLE_AVX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -mavx -fPIE")
  endif()

  if(ENABLE_TBB)
    # Try system TBB first, fall back to bundled oneTBB
    find_package(TBB QUIET)
    if(NOT TBB_FOUND)
      # Fetch oneTBB from GitHub
      include(FetchContent)
      FetchContent_Declare(
        oneTBB
        GIT_REPOSITORY https://github.com/oneapi-src/oneTBB.git
        GIT_TAG        v2021.10.0
        GIT_SHALLOW    TRUE
        SOURCE_DIR     "${CMAKE_BINARY_DIR}/oneTBB-src"
      )
      set(TBB_ENABLE_IPO OFF CACHE BOOL "Disable IPO/LTO in oneTBB to avoid relocation errors" FORCE)
      FetchContent_MakeAvailable(oneTBB)
      message(STATUS "Built oneTBB from source")
    endif()
    # TBB::tbb target is provided by oneTBB CMake or system find_package
    set(TBB_DEFINITIONS "-DTBB_ENABLE_IPO=OFF -D__TBB_DYNAMIC_LOAD_ENABLED=0")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TBB_DEFINITIONS}")
    link_libraries(TBB::tbb)
  endif()

  if(ENABLE_OPENMP)
    find_package(OpenMP REQUIRED)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
  endif()
endif() # NOT TURBOLYNX_WASM

# --- libnuma: replaced by numa_shim.hpp (direct syscalls, no system package) ---
# NUMA functions are implemented in src/include/storage/cache/disk_aio/numa_shim.hpp
# No NUMA_LIBRARY needed; set empty so src/CMakeLists.txt link line is a no-op.
set(NUMA_LIBRARY "" CACHE INTERNAL "")
message(STATUS "libnuma: using built-in syscall shim (no system package required)")

if(NOT TURBOLYNX_WASM)
# --- hwloc (system or bundled autoconf) ---
set(_HWLOC_SRC  "${CMAKE_BINARY_DIR}/hwloc-src")
set(_HWLOC_BLD  "${CMAKE_BINARY_DIR}/hwloc-build")
set(_HWLOC_INST "${CMAKE_BINARY_DIR}/hwloc-install")

# Try to detect system hwloc first
find_library(_HWLOC_SYS_LIB hwloc)
find_path(_HWLOC_SYS_INC hwloc.h)
if(_HWLOC_SYS_LIB AND _HWLOC_SYS_INC)
  set(HWLOC_FOUND TRUE)
  set(HWLOC_LIBRARY "${_HWLOC_SYS_LIB}" CACHE INTERNAL "")
  set(HWLOC_INCLUDE_DIRS "${_HWLOC_SYS_INC}" CACHE INTERNAL "")
  message(STATUS "Found system hwloc: ${_HWLOC_SYS_LIB}")
endif()

if(NOT HWLOC_FOUND)
  if(NOT EXISTS "${_HWLOC_SRC}/configure")
    if(NOT EXISTS "${_HWLOC_SRC}/configure.ac")
      include(FetchContent)
      FetchContent_Declare(
        hwloc
        GIT_REPOSITORY https://github.com/open-mpi/hwloc.git
        GIT_TAG        hwloc-2.8.0
        GIT_SHALLOW    TRUE
        SOURCE_DIR     "${_HWLOC_SRC}"
      )
      FetchContent_Populate(hwloc)
    endif()
    # hwloc ships configure.ac only; generate configure via autogen.sh
    execute_process(COMMAND ./autogen.sh
                    WORKING_DIRECTORY "${_HWLOC_SRC}"
                    RESULT_VARIABLE _r)
    if(NOT _r EQUAL 0)
      message(FATAL_ERROR "hwloc autogen.sh failed with code ${_r}")
    endif()
  endif()

  if(NOT EXISTS "${_HWLOC_INST}/lib/libhwloc.a")
    file(MAKE_DIRECTORY "${_HWLOC_BLD}")
    include(ProcessorCount)
    ProcessorCount(_nproc)
    if(_nproc EQUAL 0)
      set(_nproc 4)
    endif()

    # Run configure with minimal dependencies
    execute_process(COMMAND "${_HWLOC_SRC}/configure"
                            --prefix=${_HWLOC_INST}
                            --enable-static --disable-shared
                            --disable-libxml2 --disable-libudev --disable-pci
                            "CFLAGS=-fPIC" "CXXFLAGS=-fPIC"
                    WORKING_DIRECTORY "${_HWLOC_BLD}"
                    RESULT_VARIABLE _r)
    if(NOT _r EQUAL 0)
      message(FATAL_ERROR "hwloc configure failed with code ${_r}")
    endif()

    # Run make
    execute_process(COMMAND make -j${_nproc}
                    WORKING_DIRECTORY "${_HWLOC_BLD}"
                    RESULT_VARIABLE _r)
    if(NOT _r EQUAL 0)
      message(FATAL_ERROR "hwloc make failed with code ${_r}")
    endif()

    # Run make install
    execute_process(COMMAND make install
                    WORKING_DIRECTORY "${_HWLOC_BLD}"
                    RESULT_VARIABLE _r)
    if(NOT _r EQUAL 0)
      message(FATAL_ERROR "hwloc make install failed with code ${_r}")
    endif()
    message(STATUS "Built hwloc from source")
  endif()

  set(HWLOC_INCLUDE_DIRS "${_HWLOC_INST}/include" CACHE INTERNAL "")
  set(HWLOC_LIBRARY "${_HWLOC_INST}/lib/libhwloc.a" CACHE INTERNAL "")
  set(HWLOC_FOUND TRUE CACHE INTERNAL "")
endif()
include_directories(${HWLOC_INCLUDE_DIRS})
else() # TURBOLYNX_WASM
  set(HWLOC_LIBRARY "" CACHE INTERNAL "")
  set(HWLOC_FOUND FALSE CACHE INTERNAL "")
  message(STATUS "hwloc: skipped for WASM build")
endif() # NOT TURBOLYNX_WASM (hwloc)

if(NOT MSVC)
  if(TREAT_WARNINGS_AS_ERRORS)
    set(CXX_EXTRA "${CXX_EXTRA} -Werror")
  endif()
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -DDEBUG -Wall ${CXX_EXTRA}")
  set(CMAKE_CXX_FLAGS_RELEASE
      "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG ${CXX_EXTRA}")
  if("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$" AND CMAKE_LTO)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto=${CMAKE_LTO}")
  elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_LTO)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto")
  endif()

  set(CXX_EXTRA_DEBUG
      "${CXX_EXTRA_DEBUG} -Wunused -Wnarrowing"
  )

  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                                   VERSION_GREATER 8.0)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CXX_EXTRA_DEBUG}")
  elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$"
         AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 9.0)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CXX_EXTRA_DEBUG}")
  else()
    message(WARNING "Please use a recent compiler for debug builds")
  endif()
else()
  message(FATAL_ERROR "MSVC is not supported")
endif()

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(DEFAULT_BUILD_TYPE "Release")
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}'.")
  set(CMAKE_BUILD_TYPE
      "${DEFAULT_BUILD_TYPE}"
      CACHE STRING "Choose the type of build." FORCE)
endif()

add_definitions(-DDUCKDB_SOURCE_ID="\""1"\"")
add_definitions(-DDUCKDB_VERSION="\""1"\"")

# todo only regenerate ub file if one of the input files changed hack alert
function(enable_unity_build UB_SUFFIX SOURCE_VARIABLE_NAME)
  set(files ${${SOURCE_VARIABLE_NAME}})

  # Generate a unique filename for the unity build translation unit
  set(unit_build_file ${CMAKE_CURRENT_BINARY_DIR}/ub_${UB_SUFFIX}.cpp)
  set(temp_unit_build_file ${CMAKE_CURRENT_BINARY_DIR}/ub_${UB_SUFFIX}.cpp.tmp)
  # Exclude all translation units from compilation
  set_source_files_properties(${files} PROPERTIES HEADER_FILE_ONLY true)

  set(rebuild FALSE)
  # check if any of the source files have changed
  foreach(source_file ${files})
    if(${source_file} IS_NEWER_THAN
       ${unit_build_file})
      set(rebuild TRUE)
    endif()
  endforeach(source_file)
  # write a temporary file
  file(WRITE ${temp_unit_build_file} "// Unity Build generated by CMake\n")
  foreach(source_file ${files})
    file(
      APPEND ${temp_unit_build_file}
      "#include <${source_file}>\n"
    )
  endforeach(source_file)

  execute_process(
    COMMAND ${CMAKE_COMMAND} -E compare_files ${unit_build_file}
            ${temp_unit_build_file}
    RESULT_VARIABLE compare_result
    OUTPUT_VARIABLE bla
    ERROR_VARIABLE bla)
  if(compare_result EQUAL 0)
    # files are identical: do nothing
  elseif(compare_result EQUAL 1)
    # files are different: rebuild
    set(rebuild TRUE)
  else()
    # error while compiling: rebuild
    set(rebuild TRUE)
  endif()

  if(${rebuild})
    file(WRITE ${unit_build_file} "// Unity Build generated by CMake\n")
    foreach(source_file ${files})
      file(
        APPEND ${unit_build_file}
        "#include <${source_file}>\n"
      )
    endforeach(source_file)
  endif()

  # Complement list of translation units with the name of ub
  set(${SOURCE_VARIABLE_NAME}
      ${${SOURCE_VARIABLE_NAME}} ${unit_build_file}
      PARENT_SCOPE)
endfunction(enable_unity_build)

function(add_library_unity NAME MODE)
  set(SRCS ${ARGN})
  if(NOT DISABLE_UNITY)
    enable_unity_build(${NAME} SRCS)
  endif()
  add_library(${NAME} OBJECT ${SRCS})
endfunction()

if(TURBOLYNX_WASM)
  # WASM OpenMP stub header must come first to shadow system omp.h
  include_directories(BEFORE src/include/wasm_compat)
endif()
include_directories(src/include)
include_directories(src/include/optimizer/orca/gpopt)
include_directories(src/include/optimizer/orca/gporca/libgpdbcost)
include_directories(src/include/optimizer/orca/gporca/libgpopt)
include_directories(src/include/optimizer/orca/gporca/libgpos)
include_directories(src/include/optimizer/orca/gporca/libnaucrates)


# Third party includes
include_directories(third_party/antlr4_cypher/antlr4)
include_directories(third_party/antlr4_runtime/src)
include_directories(third_party/fastpforlib)
include_directories(third_party/simdcsv/src)
include_directories(third_party/icecream-cpp/include)
include_directories(third_party/json/include)
include_directories(third_party/tblr)
include_directories(third_party/cuckoofilter/src)
include_directories(third_party/fplus)
include_directories(third_party/range-v3)
include_directories(third_party/duckdb_fmt/include)
include_directories(third_party/duckdb_re2)
include_directories(third_party/fast_float)
include_directories(third_party/utf8proc/include)
include_directories(third_party/yyjson/include)
include_directories(third_party/csv-parser)
include_directories(third_party/spdlog)
include_directories(third_party/simdjson)
include_directories(third_party/linenoise)

# --- gp-xerces (needed by Orca optimizer headers, included globally) ---
# FindXerces.cmake lives in the orca cmake directory.
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/src/optimizer/orca/gporca/cmake")
find_package(Xerces QUIET)
if(NOT XERCES_FOUND)
  # Source goes into the BUILD tree (not source tree) to avoid embedded-git-repo warnings.
  set(_GP_XERCES_SRC  "${CMAKE_BINARY_DIR}/gp-xerces-src")
  set(_GP_XERCES_BLD  "${CMAKE_BINARY_DIR}/gp-xerces-build")
  set(_GP_XERCES_INST "${CMAKE_BINARY_DIR}/gp-xerces-install")

  if(NOT EXISTS "${_GP_XERCES_SRC}/configure")
    include(FetchContent)
    FetchContent_Declare(
      gp_xerces_src
      GIT_REPOSITORY https://github.com/greenplum-db/gp-xerces-archive.git
      GIT_TAG        main
      GIT_SHALLOW    TRUE
      SOURCE_DIR     "${_GP_XERCES_SRC}"
    )
    FetchContent_Populate(gp_xerces_src)
  endif()

  if(NOT EXISTS "${_GP_XERCES_INST}/lib/libxerces-c.a" AND
     NOT EXISTS "${_GP_XERCES_INST}/lib/libxerces-c.so")
    file(MAKE_DIRECTORY "${_GP_XERCES_BLD}")
    include(ProcessorCount)
    ProcessorCount(_nproc)
    if(_nproc EQUAL 0)
      set(_nproc 4)
    endif()

    if(TURBOLYNX_WASM)
      # Cross-compile Xerces-C for WASM via emconfigure
      # Note: emconfigure replaces CC/CXX automatically, no --host needed
      execute_process(COMMAND emconfigure "${_GP_XERCES_SRC}/configure"
                              --prefix=${_GP_XERCES_INST}
                              --disable-shared --enable-static
                              --disable-network --disable-threads
                              --disable-transcoder-gnuiconv
                              --disable-transcoder-icu
                      WORKING_DIRECTORY "${_GP_XERCES_BLD}"
                      RESULT_VARIABLE _r)
    else()
      execute_process(COMMAND "${_GP_XERCES_SRC}/configure"
                              --prefix=${_GP_XERCES_INST}
                              --disable-shared --enable-static
                              --disable-network --disable-threads
                              --enable-transcoder-gnuiconv
                              --disable-transcoder-icu
                              "CFLAGS=-fPIC" "CXXFLAGS=-fPIC"
                      WORKING_DIRECTORY "${_GP_XERCES_BLD}"
                      RESULT_VARIABLE _r)
    endif()
    if(NOT _r EQUAL 0)
      message(FATAL_ERROR "gp-xerces configure failed")
    endif()

    if(TURBOLYNX_WASM)
      execute_process(COMMAND emmake make -j${_nproc}
                      WORKING_DIRECTORY "${_GP_XERCES_BLD}"
                      RESULT_VARIABLE _r)
    else()
      execute_process(COMMAND make -j${_nproc}
                      WORKING_DIRECTORY "${_GP_XERCES_BLD}"
                      RESULT_VARIABLE _r)
    endif()
    if(NOT _r EQUAL 0)
      message(FATAL_ERROR "gp-xerces make failed")
    endif()
    execute_process(COMMAND make install
                    WORKING_DIRECTORY "${_GP_XERCES_BLD}"
                    RESULT_VARIABLE _r)
    if(NOT _r EQUAL 0)
      message(FATAL_ERROR "gp-xerces make install failed")
    endif()
  endif()

  set(XERCES_INCLUDE_DIRS "${_GP_XERCES_INST}/include" CACHE INTERNAL "")
  set(XERCES_LIBRARY      "${_GP_XERCES_INST}/lib/libxerces-c.a" CACHE INTERNAL "")
  set(XERCES_LIBRARIES    "${_GP_XERCES_INST}/lib/libxerces-c.a" CACHE INTERNAL "")
  set(XERCES_FOUND        TRUE CACHE INTERNAL "")
endif()
# Make Xerces headers visible to ALL targets (catalog, main, planner, etc.)
include_directories(${XERCES_INCLUDE_DIRS})
# ----------------------------------------------------------------------

add_subdirectory(third_party)
add_subdirectory(src)

if(${BUILD_UNITTESTS})
  enable_testing()         # Must be at root scope so CTest finds tests from build root
  add_subdirectory(test)
endif()

if(${BUILD_TOOLS})
  add_subdirectory(tools)
endif()
