add_subdirectory(binder)
add_subdirectory(catalog)
add_subdirectory(converter)
add_subdirectory(common)
add_subdirectory(execution)
add_subdirectory(function)
if(NOT TURBOLYNX_WASM)
  # Loader uses SIMD CSV parser (requires AVX/SSE/NEON) and OpenMP — skip for read-only WASM
  add_subdirectory(loader)
endif()
add_subdirectory(main)
if(NOT TURBOLYNX_WASM)
  # NL2Cypher is not needed for WASM read-only alpha
  add_subdirectory(nl2cypher)
endif()
add_subdirectory(optimizer)
add_subdirectory(parser)
add_subdirectory(planner)
add_subdirectory(storage)

set(S62GDB_SYSTEM_LIBS ${CMAKE_DL_LIBS})

if(TURBOLYNX_OPENMP_LIBS)
    list(APPEND S62GDB_SYSTEM_LIBS ${TURBOLYNX_OPENMP_LIBS})
endif()

if(APPLE)
    list(APPEND S62GDB_SYSTEM_LIBS
        "-framework CoreFoundation"
        "-framework CoreServices"
    )
endif()

# Private linking to prevent ODR violations
set(S62GDB_THIRD_PARTY_LIBS_PRIVATE
    antlr4_runtime 
) 

set(S62GDB_THIRD_PARTY_LIBS_PUBLIC
    antlr4_cypher
    cuckoofilter
    duckdb_fmt
    duckdb_re2
    duckdb_fastpforlib
    nlohmann_json::nlohmann_json
    turbolynx_simdcsv
    s62_simdjson
    duckdb_utf8proc
    duckdb_yyjson
)

if(TURBOLYNX_WASM)
    # WASM build: no pthread, rt, NUMA, hwloc, tcmalloc
    set(S62GDB_INSTALLED_LIBS
        ${XERCES_LIBRARIES}
    )
else()
    set(S62GDB_INSTALLED_LIBS
        pthread
        ${NUMA_LIBRARY}
        ${HWLOC_LIBRARY}
        ${XERCES_LIBRARIES}
    )

    if(NOT APPLE)
        set(S62GDB_INSTALLED_LIBS
            ${S62GDB_INSTALLED_LIBS}
            rt
        )
    endif()

    if(ENABLE_TCMALLOC)
        set(S62GDB_INSTALLED_LIBS
            ${S62GDB_INSTALLED_LIBS}
            tcmalloc
        )
    endif()
endif()

set(S62GDB_LINK_LIBS_PUBLIC
    ${S62GDB_SYSTEM_LIBS}
    ${S62GDB_THIRD_PARTY_LIBS_PUBLIC}
    ${S62GDB_INSTALLED_LIBS}
)

set(S62GDB_LINK_LIBS_PRIVATE
    ${S62GDB_THIRD_PARTY_LIBS_PRIVATE}
)

if(TURBOLYNX_WASM)
    # WASM: build static library, then link into Emscripten executable
    add_library(turbolynx_static STATIC ${ALL_OBJECT_FILES})
    target_link_libraries(turbolynx_static PUBLIC ${S62GDB_LINK_LIBS_PUBLIC} PRIVATE ${S62GDB_LINK_LIBS_PRIVATE})
    target_include_directories(
        turbolynx_static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                      $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

    # Emscripten executable producing turbolynx.js + turbolynx.wasm
    add_executable(turbolynx_wasm ${CMAKE_SOURCE_DIR}/src/main/capi/turbolynx_wasm.cpp)
    target_link_libraries(turbolynx_wasm PRIVATE turbolynx_static)
    target_include_directories(turbolynx_wasm PRIVATE ${CMAKE_SOURCE_DIR}/src/include)
    set_target_properties(turbolynx_wasm PROPERTIES
        OUTPUT_NAME "turbolynx"
        SUFFIX ".js"
    )
    # Apply Emscripten link flags
    target_link_options(turbolynx_wasm PRIVATE ${TURBOLYNX_WASM_LINK_FLAGS})
else()
    add_library(turbolynx SHARED ${ALL_OBJECT_FILES})

    target_link_libraries(turbolynx PUBLIC ${S62GDB_LINK_LIBS_PUBLIC} PRIVATE ${S62GDB_LINK_LIBS_PRIVATE})

    target_include_directories(
        turbolynx PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                      $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

    if(APPLE)
        # Ship the dylib with clients like the Python wheel and resolve it via rpath.
        set_target_properties(turbolynx PROPERTIES
            MACOSX_RPATH ON
            INSTALL_NAME_DIR "@rpath"
        )
    endif()

    if(${BUILD_STATIC})
        add_library(turbolynx_static STATIC ${ALL_OBJECT_FILES})
        target_link_libraries(turbolynx_static ${S62GDB_LINK_LIBS})
        target_link_options(turbolynx_static PRIVATE -static)
        target_include_directories(
        turbolynx_static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    endif()
endif()
