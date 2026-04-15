# cmake/wasm.cmake — WASM build profile for TurboLynx
#
# Usage:
#   mkdir build-wasm && cd build-wasm
#   emcmake cmake .. -DTURBOLYNX_WASM=ON -DCMAKE_BUILD_TYPE=Release
#   emmake make -j$(nproc)

option(TURBOLYNX_WASM "Build for WebAssembly via Emscripten" OFF)

if(TURBOLYNX_WASM)
    message(STATUS "=== TurboLynx WASM build profile ===")

    # Force-disable Linux-only and heavy features
    set(NATIVE_ARCH    OFF CACHE BOOL "" FORCE)
    set(ENABLE_AVX     OFF CACHE BOOL "" FORCE)
    set(ENABLE_TBB     OFF CACHE BOOL "" FORCE)
    set(ENABLE_OPENMP  OFF CACHE BOOL "" FORCE)
    set(ENABLE_TCMALLOC OFF CACHE BOOL "" FORCE)
    set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
    set(BUILD_TOOLS    OFF CACHE BOOL "" FORCE)
    set(BUILD_STATIC   OFF CACHE BOOL "" FORCE)

    # WASM compile definitions
    add_definitions(-DTURBOLYNX_WASM)
    add_definitions(-DDUCKDB_NO_THREADS)
    # Force D_ASSERT to throw InternalException instead of abort(),
    # so assertions are catchable by the C API try-catch wrapper.
    add_definitions(-DDUCKDB_FORCE_ASSERT)

    # Emscripten link flags (applied to the final executable target)
    set(TURBOLYNX_WASM_LINK_FLAGS
        "-sWASM=1"
        "-sALLOW_MEMORY_GROWTH=1"
        "-sMAXIMUM_MEMORY=4GB"
        "-sMODULARIZE=1"
        "-sEXPORT_ES6=0"
        "-sEXPORTED_RUNTIME_METHODS=['ccall','cwrap','FS','MEMFS','NODEFS']"
        "-lnodefs.js"
        "-sEXPORTED_FUNCTIONS=['_turbolynx_wasm_open','_turbolynx_wasm_query','_turbolynx_wasm_explain','_turbolynx_wasm_close','_turbolynx_wasm_get_version','_turbolynx_wasm_get_labels','_turbolynx_wasm_get_schema','_turbolynx_wasm_free','_malloc','_free']"
        "-sINITIAL_MEMORY=268435456"
        "-sSTACK_SIZE=2097152"
        "-sNO_EXIT_RUNTIME=1"
        "-sENVIRONMENT=web,worker,node"
        "--no-entry"
        "-sASSERTIONS=0"
        "-sDISABLE_EXCEPTION_CATCHING=0"
    )

    message(STATUS "  WASM profile: read-only alpha")
    message(STATUS "  Disabled: TBB, OpenMP, TCMalloc, AVX, NUMA, hwloc")
    message(STATUS "  Defines: TURBOLYNX_WASM, DUCKDB_NO_THREADS")
endif()
