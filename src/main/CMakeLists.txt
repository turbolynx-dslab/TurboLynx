file(GLOB_RECURSE MAIN_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

if(TURBOLYNX_WASM)
    # Exclude socket server — no BSD sockets in WASM
    list(FILTER MAIN_SRC_FILES EXCLUDE REGEX "socket/")
    # Exclude turbolynx_wasm.cpp — compiled separately as the Emscripten executable
    list(FILTER MAIN_SRC_FILES EXCLUDE REGEX "turbolynx_wasm\\.cpp")
endif()

add_library_unity(
  turbolynx_main
  OBJECT
  ${MAIN_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_main>
    PARENT_SCOPE)
