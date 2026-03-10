add_executable(turbolynx_bin
    turbolynx.cpp
    shell/shell.cpp
    shell/renderer.cpp
    shell/commands.cpp
    shell/completion.cpp
)

set_target_properties(turbolynx_bin PROPERTIES OUTPUT_NAME turbolynx)

target_include_directories(turbolynx_bin PRIVATE
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_SOURCE_DIR}/third_party/antlr4_runtime/src
    ${CMAKE_SOURCE_DIR}/third_party/spdlog/include
)

target_link_libraries(turbolynx_bin turbolynx replxx)
