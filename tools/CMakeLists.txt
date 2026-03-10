add_executable(turbolynx_bin
    turbolynx.cpp
    client.cpp
    bulkload.cpp
)

set_target_properties(turbolynx_bin PROPERTIES OUTPUT_NAME turbolynx)

target_link_libraries(turbolynx_bin turbolynx linenoise)
