if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(turbolynx_yyjson STATIC yyjson.c)

target_include_directories(
        turbolynx_yyjson
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

set_target_properties(turbolynx_yyjson PROPERTIES EXPORT_NAME turbolynx_yyjson)