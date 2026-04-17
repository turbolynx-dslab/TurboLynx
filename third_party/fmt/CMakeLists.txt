if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(fmt STATIC format.cc)

target_include_directories(fmt PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
set_target_properties(fmt PROPERTIES EXPORT_NAME fmt)