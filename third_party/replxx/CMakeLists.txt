add_library(replxx STATIC
    src/ConvertUTF.cpp
    src/conversion.cxx
    src/escape.cxx
    src/history.cxx
    src/prompt.cxx
    src/replxx.cxx
    src/replxx_impl.cxx
    src/terminal.cxx
    src/util.cxx
    src/wcwidth.cpp
)

target_include_directories(replxx PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(replxx PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

if(UNIX)
    target_compile_definitions(replxx PRIVATE _XOPEN_SOURCE=600)
endif()

set_target_properties(replxx PROPERTIES
    CXX_STANDARD 17
    POSITION_INDEPENDENT_CODE ON
)
