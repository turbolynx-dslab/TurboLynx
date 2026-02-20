add_library(s62_simdjson STATIC simdjson.cpp)
target_compile_options(s62_simdjson PRIVATE -O3)
target_include_directories(s62_simdjson PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
