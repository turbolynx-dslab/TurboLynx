set(CMAKE_C_VISIBILITY_PRESET hidden)

set(JSON_BuildTests OFF CACHE INTERNAL "")

# Note: header only libraries are excluded
add_subdirectory(antlr4_cypher)
add_subdirectory(antlr4_runtime)
add_subdirectory(cuckoofilter)
add_subdirectory(fmt)
add_subdirectory(re2)
add_subdirectory(fastpforlib)
add_subdirectory(json)
add_subdirectory(replxx)
add_subdirectory(simdcsv)
add_subdirectory(simdjson)
add_subdirectory(utf8proc)
add_subdirectory(yyjson)
