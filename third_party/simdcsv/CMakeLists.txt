if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(turbolynx_simdcsv STATIC src/io_util.cpp)

target_include_directories(
        turbolynx_simdcsv
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>)
set_target_properties(turbolynx_simdcsv PROPERTIES EXPORT_NAME turbolynx_simdcsv)

install(TARGETS turbolynx_simdcsv
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

      #disable_target_warnings(turbolynx_simdcsv)
