if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(turbolynx_fastpforlib STATIC bitpacking.cpp)

target_include_directories(
        turbolynx_fastpforlib
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(turbolynx_fastpforlib PROPERTIES EXPORT_NAME turbolynx_fastpforlib)

install(TARGETS turbolynx_fastpforlib
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

      #disable_target_warnings(turbolynx_fastpforlib)
