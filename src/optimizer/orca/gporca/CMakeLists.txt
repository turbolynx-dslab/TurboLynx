add_compile_options(-DUSE_CMAKE)
# Turn on platform-specific defines.
set(GPOS_${CMAKE_SYSTEM_NAME} 1)
set(GPOS_${CMAKE_SYSTEM_PROCESSOR} 1)

# Xerces is resolved at the root CMakeLists level (bundled or system).
# XERCES_INCLUDE_DIRS / XERCES_LIBRARIES are already set as cache variables.
include_directories(${XERCES_INCLUDE_DIRS})

# Sub-libraries.
add_subdirectory(libgpos)
add_subdirectory(libnaucrates)
add_subdirectory(libgpdbcost)
add_subdirectory(libgpopt)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    PARENT_SCOPE)
