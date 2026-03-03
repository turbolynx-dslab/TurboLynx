add_subdirectory(gporca)

# add libraries
file (GLOB_RECURSE ORCA_SOURCES "gpopt/*.c*" "nodes/*.c*" "utils/*.c*")

add_library_unity(
  turbolynx_orca
  OBJECT
  ${ORCA_SOURCES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_orca>
    PARENT_SCOPE)