file(GLOB_RECURSE LOADER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_loader
  OBJECT
  ${LOADER_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_loader>
    PARENT_SCOPE)
