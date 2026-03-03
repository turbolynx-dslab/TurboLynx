file(GLOB_RECURSE LIBGPOS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_libgpos
  OBJECT
  ${LIBGPOS_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_libgpos>
    PARENT_SCOPE)