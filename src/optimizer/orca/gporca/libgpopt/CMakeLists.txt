file(GLOB_RECURSE LIBGPOPT_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_library_unity(
  turbolynx_libgpopt
  OBJECT
  ${LIBGPOPT_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_libgpopt>
    PARENT_SCOPE)