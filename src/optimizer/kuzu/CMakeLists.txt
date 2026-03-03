file(GLOB_RECURSE KUZU_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c*")

add_library_unity(
  turbolynx_kuzu
  OBJECT
  ${KUZU_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_kuzu>
    PARENT_SCOPE)