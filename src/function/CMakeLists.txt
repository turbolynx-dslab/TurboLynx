file(GLOB_RECURSE FUNCTION_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_function
  OBJECT
  ${FUNCTION_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_function>
    PARENT_SCOPE)