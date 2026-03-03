file(GLOB_RECURSE EXECUTION_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_execution
  OBJECT
  ${EXECUTION_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_execution>
    PARENT_SCOPE)