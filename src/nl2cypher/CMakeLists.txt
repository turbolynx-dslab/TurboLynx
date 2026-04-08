file(GLOB_RECURSE NL2CYPHER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_nl2cypher
  OBJECT
  ${NL2CYPHER_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_nl2cypher>
    PARENT_SCOPE)
