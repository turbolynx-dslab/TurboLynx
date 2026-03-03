file(GLOB_RECURSE PLANNER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_planner
  OBJECT
  ${PLANNER_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_planner>
    PARENT_SCOPE)