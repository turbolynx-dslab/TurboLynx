file(GLOB_RECURSE MDPROVIDER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_mdprovider
  OBJECT
  ${MDPROVIDER_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_mdprovider>
    PARENT_SCOPE)