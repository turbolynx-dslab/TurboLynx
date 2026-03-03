file(GLOB_RECURSE CATALOG_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_catalog
  OBJECT
  ${CATALOG_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_catalog>
    PARENT_SCOPE)