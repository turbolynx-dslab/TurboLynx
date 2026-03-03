file(GLOB_RECURSE STORAGE_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_storage
  OBJECT
  ${STORAGE_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_storage>
    PARENT_SCOPE)