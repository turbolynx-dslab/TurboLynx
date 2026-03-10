file(GLOB_RECURSE BINDER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_binder
  OBJECT
  ${BINDER_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_binder>
    PARENT_SCOPE)
