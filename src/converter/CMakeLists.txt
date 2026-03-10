file(GLOB_RECURSE CONVERTER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library_unity(
  turbolynx_converter
  OBJECT
  ${CONVERTER_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_converter>
    PARENT_SCOPE)
