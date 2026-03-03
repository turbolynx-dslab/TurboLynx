file(GLOB_RECURSE LIBNAUCRATES_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_library_unity(
    turbolynx_libnaucrates
    OBJECT
    ${LIBNAUCRATES_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_libnaucrates>
    PARENT_SCOPE)