file(GLOB_RECURSE EXEC_TEST_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")

add_library(
    s62gdb_execution_test
    OBJECT
    ${EXEC_TEST_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:s62gdb_execution_test>
    PARENT_SCOPE)