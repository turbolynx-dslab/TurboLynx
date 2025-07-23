include_directories(../third_party/catch)
include_directories(include)

if(${BUILD_UNITTESTS})
    add_subdirectory(execution)
endif()

add_executable(unittest unittest.cpp test_config.cpp ${ALL_OBJECT_FILES})

target_link_libraries(unittest s62gdb)