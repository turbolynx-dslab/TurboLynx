include_directories(../third_party/catch)

if(${BUILD_UNITTESTS})
    add_subdirectory(execution)
endif()

add_executable(unittest unittest.cpp ${ALL_OBJECT_FILES})

target_link_libraries(unittest s62gdb)