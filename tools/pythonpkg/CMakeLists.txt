find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

# Find pybind11 via Python's site-packages
execute_process(
    COMMAND ${Python3_EXECUTABLE} -c "import pybind11; print(pybind11.get_cmake_dir())"
    OUTPUT_VARIABLE pybind11_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
find_package(pybind11 REQUIRED HINTS ${pybind11_DIR})

set(TURBOLYNX_PYTHON_SOURCES
    turbolynx_python.cpp
    src/pyconnection.cpp
    src/pyresult.cpp
)

pybind11_add_module(turbolynx_core ${TURBOLYNX_PYTHON_SOURCES})

target_include_directories(turbolynx_core PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/include
    ${CMAKE_SOURCE_DIR}/src/include
    ${CMAKE_SOURCE_DIR}/third_party/fmt/include
    ${CMAKE_SOURCE_DIR}/third_party/spdlog/include
    ${CMAKE_SOURCE_DIR}/third_party/re2
)

target_link_libraries(turbolynx_core PRIVATE turbolynx)

# Copy the Python package alongside the built module
set(TURBOLYNX_PYTHON_PKG_DIR ${CMAKE_CURRENT_BINARY_DIR}/turbolynx)
add_custom_command(TARGET turbolynx_core POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory ${TURBOLYNX_PYTHON_PKG_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/turbolynx/__init__.py
        ${TURBOLYNX_PYTHON_PKG_DIR}/__init__.py
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/turbolynx/_cypher_rewriter.py
        ${TURBOLYNX_PYTHON_PKG_DIR}/_cypher_rewriter.py
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/turbolynx/relation.py
        ${TURBOLYNX_PYTHON_PKG_DIR}/relation.py
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/turbolynx/graph.py
        ${TURBOLYNX_PYTHON_PKG_DIR}/graph.py
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/turbolynx/_version.py
        ${TURBOLYNX_PYTHON_PKG_DIR}/_version.py
    COMMAND ${CMAKE_COMMAND} -E copy
        $<TARGET_FILE:turbolynx_core>
        ${TURBOLYNX_PYTHON_PKG_DIR}/
)
