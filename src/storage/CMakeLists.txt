if(TURBOLYNX_WASM)
  # WASM: exclude real disk_aio files, use WASM stubs instead
  file(GLOB_RECURSE STORAGE_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")
  # Remove all disk_aio source files
  file(GLOB_RECURSE DISK_AIO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/*.c*")
  list(REMOVE_ITEM STORAGE_SRC_FILES ${DISK_AIO_FILES})
  # Add back only the WASM stub
  list(APPEND STORAGE_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/disk_aio_wasm.cpp")
  list(APPEND STORAGE_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/TypeDef.cpp")
elseif(TURBOLYNX_PORTABLE_DISK_IO)
  file(GLOB_RECURSE STORAGE_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")
  list(REMOVE_ITEM STORAGE_SRC_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/disk_aio_factory.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/disk_aio_interface.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/disk_aio_request.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/disk_aio_thread.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/eXDB_dist_internal.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/thread.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/cache/disk_aio/disk_aio_wasm.cpp"
  )
else()
  file(GLOB_RECURSE STORAGE_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.c*")
endif()

add_library_unity(
  turbolynx_storage
  OBJECT
  ${STORAGE_SRC_FILES}
)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:turbolynx_storage>
    PARENT_SCOPE)
