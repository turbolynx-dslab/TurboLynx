#ifndef UNDO_DISK_H
#define UNDO_DISK_H

#include "memory.h"

// Stub UndoLogDisk — crash recovery removed (Milestone 14).
// Write() performs direct assignment only; no shm log file.
struct UndoLogDisk {
  uint8_t *shm_base_;

  UndoLogDisk(size_t /*log_size*/, uint8_t *shm_base, size_t /*shm_size*/)
      : shm_base_(shm_base) {}

  void BeginTx()  {}
  void CommitTx() {}

  void Write(sm_offset offset, uint64_t value) {
    *(uint64_t *)&shm_base_[offset] = value;
  }
};

// If disk is null, fall back to a direct assignment.
#define LOGGED_WRITE(lval, rval, hdr_ptr, log_ptr)                             \
  do {                                                                         \
    if (log_ptr) {                                                             \
      sm_offset _offset = (uint8_t *)(&(lval)) - (uint8_t *)(hdr_ptr);        \
      (log_ptr)->Write(_offset, (uint64_t)(rval));                             \
    } else {                                                                   \
      (lval) = (rval);                                                         \
    }                                                                          \
  } while (false)

#endif // UNDO_DISK_H
