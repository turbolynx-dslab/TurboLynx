// libaio_shim.hpp — Drop-in replacement for <libaio.h>
//
// Under TURBOLYNX_WASM/TURBOLYNX_PORTABLE_DISK_IO: provides empty struct definitions only (no syscalls).

#pragma once

#if defined(TURBOLYNX_WASM) || defined(TURBOLYNX_PORTABLE_DISK_IO)
// ======== portable stubs: no kernel AIO ========
#include <string.h>
#include <assert.h>

typedef unsigned long io_context_t;

enum io_iocb_cmd {
    IO_CMD_PREAD  = 0,
    IO_CMD_PWRITE = 1,
    IO_CMD_FSYNC  = 2,
    IO_CMD_FDSYNC = 3,
    IO_CMD_NOOP   = 6,
};

struct iocb {
    void         *data;
    unsigned      key;
    unsigned      aio_rw_flags;
    short         aio_lio_opcode;
    short         aio_reqprio;
    int           aio_fildes;
    union {
        struct {
            void          *buf;
            unsigned long  nbytes;
            long long      offset;
            long long      __pad;
            unsigned       flags;
            unsigned       resfd;
        } c;
    } u;
};

struct io_event {
    void         *data;
    struct iocb  *obj;
    long long     res;
    long long     res2;
};

static inline int io_setup(unsigned, io_context_t *ctx) { *ctx = 0; return 0; }
static inline int io_destroy(io_context_t) { return 0; }
static inline int io_submit(io_context_t, long, struct iocb **) { return 0; }
static inline int io_getevents(io_context_t, long, long, struct io_event *, struct timespec *) { return 0; }
static inline int io_cancel(io_context_t, struct iocb *, struct io_event *) { return 0; }

static inline void io_prep_pread(struct iocb *iocb, int fd,
                                 void *buf, size_t count, long long offset) {
    memset(iocb, 0, sizeof(*iocb));
    iocb->aio_fildes = fd;
    iocb->aio_lio_opcode = IO_CMD_PREAD;
    iocb->u.c.buf = buf;
    iocb->u.c.nbytes = count;
    iocb->u.c.offset = offset;
}

static inline void io_prep_pwrite(struct iocb *iocb, int fd,
                                  void *buf, size_t count, long long offset) {
    memset(iocb, 0, sizeof(*iocb));
    iocb->aio_fildes = fd;
    iocb->aio_lio_opcode = IO_CMD_PWRITE;
    iocb->u.c.buf = buf;
    iocb->u.c.nbytes = count;
    iocb->u.c.offset = offset;
}

static inline int io_queue_init(int, io_context_t *ctx) { *ctx = 0; return 0; }

#else // real Linux AIO

#include <sys/syscall.h>   // SYS_io_setup / submit / getevents / destroy
#include <unistd.h>        // syscall()
#include <time.h>          // struct timespec
#include <string.h>        // memset
#include <errno.h>         // EINTR
#include <assert.h>

// --------------------------------------------------------------------------
// io_context_t  (opaque kernel handle; kernel stores it as unsigned long)
// --------------------------------------------------------------------------
typedef unsigned long io_context_t;

// --------------------------------------------------------------------------
// I/O opcodes  (subset used in this project)
// --------------------------------------------------------------------------
enum io_iocb_cmd {
    IO_CMD_PREAD  = 0,
    IO_CMD_PWRITE = 1,
    IO_CMD_FSYNC  = 2,
    IO_CMD_FDSYNC = 3,
    IO_CMD_NOOP   = 6,
};

// --------------------------------------------------------------------------
// struct iocb — must match kernel uapi/linux/aio_abi.h layout (64 bytes)
//
// x86_64 offsets:
//   0  : data          (8)   user pointer returned in io_event
//   8  : key           (4)   request key
//  12  : aio_rw_flags  (4)   kernel RW flags (zero for standard pread/pwrite)
//  16  : aio_lio_opcode(2)   IO_CMD_*
//  18  : aio_reqprio   (2)   priority
//  20  : aio_fildes    (4)   file descriptor
//  24  : u.c.buf       (8)   buffer pointer
//  32  : u.c.nbytes    (8)   byte count
//  40  : u.c.offset    (8)   file offset
//  48  : u.c.__pad     (8)   reserved (aio_reserved2)
//  56  : u.c.flags     (4)   aio_flags
//  60  : u.c.resfd     (4)   eventfd (if IOCB_FLAG_RESFD)
// --------------------------------------------------------------------------
struct iocb {
    void         *data;            // offset  0
    unsigned      key;             // offset  8
    unsigned      aio_rw_flags;    // offset 12
    short         aio_lio_opcode;  // offset 16
    short         aio_reqprio;     // offset 18
    int           aio_fildes;      // offset 20
    union {
        struct {
            void          *buf;    // offset 24
            unsigned long  nbytes; // offset 32
            long long      offset; // offset 40
            long long      __pad;  // offset 48  (aio_reserved2)
            unsigned       flags;  // offset 56  (aio_flags)
            unsigned       resfd;  // offset 60  (aio_resfd)
        } c;
    } u;
};
static_assert(sizeof(struct iocb) == 64, "libaio_shim: iocb size mismatch");
static_assert(__builtin_offsetof(struct iocb, u.c.buf)    == 24, "");
static_assert(__builtin_offsetof(struct iocb, u.c.nbytes) == 32, "");
static_assert(__builtin_offsetof(struct iocb, u.c.offset) == 40, "");

// --------------------------------------------------------------------------
// struct io_event — must match kernel uapi/linux/aio_abi.h (32 bytes)
//
//   0 : data  (8)  copy of iocb.data
//   8 : obj   (8)  pointer to the completed iocb
//  16 : res   (8)  result (bytes transferred, or -errno)
//  24 : res2  (8)  secondary result
// --------------------------------------------------------------------------
struct io_event {
    void         *data;   // offset  0
    struct iocb  *obj;    // offset  8
    long long     res;    // offset 16
    long long     res2;   // offset 24
};
static_assert(sizeof(struct io_event) == 32, "libaio_shim: io_event size mismatch");

// --------------------------------------------------------------------------
// Syscall wrappers
// --------------------------------------------------------------------------
static inline int io_setup(unsigned maxevents, io_context_t *ctx) {
    return (int)syscall(SYS_io_setup, (unsigned long)maxevents, ctx);
}

static inline int io_destroy(io_context_t ctx) {
    return (int)syscall(SYS_io_destroy, ctx);
}

static inline int io_submit(io_context_t ctx, long nr, struct iocb **iocbs) {
    return (int)syscall(SYS_io_submit, ctx, nr, iocbs);
}

static inline int io_getevents(io_context_t ctx, long min_nr, long nr,
                               struct io_event *events, struct timespec *timeout) {
    return (int)syscall(SYS_io_getevents, ctx, min_nr, nr, events, timeout);
}

static inline int io_cancel(io_context_t ctx, struct iocb *iocb,
                            struct io_event *result) {
    return (int)syscall(SYS_io_cancel, ctx, iocb, result);
}

// --------------------------------------------------------------------------
// Inline helpers  (mirror of libaio.h static inlines)
// --------------------------------------------------------------------------
static inline void io_prep_pread(struct iocb *iocb, int fd,
                                 void *buf, size_t count, long long offset) {
    memset(iocb, 0, sizeof(*iocb));
    iocb->aio_fildes     = fd;
    iocb->aio_lio_opcode = IO_CMD_PREAD;
    iocb->aio_reqprio    = 0;
    iocb->u.c.buf        = buf;
    iocb->u.c.nbytes     = count;
    iocb->u.c.offset     = offset;
}

static inline void io_prep_pwrite(struct iocb *iocb, int fd,
                                  void *buf, size_t count, long long offset) {
    memset(iocb, 0, sizeof(*iocb));
    iocb->aio_fildes     = fd;
    iocb->aio_lio_opcode = IO_CMD_PWRITE;
    iocb->aio_reqprio    = 0;
    iocb->u.c.buf        = buf;
    iocb->u.c.nbytes     = count;
    iocb->u.c.offset     = offset;
}

static inline int io_queue_init(int maxevents, io_context_t *ctx) {
    *ctx = 0;
    return io_setup((unsigned)maxevents, ctx);
}

#endif
