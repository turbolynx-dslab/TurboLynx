// disk_aio_wasm.cpp — WASM stub replacing all disk_aio source files
//
// Provides synchronous I/O via standard POSIX read/lseek (Emscripten compat).
// No kernel AIO, no background threads, no NUMA, no OpenMP.

#ifdef TURBOLYNX_WASM

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <assert.h>
#include <string>
#include <vector>

#include "storage/cache/disk_aio/disk_aio_factory.hpp"

// --- DiskAioFactory singleton ---
DiskAioFactory* DiskAioFactory::ptr = nullptr;

DiskAioFactory::DiskAioFactory(int &res, int, int) {
    assert(ptr == nullptr);
    daio = new diskaio::DiskAio(0, 0);  // no threads
    DiskAioFactory::ptr = this;
    res = true;
}

DiskAioFactory::~DiskAioFactory() {
    DiskAioFactory::ptr = nullptr;
    delete daio;
}

int DiskAioFactory::OpenAioFile(const char *file_path, int flag) {
    // Strip O_DIRECT (not supported on Emscripten)
    flag &= ~040000;  // O_DIRECT = 040000 on Linux
    int fd = open(file_path, flag, S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd < 0) {
        perror("WASM OpenAioFile");
    }
    return fd;
}

void DiskAioFactory::RemoveAioFile(int) {}

void DiskAioFactory::CloseAioFile(int file_id, bool) {
    if (file_id >= 0) close(file_id);
}

std::size_t DiskAioFactory::GetAioFileSize(int file_id) {
    assert(file_id >= 0);
    off_t cur = lseek(file_id, 0, SEEK_CUR);
    size_t fsize = lseek(file_id, 0, SEEK_END);
    lseek(file_id, cur, SEEK_SET);
    return fsize;
}

int DiskAioFactory::Getfd(int file_id) {
    return file_id;
}

diskaio::DiskAioInterface* DiskAioFactory::CreateAioInterface(int max_num_ongoing, int) {
    return new diskaio::DiskAioInterface(this, max_num_ongoing, nullptr);
}

diskaio::DiskAioInterface* DiskAioFactory::GetAioInterface(int) {
    return CreateAioInterface();
}

void DiskAioFactory::CreateAioInterfaces(int) {}

// Synchronous read
int DiskAioFactory::ARead(AioRequest &req, diskaio::DiskAioInterface*) {
    ssize_t n = pread(req.user_info.file_id >= 0 ? req.user_info.file_id : 0,
                      req.buf, req.io_size, req.start_pos);
    return (n >= 0) ? 0 : -1;
}

int DiskAioFactory::AWrite(AioRequest &req, diskaio::DiskAioInterface*) {
    // Read-only in WASM
    return -1;
}

int DiskAioFactory::AAppend(AioRequest &req, diskaio::DiskAioInterface*) {
    return -1;
}

int DiskAioFactory::WaitForAllResponses(diskaio::DiskAioInterface**) {
    return 0;  // all I/O is synchronous
}

int DiskAioFactory::GetNumOngoing(diskaio::DiskAioInterface**) {
    return 0;
}

diskaio::DiskAioStats DiskAioFactory::GetStats() {
    return {};
}

void DiskAioFactory::ResetStats() {}

// --- DiskAioInterface (sync stub) ---
namespace diskaio {

void DiskAioInterface::Register(DiskAioThread*) {}

int DiskAioInterface::ProcessResponses() { return 0; }

DiskAioRequest* DiskAioInterface::PackRequest(int fd, off_t offset, ssize_t iosize, char* buf, int) {
    DiskAioRequest* req = nullptr;
    if (request_allocator_.fetch(&req, 1) == 0) return nullptr;
    io_prep_pread(&req->cb, fd, buf, iosize, offset);
    return req;
}

bool DiskAioInterface::Request(int fd, off_t offset, ssize_t iosize, char* buf, int io_type, void*) {
    // Synchronous read directly
    if (io_type == DISK_AIO_READ) {
        pread(fd, buf, iosize, offset);
    }
    return true;
}

bool DiskAioInterface::Request(int fd, off_t offset, ssize_t iosize, char* buf, int io_type, DiskAioRequestUserInfo&) {
    return Request(fd, offset, iosize, buf, io_type);
}

int DiskAioInterface::GetNumOngoing() { return 0; }
int DiskAioInterface::WaitForResponses() { return 0; }
int DiskAioInterface::WaitForResponses(int) { return 0; }

// --- DiskAioRequest ---
int DiskAioRequest::Complete() { return 0; }

}  // namespace diskaio

// --- DiskAioThread (stub) ---
namespace diskaio {

void DiskAioThread::RegisterInterface(void*) {}

}  // namespace diskaio

// --- core_id / per_thread stubs ---
#include "storage/cache/disk_aio/eXDB_dist_internal.hpp"

// Static member definitions — must match header declarations exactly
__thread int64_t core_id::my_core_id_ = 0;
int64_t core_id::core_counts_ = 0;

void core_id::set_core_ids(int) {}

// --- TypeDef statics (provided by TypeDef.cpp in normal build, included here) ---
// Note: TypeDef.cpp is still compiled separately, so skip these if it's also in the build.

// --- Turbo_bin_aio_handler static members ---
#include "storage/cache/disk_aio/Turbo_bin_aio_handler.hpp"
#include <mutex>

__thread int64_t Turbo_bin_aio_handler::my_core_id_ = 0;
int64_t Turbo_bin_aio_handler::core_counts_ = 0;
per_thread_lazy<diskaio::DiskAioInterface*> Turbo_bin_aio_handler::per_thread_aio_interface_read;
per_thread_lazy<diskaio::DiskAioInterface*> Turbo_bin_aio_handler::per_thread_aio_interface_write;
// Note: free_core_ids_ / free_core_ids_mu_ are no longer static members —
// Turbo_bin_aio_handler uses Meyers-singleton accessors FreeCoreIds() /
// FreeCoreIdsMutex() defined inline in the header. Nothing to instantiate
// here.

// --- InitializeDiskAio (WASM stub) ---
#include "common/disk_aio_init.hpp"
#include "common/logger.hpp"

namespace turbolynx {
DiskAioFactory* InitializeDiskAio(const std::string& workspace) {
    if (DiskAioFactory::GetPtr() != NULL) {
        DiskAioParameters::WORKSPACE = workspace;
        return DiskAioFactory::GetPtr();
    }
    DiskAioParameters::NUM_THREADS = 1;
    DiskAioParameters::NUM_TOTAL_CPU_CORES = 1;
    DiskAioParameters::NUM_CPU_SOCKETS = 1;
    DiskAioParameters::NUM_DISK_AIO_THREADS = 1;
    DiskAioParameters::WORKSPACE = workspace;
    int res;
    DiskAioFactory* factory = new DiskAioFactory(res, 1, 128);
    core_id::set_core_ids(1);
    return factory;
}
} // namespace turbolynx

// --- fallocate stub (Emscripten doesn't have it) ---
extern "C" {
int fallocate(int fd, int mode, off_t offset, off_t len) {
    // Emulate by extending file with ftruncate if needed
    struct stat st;
    if (fstat(fd, &st) != 0) return -1;
    off_t needed = offset + len;
    if (st.st_size < needed) {
        return ftruncate(fd, needed);
    }
    return 0;
}
}

// --- NumaHelper statics ---
int64_t NumaHelper::sockets = 1;
int64_t NumaHelper::cores = 1;
int64_t NumaHelper::cores_per_socket = 1;
int64_t NumaHelper::numa_policy = 0;
NumaHelper NumaHelper::numa_helper;

// --- my_thread statics (WASM stub is header-only, but thread.cpp defines some) ---
// The WASM my_thread class is fully inline in thread.h, no .cpp needed.

#endif // TURBOLYNX_WASM
