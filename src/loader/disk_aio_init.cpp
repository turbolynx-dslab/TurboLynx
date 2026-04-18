#include "common/disk_aio_init.hpp"
#include "storage/cache/disk_aio/disk_aio_factory.hpp"
#include "storage/cache/disk_aio/util.hpp"
#include "common/logger.hpp"

#include <algorithm>
#include <thread>

namespace duckdb {
}
namespace turbolynx {

DiskAioFactory* InitializeDiskAio(const std::string& workspace) {
    // Reuse existing singleton if already initialized
    if (DiskAioFactory::GetPtr() != NULL) {
        DiskAioParameters::WORKSPACE = workspace;
        spdlog::debug("DiskAioFactory already initialized, reusing (workspace: {})", workspace);
        return DiskAioFactory::GetPtr();
    }

#ifdef TURBOLYNX_PORTABLE_DISK_IO
    auto detected_cores = std::max<int64_t>(1, static_cast<int64_t>(std::thread::hardware_concurrency()));
    DiskAioParameters::NUM_THREADS = std::min<int64_t>(detected_cores, MAX_NUM_PER_THREAD_DATASTRUCTURE);
    DiskAioParameters::NUM_TOTAL_CPU_CORES = detected_cores;
    DiskAioParameters::NUM_CPU_SOCKETS = 1;
    DiskAioParameters::NUM_DISK_AIO_THREADS = 1;
#else
    DiskAioParameters::NUM_THREADS = 32;
    DiskAioParameters::NUM_TOTAL_CPU_CORES = 32;
    DiskAioParameters::NUM_CPU_SOCKETS = 2;
    DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
#endif
    DiskAioParameters::WORKSPACE = workspace;

    int res;
    DiskAioFactory* factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
    core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

    spdlog::info("DiskAioParameters::NUM_DISK_AIO_THREADS: {}", DiskAioParameters::NUM_DISK_AIO_THREADS);
    spdlog::info("DiskAioParameters::WORKSPACE: {}", DiskAioParameters::WORKSPACE);

    return factory;
}

} // namespace turbolynx
