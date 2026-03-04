#include "common/disk_aio_init.hpp"
#include "storage/cache/disk_aio/disk_aio_factory.hpp"
#include "storage/cache/disk_aio/util.hpp"
#include "common/logger.hpp"

namespace duckdb {

DiskAioFactory* InitializeDiskAio(const std::string& workspace) {
    DiskAioParameters::NUM_THREADS = 32;
    DiskAioParameters::NUM_TOTAL_CPU_CORES = 32;
    DiskAioParameters::NUM_CPU_SOCKETS = 2;
    DiskAioParameters::NUM_DISK_AIO_THREADS = DiskAioParameters::NUM_CPU_SOCKETS * 2;
    DiskAioParameters::WORKSPACE = workspace;

    int res;
    DiskAioFactory* factory = new DiskAioFactory(res, DiskAioParameters::NUM_DISK_AIO_THREADS, 128);
    core_id::set_core_ids(DiskAioParameters::NUM_THREADS);

    spdlog::info("DiskAioParameters::NUM_DISK_AIO_THREADS: {}", DiskAioParameters::NUM_DISK_AIO_THREADS);
    spdlog::info("DiskAioParameters::WORKSPACE: {}", DiskAioParameters::WORKSPACE);

    return factory;
}

} // namespace duckdb
