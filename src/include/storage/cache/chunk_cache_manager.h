#ifndef CHUNK_CACHE_MANAGER_H
#define CHUNK_CACHE_MANAGER_H

#include <string>
#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <vector>
#include "common/unordered_map.hpp"

#include "common/constants.hpp"
#include "storage/cache/common.h"
#include "storage/cache/buffer_pool.h"
#include "storage/cache/disk_aio/Turbo_bin_aio_handler.hpp"

namespace duckdb {

#define NUM_MAX_SEGMENTS 65536

class ChunkCacheManager {
public:
  static ChunkCacheManager *ccm;

public:
  ChunkCacheManager(const char *path, bool read_only=false);
  ~ChunkCacheManager();

  void InitializeFileHandlersUsingMetaInfo(const char *path);
  void FlushMetaInfo(const char *path);

  // ChunkCacheManager APIs
  ReturnStatus PinSegment(ChunkID cid, std::string file_path, uint8_t** ptr, size_t* size, bool read_data_async=false, bool is_initial_loading=false);
  ReturnStatus UnPinSegment(ChunkID cid);
  ReturnStatus SetDirty(ChunkID cid);
  ReturnStatus CreateSegment(ChunkID cid, std::string file_path, size_t alloc_size, bool can_destroy);
  ReturnStatus DestroySegment(ChunkID cid);
  ReturnStatus FinalizeIO(ChunkID cid, bool read=true, bool write=true);
  ReturnStatus FlushDirtySegmentsAndDeleteFromcache(bool destroy_segment=false);
  ReturnStatus GetRemainingMemoryUsage(size_t &remaining_memory_usage);

  // Backpressure: call inside write loops. Wakes bg flush at HIGH_WATERMARK;
  // blocks with wait_for(100ms) loops at CRITICAL_WATERMARK until ratio drops.
  void ThrottleIfNeeded();

  // APIs for Debugging purpose
  int GetRefCount(ChunkID cid);

  // ChunkCacheManager Internal Functions
  Turbo_bin_aio_handler* GetFileHandler(ChunkID cid);
  void ReadData(ChunkID cid, std::string file_path, void *ptr, size_t size_to_read, bool read_data_async);
  void WriteData(ChunkID cid);
  ReturnStatus CreateNewFile(ChunkID cid, std::string file_path, size_t alloc_size, bool can_destroy);
  void UnswizzleFlushSwizzle(ChunkID cid, Turbo_bin_aio_handler* file_handler, bool close_file=true);

  // Lock upgrade: acquire exclusive write lock on store.db.
  // Called lazily on first write operation. Throws IOException if
  // another reader/writer prevents the upgrade.
  void UpgradeToWriteLock();

public:
  // Member Variables
  std::unique_ptr<BufferPool> pool_;
  unordered_map<ChunkID, Turbo_bin_aio_handler*> file_handlers;
  const std::string file_meta_info_name = ".file_meta_info";
  uint64_t total_read_size = 0;

  // Two-watermark backpressure
  static constexpr float HIGH_WATERMARK     = 0.75f;
  static constexpr float CRITICAL_WATERMARK = 0.95f;
  std::atomic<size_t> dirty_count_{0};
  std::atomic<size_t> total_segment_count_{0};
  std::atomic<bool>   stop_flush_{false};
  std::thread         flush_thread_;
  std::condition_variable flush_cv_;
  std::mutex          flush_mu_;

  bool read_only_ = false;
  std::atomic<bool> has_write_lock_{false};
  std::mutex write_lock_mu_;

  // Serializes PinSegment so a cache-miss load (Alloc → ReadData → Swizzle)
  // is atomic from the perspective of concurrent pinners. Without this, a
  // second thread can hit the entry inserted by Alloc before ReadData has
  // filled it, and read partially-loaded / garbage data — observed under
  // parallel filter-pushdown NodeScan in TPC-H Q10.
  std::mutex pin_mu_;

  // Single-file block store
  int store_fd_ = -1;
  std::atomic<int64_t> store_file_size_{512};     // logical end (next alloc offset)
  std::atomic<int64_t> store_allocated_size_{0};  // physical size (fallocate'd)
  std::mutex store_extend_mutex_;
  static constexpr int64_t STORE_PREALLOC_SIZE = 1LL << 30;  // 1 GB
  const std::string store_db_name_   = "store.db";
  const std::string store_meta_name_ = ".store_meta";
};

}

#endif // CHUNK_CACHE_MANAGER_H
