#include <string>
#include <thread>
#include <unordered_set>
#include <filesystem>
#include <cstdint>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "storage/cache/chunk_cache_manager.h"
#include "storage/cache/buffer_pool.h"
#include "storage/cache/disk_aio/Turbo_bin_io_handler.hpp"
#include "storage/cache/disk_aio/Turbo_bin_aio_handler.hpp"
#include "storage/cache/cache_data_transformer.h"

#include "common/exception.hpp"
#include "common/string_util.hpp"
#include "common/logger.hpp"
#include "common/types/string_type.hpp"

namespace duckdb {

ChunkCacheManager* ChunkCacheManager::ccm;

ChunkCacheManager::ChunkCacheManager(const char *path, bool standalone, bool read_only)
    : read_only_(read_only) {
  spdlog::debug("[ChunkCacheManager] Construct ChunkCacheManager (read_only={})", read_only);
  pool_ = std::make_unique<BufferPool>();  // memory_limit = 0 → 80% of RAM

  // Open (or create) the single store file
  std::string store_path = std::string(path) + "/" + store_db_name_;
  bool store_exists = (access(store_path.c_str(), F_OK) == 0);

  if (read_only_) {
    // Explicit read-only: open without write permission
    if (!store_exists)
      throw duckdb::IOException("store.db does not exist at: " + std::string(path));
    store_fd_ = open(store_path.c_str(), O_RDONLY | O_DIRECT, 0666);
  } else {
    // Read-write capable: open O_RDWR so we can upgrade lock later
    store_fd_ = open(store_path.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0666);
  }
  D_ASSERT(store_fd_ >= 0);

  // Always start with F_RDLCK (shared read lock).
  // Multiple processes can coexist with F_RDLCK.
  // Write lock (F_WRLCK) is acquired lazily via UpgradeToWriteLock()
  // on the first write operation.
  struct flock fl = {};
  fl.l_type   = F_RDLCK;
  fl.l_whence = SEEK_SET;
  fl.l_start  = 0;
  fl.l_len    = 0;  // entire file
  if (fcntl(store_fd_, F_SETLK, &fl) < 0) {
    close(store_fd_);
    store_fd_ = -1;
    throw duckdb::IOException(
        "store.db is locked by a writer (another process is writing)");
  }

  if (!read_only_) {
    if (!store_exists) {
      // New database: must acquire write lock immediately for pre-allocation
      UpgradeToWriteLock();
      if (fallocate(store_fd_, 0, 0, STORE_PREALLOC_SIZE) != 0) {
        ftruncate(store_fd_, STORE_PREALLOC_SIZE);
      }
      store_allocated_size_.store(STORE_PREALLOC_SIZE);
      store_file_size_.store(512);
    } else {
      // Existing database: stay with read lock, upgrade on first write
      struct stat st;
      fstat(store_fd_, &st);
      store_allocated_size_.store(st.st_size);
      // store_file_size_ will be restored by InitializeFileHandlersUsingMetaInfo
    }
  } else {
    // Read-only: restore physical file size (no alloc)
    struct stat st;
    fstat(store_fd_, &st);
    store_allocated_size_.store(st.st_size);
  }

  // Load chunk handlers from .store_meta (new format)
  std::string meta_path = std::string(path) + "/" + store_meta_name_;
  if (std::filesystem::exists(meta_path)) {
    InitializeFileHandlersUsingMetaInfo(path);
    // Restore counters from loaded segments (all start clean on open)
    total_segment_count_.store(file_handlers.size());
  }
  // else: fresh database, no chunks loaded yet

  // Background flush thread is started lazily by UpgradeToWriteLock()
  // when the first write operation occurs.
}

ChunkCacheManager::~ChunkCacheManager() {
  // Graceful shutdown: signal the background flush thread and join (write mode only)
  if (!read_only_) {
    {
      std::lock_guard<std::mutex> lock(flush_mu_);
      stop_flush_.store(true);
    }
    flush_cv_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();
  }

  spdlog::debug("[~ChunkCacheManager] Deconstruct ChunkCacheManager");

  if (!read_only_) {
    for (auto &file_handler: file_handlers) {
      if (file_handler.second == nullptr) continue;

      if (!pool_->GetDirty(file_handler.first)) continue;

      spdlog::trace("[~ChunkCacheManager] Flush file: {} with size {}", file_handler.second->GetFilePath(), file_handler.second->file_size());

      UnswizzleFlushSwizzle(file_handler.first, file_handler.second, false);
      pool_->ClearDirty(file_handler.first);
    }
  }

  for (auto &file_handler: file_handlers) {
    if (file_handler.second == nullptr) continue;
    delete file_handler.second;
  }

  if (store_fd_ >= 0) {
    close(store_fd_);
    store_fd_ = -1;
  }

}

void ChunkCacheManager::UpgradeToWriteLock() {
  if (has_write_lock_.load(std::memory_order_acquire)) return;  // already upgraded

  std::lock_guard<std::mutex> lk(write_lock_mu_);
  if (has_write_lock_.load(std::memory_order_relaxed)) return;  // double-check

  if (read_only_) {
    throw duckdb::IOException("Cannot write: database opened in read-only mode");
  }

  struct flock fl = {};
  fl.l_type   = F_WRLCK;
  fl.l_whence = SEEK_SET;
  fl.l_start  = 0;
  fl.l_len    = 0;  // entire file
  if (fcntl(store_fd_, F_SETLK, &fl) < 0) {
    throw duckdb::IOException(
        "Cannot acquire write lock: another reader or writer is active. "
        "Close other connections to this database first.");
  }

  has_write_lock_.store(true, std::memory_order_release);

  // Start background flush thread now that we have write access
  if (!flush_thread_.joinable()) {
    flush_thread_ = std::thread([this]() {
      while (true) {
        std::unique_lock<std::mutex> lock(flush_mu_);
        flush_cv_.wait(lock, [this]() {
          return stop_flush_.load() ||
                 (total_segment_count_.load() > 0 &&
                  (float)dirty_count_.load() / (float)total_segment_count_.load() >= HIGH_WATERMARK);
        });
        if (stop_flush_.load()) break;
        lock.unlock();
        spdlog::debug("[FlushThread] Dirty ratio above HIGH_WATERMARK, flushing");
        FlushDirtySegmentsAndDeleteFromcache(false);
        flush_cv_.notify_all();
      }
    });
  }
}

void ChunkCacheManager::UnswizzleFlushSwizzle(ChunkID cid, Turbo_bin_aio_handler* file_handler, bool close_file) {
  uint8_t *ptr;
  size_t size;

  /**
   * TODO we need a write lock
   * We flush in-memory data format to disk format.
   * Thus, unswizzling is needed.
   * However, since change the data in memory, we need to swizzle again.
   * After implementing eviction and so one, this code should be changed.
   * 
   * Ideal code is like below:
   * 1. Extent Manager write unswizzled data into memory
   * 2. ChunkCacheManager flush the data into disk, and remove all objects from store
   * 3. Proceeding clients can read the data from disk, and swizzle it.
  */
  PinSegment(cid, file_handler->GetFilePath(), &ptr, &size, false, false);
  CacheDataTransformer::Unswizzle(ptr);
  file_handler->FlushAllBlocking();
  file_handler->WaitAllPendingDiskIO(false);
  CacheDataTransformer::Swizzle(ptr);
  if (close_file)
    file_handler->Close();
  UnPinSegment(cid);
}

void ChunkCacheManager::InitializeFileHandlersUsingMetaInfo(const char *path)
{
  struct StoreMetaEntry { uint64_t chunk_id, file_offset, alloc_size, requested_size; };

  std::string meta_path = std::string(path) + "/" + store_meta_name_;
  int fd = open(meta_path.c_str(), O_RDONLY, 0666);
  D_ASSERT(fd >= 0);

  uint64_t num_entries = 0;
  ssize_t n = pread(fd, &num_entries, sizeof(uint64_t), 0);
  D_ASSERT(n == (ssize_t)sizeof(uint64_t));

  std::vector<StoreMetaEntry> entries(num_entries);
  if (num_entries > 0) {
    n = pread(fd, entries.data(), num_entries * sizeof(StoreMetaEntry), sizeof(uint64_t));
    D_ASSERT(n == (ssize_t)(num_entries * sizeof(StoreMetaEntry)));
  }
  close(fd);

  int64_t max_end = 512;
  for (auto& e : entries) {
    D_ASSERT(file_handlers.find(e.chunk_id) == file_handlers.end());
    file_handlers[e.chunk_id] = new Turbo_bin_aio_handler();
    file_handlers[e.chunk_id]->InitFromStore(store_fd_, (int64_t)e.file_offset,
                                             (int64_t)e.alloc_size, (int64_t)e.requested_size, false);
    int64_t end = (int64_t)(e.file_offset + e.alloc_size);
    if (end > max_end) max_end = end;
  }
  store_file_size_.store(max_end);
}

void ChunkCacheManager::FlushMetaInfo(const char *path)
{
  if (read_only_) return;
  UpgradeToWriteLock();
  spdlog::debug("[FlushMetaInfo] Start to flush meta info");

  struct StoreMetaEntry { uint64_t chunk_id, file_offset, alloc_size, requested_size; };

  uint64_t n = (uint64_t)file_handlers.size();
  std::vector<StoreMetaEntry> entries;
  entries.reserve(n);
  for (auto& [cid, h] : file_handlers) {
    D_ASSERT(h != nullptr);
    entries.push_back({ (uint64_t)cid, (uint64_t)h->GetBaseOffset(),
                        (uint64_t)h->file_size(), (uint64_t)h->GetRequestedSize() });
  }

  std::string tmp_path  = std::string(path) + "/" + store_meta_name_ + ".tmp";
  std::string meta_path = std::string(path) + "/" + store_meta_name_;

  // Write-ahead: tmp → fsync → rename (crash-safe)
  int fd = open(tmp_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
  D_ASSERT(fd >= 0);
  pwrite(fd, &n, sizeof(uint64_t), 0);
  if (n > 0) {
    pwrite(fd, entries.data(), n * sizeof(StoreMetaEntry), sizeof(uint64_t));
  }
  fsync(fd);
  close(fd);

  rename(tmp_path.c_str(), meta_path.c_str());
}

ReturnStatus ChunkCacheManager::PinSegment(ChunkID cid, std::string file_path, uint8_t** ptr, size_t* size, bool read_data_async, bool is_initial_loading) {
  spdlog::trace("[PinSegment] Start to pin segment: {}", cid);
  auto file_handler = GetFileHandler(cid);
  size_t segment_size = file_handler->GetRequestedSize();
  size_t file_size = file_handler->file_size();
  size_t required_memory_size = file_size + 512; // Add 512 byte for memory aligning
  D_ASSERT(file_size >= segment_size);

  uint8_t* raw_ptr = nullptr;
  size_t   raw_size = 0;

  if (pool_->Get(cid, &raw_ptr, &raw_size)) {
    // Cache hit: restore file_handler data pointer and return caller view.
    file_handler->SetDataPtr(raw_ptr);
    *ptr  = raw_ptr + sizeof(size_t);
    *size = segment_size - sizeof(size_t);
    return NOERROR;
  }

  // Cache miss: evict until we have enough room, then load from disk.
  while (pool_->FreeMemory() < file_size) {
    ChunkID victim = pool_->PickVictim();
    if (victim == ChunkID(-1)) {
      // All pages pinned — should not happen in normal operation.
      throw duckdb::IOException("BufferPool full: all pages pinned");
    }
    if (pool_->GetDirty(victim)) {
      // Flush victim to disk before evicting (calls PinSegment → cache hit).
      UnswizzleFlushSwizzle(victim, file_handlers[victim], false);
      pool_->ClearDirty(victim);
      dirty_count_.fetch_sub(1, std::memory_order_relaxed);
    }
    pool_->Remove(victim);
  }

  if (!pool_->Alloc(cid, file_size, &raw_ptr)) {
    throw duckdb::IOException("BufferPool: posix_memalign failed");
  }

  // Set data pointer (raw_ptr is 512-byte aligned; size header lives at [0..7]).
  file_handler->SetDataPtr(raw_ptr);

  // Read full region from disk (includes size header at offset 0).
  ReadData(cid, file_path, raw_ptr, file_size, false);

  if (!is_initial_loading) {
    CacheDataTransformer::Swizzle(raw_ptr + sizeof(size_t));
  }

  *ptr  = raw_ptr + sizeof(size_t);
  *size = segment_size - sizeof(size_t);
  return NOERROR;
}

ReturnStatus ChunkCacheManager::UnPinSegment(ChunkID cid) {
  pool_->Release(cid);
  return NOERROR;
}

ReturnStatus ChunkCacheManager::SetDirty(ChunkID cid) {
  UpgradeToWriteLock();
  pool_->SetDirty(cid);
  dirty_count_.fetch_add(1, std::memory_order_relaxed);
  return NOERROR;
}

ReturnStatus ChunkCacheManager::CreateSegment(ChunkID cid, std::string file_path, size_t alloc_size, bool can_destroy) {
  UpgradeToWriteLock();
  spdlog::trace("[CreateSegment] Start to create segment: {}", cid);
  auto ret = CreateNewFile(cid, file_path, alloc_size, can_destroy);
  if (ret == NOERROR) {
    total_segment_count_.fetch_add(1, std::memory_order_relaxed);
  }
  return ret;
}

ReturnStatus ChunkCacheManager::DestroySegment(ChunkID cid) {
  spdlog::trace("[DestroySegment] Start to destroy segment: {}", cid);
  D_ASSERT(file_handlers.find(cid) != file_handlers.end());
  pool_->Remove(cid);
  // jhha: disable file_handler close, due to flushMetaInfo
  // file_handlers[cid]->Close();
  // file_handlers[cid] = nullptr;
  //AdjustMemoryUsage(-GetSegmentSize(cid)); // need type casting
  return NOERROR;
}

ReturnStatus ChunkCacheManager::FinalizeIO(ChunkID cid, bool read, bool write) {
  auto it = file_handlers.find(cid);
  if (it != file_handlers.end() && it->second) {
    it->second->WaitForMyIoRequests(read, write);
  }
  return NOERROR;
}

ReturnStatus ChunkCacheManager::FlushDirtySegmentsAndDeleteFromcache(bool destroy_segment) {
  spdlog::debug("[FlushDirtySegmentsAndDeleteFromcache] Start to {} flush dirty segments and delete from cache", file_handlers.size());

    // Collect iterators into a vector
    vector<unordered_map<ChunkID, Turbo_bin_aio_handler*>::iterator> iterators;
    iterators.reserve(file_handlers.size());
    for (auto it = file_handlers.begin(); it != file_handlers.end(); ++it) {
        iterators.push_back(it);
    }

  #pragma omp parallel for num_threads(32) 
  for (size_t i = 0; i < iterators.size(); i++) {
    auto &file_handler = *(iterators[i]);
    if (file_handler.second == nullptr) continue;

    if (!pool_->GetDirty(file_handler.first)) continue;

    spdlog::trace("[FlushDirtySegmentsAndDeleteFromcache] Flush file: {} with size {}", file_handler.second->GetFilePath(), file_handler.second->file_size());

    UnswizzleFlushSwizzle(file_handler.first, file_handler.second, false);
    pool_->ClearDirty(file_handler.first);
    dirty_count_.fetch_sub(1, std::memory_order_relaxed);
    if (destroy_segment) {
      DestroySegment(file_handler.first);
    }
  }
  return NOERROR;
}

void ChunkCacheManager::ThrottleIfNeeded() {
  size_t total = total_segment_count_.load(std::memory_order_relaxed);
  if (total == 0) return;
  float ratio = (float)dirty_count_.load(std::memory_order_relaxed) / (float)total;

  if (ratio >= CRITICAL_WATERMARK) {
    // Block until flush thread brings ratio below HIGH_WATERMARK
    spdlog::warn("[ThrottleIfNeeded] CRITICAL watermark reached ({:.1f}%), blocking pipeline", ratio * 100.0f);
    std::unique_lock<std::mutex> lock(flush_mu_);
    flush_cv_.notify_all();  // ensure flush thread is awake
    while (true) {
      bool released = flush_cv_.wait_for(lock, std::chrono::milliseconds(100), [this]() {
        size_t t = total_segment_count_.load(std::memory_order_relaxed);
        if (t == 0) return true;
        float r = (float)dirty_count_.load(std::memory_order_relaxed) / (float)t;
        return r < HIGH_WATERMARK || stop_flush_.load();
      });
      if (released) break;
      // Timeout: log and retry (guards against flush thread stall)
      spdlog::warn("[ThrottleIfNeeded] Still waiting for flush thread...");
    }
    spdlog::debug("[ThrottleIfNeeded] Resumed after flush");
  } else if (ratio >= HIGH_WATERMARK) {
    // Async: just wake up the flush thread and continue
    flush_cv_.notify_one();
  }
}

ReturnStatus ChunkCacheManager::GetRemainingMemoryUsage(size_t &remaining_memory_usage) {
  remaining_memory_usage = pool_->FreeMemory();
  return NOERROR;
}

int ChunkCacheManager::GetRefCount(ChunkID cid) {
  return pool_->RefCount(cid);
}

Turbo_bin_aio_handler* ChunkCacheManager::GetFileHandler(ChunkID cid) {
  auto file_handler = file_handlers.find(cid);
  if (file_handler == file_handlers.end()) {
    fprintf(stderr, "[CCM] GetFileHandler MISS: cid=%lu (0x%lx), file_handlers.size=%zu\n",
            (unsigned long)cid, (unsigned long)cid, file_handlers.size());
    // Dump first 20 registered cids for diagnosis
    int cnt = 0;
    for (auto &kv : file_handlers) {
      fprintf(stderr, "  registered cid=%lu (0x%lx)\n", (unsigned long)kv.first, (unsigned long)kv.first);
      if (++cnt >= 20) { fprintf(stderr, "  ... (%zu total)\n", file_handlers.size()); break; }
    }
  }
  if (file_handler == file_handlers.end()) {
    throw duckdb::IOException("GetFileHandler: chunk not found in store_meta for cid " + std::to_string(cid));
  }
  D_ASSERT(file_handler->second != nullptr);
  if (file_handler->second->GetFileID() == -1) {
    throw duckdb::IOException("GetFileHandler: file not open for cid " + std::to_string(cid));
  }
  return file_handler->second;
}

void ChunkCacheManager::ReadData(ChunkID cid, std::string file_path, void* ptr, size_t size_to_read, bool read_data_async) {
  auto file_handler = file_handlers.find(cid);
  if (file_handlers[cid]->GetFileID() == -1) {
    throw duckdb::IOException("ReadData: file not open for cid " + std::to_string(cid));
  }
  // file_handlers[cid]->Read(0, (int64_t) size_to_read, (char*) ptr, nullptr, nullptr);
  file_handlers[cid]->ReadWithSplittedIORequest(0, (int64_t) size_to_read, (char*) ptr, nullptr, nullptr);
  if (!read_data_async) file_handlers[cid]->WaitForMyIoRequests(true, true);
  total_read_size += size_to_read;
}

void ChunkCacheManager::WriteData(ChunkID cid) {
  D_ASSERT(file_handlers.find(cid) != file_handlers.end());
  // Flush is handled via UnswizzleFlushSwizzle; nothing to do here.
}

ReturnStatus ChunkCacheManager::CreateNewFile(ChunkID cid, std::string file_path, size_t alloc_size, bool can_destroy) {
  D_ASSERT(file_handlers.find(cid) == file_handlers.end());
  D_ASSERT(store_fd_ >= 0);

  // Align to 512B boundary (required for O_DIRECT)
  int64_t alloc_file_size = (int64_t)(((alloc_size + sizeof(size_t) + 511) / 512) * 512);

  // Atomic offset allocation (lock-free fast path)
  int64_t offset = store_file_size_.fetch_add(alloc_file_size);
  int64_t needed = offset + alloc_file_size;

  // Pre-allocate file space in 1GB chunks when needed (double-checked locking)
  if (needed > store_allocated_size_.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> lk(store_extend_mutex_);
    if (needed > store_allocated_size_.load(std::memory_order_relaxed)) {
      int64_t new_size = ((needed + STORE_PREALLOC_SIZE - 1) / STORE_PREALLOC_SIZE) * STORE_PREALLOC_SIZE;
      if (fallocate(store_fd_, 0, 0, new_size) != 0) {
        ftruncate(store_fd_, new_size);
      }
      store_allocated_size_.store(new_size, std::memory_order_release);
    }
  }

  // Create handler using shared store fd + per-chunk base offset
  file_handlers[cid] = new Turbo_bin_aio_handler();
  file_handlers[cid]->InitFromStore(store_fd_, offset, alloc_file_size, alloc_size + sizeof(size_t));
  file_handlers[cid]->SetCanDestroy(can_destroy);
  return NOERROR;
}


}
