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
#include "storage/cache/disk_aio/Turbo_bin_io_handler.hpp"
#include "storage/cache/disk_aio/Turbo_bin_aio_handler.hpp"
#include "storage/cache/cache_data_transformer.h"

#include "common/exception.hpp"
#include "common/string_util.hpp"
#include "common/logger.hpp"
#include "common/types/string_type.hpp"

namespace duckdb {

ChunkCacheManager* ChunkCacheManager::ccm;

ChunkCacheManager::ChunkCacheManager(const char *path, bool standalone) {
  spdlog::debug("[ChunkCacheManager] Construct ChunkCacheManager");
  client = new LightningClient("/tmp/lightning", "password", standalone);

  // Open (or create) the single store file
  std::string store_path = std::string(path) + "/" + store_db_name_;
  bool store_exists = (access(store_path.c_str(), F_OK) == 0);
  store_fd_ = open(store_path.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0666);
  D_ASSERT(store_fd_ >= 0);

  if (!store_exists) {
    // Pre-allocate 1GB and reserve the first 512B as header space
    if (fallocate(store_fd_, 0, 0, STORE_PREALLOC_SIZE) != 0) {
      ftruncate(store_fd_, STORE_PREALLOC_SIZE);
    }
    store_allocated_size_.store(STORE_PREALLOC_SIZE);
    store_file_size_.store(512);
  } else {
    // Restore physical file size
    struct stat st;
    fstat(store_fd_, &st);
    store_allocated_size_.store(st.st_size);
    // store_file_size_ will be restored by InitializeFileHandlersUsingMetaInfo
  }

  // Load chunk handlers from .store_meta (new format)
  std::string meta_path = std::string(path) + "/" + store_meta_name_;
  if (std::filesystem::exists(meta_path)) {
    InitializeFileHandlersUsingMetaInfo(path);
    // Restore counters from loaded segments (all start clean on open)
    total_segment_count_.store(file_handlers.size());
  }
  // else: fresh database, no chunks loaded yet

  // Start background flush thread
  flush_thread_ = std::thread([this]() {
    while (true) {
      std::unique_lock<std::mutex> lock(flush_mu_);
      // Wait until signalled to flush or stopped
      flush_cv_.wait(lock, [this]() {
        return stop_flush_.load() ||
               (total_segment_count_.load() > 0 &&
                (float)dirty_count_.load() / (float)total_segment_count_.load() >= HIGH_WATERMARK);
      });
      if (stop_flush_.load()) break;
      lock.unlock();
      spdlog::debug("[FlushThread] Dirty ratio above HIGH_WATERMARK, flushing");
      FlushDirtySegmentsAndDeleteFromcache(false);
      flush_cv_.notify_all();  // wake any blocked ThrottleIfNeeded callers
    }
  });
}

ChunkCacheManager::~ChunkCacheManager() {
  // Graceful shutdown: signal the background flush thread and join
  {
    std::lock_guard<std::mutex> lock(flush_mu_);
    stop_flush_.store(true);
  }
  flush_cv_.notify_all();
  if (flush_thread_.joinable()) flush_thread_.join();

  fprintf(stderr, "Total read size = %ld\n", total_read_size);
  spdlog::debug("[~ChunkCacheManager] Deconstruct ChunkCacheManager");
  for (auto &file_handler: file_handlers) {
    if (file_handler.second == nullptr) continue;

    bool is_dirty;
    client->GetDirty(file_handler.first, is_dirty);
    if (!is_dirty) continue;

    spdlog::trace("[~ChunkCacheManager] Flush file: {} with size {}", file_handler.second->GetFilePath(), file_handler.second->file_size());

    // TODO we need a write lock
    // close_file=false: the store fd is shared across all handlers;
    // do not close it here.  store_fd_ is closed once below.
    UnswizzleFlushSwizzle(file_handler.first, file_handler.second, false);
    client->ClearDirty(file_handler.first);
  }

  for (auto &file_handler: file_handlers) {
    if (file_handler.second == nullptr) continue;
    delete file_handler.second;
  }

  if (store_fd_ >= 0) {
    close(store_fd_);
    store_fd_ = -1;
  }

  delete client;
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

void ChunkCacheManager::InitializeFileHandlersByIteratingDirectories(const char *path)
{
  std::string partition_path = std::string(path);
  for (const auto &partition_entry : std::filesystem::directory_iterator(partition_path)) { // /path/
    std::string partition_entry_path = std::string(partition_entry.path());
    std::string partition_entry_name = partition_entry_path.substr(partition_entry_path.find_last_of("/") + 1);
    if (StringUtil::StartsWith(partition_entry_name, "part_")) {
      std::string extent_path = partition_entry_path + std::string("/");
      for (const auto &extent_entry : std::filesystem::directory_iterator(extent_path)) { // /path/part_/
        std::string extent_entry_path = std::string(extent_entry.path());
        std::string extent_entry_name = extent_entry_path.substr(extent_entry_path.find_last_of("/") + 1);
        if (StringUtil::StartsWith(extent_entry_name, "ext_")) {
          std::string chunk_path = extent_entry_path + std::string("/");
          for (const auto &chunk_entry : std::filesystem::directory_iterator(chunk_path)) { // /path/part_/ext_/
            std::string chunk_entry_path = std::string(chunk_entry.path());
            std::string chunk_entry_name = chunk_entry_path.substr(chunk_entry_path.find_last_of("/") + 1);
            ChunkDefinitionID chunk_id = (ChunkDefinitionID) std::stoull(chunk_entry_name.substr(chunk_entry_name.find("_") + 1));

            // Open File & Insert into file_handlers
            D_ASSERT(file_handlers.find(chunk_id) == file_handlers.end());
            file_handlers[chunk_id] = new Turbo_bin_aio_handler();
            ReturnStatus rs = file_handlers[chunk_id]->OpenFile(chunk_entry_path.c_str(), false, true, false, true);
            D_ASSERT(rs == NOERROR);

            // Read First Block & SetRequestedSize
            char *first_block;
            int status = posix_memalign((void **)&first_block, 512, 512);
            if (status != 0) throw InvalidInputException("posix_memalign fail"); // XXX wrong exception type

            file_handlers[chunk_id]->Read(0, 512, first_block, this, nullptr);
            file_handlers[chunk_id]->WaitAllPendingDiskIO(true);

            size_t requested_size = ((size_t *)first_block)[0];
            file_handlers[chunk_id]->SetRequestedSize(requested_size + 8);
            free(first_block);
          }
        }
      }
    }
  }
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
                                             (int64_t)e.alloc_size, (int64_t)e.requested_size);
    int64_t end = (int64_t)(e.file_offset + e.alloc_size);
    if (end > max_end) max_end = end;
  }
  store_file_size_.store(max_end);
}

void ChunkCacheManager::FlushMetaInfo(const char *path)
{
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
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);
    //TODO: exception 추가
    //throw InvalidInputException("[PinSegment] invalid cid");

  auto file_handler = GetFileHandler(cid);
  size_t segment_size = file_handler->GetRequestedSize();
  size_t file_size = file_handler->file_size();
  size_t required_memory_size = file_size + 512; // Add 512 byte for memory aligning
  D_ASSERT(file_size >= segment_size);

  // Pin Segment using Lightning Get()
  if (client->Get(cid, ptr, size) != 0) {
    // Get() fail: 1) object not found, 2) object is not sealed yet
    // TODO: Check if there is enough memory space

    //size_t deleted_size = 0;
    //if (!IsMemorySpaceEnough(segment_size)) {
    //  FindVictimAndDelete(segment_size, deleted_size);
    //  // Replacement algorithm은 일단 지원 X
    //}

    if (client->Create(cid, ptr, required_memory_size) == 0) {
      // Align memory
      void *file_ptr = MemAlign(ptr, segment_size, required_memory_size, file_handler);

      // Read data & Seal object
      // TODO: we fix read_data_async as false, due to swizzling. May move logic to iterator.
      // ReadData(cid, file_path, file_ptr, file_size, read_data_async);
      ReadData(cid, file_path, file_ptr, file_size, false);

      if (!is_initial_loading) {
        CacheDataTransformer::Swizzle(*ptr);
      }
      client->Seal(cid);
      // if (!read_data_async) client->Seal(cid); // WTF???
      *size = segment_size - sizeof(size_t);
    } else {
      // Create fail -> Subscribe object
      client->Subscribe(cid);

      int status = client->Get(cid, ptr, size);
      D_ASSERT(status == 0);

      // Align memory & adjust size
      MemAlign(ptr, segment_size, required_memory_size, file_handler);
      *size = segment_size - sizeof(size_t);
    }
    return NOERROR;
  }
  // Get() success. Align memory & adjust size
  MemAlign(ptr, segment_size, required_memory_size, file_handler);
  *size = segment_size - sizeof(size_t);
  
  return NOERROR;
}

ReturnStatus ChunkCacheManager::UnPinSegment(ChunkID cid) {
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);
    // TODO
    // throw InvalidInputException("[UnpinSegment] invalid cid");

  // Unpin Segment using Lightning Release()
  // client->Release(cid);
  return NOERROR;
}

ReturnStatus ChunkCacheManager::SetDirty(ChunkID cid) {
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);

  if (client->SetDirty(cid) != 0) {
    exit(-1);
  }
  dirty_count_.fetch_add(1, std::memory_order_relaxed);
  return NOERROR;
}

ReturnStatus ChunkCacheManager::CreateSegment(ChunkID cid, std::string file_path, size_t alloc_size, bool can_destroy) {
  spdlog::trace("[CreateSegment] Start to create segment: {}", cid);
  if (AllocSizeValidityCheck(alloc_size))
    exit(-1);

  auto ret = CreateNewFile(cid, file_path, alloc_size, can_destroy);
  if (ret == NOERROR) {
    total_segment_count_.fetch_add(1, std::memory_order_relaxed);
  }
  return ret;
}

ReturnStatus ChunkCacheManager::DestroySegment(ChunkID cid) {
  spdlog::trace("[DestroySegment] Start to destroy segment: {}", cid);
  // Check validity of given ChunkID
  if (CidValidityCheck(cid))
    exit(-1);
    // TODO
    //throw InvalidInputException("[DestroySegment] invalid cid");

  // TODO: Check the reference count
  // If the count > 0, we cannot destroy the segment
  
  // Delete the segment from the buffer using Lightning Delete()
  D_ASSERT(file_handlers.find(cid) != file_handlers.end());
  client->Delete(cid);
  // jhha: disable file_handler close, due to flushMetaInfo
  // file_handlers[cid]->Close();
  // file_handlers[cid] = nullptr;
  //AdjustMemoryUsage(-GetSegmentSize(cid)); // need type casting
  return NOERROR;
}

ReturnStatus ChunkCacheManager::FinalizeIO(ChunkID cid, bool read, bool write) {
  file_handlers[cid]->WaitForMyIoRequests(read, write);
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

    bool is_dirty;
    client->GetDirty(file_handler.first, is_dirty);
    if (!is_dirty) continue;

    spdlog::trace("[FlushDirtySegmentsAndDeleteFromcache] Flush file: {} with size {}", file_handler.second->GetFilePath(), file_handler.second->file_size());

    // TODO we need a write lock
    UnswizzleFlushSwizzle(file_handler.first, file_handler.second, false);
    client->ClearDirty(file_handler.first);
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
  remaining_memory_usage = client->GetRemainingMemory();
  return NOERROR;
}

int ChunkCacheManager::GetRefCount(ChunkID cid) {
  return client->GetRefCount(cid);
}

// Return true if the given ChunkID is not valid
bool ChunkCacheManager::CidValidityCheck(ChunkID cid) {
  // Catalog Manager에서 validity check 등? 아예 필요 없을 수도 있고.. 해서 내려올테니
  // 이 layer에서는 고민할 필요가 없을 수도 있음. 나중에 필요하면 추가
  return false;
}

// Return true if the given alloc_size is not valid
bool ChunkCacheManager::AllocSizeValidityCheck(size_t alloc_size) {
  // TODO: 제한된 크기가 있거나, 파일 시스템 상에 남은 공간이 충분하지 않을 경우를 다뤄야 함
  return false;
}

// Return the size of segment
size_t ChunkCacheManager::GetSegmentSize(ChunkID cid, std::string file_path) {
  D_ASSERT(file_handlers[cid] != nullptr);
  if (file_handlers[cid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  return file_handlers[cid]->GetRequestedSize();
}

// Return the size of file
size_t ChunkCacheManager::GetFileSize(ChunkID cid, std::string file_path) {
  D_ASSERT(file_handlers[cid] != nullptr);
  if (file_handlers[cid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  return file_handlers[cid]->file_size();
}

Turbo_bin_aio_handler* ChunkCacheManager::GetFileHandler(ChunkID cid) {
  auto file_handler = file_handlers.find(cid);
  D_ASSERT(file_handler != file_handlers.end());
  D_ASSERT(file_handler->second != nullptr);
  if (file_handler->second->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }
  return file_handler->second;
}

void ChunkCacheManager::ReadData(ChunkID cid, std::string file_path, void* ptr, size_t size_to_read, bool read_data_async) {
  auto file_handler = file_handlers.find(cid);
  if (file_handlers[cid]->GetFileID() == -1) {
    exit(-1);
    //TODO throw exception
  }  
  // file_handlers[cid]->Read(0, (int64_t) size_to_read, (char*) ptr, nullptr, nullptr);
  file_handlers[cid]->ReadWithSplittedIORequest(0, (int64_t) size_to_read, (char*) ptr, nullptr, nullptr);
  if (!read_data_async) file_handlers[cid]->WaitForMyIoRequests(true, true);
  total_read_size += size_to_read;
}

void ChunkCacheManager::WriteData(ChunkID cid) {
  D_ASSERT(file_handlers.find(cid) != file_handlers.end());
  //client->Flush(cid, file_handlers[cid]);
  return;
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

void *ChunkCacheManager::MemAlign(uint8_t** ptr, size_t segment_size, size_t required_memory_size, Turbo_bin_aio_handler* file_handler) {
  void* target_ptr = (void*) *ptr;
  std::align(512, segment_size, target_ptr, required_memory_size);
  if (target_ptr == nullptr) {
    exit(-1);
    // TODO throw exception
  }

  size_t real_requested_segment_size = segment_size - sizeof(size_t);
  memcpy(target_ptr, &real_requested_segment_size, sizeof(size_t));
  *ptr = (uint8_t*) target_ptr;
  file_handler->SetDataPtr(*ptr);
  *ptr = *ptr + sizeof(size_t);

  return target_ptr;
}

}
