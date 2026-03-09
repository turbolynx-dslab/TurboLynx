#include <assert.h>
#include <atomic>
#include <stdint.h>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <semaphore.h>
#include <signal.h>
#include <stdlib.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "common/logger.hpp"
#include "storage/cache/common.h"
#include "storage/cache/client.h"
#include "storage/cache/disk_aio/Turbo_bin_aio_handler.hpp"

#define LOCK                                                                   \
  while (!__sync_bool_compare_and_swap(&header_->lock_flag, 0, pid_)) {        \
    nanosleep((const struct timespec[]){{0, 0L}}, NULL);                       \
  }

#define UNLOCK                                                                 \
  std::atomic_thread_fence(std::memory_order_release);                         \
  header_->lock_flag = 0

LightningClient::LightningClient() {
  spdlog::info("[LightningClient] Initializing (standalone)");

  size_t size = DEFAULT_STORE_SIZE;
  store_fd_ = shm_open(STORE_NAME, O_CREAT | O_RDWR, 0666);
  int status = ftruncate64(store_fd_, size);
  if (status < 0) {
    perror("cannot ftruncate");
    exit(-1);
  }

  LightningStoreHeader *store_header_ =
      (LightningStoreHeader *)mmap((void *)LIGHTNING_MMAP_ADDR, size, PROT_WRITE,
                                   MAP_SHARED | MAP_FIXED, store_fd_, 0);
  if (store_header_ == (LightningStoreHeader *)-1) {
    perror("mmap failed");
    exit(-1);
  }

  shm_unlink(STORE_NAME);

  store_header_ = new (store_header_) LightningStoreHeader;

  for (int i = 0; i < MAX_NUM_OBJECTS - 1; i++) {
    store_header_->memory_entries[i].free_list_next = i + 1;
  }
  store_header_->memory_entries[MAX_NUM_OBJECTS - 1].free_list_next = -1;

  for (int i = 0; i < MAX_NUM_OBJECTS - 1; i++) {
    store_header_->object_entries[i].free_list_next = i + 1;
  }
  store_header_->object_entries[MAX_NUM_OBJECTS - 1].free_list_next = -1;

  MemAllocator *store_allocator_ = new MemAllocator((LightningStoreHeader *)store_header_, nullptr);
  int64_t num_header_pages = sizeof(LightningStoreHeader) / 4096 + 1;
  store_allocator_->Init(num_header_pages * 4096, size - num_header_pages * 4096);

  for (int i = 0; i < HASHMAP_SIZE; i++) {
    store_header_->hashmap.hash_entries[i].object_list = -1;
  }

  // Client initialization — map the same shm region as the client view
  size_ = size;
  base_ = (uint8_t *)LIGHTNING_MMAP_ADDR;

  header_ = (LightningStoreHeader *)mmap((void *)base_, size_, PROT_WRITE,
                                         MAP_SHARED | MAP_FIXED, store_fd_, 0);
  if (header_ != (LightningStoreHeader *)base_) {
    perror("mmap failed");
    exit(-1);
  }

  pid_ = getpid();
  allocator_ = new MemAllocator(header_, nullptr);

  spdlog::info("[LightningClient] Initialized");
}

LightningClient::~LightningClient() {
  delete allocator_;

  if (header_ != MAP_FAILED) {
    munmap(header_, size_);
  }

  if (store_fd_ >= 0) {
    close(store_fd_);
  }
}

//===--------------------------------------------------------------------===//
// Internal helpers
//===--------------------------------------------------------------------===//

int64_t LightningClient::find_object(uint64_t object_id) {
  int64_t head_index =
      header_->hashmap.hash_entries[hash_object_id(object_id)].object_list;
  int64_t current_index = head_index;
  while (current_index >= 0) {
    ObjectEntry *current = &header_->object_entries[current_index];
    if (current->object_id == object_id) {
      return current_index;
    }
    current_index = current->next;
  }
  return -1;
}

int64_t LightningClient::alloc_object_entry() {
  int64_t i = header_->object_entry_free_list;
  header_->object_entry_free_list = header_->object_entries[i].free_list_next;
  header_->object_entries[i].free_list_next = -1;
  return i;
}

void LightningClient::dealloc_object_entry(int64_t i) {
  int64_t j = header_->object_entry_free_list;
  header_->object_entries[i].free_list_next = j;
  header_->object_entry_free_list = i;
}

int LightningClient::create_internal(uint64_t object_id, sm_offset *offset_ptr, size_t size) {
  int64_t object_index = find_object(object_id);

  if (object_index >= 0) {
    ObjectEntry *object = &header_->object_entries[object_index];
    if (object->offset > 0) {
      return -1;  // already exists
    }
    sm_offset object_buffer_offset = allocator_->MallocShared(size);
    object->offset    = object_buffer_offset;
    object->size      = size;
    object->ref_count = 1;
    object->dirty_bit = 0;
    *offset_ptr = object_buffer_offset;
    return 0;
  }

  int64_t new_object_index = alloc_object_entry();
  if (new_object_index >= MAX_NUM_OBJECTS) {
    return -1;
  }
  sm_offset object_buffer_offset = allocator_->MallocShared(size);
  ObjectEntry *new_object = &header_->object_entries[new_object_index];

  new_object->object_id  = object_id;
  new_object->num_waiters = 0;
  new_object->offset     = object_buffer_offset;
  new_object->size       = size;
  new_object->ref_count  = 1;
  new_object->dirty_bit  = 0;
  new_object->sealed     = false;

  if (hash_object_id(object_id) > HASHMAP_SIZE) {
    return -1;
  }

  int64_t head_index =
      header_->hashmap.hash_entries[hash_object_id(object_id)].object_list;
  new_object->next = head_index;
  new_object->prev = -1;

  if (head_index >= 0) {
    header_->object_entries[head_index].prev = new_object_index;
  }
  header_->hashmap.hash_entries[hash_object_id(object_id)].object_list = new_object_index;

  *offset_ptr = object_buffer_offset;
  return 0;
}

int LightningClient::seal_internal(uint64_t object_id) {
  int64_t object_index = find_object(object_id);
  assert(object_index >= 0);
  ObjectEntry *object_entry = &header_->object_entries[object_index];
  object_entry->sealed = true;
  if (object_entry->num_waiters > 0) {
    for (int i = 0; i < object_entry->num_waiters; i++) {
      assert(sem_post(&object_entry->sem) == 0);
    }
    object_entry->num_waiters = 0;
    assert(sem_destroy(&object_entry->sem) == 0);
  }
  return 0;
}

int LightningClient::set_dirty_internal(uint64_t object_id) {
  int64_t object_index = find_object(object_id);
  if (object_index < 0) return -1;
  ObjectEntry *object_entry = &header_->object_entries[object_index];
  if (!object_entry->sealed) return -1;
  object_entry->dirty_bit = 1;
  return 0;
}

int LightningClient::clear_dirty_internal(uint64_t object_id) {
  int64_t object_index = find_object(object_id);
  if (object_index < 0) return -1;
  ObjectEntry *object_entry = &header_->object_entries[object_index];
  if (!object_entry->sealed) return -1;
  object_entry->dirty_bit = 0;
  return 0;
}

int LightningClient::get_dirty_internal(uint64_t object_id, bool &is_dirty) {
  int64_t object_index = find_object(object_id);
  if (object_index < 0) {
    is_dirty = false;
    return -1;
  }
  ObjectEntry *object_entry = &header_->object_entries[object_index];
  if (!object_entry->sealed) return -1;
  is_dirty = object_entry->dirty_bit;
  return 0;
}

int LightningClient::get_internal(uint64_t object_id, sm_offset *offset_ptr, size_t *size) {
  int64_t object_index = find_object(object_id);
  if (object_index < 0) return -1;
  ObjectEntry *object_entry = &header_->object_entries[object_index];
  if (!object_entry->sealed) return -1;
  *offset_ptr = object_entry->offset;
  *size = object_entry->size;
  object_entry->ref_count++;
  return 0;
}

int LightningClient::delete_internal(uint64_t object_id) {
  int64_t object_index = find_object(object_id);
  assert(object_index >= 0);
  ObjectEntry *object_entry = &header_->object_entries[object_index];
  assert(object_entry->sealed);

  allocator_->FreeShared(object_entry->offset);
  int64_t prev_index = object_entry->prev;
  int64_t next_index = object_entry->next;

  if (prev_index < 0) {
    if (next_index >= 0) {
      header_->object_entries[next_index].prev = -1;
    }
    header_->hashmap.hash_entries[hash_object_id(object_id)].object_list = next_index;
  } else {
    header_->object_entries[prev_index].next = next_index;
    if (next_index >= 0) {
      header_->object_entries[next_index].prev = prev_index;
    }
  }
  dealloc_object_entry(object_index);
  return 0;
}

int LightningClient::flush_internal(uint64_t object_id, Turbo_bin_aio_handler *file_handler) {
  int64_t object_index = find_object(object_id);
  assert(object_index >= 0);
  ObjectEntry *object_entry = &header_->object_entries[object_index];
  assert(object_entry->sealed);
  if (object_entry->dirty_bit == 1) {
    assert(file_handler);
    if (file_handler->IsReserved()) {
      file_handler->Write(0, object_entry->size, (char *)&base_[object_entry->offset]);
    } else {
      exit(-1);
    }
  }
  return 0;
}

int LightningClient::get_refcount_internal(uint64_t object_id) {
  int64_t object_index = find_object(object_id);
  if (object_index < 0) return -1;
  ObjectEntry *object_entry = &header_->object_entries[object_index];
  if (!object_entry->sealed) return -1;
  return object_entry->ref_count;
}

int LightningClient::subscribe_internal(uint64_t object_id, sem_t **sem, bool *wait) {
  int64_t object_index = find_object(object_id);
  if (object_index < 0) {
    int64_t new_object_index = alloc_object_entry();
    ObjectEntry *new_object = &header_->object_entries[new_object_index];
    new_object->object_id   = object_id;
    new_object->offset      = -1;
    new_object->sealed      = false;
    assert(sem_init(&new_object->sem, 1, 0) == 0);
    new_object->num_waiters = 1;

    int64_t head_index =
        header_->hashmap.hash_entries[hash_object_id(object_id)].object_list;
    new_object->next = head_index;
    new_object->prev = -1;
    if (head_index >= 0) {
      header_->object_entries[head_index].prev = new_object_index;
    }
    header_->hashmap.hash_entries[hash_object_id(object_id)].object_list = new_object_index;

    *sem  = &new_object->sem;
    *wait = true;
    return 0;
  }

  ObjectEntry *object = &header_->object_entries[object_index];
  if (object->sealed) {
    *wait = false;
    return 0;
  }
  object->num_waiters++;
  *sem  = &object->sem;
  *wait = true;
  return 0;
}

//===--------------------------------------------------------------------===//
// Public API
//===--------------------------------------------------------------------===//

int LightningClient::Create(uint64_t object_id, uint8_t **ptr, size_t size) {
  LOCK;
  sm_offset offset;
  int status = create_internal(object_id, &offset, size);
  if (status != -1) {
    assert(status == 0);
    *ptr = &base_[offset];
  }
  UNLOCK;
  return status;
}

int LightningClient::Seal(uint64_t object_id) {
  LOCK;
  int status = seal_internal(object_id);
  UNLOCK;
  return status;
}

int LightningClient::SetDirty(uint64_t object_id) {
  LOCK;
  int status = set_dirty_internal(object_id);
  UNLOCK;
  return status;
}

int LightningClient::ClearDirty(uint64_t object_id) {
  LOCK;
  int status = clear_dirty_internal(object_id);
  UNLOCK;
  return status;
}

int LightningClient::GetDirty(uint64_t object_id, bool &is_dirty) {
  LOCK;
  int status = get_dirty_internal(object_id, is_dirty);
  UNLOCK;
  return status;
}

int LightningClient::Get(uint64_t object_id, uint8_t **ptr, size_t *size) {
  LOCK;
  sm_offset offset;
  int status = get_internal(object_id, &offset, size);
  if (status == 0) {
    *ptr = &base_[offset];
  }
  UNLOCK;
  return status;
}

int LightningClient::Release(uint64_t object_id) {
  LOCK;
  int64_t object_index = find_object(object_id);
  assert(object_index >= 0);
  ObjectEntry *object_entry = &header_->object_entries[object_index];
  assert(object_entry->sealed);
  object_entry->ref_count--;
  UNLOCK;
  return 0;
}

int LightningClient::Delete(uint64_t object_id) {
  LOCK;
  int status = delete_internal(object_id);
  UNLOCK;
  return status;
}

int LightningClient::Flush(uint64_t object_id, Turbo_bin_aio_handler *file_handler) {
  LOCK;
  int status = flush_internal(object_id, file_handler);
  UNLOCK;
  return status;
}

int LightningClient::Subscribe(uint64_t object_id) {
  LOCK;
  sem_t *sem;
  bool wait;
  int status = subscribe_internal(object_id, &sem, &wait);
  UNLOCK;
  if (wait) {
    assert(sem_wait(sem) == 0);
  }
  return status;
}

int LightningClient::GetRefCount(uint64_t object_id) {
  LOCK;
  int status = get_refcount_internal(object_id);
  UNLOCK;
  return status;
}

void LightningClient::PrintRemainingMemory() {
  allocator_->PrintAvalaibleMemory();
}

size_t LightningClient::GetRemainingMemory() {
  return allocator_->GetAvailableMemory();
}
