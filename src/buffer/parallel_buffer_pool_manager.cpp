//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  BufferPoolManager *bufferpool = nullptr;
  for (size_t i = 0; i < num_instances; i++) {
    bufferpool = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    bufferpool_instances_.push_back(bufferpool);
  }
  num_instances_ = num_instances;
  pool_size_ = pool_size * num_instances_;
}

// Update deconstructor to destruct all BufferPoolManagerInstances and
// deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  BufferPoolManager *bufferpool = nullptr;
  for (size_t i = 0; i < num_instances_; i++) {
    bufferpool = bufferpool_instances_[i];
    delete bufferpool;
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use
  // this method in your other methods.
  size_t instance_index = page_id % num_instances_;
  return bufferpool_instances_[instance_index];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  size_t instance_index = page_id % num_instances_;
  return bufferpool_instances_[instance_index]->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  size_t instance_index = page_id % num_instances_;
  return bufferpool_instances_[instance_index]->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  size_t instance_index = page_id % num_instances_;
  return bufferpool_instances_[instance_index]->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner
  // from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1)
  // success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a
  // different BPMI each time this function
  // is called
  Page *page = nullptr;
  for (size_t i = 0; i < num_instances_; i++) {
    page = bufferpool_instances_[i]->NewPage(page_id);
    if (page != nullptr) {
      break;
    }
  }
  return page;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  size_t instance_index = page_id % num_instances_;
  return bufferpool_instances_[instance_index]->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    bufferpool_instances_[i]->FlushAllPages();
  }
}

}  // namespace bustub
