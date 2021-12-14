//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // 根据指定page_id在page_table_查找到对应的frame，并将其刷新到磁盘
  std::lock_guard<std::mutex> lock(latch_);
  auto page_table_ite = page_table_.find(page_id);
  if (page_table_ite == page_table_.end()) {
    return false;
  }
  if (pages_[page_table_ite->second].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[page_table_ite->second].data_);
    pages_[page_table_ite->second].is_dirty_ = false;
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  // 根据page_table_将所有在bufferpool中的页面刷新到磁盘
  std::lock_guard<std::mutex> lock(latch_);
  for (auto &page_table_ite : page_table_) {
    if (pages_[page_table_ite.second].is_dirty_) {
      disk_manager_->WritePage(page_table_ite.first, pages_[page_table_ite.second].data_);
      pages_[page_table_ite.second].is_dirty_ = false;
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  // 分配一个内存页
  if (!free_list_.empty()) {
    // 从free_list_选出一个frame
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    page = &pages_[frame_id];
    page_table_.erase(page->page_id_);
    if (page->is_dirty_) {  // 脏页写回
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    Reset(page);
  } else {
    // do nothing
  }
  // 从磁盘读页到内存，并初始化内存页
  if (page != nullptr) {
    *page_id = AllocatePage();  // 分配一个新的page_id
    page_table_.insert({*page_id, frame_id});
    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    replacer_->Pin(frame_id);
  }
  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> lock(latch_);
  Page *page = nullptr;
  frame_id_t frame_id;
  // if (recycle_list_.find(page_id) != recycle_list_.end() || page_id == INVALID_PAGE_ID) {  // 已回收
  //   // do nothing
  // } else
  if (page_table_.find(page_id) != page_table_.end()) {  // 在bufferpool
    frame_id = page_table_[page_id];
    page = &pages_[frame_id];
  } else if (!free_list_.empty()) {  // 未在bufferpool，则在free_list找到一个空闲的内存页，并将磁盘页读至内存
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
    disk_manager_->ReadPage(page_id, page->data_);
    page_table_.insert({page_id, frame_id});
  } else if (replacer_->Victim(&frame_id)) {  // bufferpool没有空闲页，则替换出一页
    page = &pages_[frame_id];
    page_table_.erase(page->page_id_);
    if (page->is_dirty_) {  // 将脏页写回
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    Reset(page);
    disk_manager_->ReadPage(page_id, page->data_);
    page_table_.insert({page_id, frame_id});
  }
  if (page != nullptr) {
    replacer_->Pin(frame_id);
    page->page_id_ = page_id;
    page->pin_count_++;
  }
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> lock(latch_);
  bool ret = true;
  auto ite = page_table_.find(page_id);
  Page *page = nullptr;

  if (ite != page_table_.end()) {
    page = &pages_[ite->second];
    if (page->pin_count_ == 0) {
      DeallocatePage(page_id);
      // if (page->is_dirty_) {
      //   FlushPgImp(page_id);
      // }
      replacer_->Pin(page_id);  // 从替换列表中移除
      Reset(page);
      free_list_.push_back(ite->second);  // 加入空闲列表
      page_table_.erase(page_id);
    } else {
      ret = false;
    }
  }
  return ret;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  bool ret = true;
  auto ite = page_table_.find(page_id);
  if (ite == page_table_.end()) {
    // do nothing
  } else {
    auto frame_id = page_table_[page_id];
    if (pages_[frame_id].pin_count_ <= 0) {
      ret = false;
    } else {
      pages_[frame_id].is_dirty_ |= is_dirty;  // NOTE: 保持页面被写脏的属性
      pages_[frame_id].pin_count_--;
      if (pages_[frame_id].pin_count_ == 0) {
        replacer_->Unpin(frame_id);
      }
    }
  }
  return ret;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
