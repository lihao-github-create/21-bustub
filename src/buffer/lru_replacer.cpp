//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>
namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // 根据lru策略选出应该被evict的frame
  // 若存在，则设置frame_id，返回true，否则，返回false
  std::lock_guard<std::mutex> lock(latch_);
  if (replaceable_list_.empty()) {
    return false;
  }
  // 队尾为最久未被访问的，故剔除队尾元素
  *frame_id = replaceable_list_.back();
  replaceable_list_.pop_back();
  frame_index_map_.erase(*frame_id);
  return true;
}
// pin表示正在被使用，故需移除替换列表
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  auto ite = frame_index_map_.find(frame_id);
  if (ite != frame_index_map_.end()) {
    replaceable_list_.erase(ite->second);
    frame_index_map_.erase(frame_id);
  }
}
// unpin表示不会被使用
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // 将其加入队首
  std::lock_guard<std::mutex> lock(latch_);
  auto ite = frame_index_map_.find(frame_id);
  if (ite == frame_index_map_.end()) {
    replaceable_list_.push_front(frame_id);
    frame_index_map_[frame_id] = replaceable_list_.begin();
  }
}

size_t LRUReplacer::Size() {
  // 返回可替换列表的大小
  std::lock_guard<std::mutex> lock(latch_);
  return replaceable_list_.size();
}

}  // namespace bustub
