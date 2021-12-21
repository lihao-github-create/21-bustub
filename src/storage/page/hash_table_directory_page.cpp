//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_directory_page.h"
#include <algorithm>
#include <cmath>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {
page_id_t HashTableDirectoryPage::GetPageId() const { return page_id_; }

void HashTableDirectoryPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

lsn_t HashTableDirectoryPage::GetLSN() const { return lsn_; }

void HashTableDirectoryPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

uint32_t HashTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) {
  uint32_t mask = 0;
  if (local_depths_[bucket_idx] == 0) {
    mask = 0;
  } else {
    mask = 1 << (local_depths_[bucket_idx] - 1);
  }
  return mask;
}

uint32_t HashTableDirectoryPage::GetGlobalDepth() { return global_depth_; }

uint32_t HashTableDirectoryPage::GetGlobalDepthMask() {
  uint32_t mask = 0;
  if (global_depth_ == 0) {
    mask = 0;
  } else {
    mask = 1 << (global_depth_ - 1);
  }
  return mask;
}

void HashTableDirectoryPage::IncrGlobalDepth() {
  global_depth_++;
  // 重新设置local_depths和bucket_page_ids
  int32_t length = std::pow(2, global_depth_);
  for (int32_t i = length - 1; i > 0; i -= 2) {
    local_depths_[i] = local_depths_[i / 2];
    local_depths_[i - 1] = local_depths_[i / 2];
    bucket_page_ids_[i] = bucket_page_ids_[i / 2];
    bucket_page_ids_[i - 1] = bucket_page_ids_[i / 2];
  }
}

void HashTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

page_id_t HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) { return bucket_page_ids_[bucket_idx]; }

void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

void HashTableDirectoryPage::SplitBucketPageId(page_id_t src_page_id, page_id_t new_page_id) {
  uint32_t bucket_idx = 0;
  for (; bucket_idx < DIRECTORY_ARRAY_SIZE; bucket_idx++) {
    if (bucket_page_ids_[bucket_idx] == src_page_id) {
      break;
    }
  }
  uint32_t count = std::pow(2, global_depth_ - local_depths_[bucket_idx]);
  for (uint32_t i = bucket_idx + count / 2; i < bucket_idx + count; i++) {
    bucket_page_ids_[i] = new_page_id;
  }
}

uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {
  uint32_t split_image_index = 0;
  uint32_t bucket_idx_high_bit = GetLocalHighBit(bucket_idx);
  uint32_t bucket_idx_mask = GetLocalDepthMask(bucket_idx);
  for (uint32_t idx = 0; idx < DIRECTORY_ARRAY_SIZE; idx++) {
    if (((GetLocalHighBit(idx) & bucket_idx_mask) ^ bucket_idx_high_bit) == 1) {
      split_image_index = idx;
      break;
    }
  }
  return split_image_index;
}

uint32_t HashTableDirectoryPage::Size() { return std::pow(2, global_depth_); }

bool HashTableDirectoryPage::CanShrink() {
  bool ret = true;
  for (auto local_depth : local_depths_) {
    if (local_depth == global_depth_) {
      ret = false;
      break;
    }
  }
  return ret;
}

uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) { return local_depths_[bucket_idx]; }

void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void HashTableDirectoryPage::IncrLocalDepthByPageId(page_id_t page_id) {
  for (uint32_t bucket_idx = 0; bucket_idx < DIRECTORY_ARRAY_SIZE; bucket_idx++) {
    if (bucket_page_ids_[bucket_idx] == page_id) {
      local_depths_[bucket_idx]++;
    }
  }
}

void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

uint32_t HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) {
  return bucket_idx & GetLocalDepthMask(bucket_idx);
}

/**
 * VerifyIntegrity - Use this for debugging but **DO NOT CHANGE**
 *
 * If you want to make changes to this, make a new function and extend it.
 *
 * Verify the following invariants:
 * (1) All LD <= GD.
 * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
 * (3) The LD is the same at each index with the same bucket_page_id
 */
void HashTableDirectoryPage::VerifyIntegrity() {
  //  build maps of {bucket_page_id : pointer_count} and {bucket_page_id : local_depth}
  std::unordered_map<page_id_t, uint32_t> page_id_to_count = std::unordered_map<page_id_t, uint32_t>();
  std::unordered_map<page_id_t, uint32_t> page_id_to_ld = std::unordered_map<page_id_t, uint32_t>();

  //  verify for each bucket_page_id, pointer
  for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
    page_id_t curr_page_id = bucket_page_ids_[curr_idx];
    uint32_t curr_ld = local_depths_[curr_idx];
    assert(curr_ld <= global_depth_);

    ++page_id_to_count[curr_page_id];

    if (page_id_to_ld.count(curr_page_id) > 0 && curr_ld != page_id_to_ld[curr_page_id]) {
      uint32_t old_ld = page_id_to_ld[curr_page_id];
      LOG_WARN("Verify Integrity: curr_local_depth: %u, old_local_depth %u, for page_id: %u", curr_ld, old_ld,
               curr_page_id);
      PrintDirectory();
      assert(curr_ld == page_id_to_ld[curr_page_id]);
    } else {
      page_id_to_ld[curr_page_id] = curr_ld;
    }
  }

  auto it = page_id_to_count.begin();

  while (it != page_id_to_count.end()) {
    page_id_t curr_page_id = it->first;
    uint32_t curr_count = it->second;
    uint32_t curr_ld = page_id_to_ld[curr_page_id];
    uint32_t required_count = 0x1 << (global_depth_ - curr_ld);

    if (curr_count != required_count) {
      LOG_WARN("Verify Integrity: curr_count: %u, required_count %u, for page_id: %u", curr_ld, required_count,
               curr_page_id);
      PrintDirectory();
      assert(curr_count == required_count);
    }
    it++;
  }
}

void HashTableDirectoryPage::PrintDirectory() {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    LOG_DEBUG("|      %u     |     %u     |     %u     |", idx, bucket_page_ids_[idx], local_depths_[idx]);
  }
  LOG_DEBUG("================ END DIRECTORY ================");
}

}  // namespace bustub
