//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  申请一个page用作directory page
  page_id_t bucket_1_page_id = -1;
  page_id_t bucket_2_page_id = -1;
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
  // 申请两个bucket page
  buffer_pool_manager->NewPage(&bucket_1_page_id);
  buffer_pool_manager->NewPage(&bucket_2_page_id);
  // 设置logic hash table
  dir_page->SetPageId(directory_page_id_);
  dir_page->IncrGlobalDepth();
  dir_page->SetBucketPageId(0, bucket_1_page_id);
  dir_page->SetLocalDepth(0, 1);
  dir_page->SetBucketPageId(1, bucket_2_page_id);
  dir_page->SetLocalDepth(1, 1);
  // 采用的是将direcotry page固定在buffer pool，减少磁盘I/O
  buffer_pool_manager_->UnpinPage(bucket_1_page_id, false);
  buffer_pool_manager_->UnpinPage(bucket_2_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = 0;
  uint32_t hash_key = Hash(key);
  auto global_mask = dir_page->GetGlobalDepthMask();
  bucket_idx = global_mask & hash_key;
  return bucket_idx;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = nullptr;
  HashTableDirectoryPage *dir_page = nullptr;
  if ((page = buffer_pool_manager_->FetchPage(directory_page_id_)) == nullptr) {
    LOG_INFO("Fetch Directory Page Failed");
  } else {
    dir_page = reinterpret_cast<HashTableDirectoryPage *>(page);
  }
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = nullptr;
  HASH_TABLE_BUCKET_TYPE *bucket_page = nullptr;
  if ((page = buffer_pool_manager_->FetchPage(bucket_page_id)) == nullptr) {
    LOG_INFO("Fetch Bucket Page Failed");
  } else {
    bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page);
  }
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  bool ret = true;
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = nullptr;
  HASH_TABLE_BUCKET_TYPE *bucket_page = nullptr;
  dir_page = FetchDirectoryPage();
  if (dir_page->GetGlobalDepth() == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return false;
  }
  auto bucket_page_id = KeyToPageId(key, dir_page);
  if ((bucket_page = FetchBucketPage(bucket_page_id)) == nullptr) {
    LOG_INFO("Fetch BucketkPage Failed");
  } else {
    reinterpret_cast<Page *>(bucket_page)->RLatch();
    ret = bucket_page->GetValue(key, comparator_, result);
    reinterpret_cast<Page *>(bucket_page)->RUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  }
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = true;
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = nullptr;
  HASH_TABLE_BUCKET_TYPE *bucket_page = nullptr;
  dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  bucket_page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  if (!bucket_page->IsFull()) {  // 未满
    ret = bucket_page->Insert(key, value, comparator_);
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, ret);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
  } else {  // 已满
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    ret = SplitInsert(transaction, key, value);
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = true;
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = nullptr;
  dir_page = FetchDirectoryPage();
  auto split_bucket_page_id = KeyToPageId(key, dir_page);
  auto split_bucket_page = FetchBucketPage(split_bucket_page_id);
  if (!split_bucket_page->IsFull()) {  // 未满: 在insert调用splitinsert期间可能进行了remove操作
    ret = split_bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(split_bucket_page_id, ret);
    table_latch_.WUnlock();
    return ret;
  }
  auto split_bucket_idx = KeyToDirectoryIndex(key, dir_page);
  dir_page->IncrLocalDepth(split_bucket_idx);
  auto split_local_depth = dir_page->GetLocalDepth(split_bucket_idx);
  auto global_depth = dir_page->GetGlobalDepth();
  if (split_local_depth > global_depth) {
    // 使logical hash bucket table增大一倍
    dir_page->IncrGlobalDepth();
  }
  // alloc a new bucket
  page_id_t split_image_bucket_page_id = -1;
  auto split_image_bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&split_image_bucket_page_id));
  assert(split_image_bucket_page == nullptr);
  // only rehash overflow bucket
  for (uint32_t bucket_idx = 0; bucket_idx < BLOCK_ARRAY_SIZE; bucket_idx++) {
    auto insert_bucket = KeyToDirectoryIndex(split_bucket_page->KeyAt(bucket_idx), dir_page);
    if (insert_bucket != split_bucket_idx) {
      split_bucket_page->RemoveAt(bucket_idx);
      split_image_bucket_page->Insert(
          split_bucket_page->KeyAt(bucket_idx), split_bucket_page->ValueAt(bucket_idx),
          comparator_);  // 可以进一步优化，因为new bucket使empty, 故可以按idx从0开始顺序插入
    }
  }
  // make a link from logical hash table to physical hash table and incre local depth
  uint32_t diff = 1 << split_local_depth;
  for (uint32_t i = split_bucket_idx; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, split_local_depth);
    if (i < diff) {
      // avoid negative because we are using unsigned int32 i
      break;
    }
  }
  for (uint32_t i = split_bucket_idx; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, split_bucket_page_id);
    dir_page->SetLocalDepth(i, split_local_depth);
  }
  auto split_image_bucket_idx = split_bucket_idx ^ (1 << (split_local_depth - 1));
  for (uint32_t i = split_image_bucket_idx; i >= 0; i -= diff) {
    dir_page->SetBucketPageId(i, split_image_bucket_page_id);
    dir_page->SetLocalDepth(i, split_local_depth);
    if (i < diff) {
      // avoid negative
      break;
    }
  }
  for (uint32_t i = split_image_bucket_idx; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, split_image_bucket_page_id);
    dir_page->SetLocalDepth(i, split_local_depth);
  }
  // insert
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(split_bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(split_image_bucket_page_id, true);
  table_latch_.WUnlock();
  ret = Insert(transaction, key, value);
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = true;
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = nullptr;
  HASH_TABLE_BUCKET_TYPE *bucket_page = nullptr;
  dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  bucket_page = FetchBucketPage(bucket_page_id);
  reinterpret_cast<Page *>(bucket_page)->WLatch();
  ret = bucket_page->Remove(key, value, comparator_);
  // bool is_empty = bucket_page->IsEmpty();
  reinterpret_cast<Page *>(bucket_page)->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, ret);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  // if (is_empty) {
  //   Merge(transaction, key, value);
  // }
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = nullptr;
  HASH_TABLE_BUCKET_TYPE *bucket_page = nullptr;
  dir_page = FetchDirectoryPage();
  auto bucket_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  bucket_page = FetchBucketPage(bucket_page_id);
  uint32_t bucket_depth = dir_page->GetLocalDepth(bucket_idx);
  if (!bucket_page->IsEmpty() || (bucket_depth == 0)) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return;
  }
  // 此时bucket_page为空，需要合并
  uint32_t split_image_bucket_idx = dir_page->GetSplitImageIndex(bucket_idx);
  uint32_t split_image_bucket_depth = dir_page->GetLocalDepth(split_image_bucket_idx);
  bool flag = false;
  if (bucket_depth == split_image_bucket_depth) {
    flag = true;
    auto split_image_bucket_page_id = dir_page->GetBucketPageId(split_image_bucket_idx);
    dir_page->DecrLocalDepth(bucket_idx);
    dir_page->DecrLocalDepth(split_image_bucket_idx);
    uint32_t size = dir_page->Size();
    for (uint32_t idx = 0; idx < size; idx++) {
      if ((dir_page->GetBucketPageId(idx) == bucket_page_id) ||
          (dir_page->GetBucketPageId(idx) == split_image_bucket_page_id)) {
        dir_page->SetBucketPageId(idx, split_image_bucket_page_id);
        dir_page->SetLocalDepth(idx, dir_page->GetLocalDepth(split_image_bucket_idx));
      }
    }
    if (dir_page->CanShrink()) {
      dir_page->DecrGlobalDepth();
    }
  }

  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, flag);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
