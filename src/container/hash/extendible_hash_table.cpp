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
  Page *b_dir_page = nullptr;
  Page *b_bucket_page = nullptr;
  page_id_t bucket_page_id = -1;
  if ((b_dir_page = buffer_pool_manager_->NewPage(&directory_page_id_)) == nullptr) {
    // LOG_ERROR("New direcotry page Failed");
  } else if ((b_bucket_page = buffer_pool_manager->NewPage(&bucket_page_id)) == nullptr) {
    // LOG_ERROR("New bucket_page_id Failed");
  } else {
    b_dir_page->WLatch();
    HashTableDirectoryPage *dir_page = reinterpret_cast<HashTableDirectoryPage *>(b_dir_page);
    dir_page->SetPageId(b_dir_page->GetPageId());
    dir_page->SetBucketPageId(0, bucket_page_id);
    b_dir_page->WUnlatch();
    // 采用的是将direcotry page固定在buffer pool，减少磁盘I/O
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  }
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
    // LOG_INFO("Fetch Directory Page Failed");
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
    // LOG_INFO("Fetch Bucket Page Failed");
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
  if ((dir_page = FetchDirectoryPage()) == nullptr) {
    // LOG_INFO("Fetch Directory Page Failed");
  } else {
    auto bucket_page_id = KeyToPageId(key, dir_page);
    if ((bucket_page = FetchBucketPage(bucket_page_id)) == nullptr) {
      // LOG_INFO("Fetch BucketkPage Failed");
    } else {
      reinterpret_cast<Page *>(bucket_page)->RLatch();
      ret = bucket_page->GetValue(key, comparator_, result);
      reinterpret_cast<Page *>(bucket_page)->RUnlatch();
    }
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
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = nullptr;
  HASH_TABLE_BUCKET_TYPE *bucket_page = nullptr;
  if ((dir_page = FetchDirectoryPage()) == nullptr) {
    // LOG_INFO("Fetch Directory Page Failed");
  } else {
    auto bucket_page_id = KeyToPageId(key, dir_page);
    if ((bucket_page = FetchBucketPage(bucket_page_id)) == nullptr) {
      LOG_INFO("Fetch BucketkPage Failed");
    } else {
      // reinterpret_cast<Page *>(bucket_page)->WLatch();
      ret = bucket_page->Insert(key, value, comparator_);
      // reinterpret_cast<Page *>(bucket_page)->WUnlatch();
      if (ret) {
        // LOG_INFO("Direct Insert Success");
      } else if (bucket_page->IsFull()) {
        ret = SplitInsert(transaction, key, value);
      } else {
        // LOG_INFO("Do not insert the same KV");
      }
    }
  }
  table_latch_.WUnlock();
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = true;
  HashTableDirectoryPage *dir_page = nullptr;
  if ((dir_page = FetchDirectoryPage()) == nullptr) {
    // LOG_INFO("Fetch Directory Page Failed");
    ret = false;
  } else {
    // reinterpret_cast<Page *>(dir_page)->WLatch();
    auto global_depth = dir_page->GetGlobalDepth();
    auto split_bucket_idx = KeyToDirectoryIndex(key, dir_page);
    auto split_bucket_page_id = dir_page->GetBucketPageId(split_bucket_idx);
    auto split_local_depth = dir_page->GetLocalDepth(split_bucket_idx);
    // 设置depth
    if (split_local_depth == global_depth) {
      dir_page->IncrGlobalDepth();
    }
    // 分裂split_bucket
    Page *new_page = nullptr;
    page_id_t new_page_id;
    HASH_TABLE_BUCKET_TYPE *new_bucket_page = nullptr;
    HASH_TABLE_BUCKET_TYPE *split_bucket_page = nullptr;
    if ((split_bucket_page = FetchBucketPage(split_bucket_page_id)) == nullptr) {
      // LOG_INFO("Fetch Bucket Page Failed");
      ret = false;
    } else if ((new_page = buffer_pool_manager_->NewPage(&new_page_id)) == nullptr) {
      // LOG_INFO("New Page Failed");
      ret = false;
    } else {
      // new_page->WLatch();
      // reinterpret_cast<Page *>(split_bucket_page)->WLatch();
      new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page);
      dir_page->SplitBucketPageId(split_bucket_page_id, new_page_id);
      dir_page->IncrLocalDepthByPageId(split_bucket_page_id);
      dir_page->IncrLocalDepthByPageId(new_page_id);
      KeyType tmp_key;
      ValueType tmp_value;
      page_id_t tmp_bucket_page_id;
      // 根据key将key/value分到split_bucket_page和new_bucket_page中
      for (uint32_t bucket_idx = 0; bucket_idx < (BUCKET_ARRAY_SIZE - 1) / 8 + 1; bucket_idx++) {
        if (split_bucket_page->IsOccupied(bucket_idx) && split_bucket_page->IsReadable(bucket_idx)) {
          tmp_key = split_bucket_page->KeyAt(bucket_idx);
          tmp_value = split_bucket_page->ValueAt(bucket_idx);
          tmp_bucket_page_id = KeyToPageId(tmp_key, dir_page);
          if (tmp_bucket_page_id == split_bucket_page_id) {
            // nothing
          } else {
            split_bucket_page->Remove(tmp_key, tmp_value, comparator_);
            new_bucket_page->Insert(tmp_key, tmp_value, comparator_);
          }
        }
      }
      // 将key/value插入
      // note: 有可能分裂后仍然不可插入
      page_id_t bucket_page_id = KeyToPageId(key, dir_page);
      if (bucket_page_id == split_bucket_page_id) {
        ret = split_bucket_page->Insert(key, value, comparator_);
      } else {
        ret = new_bucket_page->Insert(key, value, comparator_);
      }
      // new_page->WUnlatch();
      // reinterpret_cast<Page *>(split_bucket_page)->WUnlatch();
    }
    // reinterpret_cast<Page *>(dir_page)->WUnlatch();
  }
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = true;
  table_latch_.WUnlock();
  HashTableDirectoryPage *dir_page = nullptr;
  HASH_TABLE_BUCKET_TYPE *bucket_page = nullptr;
  if ((dir_page = FetchDirectoryPage()) == nullptr) {
    // LOG_INFO("Fetch Directory Page Failed");
  } else {
    auto bucket_page_id = KeyToPageId(key, dir_page);
    if ((bucket_page = FetchBucketPage(bucket_page_id)) == nullptr) {
      // LOG_INFO("Fetch BucketkPage Failed");
    } else if (!(ret = bucket_page->Remove(key, value, comparator_))) {
      // LOG_INFO("Cann't Find");
    } else if (!bucket_page->IsEmpty()) {
      // LOG_INFO("Bucket Is Not Empty");
    } else {
      Merge(transaction, key, value);
    }
  }
  table_latch_.WUnlock();
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = nullptr;
  if ((dir_page = FetchDirectoryPage()) == nullptr) {
    // LOG_INFO("Fetch Directory Page Failed");
  } else {
    uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
    Coalesce(transaction, bucket_idx, dir_page);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Coalesce(Transaction *transaction, uint32_t bucket_idx, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx_depth = dir_page->GetLocalDepth(bucket_idx);
  if (bucket_idx_depth == 0) {
    return;
  }
  uint32_t bucket_idx_k = dir_page->GetSplitImageIndex(bucket_idx);
  if (dir_page->GetLocalDepth(bucket_idx_k) != bucket_idx_depth) {
    return;
  }
  HASH_TABLE_BUCKET_TYPE *bucket_page_k = nullptr;
  if ((bucket_page_k = FetchBucketPage(dir_page->GetBucketPageId(bucket_idx_k))) == nullptr) {
    // LOG_INFO("Fetch Bucket Page Failed");
  } else {
    // 合并到bucket_page_k
    dir_page->DecrLocalDepth(bucket_idx);
    dir_page->DecrLocalDepth(bucket_idx_k);
    dir_page->SetBucketPageId(bucket_idx, dir_page->GetBucketPageId(bucket_idx_k));

    if (bucket_page_k->IsEmpty()) {  // 为空时，会继续合并，直至不为空
      Coalesce(transaction, bucket_idx_k, dir_page);
    }
  }
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
