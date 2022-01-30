//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx) && cmp(KeyAt(bucket_idx), key) == 0) {
      result->push_back(ValueAt(bucket_idx));
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  int32_t available = -1;
  // 遍历所有位置，找到一个可以插入的位置，并且确定有无完全相同的K/V，有则不插入
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        return false;
      }
    } else if (available == -1) {
      available = i;
    }
  }

  // 遍历完看看找没找到空位
  if (available == -1) {
    return false;
  }

  // 插入数据
  array_[available] = MappingType(key, value);
  SetOccupied(available);
  SetReadable(available);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  bool ret = false;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (IsReadable(bucket_idx) && (cmp(array_[bucket_idx].first, key) == 0) && (array_[bucket_idx].second == value)) {
      RemoveAt(bucket_idx);
      ret = true;
      break;
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  uint32_t byte_idx = bucket_idx / 8;
  uint32_t idx = bucket_idx % 8;
  clrbit(readable_[byte_idx], idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  uint32_t byte_idx = bucket_idx / 8;
  uint32_t idx = bucket_idx % 8;
  // 通过位运算，找到byte_idx这个char(8-bit)的从右往左数第idx个元素，并判断是不是1
  return getbit(occupied_[byte_idx], idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t byte_idx = bucket_idx / 8;
  uint32_t idx = bucket_idx % 8;
  setbit(occupied_[byte_idx], idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  uint32_t byte_idx = bucket_idx / 8;
  uint32_t idx = bucket_idx % 8;
  // 通过位运算，找到byte_idx这个char(8-bit)的从右往左数第idx个元素，并判断是不是1
  return getbit(readable_[byte_idx], idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t byte_idx = bucket_idx / 8;
  uint32_t idx = bucket_idx % 8;
  setbit(readable_[byte_idx], idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  bool ret = true;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsReadable(bucket_idx)) {
      ret = false;
      break;
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t readable_num = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx)) {
      readable_num++;
    }
  }
  return readable_num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  bool ret = true;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    if (IsReadable(bucket_idx)) {
      ret = false;
      break;
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
