//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <utility>
#include <vector>
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {  // read_uncommited下无读锁
    return true;
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto &lock_request_queue = lock_table_[rid];
  LockRequest request(txn->GetTransactionId(), LockMode::SHARED);
  lock_request_queue.request_queue_.push_back(request);
  while (txn->GetState() != TransactionState::ABORTED) {
    auto iter = lock_request_queue.request_queue_.begin();
    bool need_wait = false;
    while (iter != lock_request_queue.request_queue_.end()) {
      if (iter->txn_id_ < txn->GetTransactionId() && iter->lock_mode_ == LockMode::EXCLUSIVE) {
        // 当前事务为新事物，则需要等待
        need_wait = true;
        break;
      }
      if (iter->txn_id_ > txn->GetTransactionId() && iter->lock_mode_ == LockMode::EXCLUSIVE) {
        // 当前事务为老事务，则abort新事务
        auto trans = TransactionManager::GetTransaction(iter->txn_id_);
        trans->SetState(TransactionState::ABORTED);
        if (iter->granted_) {
          trans->GetSharedLockSet()->erase(rid);
        }
        iter = lock_request_queue.request_queue_.erase(iter);
      } else {
        ++iter;
      }
    }
    if (!need_wait) {  // 表示已获得该tuple锁
      // 设置txn以及request.granted
      lock_request_queue.SetGranted(txn->GetTransactionId(), LockMode::SHARED);
      txn->GetSharedLockSet()->emplace(rid);
      return true;
    }
    lock_request_queue.cv_.wait(lock);
  }
  return false;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto &lock_request_queue = lock_table_[rid];
  LockRequest request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_request_queue.request_queue_.push_back(request);
  while (txn->GetState() != TransactionState::ABORTED) {
    auto iter = lock_request_queue.request_queue_.begin();
    bool need_wait = false;
    while (iter != lock_request_queue.request_queue_.end()) {
      if (iter->txn_id_ < txn->GetTransactionId()) {
        // 当前事务为新事物，则需要等待
        need_wait = true;
        break;
      }
      if (iter->txn_id_ > txn->GetTransactionId()) {
        // 当前事务为老事务，则abort新事务
        auto trans = TransactionManager::GetTransaction(iter->txn_id_);
        trans->SetState(TransactionState::ABORTED);
        if (iter->granted_ && iter->lock_mode_ == LockMode::SHARED) {
          trans->GetSharedLockSet()->erase(rid);
        } else if (iter->granted_ && iter->lock_mode_ == LockMode::EXCLUSIVE) {
          trans->GetExclusiveLockSet()->erase(rid);
        }
        iter = lock_request_queue.request_queue_.erase(iter);
      } else {
        ++iter;
      }
    }
    if (!need_wait) {  // 表示已获得该tuple锁
      // 设置txn以及request.granted
      lock_request_queue.SetGranted(txn->GetTransactionId(), LockMode::EXCLUSIVE);
      txn->GetExclusiveLockSet()->emplace(rid);
      return true;
    }
    lock_request_queue.cv_.wait(lock);
  }
  return false;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto &lock_request_queue = lock_table_[rid];
  LockRequest request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  lock_request_queue.request_queue_.push_back(request);
  while (txn->GetState() != TransactionState::ABORTED) {
    auto iter = lock_request_queue.request_queue_.begin();
    bool need_wait = false;
    while (iter != lock_request_queue.request_queue_.end()) {
      if (iter->txn_id_ < txn->GetTransactionId()) {
        // 当前事务为新事物，则需要等待
        need_wait = true;
        break;
      }
      if (iter->txn_id_ > txn->GetTransactionId()) {
        // 当前事务为老事务，则abort新事务
        auto trans = TransactionManager::GetTransaction(iter->txn_id_);
        trans->SetState(TransactionState::ABORTED);
        if (iter->granted_ && iter->lock_mode_ == LockMode::SHARED) {
          trans->GetSharedLockSet()->erase(rid);
        } else if (iter->granted_ && iter->lock_mode_ == LockMode::EXCLUSIVE) {
          trans->GetExclusiveLockSet()->erase(rid);
        }
        iter = lock_request_queue.request_queue_.erase(iter);
      } else {
        ++iter;
      }
    }
    if (!need_wait) {  // 表示已获得该tuple锁
      // 设置txn以及request.granted,同时去除之前的shared锁请求
      lock_request_queue.SetGranted(txn->GetTransactionId(), LockMode::EXCLUSIVE);
      lock_request_queue.EraseRequest(txn->GetTransactionId(), LockMode::SHARED);
      txn->GetSharedLockSet()->erase(rid);
      txn->GetExclusiveLockSet()->emplace(rid);
      return true;
    }
    lock_request_queue.cv_.wait(lock);
  }
  // note：可能有读锁没释放
  lock_request_queue.EraseRequest(txn->GetTransactionId(), LockMode::SHARED);
  txn->GetSharedLockSet()->erase(rid);
  return false;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::lock_guard<std::mutex> lock(latch_);
  auto &lock_request_queue = lock_table_[rid];
  auto txn_id = txn->GetTransactionId();
  auto request_ite = std::find_if(lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
                                  [txn_id](const LockRequest &request) { return request.txn_id_ == txn_id; });
  if (request_ite == lock_request_queue.request_queue_.end()) {
    return false;
  }
  if (request_ite->lock_mode_ == LockMode::SHARED) {
    txn->GetSharedLockSet()->erase(rid);
  } else {
    txn->GetExclusiveLockSet()->erase(rid);
  }
  lock_request_queue.request_queue_.erase(request_ite);
  if (txn->GetState() == TransactionState::GROWING) {  // 切换状态
    txn->SetState(TransactionState::SHRINKING);
  }
  lock_request_queue.cv_.notify_all();
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].push_back(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].erase(std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2));
}

bool LockManager::Dfs(txn_id_t txn_id, txn_id_t *new_txn_id) {
  visited_[txn_id] = false;
  *new_txn_id = *new_txn_id < txn_id ? txn_id : *new_txn_id;
  auto &link_set = waits_for_[txn_id];
  for (auto &link_txn_id : link_set) {
    auto visited_ite = visited_.find(link_txn_id);
    if (visited_ite != visited_.end()) {
      if (!visited_ite->second) {
        return true;
      }
      continue;
    }
    if (Dfs(link_txn_id, new_txn_id)) {
      return true;
    }
  }
  visited_[txn_id] = true;
  return false;
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  // 通过dfs， 检测是否有环
  visited_.clear();
  for (auto &waits_for_item : waits_for_) {
    auto visited_ite = visited_.find(waits_for_item.first);
    if (visited_ite != visited_.end()) {  // 此处visited_ite.second一定为true
      continue;
    }
    *txn_id = -1;
    if (Dfs(waits_for_item.first, txn_id)) {  // 开启一个新的连通图遍历
      return true;
    }
  }
  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> edge_list;
  for (auto &wait_for_item : waits_for_) {
    for (auto outer : wait_for_item.second) {
      edge_list.emplace_back(wait_for_item.first, outer);
    }
  }
  return edge_list;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here

      continue;
    }
  }
}

}  // namespace bustub
