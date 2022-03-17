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
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::CanWound(const LockRequestQueue &lock_request_queue, txn_id_t txn_id) {
  for (auto &request : lock_request_queue.request_queue_) {
    if (request.granted_) {
      if (request.txn_id_ < txn_id) {
        return false;
      }
    } else {
      break;
    }
  }
  return true;
}

void LockManager::Wound(txn_id_t txn_id, const RID &rid, LockRequestQueue &lock_request_queue) {
  for (auto iter = lock_request_queue.request_queue_.begin(); iter != lock_request_queue.request_queue_.end(); ++iter) {
    if (iter->granted_) {
      TransactionManager::GetTransaction(iter->txn_id_)->GetSharedLockSet()->erase(rid);
      TransactionManager::GetTransaction(iter->txn_id_)->GetExclusiveLockSet()->erase(rid);
      if (iter->txn_id_ != txn_id) {
        TransactionManager::GetTransaction(iter->txn_id_)->SetState(TransactionState::ABORTED);
      }
      lock_request_queue.request_queue_.erase(iter++);
    } else {
      break;
    }
  }
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return true;
  }
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto &lock_request_queue = lock_table_[rid];
  LockRequest request(txn->GetTransactionId(), LockMode::SHARED);
  if (lock_request_queue.request_queue_.empty()) {  // no lock
    lock_request_queue.request_queue_.push_back(request);
    lock_request_queue.request_queue_.back().granted_ = true;
  } else {  // locked
    auto &pre_request = lock_request_queue.request_queue_.back();
    if (pre_request.granted_ && IsCompatible(pre_request.lock_mode_, request.lock_mode_)) {  // compatible
      request.granted_ = true;
      lock_request_queue.request_queue_.push_back(request);
    } else if (CanWound(lock_request_queue, txn->GetTransactionId())) {  // wound
      Wound(txn->GetTransactionId(), rid, lock_request_queue);
      request.granted_ = true;
      lock_request_queue.request_queue_.push_front(request);
    } else {  // wait
      lock_request_queue.request_queue_.push_back(request);
      auto request_ite = lock_request_queue.request_queue_.rbegin();  // 记录当前请求的索引
      lock_request_queue.cv_.wait(
          lock, [request_ite, txn]() { return txn->GetState() == TransactionState::ABORTED || request_ite->granted_; });
    }
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  auto &lock_request_queue = lock_table_[rid];
  LockRequest request(txn->GetTransactionId(), LockMode::SHARED);
  if (lock_request_queue.request_queue_.empty()) {  // no lock
    request.granted_ = true;
    lock_request_queue.request_queue_.push_back(request);
  } else {                                                        // lock
    if (CanWound(lock_request_queue, txn->GetTransactionId())) {  // wound
      Wound(txn->GetTransactionId(), rid, lock_request_queue);
      request.granted_ = true;
      lock_request_queue.request_queue_.push_front(request);
    } else {  // wait
      lock_request_queue.request_queue_.push_back(request);
      auto request_ite = lock_request_queue.request_queue_.rbegin();  // 记录当前请求的索引
      lock_request_queue.cv_.wait(
          lock, [request_ite, txn]() { return request_ite->granted_ || txn->GetState() == TransactionState::ABORTED; });
    }
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  // txn被abort的情况
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  auto &lock_request_queue = lock_table_[rid];
  LockRequest request(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  if (CanWound(lock_request_queue, txn->GetTransactionId())) {  // wound
    Wound(txn->GetTransactionId(), rid, lock_request_queue);
    request.granted_ = true;
    lock_request_queue.request_queue_.push_front(request);
  } else {  // wait
    lock_request_queue.upgrading_ = true;
    lock_request_queue.request_queue_.push_back(request);
    auto request_ite = lock_request_queue.request_queue_.rbegin();  // 记录当前请求的索引
    lock_request_queue.cv_.wait(
        lock, [request_ite, txn]() { return request_ite->granted_ || txn->GetState() == TransactionState::ABORTED; });
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::lock_guard<std::mutex> lock(latch_);
  auto &lock_request_queue = lock_table_[rid];
  auto txn_id = txn->GetTransactionId();
  auto request_ite = std::find_if(lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
                                  [txn_id](const LockRequest &request) { return request.txn_id_ == txn_id; });
  auto next_request_ite = request_ite;
  next_request_ite++;
  
  lock_request_queue.request_queue_.erase(request_ite);
  bool has_granted = false;
  while (next_request_ite != lock_request_queue.request_queue_.end()) {
    if (!next_request_ite->granted_) {
      if (next_request_ite == lock_request_queue.request_queue_.begin()) { // no lock
        next_request_ite->granted_ = true;
        has_granted = true;
      } else { // locked
        auto pre_request_ite = next_request_ite;
        --pre_request_ite;
        if (pre_request_ite->granted_ && IsCompatible(pre_request_ite->lock_mode_, next_request_ite->lock_mode_)) { // compatible
          next_request_ite->granted_ = true;
          has_granted = true;
        } else if (CanWound(lock_request_queue, txn->GetTransactionId())) { // wound
          Wound(txn->GetTransactionId(), rid, lock_request_queue);
          LockRequest request = *next_request_ite;
          request.granted_ = true;
          lock_request_queue.request_queue_.erase(next_request_ite++);
          lock_request_queue.request_queue_.push_front(request);
        } else { // continue wait
          // do nothing
        }
      }
    }
    next_request_ite++;
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  if (txn->GetState() == TransactionState::GROWING) {  // 切换状态
    txn->SetState(TransactionState::SHRINKING);
  }
  if (has_granted) {
    lock_request_queue.cv_.notify_all();
  }

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].push_back(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].erase(std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2));
}

bool LockManager::Dfs(txn_id_t txn_id, std::unordered_map<txn_id_t, bool> &visited, txn_id_t *new_txn_id) {
  visited[txn_id] = false;
  *new_txn_id = *new_txn_id < txn_id ? txn_id : *new_txn_id;
  auto &link_set = waits_for_[txn_id];
  for (auto &link_txn_id : link_set) {
    auto visited_ite = visited.find(link_txn_id);
    if (visited_ite != visited.end()) {
      if (!visited_ite->second) {
        return true;
      } else {
        continue;
      }
    } else {
      if (Dfs(link_txn_id, visited, new_txn_id)) {
        return true;
      }
    }
  }
  visited[txn_id] = true;
  return false;
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  // 通过dfs， 检测是否有环
  std::unordered_map<txn_id_t, bool>
      visited;  // bool为false表示该节点访问过，bool为true表示该节点的所有邻接结点都访问过
  for (auto &waits_for_item : waits_for_) {
    auto visited_ite = visited.find(waits_for_item.first);
    if (visited_ite != visited.end()) {  // 此处visited_ite.second一定为true
      continue;
    }
    *txn_id = -1;
    if (Dfs(waits_for_item.first, visited, txn_id)) {  // 开启一个新的连通图遍历
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
