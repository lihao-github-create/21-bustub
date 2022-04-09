//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"

#include <memory>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      raw_insert_index_(0) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto txn = exec_ctx_->GetTransaction();
  auto lock_mgr = exec_ctx_->GetLockManager();
  if (plan_->IsRawInsert()) {
    if (raw_insert_index_ < plan_->RawValues().size()) {
      Tuple temp_tuple(plan_->RawValuesAt(raw_insert_index_++), &(table_info_->schema_));
      if (table_info_->table_->InsertTuple(temp_tuple, rid, txn)) {
        lock_mgr->LockExclusive(txn, *rid);
        InsertIndexEntry(&temp_tuple, rid);
        return true;
      }
      throw Exception(ExceptionType::UNKNOWN_TYPE, "Insert fail");
    }
  } else {
    // 从子查询/child_executor获取tuple,并将tuple插入到table_info_->table_中
    if (child_executor_->Next(tuple, rid)) {
      if (table_info_->table_->InsertTuple(*tuple, rid, txn)) {
        lock_mgr->LockExclusive(txn, *rid);
        InsertIndexEntry(tuple, rid);
        return true;
      }
    }
  }
  return false;
}

void InsertExecutor::InsertIndexEntry(Tuple *tuple, RID *rid) {
  auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto txn = exec_ctx_->GetTransaction();
  for (auto &index_info : index_infos) {
    index_info->index_->InsertEntry(
        tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
        txn);
    txn->AppendTableWriteRecord(IndexWriteRecord(*rid, table_info_->oid_, WType::INSERT, *tuple, index_info->index_oid_,
                                                 exec_ctx_->GetCatalog()));
  }
}

}  // namespace bustub
