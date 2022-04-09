//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/update_executor.h"

#include <memory>

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto txn = exec_ctx_->GetTransaction();
  auto lock_mgr = exec_ctx_->GetLockManager();
  if (child_executor_->Next(tuple, rid)) {
    Tuple insert_tuple = GenerateUpdatedTuple(*tuple);
    if (txn->IsSharedLocked(*rid)) {  // repeateable read
      lock_mgr->LockUpgrade(txn, *rid);
    } else if (!txn->IsExclusiveLocked(*rid)) {  // 所有隔离级别都有可能
      lock_mgr->LockExclusive(txn, *rid);
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "Delete fail");
    }
    if (table_info_->table_->UpdateTuple(insert_tuple, *rid, txn)) {
      auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_infos) {
        index_info->index_->DeleteEntry(
            tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
            txn);
        index_info->index_->InsertEntry(
            insert_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
            *rid, txn);
        IndexWriteRecord index_record(*rid, table_info_->oid_, WType::UPDATE, insert_tuple, index_info->index_oid_,
                                      exec_ctx_->GetCatalog());
        index_record.old_tuple_ = *tuple;
        txn->AppendTableWriteRecord(index_record);
      }
      return true;
    }
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
