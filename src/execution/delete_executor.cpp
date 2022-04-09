//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/delete_executor.h"

#include <memory>

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  auto txn = exec_ctx_->GetTransaction();
  auto lock_mgr = exec_ctx_->GetLockManager();
  if (child_executor_->Next(tuple, rid)) {
    if (txn->IsSharedLocked(*rid)) {  // repeateable read
      lock_mgr->LockUpgrade(txn, *rid);
    } else if (!txn->IsExclusiveLocked(*rid)) {  // 所有隔离级别都有可能
      lock_mgr->LockExclusive(txn, *rid);
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "Delete fail");
    }
    if (table_info_->table_->MarkDelete(*rid, txn)) {
      auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto &index_info : index_infos) {
        index_info->index_->DeleteEntry(tuple->KeyFromTuple(*child_executor_->GetOutputSchema(),
                                                            index_info->key_schema_, index_info->index_->GetKeyAttrs()),
                                        *rid, txn);
        txn->AppendTableWriteRecord(IndexWriteRecord(*rid, table_info_->oid_, WType::DELETE, *tuple,
                                                     index_info->index_oid_, exec_ctx_->GetCatalog()));
      }
      return true;
    }
  }
  return false;
}

}  // namespace bustub
