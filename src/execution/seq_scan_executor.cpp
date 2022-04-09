//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      table_iterator_(nullptr, RID(INVALID_PAGE_ID, 0), nullptr) {}

void SeqScanExecutor::Init() { table_iterator_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto txn = exec_ctx_->GetTransaction();
  bool ret = false;
  while (table_iterator_ != table_info_->table_->End()) {
    if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !txn->IsSharedLocked(table_iterator_->GetRid()) && !txn->IsExclusiveLocked(table_iterator_->GetRid())) {
      exec_ctx_->GetLockManager()->LockShared(txn, table_iterator_->GetRid());
    }
    if (txn->GetState() == TransactionState::ABORTED) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "Read fail");
    }
    if (plan_->GetPredicate() == nullptr ||
        plan_->GetPredicate()->Evaluate(&(*table_iterator_), &(table_info_->schema_)).GetAs<bool>()) {
      ret = true;
      auto &out_columns = plan_->OutputSchema()->GetColumns();
      std::vector<Value> values;
      values.reserve(out_columns.size());
      for (auto &col : out_columns) {
        values.push_back(col.GetExpr()->Evaluate(&(*table_iterator_), &table_info_->schema_));
      }
      Tuple temp_tuple(values, plan_->OutputSchema());
      *tuple = temp_tuple;
      *rid = table_iterator_->GetRid();
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
        !txn->IsExclusiveLocked(table_iterator_->GetRid())) {
      exec_ctx_->GetLockManager()->Unlock(txn, table_iterator_->GetRid());
    }
    ++table_iterator_;
    if (ret) {
      break;
    }
  }
  return ret;
}

}  // namespace bustub
