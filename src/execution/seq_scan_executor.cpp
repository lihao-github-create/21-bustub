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
  bool ret = false;
  while (table_iterator_ != table_info_->table_->End()) {
    if (exec_ctx_->GetTransaction()->GetState() == TransactionState::ABORTED) {
      throw TransactionAbortException(exec_ctx_->GetTransaction()->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
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
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      exec_ctx_->GetLockManager()->Unlock(exec_ctx_->GetTransaction(), table_iterator_->GetRid());
    }
    ++table_iterator_;
    if (ret) {
      break;
    }
  }

  return ret;
}

}  // namespace bustub
