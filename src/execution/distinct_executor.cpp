//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() { child_executor_->Init(); }

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple temp_tuple;
  while (child_executor_->Next(&temp_tuple, rid)) {
    std::vector<Value> values;
    auto &out_columns = GetOutputSchema()->GetColumns();
    values.reserve(out_columns.size());
    for (auto &col : out_columns) {
      values.push_back(col.GetExpr()->Evaluate(&temp_tuple, child_executor_->GetOutputSchema()));
    }
    DistinctKey key;
    key.distinct_values_ = values;
    if (ht_.find(key) == ht_.end()) {
      ht_.insert(key);
      *tuple = Tuple(values, GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
