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
  while (child_executor_->Next(tuple, rid)) {
    std::vector<Value> values;
    for (size_t i = 0; i < child_executor_->GetOutputSchema()->GetColumnCount(); i++) {
      values.push_back(tuple->GetValue(child_executor_->GetOutputSchema(), i));
    }
    DistinctKey key;
    key.distinct_values_ = values;
    if (ht_.find(key) == ht_.end()) {
      ht_.insert(key);
      return true;
    }
  }
  return false;
}

}  // namespace bustub
