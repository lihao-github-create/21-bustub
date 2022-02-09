//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_is_empty_(false) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID rid;
  if (!left_executor_->Next(&left_tuple_, &rid)) {
    left_is_empty_ = true;
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (left_is_empty_) {
    return false;
  }
  while (true) {
    Tuple right_tuple;
    RID rid;
    if (!right_executor_->Next(&right_tuple, &rid)) {
      RID temp_rid;
      if (!left_executor_->Next(&left_tuple_, &temp_rid)) {
        return false;
      }
      right_executor_->Init();
    } else {
      if (plan_->Predicate() == nullptr || plan_->Predicate()
                                               ->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                                              &right_tuple, right_executor_->GetOutputSchema())
                                               .GetAs<bool>()) {
        std::vector<Value> values;
        auto &out_columns = plan_->OutputSchema()->GetColumns();
        values.reserve(out_columns.size());
        for (auto &col : out_columns) {
          values.emplace_back(col.GetExpr()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                          right_executor_->GetOutputSchema()));
        }
        *tuple = Tuple(values, plan_->OutputSchema());
        return true;
      }
    }
  }
}

}  // namespace bustub
