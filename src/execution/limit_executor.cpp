//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), emit_tuple_number_(0) {}

void LimitExecutor::Init() { child_executor_->Init(); }

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (emit_tuple_number_ < plan_->GetLimit()) {
    ++emit_tuple_number_;
    Tuple temp_tuple;
    if (child_executor_->Next(&temp_tuple, rid)) {
      std::vector<Value> values;
      auto &out_columns = GetOutputSchema()->GetColumns();
      values.reserve(out_columns.size());
      for (auto &col : out_columns) {
        values.push_back(col.GetExpr()->Evaluate(&temp_tuple, child_executor_->GetOutputSchema()));
      }
      *tuple = temp_tuple;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
