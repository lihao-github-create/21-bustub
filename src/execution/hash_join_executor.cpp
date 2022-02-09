//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)),
      join_bucket_(nullptr),
      next_join_tuple_(0) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (left_executor_->Next(&tuple, &rid)) {
    JoinKey join_key;
    join_key.join_value_ = plan_->LeftJoinKeyExpression()->Evaluate(&tuple, left_executor_->GetOutputSchema());
    hash_map_[join_key].push_back(tuple);
  }
}
bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (join_bucket_ != nullptr) {  // join_bucket_仍有剩余tuple未做join
    auto &left_tuple = join_bucket_->at(next_join_tuple_);
    next_join_tuple_++;
    if (next_join_tuple_ == join_bucket_->size()) {
      join_bucket_ = nullptr;
      next_join_tuple_ = 0;
    }
    auto &out_columns = plan_->OutputSchema()->GetColumns();
    std::vector<Value> join_values;
    join_values.reserve(out_columns.size());
    for (auto &col : out_columns) {
      join_values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                        &right_join_tuple_, right_executor_->GetOutputSchema()));
    }
    *tuple = Tuple(join_values, plan_->OutputSchema());
    return true;
  }
  // join_bucket已无剩余tuple
  while (true) {
    RID rid;
    if (!right_executor_->Next(&right_join_tuple_, &rid)) {  // right_table已无tuple
      return false;
    }
    // right_table仍然有tuple
    auto ht_ite = hash_map_.find(
        JoinKey(plan_->RightJoinKeyExpression()->Evaluate(&right_join_tuple_, right_executor_->GetOutputSchema())));
    if (ht_ite == hash_map_.end()) {
      continue;
    }
    // 可以做join
    join_bucket_ = &ht_ite->second;
    auto &left_tuple = join_bucket_->at(next_join_tuple_);
    next_join_tuple_++;
    if (next_join_tuple_ == join_bucket_->size()) {
      join_bucket_ = nullptr;
      next_join_tuple_ = 0;
    }
    auto &out_columns = plan_->OutputSchema()->GetColumns();
    std::vector<Value> join_values;
    join_values.reserve(out_columns.size());
    for (auto &col : out_columns) {
      join_values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                        &right_join_tuple_, right_executor_->GetOutputSchema()));
    }
    *tuple = Tuple(join_values, plan_->OutputSchema());
    return true;
  }
}

}  // namespace bustub
