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

#include <memory>

#include "execution/executors/insert_executor.h"

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
  if (plan_->IsRawInsert()) {
    if (raw_insert_index_ < plan_->RawValues().size()) {
      Tuple temp_tuple(plan_->RawValuesAt(raw_insert_index_++), &(table_info_->schema_));
      if (table_info_->table_->InsertTuple(temp_tuple, rid, exec_ctx_->GetTransaction())) {
        InsertIndexEntry(&temp_tuple, rid);
        return true;
      }
      throw Exception(ExceptionType::UNKNOWN_TYPE, "Insert fail");
    }
  } else {
    // 从子查询/child_executor获取tuple,并将tuple插入到table_info_->table_中
    try {
      if (child_executor_->Next(tuple, rid)) {
        if (table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
          InsertIndexEntry(tuple, rid);
          return true;
        }
        throw Exception(ExceptionType::UNKNOWN_TYPE, "Insert fail");
      }
    } catch (const Exception &e) {
      throw;
    }
  }
  return false;
}

void InsertExecutor::InsertIndexEntry(Tuple *tuple, RID *rid) {
  auto index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (auto &index_info : index_infos) {
    index_info->index_->InsertEntry(
        tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()), *rid,
        exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
