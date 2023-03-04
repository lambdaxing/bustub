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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }

  Tuple child_tuple{};

  // Get the next tuple
  bool status = child_executor_->Next(&child_tuple, rid);

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  int32_t inserted_num = 0;

  while (status) {
    if (table_info->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction())) {
      inserted_num++;
      // Insert indexs of new tuple
      for (auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_)) {
        auto key =
            child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      }
    }
    status = child_executor_->Next(&child_tuple, rid);
  }

  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, inserted_num);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
