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

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(is_end_) { return false; }

  Tuple child_tuple{};

  // Get the next tuple
  bool status = child_executor_->Next(&child_tuple, rid);

  auto table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  int32_t deleted_num = 0;
  
  while(status) {
    if(table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      deleted_num++;
      // Delete indexs of new tuple
      for(auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        auto key = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
      }
    }
    status = child_executor_->Next(&child_tuple, rid);
  }
  
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, deleted_num);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
