//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_(dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get())),
      index_iter_(tree_->GetBeginIterator()) {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_iter_ == tree_->GetEndIterator()) {
    return false;
  }
  auto table_info = exec_ctx_->GetCatalog()->GetTable(tree_->GetMetadata()->GetTableName());
  *rid = (*index_iter_).second;
  table_info->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++index_iter_;
  return true;
}

}  // namespace bustub
