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
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  txn_ = exec_ctx_->GetTransaction();
  lock_manager_ = exec_ctx_->GetLockManager();
  isolation_level_ = txn_->GetIsolationLevel();
  table_oid_ = plan_->GetTableOid();

  if (isolation_level_ != IsolationLevel::READ_UNCOMMITTED) {
    try {
      lock_manager_->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, table_oid_);
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.GetInfo());
    }
  }

  table_iter_ = std::make_shared<TableIterator>(exec_ctx_->GetCatalog()->GetTable(table_oid_)->table_->Begin(txn_));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*table_iter_ == exec_ctx_->GetCatalog()->GetTable(table_oid_)->table_->End()) {
    if (isolation_level_ == IsolationLevel::READ_COMMITTED && txn_->GetState() == TransactionState::GROWING) {
      try {
        lock_manager_->UnlockTable(txn_, table_oid_);
      } catch (TransactionAbortException &e) {
        throw ExecutionException(e.GetInfo());
      }
    }
    return false;
  }

  *rid = table_iter_->operator*().GetRid();

  if (isolation_level_ != IsolationLevel::READ_UNCOMMITTED) {
    try {
      lock_manager_->LockRow(txn_, LockManager::LockMode::SHARED, table_oid_, *rid);
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.GetInfo());
    }
  }

  *tuple = *(*table_iter_)++;

  if (isolation_level_ == IsolationLevel::READ_COMMITTED && txn_->GetState() == TransactionState::GROWING) {
    try {
      lock_manager_->UnlockRow(txn_, table_oid_, *rid);
    } catch (TransactionAbortException &e) {
      throw ExecutionException(e.GetInfo());
    }
  }

  return true;
}

}  // namespace bustub
