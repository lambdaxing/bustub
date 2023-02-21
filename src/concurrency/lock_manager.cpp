//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 判断合理性
  if (!CheckLockReasonability(txn, lock_mode, txn->GetIsolationLevel(), false)) {
    return false;
  }
  // 判断可升级性
  int check_code = CheckUpgradability(txn, lock_mode, oid, RID(), false);
  if (check_code == -1) {
    return false;
  }
  if (check_code == 1) {
    return true;
  }
  bool upgrade = check_code == 0;
  txn_id_t txn_id = txn->GetTransactionId();
  // 锁住 table lock map
  std::unique_lock<std::mutex> map_lock(table_lock_map_latch_);
  // 是否为资源 oid 上的第一个锁请求
  if (table_lock_map_.count(oid) == 0) {
    // 创建资源 oid 的请求队列
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
    // 获取新创建的请求队列
    auto &request_queue = table_lock_map_[oid]->request_queue_;
    // 锁转换，锁住 oid 的请求队列，释放 table lock map
    std::unique_lock<std::mutex> lock(table_lock_map_[oid]->latch_);
    map_lock.unlock();
    // 往请求队列插入新的当前请求对象
    auto iter = request_queue.insert(request_queue.cend(), std::make_shared<LockRequest>(txn_id, lock_mode, oid));
    // 已分配锁，更新对象状态
    (*iter)->granted_ = true;
    // 添加加锁记录到事务 txn 中
    InsertLockToTransaction(txn, iter, false);
    return true;
  }
  // 存在资源 oid 上的请求队列，获取该队列
  auto table_lock_queue = table_lock_map_[oid];
  // 锁转换，锁住 oid 的请求队列，释放 table lock map
  std::unique_lock<std::mutex> queue_lock(table_lock_queue->latch_);
  map_lock.unlock();
  auto &request_queue = table_lock_queue->request_queue_;
  // 插入新请求对象返回的迭代器
  decltype(request_queue.begin()) iter;
  if (upgrade) {
    // 是否存在冲突的并发锁升级
    if (table_lock_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    table_lock_queue->upgrading_ = txn_id;
    // 查找该事务之前在资源 oid 上的锁请求对象
    iter = std::find_if(request_queue.begin(), request_queue.end(),
                        [&txn_id](const auto &it) { return it->txn_id_ == txn_id; });
    // 删除该事务在资源 oid 上的加锁记录
    DeleteLockInTransaction(txn, iter, false);
    // 删除原来的锁请求对象
    request_queue.erase(iter);
    // 查找第一个未分配锁的请求对象的位置
    auto pos = std::find_if(request_queue.cbegin(), request_queue.cend(),
                            [](const auto &it) { return it->granted_ == false; });
    // 将升级后的请求对象插入第一个未分配锁的请求对象之前
    iter = request_queue.insert(pos, std::make_shared<LockRequest>(txn_id, lock_mode, oid));
  } else {
    // 不是锁升级请求，在请求队列尾部插入该请求对象
    iter = request_queue.insert(request_queue.cend(), std::make_shared<LockRequest>(txn_id, lock_mode, oid));
  }

  // 进入相容性检测循环，直到满足相容性加锁条件（并且队列中位于前面的锁请求都已被分配）或成为队头。
  while (!CheckCompability(request_queue, iter)) {
    table_lock_queue->cv_.wait(queue_lock);
  }
  if (table_lock_queue->upgrading_ == txn_id) {
    table_lock_queue->upgrading_ = INVALID_TXN_ID;
  }
  (*iter)->granted_ = true;
  InsertLockToTransaction(txn, iter, false);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 判断合理性
  if (!CheckLockReasonability(txn, lock_mode, txn->GetIsolationLevel(), true)) {
    return false;
  }
  // 判断可升级性
  int check_code = CheckUpgradability(txn, lock_mode, oid, rid, true);
  if (check_code == -1) {
    return false;
  }
  if (check_code == 1) {
    return true;
  }
  bool upgrade = check_code == 0;
  txn_id_t txn_id = txn->GetTransactionId();
  // 锁住 row lock map
  std::unique_lock<std::mutex> map_lock(row_lock_map_latch_);
  // 是否为资源 oid 上的第一个锁请求
  if (row_lock_map_.count(rid) == 0) {
    // 创建资源 oid 的请求队列
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
    // 获取新创建的请求队列
    auto &request_queue = row_lock_map_[rid]->request_queue_;
    // 锁转换，锁住 oid 的请求队列，释放 row lock map
    std::unique_lock<std::mutex> lock(row_lock_map_[rid]->latch_);
    map_lock.unlock();
    // 往请求队列插入新的当前请求对象
    auto iter = request_queue.insert(request_queue.cend(), std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid));
    // 已分配锁，更新对象状态
    (*iter)->granted_ = true;
    // 添加加锁记录到事务 txn 中
    InsertLockToTransaction(txn, iter, true);
    return true;
  }
  // 存在资源 oid 上的请求队列，获取该队列
  auto row_lock_queue = row_lock_map_[rid];
  // 锁转换，锁住 oid 的请求队列，释放 row lock map
  std::unique_lock<std::mutex> queue_lock(row_lock_queue->latch_);
  map_lock.unlock();
  auto &request_queue = row_lock_queue->request_queue_;
  // 插入新请求对象返回的迭代器
  decltype(request_queue.begin()) iter;
  if (upgrade) {
    // 是否存在冲突的并发锁升级
    if (row_lock_queue->upgrading_ != INVALID_TXN_ID) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
      return false;
    }
    row_lock_queue->upgrading_ = txn_id;
    // 查找该事务之前在资源 oid 上的锁请求对象
    iter = std::find_if(request_queue.begin(), request_queue.end(),
                        [&txn_id](const auto &it) { return it->txn_id_ == txn_id; });
    // 删除该事务在资源 oid 上的加锁记录
    DeleteLockInTransaction(txn, iter, true);
    // 删除原来的锁请求对象
    request_queue.erase(iter);
    // 查找第一个未分配锁的请求对象的位置
    auto pos = std::find_if(request_queue.cbegin(), request_queue.cend(),
                            [](const auto &it) { return it->granted_ == false; });
    // 将升级后的请求对象插入第一个未分配锁的请求对象之前
    iter = request_queue.insert(pos, std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid));
  } else {
    // 不是锁升级请求，在请求队列尾部插入该请求对象
    iter = request_queue.insert(request_queue.cend(), std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid));
  }

  // 进入相容性检测循环，直到满足相容性加锁条件（并且队列中位于前面的锁请求都已被分配）或成为队头。
  while (!CheckCompability(request_queue, iter)) {
    row_lock_queue->cv_.wait(queue_lock);
  }

  if (row_lock_queue->upgrading_ == txn_id) {
    row_lock_queue->upgrading_ = INVALID_TXN_ID;
  }
  (*iter)->granted_ = true;
  InsertLockToTransaction(txn, iter, true);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 合理性检查：是否持有要释放的锁，对于 unlock table，还检查 txn 是否持有该 table 上的 row lock。
  if (!CheckUnlockReasonability(txn, oid, RID(), false)) {
    return false;
  }
  txn_id_t txn_id = txn->GetTransactionId();
  // 锁住 table lock map
  std::unique_lock<std::mutex> map_lock(table_lock_map_latch_);
  assert(table_lock_map_.count(oid) != 0);
  // 获取相应的请求队列
  auto table_lock_queue = table_lock_map_[oid];
  // 锁转换，锁住 oid 的请求队列，释放 table lock map
  std::unique_lock<std::mutex> queue_lock(table_lock_queue->latch_);
  map_lock.unlock();
  auto &request_queue = table_lock_queue->request_queue_;
  // 在请求队列中查找要释放的锁
  auto iter = std::find_if(request_queue.cbegin(), request_queue.cend(),
                           [&txn_id](const auto &it) { return it->txn_id_ == txn_id; });
  // 记录释放的锁类型，后面更新事务状态
  LockMode unlocked_mode = (*iter)->lock_mode_;
  // 删除事务中关于已解锁的记录
  DeleteLockInTransaction(txn, iter, false);
  request_queue.erase(iter);
  table_lock_queue->cv_.notify_all();
  // 释放请求队列
  queue_lock.unlock();
  // 更新事务状态
  UpdateTransactionState(txn, unlocked_mode);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  // 合理性检查：是否持有要释放的锁，对于 unlock table，还检查 txn 是否持有该 table 上的 row lock。
  if (!CheckUnlockReasonability(txn, oid, rid, true)) {
    return false;
  }
  txn_id_t txn_id = txn->GetTransactionId();
  // 锁住 table lock map
  std::unique_lock<std::mutex> map_lock(row_lock_map_latch_);
  assert(row_lock_map_.count(rid) != 0);
  // 获取相应的请求队列
  auto row_lock_queue = row_lock_map_[rid];
  // 锁转换，锁住 oid 的请求队列，释放 table lock map
  std::unique_lock<std::mutex> queue_lock(row_lock_queue->latch_);
  map_lock.unlock();
  auto &request_queue = row_lock_queue->request_queue_;
  // 在请求队列中查找要释放的锁
  auto iter = std::find_if(request_queue.cbegin(), request_queue.cend(),
                           [&txn_id](const auto &it) { return it->txn_id_ == txn_id; });
  // 记录释放的锁类型，后面更新事务状态
  LockMode unlocked_mode = (*iter)->lock_mode_;
  // 删除事务中关于已解锁的记录
  DeleteLockInTransaction(txn, iter, true);
  request_queue.erase(iter);
  row_lock_queue->cv_.notify_all();
  // 释放请求队列
  queue_lock.unlock();
  // 更新事务状态
  UpdateTransactionState(txn, unlocked_mode);
  return true;
}

void LockManager::UpdateTransactionState(Transaction *txn, LockMode unlocked_mode) {
  IsolationLevel isolation_level = txn->GetIsolationLevel();
  if (isolation_level == IsolationLevel::REPEATABLE_READ &&
      (unlocked_mode == LockMode::SHARED || unlocked_mode == LockMode::EXCLUSIVE) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  if (isolation_level == IsolationLevel::READ_COMMITTED && unlocked_mode == LockMode::EXCLUSIVE &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  if (isolation_level == IsolationLevel::READ_UNCOMMITTED && unlocked_mode == LockMode::EXCLUSIVE &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
}

auto LockManager::CheckUnlockReasonability(Transaction *txn, const table_oid_t &oid, const RID &rid, bool row) -> bool {
  /**
   * Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   * If not, LockManager should set the TransactionState as ABORTED and throw
   * a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   */
  txn_id_t txn_id = txn->GetTransactionId();
  if (row) {
    if ((txn->GetSharedRowLockSet()->count(oid) != 0 && txn->GetSharedRowLockSet()->at(oid).count(rid) != 0) ||
        (txn->GetExclusiveRowLockSet()->count(oid) != 0 && txn->GetExclusiveRowLockSet()->at(oid).count(rid) != 0)) {
      return true;
    } else {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      return false;
    }
  } else {
    if (txn->GetSharedTableLockSet()->count(oid) == 0 && txn->GetExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionSharedTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionExclusiveTableLockSet()->count(0) == 0 &&
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
      return false;
    }
    /**
     * If the transaction holds locks on rows of the table, Unlock should set the Transaction State
     * as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
     */
    if ((txn->GetSharedRowLockSet()->count(oid) != 0 && txn->GetSharedRowLockSet()->at(oid).size() != 0) ||
        (txn->GetExclusiveRowLockSet()->count(oid) != 0 && txn->GetExclusiveRowLockSet()->at(oid).size() != 0)) {
      // 虽然之前 unlock row 操作会删除事务中 row s/x lock set 中行锁为 0 的表元素，但是事务处理中仍有一些行锁为 0
      // 的表元素未被删除 因此，需要判断里面的行锁集是否为 0。
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
      return false;
    }
  }
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

/**
 *
 *  Helper functions:
 *
 */

auto LockManager::CheckCompability(std::list<std::shared_ptr<LockRequest>> &request_queue,
                                   std::list<std::shared_ptr<LockRequest>>::const_iterator iter) -> bool {
  for (auto i = request_queue.cbegin(); i != iter; i++) {
    for (auto j = request_queue.cbegin(); j != i; j++) {
      if (!CompatibleMatrix[static_cast<int>((*i)->lock_mode_)][static_cast<int>((*j)->lock_mode_)]) {
        return false;
      }
    }
  }
  return true;
}

void LockManager::InsertLockToTransaction(Transaction *txn, std::list<std::shared_ptr<LockRequest>>::const_iterator it,
                                          bool row) {
  LockMode lock_mode = (*it)->lock_mode_;
  txn_id_t oid = (*it)->oid_;
  RID rid = (*it)->rid_;
  if (row) {
    if (lock_mode == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->operator[](oid).insert(rid);
    } else {
      txn->GetExclusiveRowLockSet()->operator[](oid).insert(rid);
    }
  } else {
    if (lock_mode == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->insert(oid);
    } else if (lock_mode == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->insert(oid);
    } else if (lock_mode == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->insert(oid);
    } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->insert(oid);
    } else {
      txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
    }
  }
}

void LockManager::DeleteLockInTransaction(Transaction *txn, std::list<std::shared_ptr<LockRequest>>::const_iterator it,
                                          bool row) {
  LockMode locked_mode = (*it)->lock_mode_;
  txn_id_t oid = (*it)->oid_;
  RID rid = (*it)->rid_;
  if (row) {
    if (locked_mode == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
      if (txn->GetSharedRowLockSet()->at(oid).size() == 0) {
        txn->GetSharedRowLockSet()->erase(oid);
      }
    } else if (locked_mode == LockMode::EXCLUSIVE) {
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
      if (txn->GetExclusiveRowLockSet()->at(oid).size() == 0) {
        txn->GetExclusiveRowLockSet()->erase(oid);
      }
    } else {
    }
  } else {
    if (locked_mode == LockMode::SHARED) {
      txn->GetSharedTableLockSet()->erase(oid);
    } else if (locked_mode == LockMode::INTENTION_SHARED) {
      txn->GetIntentionSharedTableLockSet()->erase(oid);
    } else if (locked_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->GetIntentionExclusiveTableLockSet()->erase(oid);
    } else if (locked_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
    } else if (locked_mode == LockMode::EXCLUSIVE) {
      txn->GetExclusiveTableLockSet()->erase(oid);
    } else {
    }
  }
}

auto LockManager::CheckUpgradability(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid,
                                     bool row) -> int {
  if (row) {
    if (lock_mode == LockMode::EXCLUSIVE && txn->GetExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0 &&
        txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) == 0) {
      txn->SetState(TransactionState::ABORTED);

      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return -1;
    }
    /* 这里,我不清楚对于 S 而言，是否允许 table 上的 X 进行 row 上的 S */
    if (lock_mode == LockMode::SHARED && txn->GetSharedTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionSharedTableLockSet()->count(oid) == 0 &&
        txn->GetIntentionExclusiveTableLockSet()->count(oid) == 0) {
      txn->SetState(TransactionState::ABORTED);
      assert(txn->GetExclusiveTableLockSet()->count(oid) == 0);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
      return -1;
    }

    if (txn->GetSharedRowLockSet()->count(oid) != 0 && txn->GetSharedRowLockSet()->at(oid).count(rid) != 0) {
      if (lock_mode == LockMode::SHARED) {
        return 1;
      } else if (lock_mode == LockMode::EXCLUSIVE) {
        return 0;
      } else {
        assert(false);
      }
    }
    if (txn->GetExclusiveRowLockSet()->count(oid) != 0 && txn->GetExclusiveRowLockSet()->at(oid).count(rid) != 0) {
      if (lock_mode == LockMode::EXCLUSIVE) {
        return 1;
      } else {
        assert(false);
      }
    }
  } else {
    if (txn->GetIntentionSharedTableLockSet()->count(oid) != 0) {
      if (lock_mode == LockMode::INTENTION_SHARED) {
        return 1;
      } else {
        return 0;
      }
    }
    if (txn->GetSharedTableLockSet()->count(oid) != 0) {
      if (lock_mode == LockMode::SHARED) {
        return 1;
      } else if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return 0;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return -1;
      }
    }
    if (txn->GetIntentionExclusiveTableLockSet()->count(oid) != 0) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
        return 1;
      } else if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return 0;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return -1;
      }
    }
    if (txn->GetSharedIntentionExclusiveTableLockSet()->count(oid) != 0) {
      if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        return 1;
      } else if (lock_mode == LockMode::EXCLUSIVE) {
        return 0;
      } else {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return -1;
      }
    }
  }
  return 2;
}

auto LockManager::CheckLockReasonability(Transaction *txn, LockMode lock_mode, IsolationLevel isolation_level, bool row)
    -> bool {
  auto txn_id = txn->GetTransactionId();
  auto txn_state = txn->GetState();
  /**
   * Table locking should support all lock modes.
   * Row locking should not support Intention locks. Attempting this should set the TransactionState as
   * ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   */
  if (row && (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
              lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
    return false;
  }
  /**
   * X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   * should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   */
  if (txn_state == TransactionState::SHRINKING &&
      (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  /**
   * READ_UNCOMMITTED:
   * The transaction is required to take only IX, X locks.
   * X, IX locks are allowed in the GROWING state.
   * S, IS, SIX locks are never allowed
   *
   * For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   * TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   */
  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    } else if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }
  /**
   * READ_COMMITTED:
   * The transaction is required to take all locks.
   * All locks are allowed in the GROWING state
   * Only IS, S locks are allowed in the SHRINKING state
   *
   */
  if (isolation_level == IsolationLevel::READ_COMMITTED && txn_state == TransactionState::SHRINKING &&
      lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  /**
   * REPEATABLE_READ:
   * The transaction is required to take all locks.
   * All locks are allowed in the GROWING state
   * No locks are allowed in the SHRINKING state
   */
  if (isolation_level == IsolationLevel::REPEATABLE_READ && txn_state == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  return true;
}

}  // namespace bustub
