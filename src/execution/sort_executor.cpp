#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_exec_->Init();

  Tuple child_tuple{};
  RID child_tuple_rid{};

  while (child_exec_->Next(&child_tuple, &child_tuple_rid)) {
    tuples_.push_back(child_tuple);
  }

  std::sort(
      tuples_.begin(), tuples_.end(),
      [orders = plan_->GetOrderBy(), scheme = child_exec_->GetOutputSchema()](const auto &l, const auto &r) -> bool {
        for (const auto &order : orders) {
          auto cmp_greater_bool =
              order.second->Evaluate(&l, scheme).CompareGreaterThan(order.second->Evaluate(&r, scheme));
          auto cmp_less_bool = order.second->Evaluate(&l, scheme).CompareLessThan(order.second->Evaluate(&r, scheme));
          if (cmp_greater_bool == CmpBool::CmpTrue) {
            return order.first == OrderByType::DESC;
          }
          if (cmp_less_bool == CmpBool::CmpTrue) {
            return order.first != OrderByType::DESC;
          }
        }
        return false;
      });

  tuples_iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_iter_ == tuples_.end()) {
    return false;
  }

  *tuple = *tuples_iter_++;
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
