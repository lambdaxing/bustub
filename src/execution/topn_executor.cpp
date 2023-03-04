#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_exec_->Init();

  auto cmp = [orders = plan_->GetOrderBy(), scheme = child_exec_->GetOutputSchema()](const auto &l,
                                                                                     const auto &r) -> bool {
    for (const auto &order : orders) {
      auto cmp_greater_bool = order.second->Evaluate(&l, scheme).CompareGreaterThan(order.second->Evaluate(&r, scheme));
      auto cmp_less_bool = order.second->Evaluate(&l, scheme).CompareLessThan(order.second->Evaluate(&r, scheme));
      if (cmp_greater_bool == CmpBool::CmpTrue) {
        return order.first == OrderByType::DESC;
      }
      if (cmp_less_bool == CmpBool::CmpTrue) {
        return order.first != OrderByType::DESC;
      }
    }
    return false;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> topn(cmp);

  Tuple child_tuple{};
  RID child_tuple_rid{};

  while (child_exec_->Next(&child_tuple, &child_tuple_rid)) {
    topn.push(child_tuple);
    while (topn.size() > plan_->GetN()) {
      topn.pop();
    }
  }

  while (!topn.empty()) {
    topn_tuples_.push_front(topn.top());
    topn.pop();
  }

  topn_tuples_iter_ = topn_tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (topn_tuples_iter_ == topn_tuples_.end()) {
    return false;
  }

  *tuple = *topn_tuples_iter_++;
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
