//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_exec_->Init();
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn = exec_ctx_->GetTransaction();
  auto tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
  auto inner_table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());

  Tuple child_tuple{};
  RID child_tuple_rid{};
  Tuple inner_tuple{};
  RID inner_tuple_rid{};

  while(child_exec_->Next(&child_tuple, &child_tuple_rid)) {
    std::vector<RID> result;
    auto value = plan_->KeyPredicate()->Evaluate(&child_tuple, child_exec_->GetOutputSchema());
    tree->ScanKey(Tuple{{value}, tree->GetKeySchema()}, &result, txn);

    if(!result.empty()) {
      inner_tuple_rid = result[0];
      assert(inner_table_info->table_->GetTuple(inner_tuple_rid, &inner_tuple, txn));
      std::vector<Value> values;
      for(uint32_t i = 0; i < child_exec_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(child_tuple.GetValue(&child_exec_->GetOutputSchema(), i));
      }
      for(uint32_t i = 0; i < inner_table_info->schema_.GetColumnCount(); i++) {
        values.emplace_back(inner_tuple.GetValue(&inner_table_info->schema_, i));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = tuple->GetRid();
      return true;
    }

    if(result.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for(uint32_t i = 0; i < child_exec_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(child_tuple.GetValue(&child_exec_->GetOutputSchema(), i));
      }
      for(uint32_t i = 0; i < inner_table_info->schema_.GetColumnCount(); i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(inner_table_info->schema_.GetColumn(i).GetType()));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      *rid = tuple->GetRid();
      return true;
    }

  }

  return false;
}

}  // namespace bustub
