//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "type/value_factory.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
  atomic_store(&ended_, false);
  aht_.Clear();
  child_->Init();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (atomic_load(&ended_)) {
    return false;
  }
  Tuple child_tuple{};
  auto child_out_schema = child_->GetOutputSchema();
  while (child_->Next(&child_tuple, rid)) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
  }

  // group by value as first part, aggr rst as second part
  std::vector<Value> values;
  values.reserve(GetOutputSchema().GetColumnCount());

  if (aht_iterator_ == aht_.End()) {
    aht_iterator_ = aht_.Begin();

    if (aht_iterator_ == aht_.End()) {  // empty child set

      // non-empty group with empty result set, return nothing
      if (!plan_->group_bys_.empty()) {
        atomic_store(&ended_, true);
        return false;
      }


      for (const auto &expr : plan_->group_bys_) {
        values.push_back(ValueFactory::GetNullValueByType(expr->GetReturnType()));
      }
      for (const auto &val : plan_->agg_types_) {
        values.push_back(aht_.GetInitAggValue(val));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      atomic_store(&ended_, true);
      return true;
    }
  }

  for (const auto &val : aht_iterator_.Key().group_bys_) {
    values.push_back(val);
  }
  for (const auto &val : aht_iterator_.Val().aggregates_) {
    values.push_back(val);
  }
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;
  if (aht_iterator_ == aht_.End()) {
    atomic_store(&ended_, true);
  }
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * {
  return child_.get();
}

}  // namespace bustub
