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
  child_->Init();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_ == nullptr) {
    return false;
  }
  Tuple child_tuple{};
  auto child_out_schema = child_->GetOutputSchema();
  while (child_->Next(&child_tuple, rid)) {
    aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
  }

  // group by value as first part, aggr rst as second part
  // init to own first part
  std::vector<Value> values;
  BUSTUB_ENSURE(
      GetOutputSchema().GetColumnCount() == plan_->group_bys_.size() + plan_->aggregates_.size(),
      "wrong format or intention");
  values.reserve(GetOutputSchema().GetColumnCount());

  // not reset, or it would be blocked at the first line
  if (aht_iterator_ == aht_.End()) {
    aht_iterator_ = aht_.Begin();
    // handle empty child set
    if (aht_iterator_ == aht_.End()) {
      // non-empty group with empty result set, return nothing
      if (!plan_->group_bys_.empty()) {
        child_.reset(nullptr);
        return false;
      }
      for (const auto &expr : plan_->group_bys_) {
        values.push_back(ValueFactory::GetNullValueByType(expr->GetReturnType()));
      }
      for (const auto &val : plan_->agg_types_) {
        values.push_back(aht_.GetInitAggValue(val));
      }
      *tuple = Tuple{values, &GetOutputSchema()};
      child_.reset(nullptr);
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
    child_.reset(nullptr);
  }
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * {
  return child_.get();
}

}  // namespace bustub
