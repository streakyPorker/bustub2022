//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <memory>

#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "execution/expressions/column_value_expression.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx,
                                               const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_executor)),
      right_child_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(
        fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  std::vector<Column> cols;
  cols.reserve(plan_->GetLeftPlan()->OutputSchema().GetColumnCount() +
               plan_->GetRightPlan()->OutputSchema().GetColumnCount());
  for (const auto &col : plan->GetLeftPlan()->OutputSchema().GetColumns()) {
    cols.push_back(col);
  }
  for (const auto &col : plan->GetRightPlan()->OutputSchema().GetColumns()) {
    cols.push_back(col);
  }
  output_schema_ = std::make_unique<Schema>(cols);
}

void NestedLoopJoinExecutor::Init() {
  left_child_->Init();
  left_tuple_ = std::make_unique<Tuple>();

  // empty left child, return immediately
  if (!left_child_->Next(left_tuple_.get(), &left_rid_)) {
    left_child_.reset(nullptr);
  } else {
    right_child_->Init();
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (left_child_ == nullptr) {
    return false;
  }

  while (true) {
    right_tuple_ = std::make_unique<Tuple>();
    while (!right_child_->Next(right_tuple_.get(), &right_rid_)) {
      left_tuple_ = std::make_unique<Tuple>();
      if (!left_child_->Next(left_tuple_.get(), &left_rid_)) {
        left_child_.reset(nullptr);
        right_child_.reset(nullptr);
        return false;
      }

      right_child_->Init();
      right_tuple_ = std::make_unique<Tuple>();
    }
    // now we have left & right ready
    Value rst =
        plan_->Predicate().EvaluateJoin(left_tuple_.get(), left_child_->GetOutputSchema(),
                                        right_tuple_.get(), right_child_->GetOutputSchema());
    bool is_match = !rst.IsNull() && rst.GetAs<bool>();
    switch (plan_->GetJoinType()) {
      case JoinType::INNER:
        if (!is_match) {
          continue;
        }
      case JoinType::LEFT:
      case JoinType::RIGHT:
        if (!CheckSideValid(plan_->GetJoinType())) {
          continue;
        }
      case JoinType::OUTER:
        GenerateOutTuple(tuple, rid, plan_->GetJoinType(), is_match);

        return true;
      case JoinType::INVALID:
        assert(0);
    }
  }
}
void NestedLoopJoinExecutor::GenerateOutTuple(Tuple *tuple, RID *rid, JoinType join_type,
                                              bool is_match) {
  const Schema *left_schema = &plan_->GetLeftPlan()->OutputSchema();
  const Schema *right_schema = &plan_->GetRightPlan()->OutputSchema();
  const bool left_emplace_value = is_match || join_type != JoinType::RIGHT;
  const bool right_emplace_value = is_match || join_type != JoinType::LEFT;

  std::vector<Value> values;
  values.reserve(output_schema_->GetColumnCount());
  for (uint32_t col_idx = 0; col_idx < left_schema->GetColumnCount(); ++col_idx) {
    if (left_emplace_value) {
      values.push_back(left_tuple_->GetValue(left_schema, col_idx));
    } else {
      values.push_back(ValueFactory::GetNullValueByType(left_schema->GetColumn(col_idx).GetType()));
    }
  }
  for (uint32_t col_idx = 0; col_idx < right_schema->GetColumnCount(); ++col_idx) {
    if (right_emplace_value) {
      values.push_back(right_tuple_->GetValue(right_schema, col_idx));
    } else {
      values.push_back(
          ValueFactory::GetNullValueByType(right_schema->GetColumn(col_idx).GetType()));
    }
  }
  *tuple = {values, output_schema_.get()};
  if (join_type == JoinType::LEFT || join_type == JoinType::INNER) {
  }
}

auto NestedLoopJoinExecutor::CheckSideValid(JoinType join_type) -> bool {
  if (join_type == JoinType::INNER) {
    return true;
  }

  Value test_rst =
      join_type == JoinType::LEFT
          ? plan_->Predicate().Evaluate(left_tuple_.get(), plan_->GetLeftPlan()->OutputSchema())
          : plan_->Predicate().Evaluate(right_tuple_.get(), plan_->GetRightPlan()->OutputSchema());
  return !test_rst.IsNull() && test_rst.GetAs<bool>();
}

}  // namespace bustub
