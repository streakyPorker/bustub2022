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
  outer_tuple_index_ = plan_->GetJoinType() != JoinType::RIGHT ? 0 : 1;
}

void NestedLoopJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (atomic_load(&ended_)) {
    return false;
  }
  Tuple tuples[2];
  Tuple *outer_tuple = &tuples[outer_tuple_index_];
  Tuple *inner_tuple = &tuples[outer_tuple_index_ ^ 1];
  auto outer_child = outer_tuple_index_ == 0 ? left_child_.get() : right_child_.get();
  auto inner_child = outer_tuple_index_ == 1 ? left_child_.get() : right_child_.get();
  const Schema &outer_schema = outer_tuple_index_ == 0 ? plan_->GetLeftPlan()->OutputSchema()
                                                       : plan_->GetRightPlan()->OutputSchema();

  while (true) {
    if (!outer_child->Next(outer_tuple, rid)) {
      atomic_store(&ended_, true);
      return false;
    }
    // the outer tuple has null join value, the join won't work for sure
    if (!CheckOuterTupleValid(outer_tuple, outer_schema)) {
      continue;
    }

    while (inner_child->Next(inner_tuple, rid)) {
      // now we have left & right ready
      Value rst = plan_->Predicate().EvaluateJoin(&tuples[0], left_child_->GetOutputSchema(),
                                                  &tuples[1], right_child_->GetOutputSchema());
      bool is_match = !rst.IsNull() && rst.GetAs<bool>();
      switch (plan_->GetJoinType()) {
        case JoinType::INNER:
          if (!is_match) {
            continue;
          }
          GenerateOutTuple(tuple, JoinType::INNER, true, tuples);
          inner_child->Init();  // reset inner child to get another round
          return true;
        case JoinType::LEFT:
        case JoinType::RIGHT:
          continue;
        case JoinType::OUTER:
        case JoinType::INVALID:
          assert(0);
      }
      assert(0);
    }
    // now we've traversed the inner table and can't find a match, so build a null value
    GenerateOutTuple(tuples, plan_->GetJoinType(), false, tuples);
    inner_child->Init();
  }
}

void NestedLoopJoinExecutor::GenerateOutTuple(Tuple *tuple, JoinType join_type, bool is_match,
                                              const Tuple *tuples) {
  const Schema *left_schema = &plan_->GetLeftPlan()->OutputSchema();
  const Schema *right_schema = &plan_->GetRightPlan()->OutputSchema();
  const bool left_emplace_value = is_match || join_type == JoinType::LEFT;
  const bool right_emplace_value = is_match || join_type == JoinType::RIGHT;
  assert(left_emplace_value || right_emplace_value);

  std::vector<Value> values;
  values.reserve(output_schema_->GetColumnCount());
  for (uint32_t col_idx = 0; col_idx < left_schema->GetColumnCount(); ++col_idx) {
    if (left_emplace_value) {
      values.push_back(tuples[0].GetValue(left_schema, col_idx));
    } else {
      values.push_back(ValueFactory::GetNullValueByType(left_schema->GetColumn(col_idx).GetType()));
    }
  }
  for (uint32_t col_idx = 0; col_idx < right_schema->GetColumnCount(); ++col_idx) {
    if (right_emplace_value) {
      values.push_back(tuples[1].GetValue(right_schema, col_idx));
    } else {
      values.push_back(
          ValueFactory::GetNullValueByType(right_schema->GetColumn(col_idx).GetType()));
    }
  }
  *tuple = {values, output_schema_.get()};
}

auto NestedLoopJoinExecutor::CheckOuterTupleValid(const Tuple *outer_tuple,
                                                  const Schema &outer_schema) -> bool {
  Value test_rst = plan_->Predicate().Evaluate(outer_tuple, outer_schema);
  return !test_rst.IsNull() && test_rst.GetAs<bool>();
}

}  // namespace bustub
