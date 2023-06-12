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
#include "execution/executors/index_scan_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx,
                                             const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), outer_child_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(
        fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  inner_index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  ended_ = has_outer_tuple_ = false;
  match_list_.clear();
  match_list_iter_ = match_list_.cend();
  outer_child_->Init();
}

/*

create table t1(v1 int);
insert into t1 (select * from __mock_table_123);
create index t1_v1 on t1(v1);
select * from test_simple_seq_2 t2 left join t1 on t1.v1 = t2.col1;;


 */

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ended_) {
    return false;
  }
  assert(inner_index_info_->key_schema_.GetColumnCount() == 1);

  const auto join_type = plan_->GetJoinType();
  while (true) {
    bool new_outer_tuple = !has_outer_tuple_;
    if (!has_outer_tuple_) {
      has_outer_tuple_ = true;
//      LOG_DEBUG("yielding outer tuple");
      if (!outer_child_->Next(OuterTuple(), rid)) {
//        LOG_DEBUG("ended");
        ended_ = true;
        return false;
      }
    }

    Value outer_join_value =
        plan_->KeyPredicate()->Evaluate(OuterTuple(), outer_child_->GetOutputSchema());

    bool generate_null_result =
        // outer join attrs have null values
        (outer_join_value.IsNull()
         // got a new row, but can match a thing
         // this step also reset the inner index
         || (new_outer_tuple && !RescanInnerIndex(outer_join_value)))

        // inner join never generates null result
        // need to be at the latter to ensure the index rescan
        && join_type != JoinType::INNER;

    if (generate_null_result) {
      has_outer_tuple_ = false;
      GenerateOutTuple(tuple, join_type, false, tuples_);
//      LOG_DEBUG("yielding null row");
      return true;
    }

    // now this situation only indicates that the current scan is over
    // and the match_list_ mustn't be empty
    if (match_list_iter_ == match_list_.cend()) {
//      LOG_DEBUG("get to the end of the match_list(%lu)", match_list_.size());

      has_outer_tuple_ = false;
      continue;
    }

    if (new_outer_tuple) {
//      LOG_DEBUG("got a match size of %lu", match_list_.size());
    }

    inner_table_info_->table_->GetTuple(*match_list_iter_, InnerTuple(),
                                        exec_ctx_->GetTransaction());

    GenerateOutTuple(tuple, join_type, true, tuples_);
//    LOG_DEBUG("yielding non-null row");
    ++match_list_iter_;
    return true;
  }
}
void NestIndexJoinExecutor::GenerateOutTuple(Tuple *tuple, JoinType join_type, bool is_match,
                                             const Tuple *tuples) {
  const Schema *left_schema = &outer_child_->GetOutputSchema();
  const Schema *right_schema = &inner_table_info_->schema_;
  const bool left_emplace_value = is_match || join_type != JoinType::RIGHT;
  const bool right_emplace_value = is_match || join_type == JoinType::RIGHT;

  std::vector<Value> values;
  values.reserve(plan_->OutputSchema().GetColumnCount());
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
  *tuple = {values, &plan_->OutputSchema()};
}

}  // namespace bustub
