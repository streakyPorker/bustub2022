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
  ended_ = outer_matched_ = has_outer_tuple_ = false;
  inner_matches_.clear();
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
  const auto join_type = plan_->GetJoinType();
  while (true) {
    if (!has_outer_tuple_) {
      outer_matched_ = false;
      LOG_DEBUG("nexting outer tuple");
      if (!outer_child_->Next(OuterTuple(), rid)) {
        ended_ = true;
        return false;
      }
    }

    has_outer_tuple_ = true;
    // find inner tuple
    Value outer_value_rst =
        plan_->KeyPredicate()->Evaluate(OuterTuple(), outer_child_->GetOutputSchema());

    BUSTUB_ENSURE(inner_index_info_->key_schema_.GetColumnCount() == 1,
                  "unimplemented multi index join");

    bool inner_iter_ended = inner_match_idx_ == inner_matches_.size();

    bool generate_null_result =
        // inner join never generates null result
        join_type != JoinType::INNER &&
        // outer join attrs have null values
        (outer_value_rst.IsNull()
         // can't find an inner row to match
         || (inner_iter_ended && !outer_matched_));

    if (generate_null_result) {
      GenerateOutTuple(tuple, join_type, false, tuples_);
    }

    // this would only be executed exactly once
    if (!IterInited()) {
      Tuple scan_key{std::vector{outer_value_rst}, &inner_index_info_->key_schema_};
      inner_index_info_->index_->ScanKey(scan_key, &inner_matches_, exec_ctx_->GetTransaction());
      inner_match_idx_ = 0;
    }

    // now already traversed the inner index
    if (inner_match_idx_ >= inner_matches_.size()) {
      Tuple scan_key{std::vector{outer_value_rst}, &inner_index_info_->key_schema_};
      inner_index_info_->index_->ScanKey(scan_key, &inner_matches_, exec_ctx_->GetTransaction());
      inner_match_idx_ = 0;
    }
    // both of the situation would cause a Next in the outer child
    if (generate_null_result || inner_iter_ended) {
      has_outer_tuple_ = outer_matched_ = false;
      if (generate_null_result) {
        return true;
      }
      continue;
    }

    inner_table_info_->table_->GetTuple(inner_matches_[inner_match_idx_], InnerTuple(),
                                        exec_ctx_->GetTransaction());
    ++inner_match_idx_;
    LOG_DEBUG("incremented iter %d", inner_match_idx_);
    GenerateOutTuple(tuple, join_type, true, tuples_);
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
