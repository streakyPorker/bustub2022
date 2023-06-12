//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * IndexJoinExecutor executes index join operations.
 */
class NestIndexJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new nested index join executor.
   *
   * <p>
   * only when the inner table has an index on the join attr will the
   * optimizer choose to use the NIJ
   * </p>
   *
   *
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the nested index join plan node
   * @param child_executor the outer table
   */
  NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                        std::unique_ptr<AbstractExecutor> &&child_executor);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  void GenerateOutTuple(Tuple *tuple, JoinType join_type, bool is_match, const Tuple *tuples);

  auto inline OuterTuple() -> Tuple * { return &tuples_[outer_tuple_index_]; }
  auto inline InnerTuple() -> Tuple * { return &tuples_[outer_tuple_index_ ^ 1]; }

  auto RescanInnerIndex(const Value &outer_value_rst) -> bool {
    match_list_.clear();
    Tuple scan_key{std::vector{outer_value_rst}, &inner_index_info_->key_schema_};
    inner_index_info_->index_->ScanKey(scan_key, &match_list_, exec_ctx_->GetTransaction());
    match_list_iter_ = match_list_.cbegin();
    return !match_list_.empty();
  }

  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> outer_child_;

  TableInfo *inner_table_info_{nullptr};
  IndexInfo *inner_index_info_{nullptr};

  std::atomic_bool ended_{false};
  std::atomic_bool has_outer_tuple_{false};

  Tuple tuples_[2];

  std::vector<RID> match_list_;
  std::vector<RID>::const_iterator match_list_iter_;

  size_t outer_tuple_index_{0};
};
}  // namespace bustub
