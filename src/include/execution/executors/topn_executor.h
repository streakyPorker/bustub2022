//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
               std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;

  template <typename _Tp>
  struct TopNCmp : public std::binary_function<_Tp, _Tp, bool> {
    explicit TopNCmp(const std::vector<std::pair<OrderByType, AbstractExpressionRef>> *sorters,
                     const Schema *schema)
        : sorters_(sorters), schema_(schema) {}
    TopNCmp(const TopNCmp &other) = default;

    const std::vector<std::pair<OrderByType, AbstractExpressionRef>> *sorters_{nullptr};
    const Schema *schema_{nullptr};

    auto operator()(const _Tp &__x, const _Tp &__y) const -> bool {
      for (auto sorter = sorters_->crbegin(); sorter != sorters_->crend(); ++sorter) {
        const auto &type = sorter->first;
        const auto &order_by = sorter->second;
        Value v1 = order_by->Evaluate(&__x.first, *schema_);
        Value v2 = order_by->Evaluate(&__y.first, *schema_);
        CmpBool rst =
            type != OrderByType::DESC ? v1.CompareLessThan(v2) : v1.CompareGreaterThan(v2);
        if (rst == CmpBool::CmpTrue) {
          return true;
        }
      }

      return false;
    }
  };

  using TupleRID = std::pair<Tuple, RID>;
  using TopNTuplePQ = std::priority_queue<TupleRID, std::vector<TupleRID>, TopNCmp<TupleRID>>;


  const TopNCmp<TupleRID> cmp_;
  std::unique_ptr<AbstractExecutor> child_;

  std::vector<TupleRID> tuple_rids_;
  std::unique_ptr<TopNTuplePQ> topn_tuple_pq_;

  std::atomic_bool inited_{false};
};
template struct TopNExecutor::TopNCmp<std::pair<Tuple, RID>>;
}  // namespace bustub
