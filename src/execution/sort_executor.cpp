#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  inited_ = false;
  tuples_.clear();
  rst_iter_ = tuples_.cbegin();
  child_->Init();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!inited_) {
    while (child_->Next(tuple, rid)) {
      tuples_.emplace_back(*tuple);
    }

    if (tuples_.empty()) {
      return false;
    }
    // reverse iter the sort list
    for (auto pair = plan_->order_bys_.crbegin(); pair != plan_->order_bys_.crend(); ++pair) {
      const auto &type = pair->first;
      const auto &order_by = pair->second;
      const auto &schema = child_->GetOutputSchema();
      std::stable_sort(tuples_.begin(), tuples_.end(),
                       [&type, &order_by, &schema](const Tuple &t1, const Tuple &t2) {
                         Value v1 = order_by->Evaluate(&t1, schema);
                         Value v2 = order_by->Evaluate(&t2, schema);
                         return type != OrderByType::DESC ? v1.CompareLessThan(v2)
                                                          : v1.CompareGreaterThan(v2);
                       });
    }
    rst_iter_ = tuples_.cbegin();
    inited_ = true;
  }
  assert(inited_);

  if (tuples_.empty() || rst_iter_ == tuples_.cend()) {
    return false;
  }
  *tuple = *rst_iter_;
  ++rst_iter_;
  return true;
}

}  // namespace bustub
