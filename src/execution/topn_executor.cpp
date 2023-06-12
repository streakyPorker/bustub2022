#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cmp_(&plan->order_bys_, plan->output_schema_.get()),
      child_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  inited_ = false;
  topn_tuple_pq_ = std::make_unique<TopNTuplePQ>(cmp_);
  child_->Init();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!inited_) {
    while (child_->Next(tuple, rid)) {
      topn_tuple_pq_->push(std::make_pair(*tuple, *rid));
            while (topn_tuple_pq_->size() > plan_->GetN()) {
              topn_tuple_pq_->pop();
            }
    }
    inited_ = true;
  }
  assert(inited_);
  if (topn_tuple_pq_->empty()) {
    return false;
  }
  const auto &rst_pair = topn_tuple_pq_->top();
//  LOG_DEBUG("top is %s", rst_pair.first.ToString(plan_->output_schema_.get()).c_str());
  *tuple = rst_pair.first;
  *rid = rst_pair.second;
  topn_tuple_pq_->pop();
  return true;
}

}  // namespace bustub
