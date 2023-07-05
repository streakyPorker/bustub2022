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
  tuple_rids_.clear();
  topn_tuple_pq_ = std::make_unique<TopNTuplePQ>(cmp_);
  child_->Init();
}

// select * from test_simple_seq_2 order by col2,col1 desc limit 15;
auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!inited_) {
    while (child_->Next(tuple, rid)) {
      topn_tuple_pq_->push(std::make_pair(*tuple, *rid));
      while (topn_tuple_pq_->size() > plan_->GetN()) {
        topn_tuple_pq_->pop();
      }
    }
    while (!topn_tuple_pq_->empty()) {
      tuple_rids_.push_back(topn_tuple_pq_->top());
      topn_tuple_pq_->pop();
    }
    topn_tuple_pq_.reset(nullptr);
    inited_ = true;
  }
  assert(inited_);
  if (tuple_rids_.empty()) {
    return false;
  }
  const auto &rst_pair = tuple_rids_.back();
  *tuple = rst_pair.first;
  *rid = rst_pair.second;
  tuple_rids_.pop_back();
  return true;
}

}  // namespace bustub
