//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ENSURE(table_info_->oid_ == plan_->table_oid_, "change table oid dat seq_scan executor");
  if (table_iter_ == table_info_->table_->End()) {
    table_info_ = nullptr;
    return false;
  }

  if (tuple != nullptr) {
    *tuple = *table_iter_;
  }
  if (rid != nullptr) {
    *rid = table_iter_->GetRid();
  }
  ++table_iter_;
  return true;
}

}  // namespace bustub
