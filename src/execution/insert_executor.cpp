//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      insert_rst_schema_(std::vector{Column("rows", TypeId::INTEGER)}) {}

void InsertExecutor::Init() {
  std::cout << "init insert executor" << std::endl;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ENSURE(table_info_->oid_ == plan_->table_oid_, "change table oid during insert executor");
  Tuple child_tuple{};
  const auto status = child_executor_->Next(&child_tuple, rid);
  if (!status) {
    return false;
  }
  if (!table_info_->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction())) {
    return false;
  }

  const auto indices = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (IndexInfo *index_info : indices) {
    index_info->index_->InsertEntry(child_tuple, *rid, exec_ctx_->GetTransaction());
  }
  const Value insert_rst(TypeId::INTEGER, 1);
  *tuple = {std::vector{insert_rst}, &insert_rst_schema_};
  return true;
}

}  // namespace bustub
