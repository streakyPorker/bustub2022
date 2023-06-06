//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      delete_rst_schema_(std::vector{Column("rows", TypeId::INTEGER)}) {}

void DeleteExecutor::Init() { table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple delete_tuple;
  if (!child_executor_->Next(&delete_tuple, rid)) {
    return false;
  }
  if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
    return false;
  }
  const auto indices = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  for (IndexInfo *index_info : indices) {
    index_info->index_->DeleteEntry(delete_tuple, *rid, exec_ctx_->GetTransaction());
  }
  const Value delete_rst(TypeId::INTEGER, 1);
  *tuple = {std::vector{delete_rst}, &delete_rst_schema_};
  return false;
}

}  // namespace bustub
