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
      delete_rst_schema_(std::vector{Column("rows", TypeId::BIGINT)}) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    if (deleted_rows == 0) {
      *tuple = {std::vector{Value(TypeId::BIGINT, deleted_rows)}, &delete_rst_schema_};
    }
    return false;
  }

  Tuple delete_tuple{};
  const auto &indices = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  while (true) {
//    LOG_INFO("before find tuple");
    if (!child_executor_->Next(&delete_tuple, rid)) {
      break;
    }
//    LOG_INFO("after find tuple");
    if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      break;
    }
//    LOG_INFO("after mark delete");

    for (IndexInfo *index_info : indices) {
      Tuple key_tuple = delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                  index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    deleted_rows++;
//    LOG_INFO("after incr row");
  }
  if (table_info_ != nullptr) {
    *tuple = {std::vector{Value(TypeId::BIGINT, deleted_rows)}, &delete_rst_schema_};
    table_info_ = nullptr;
    return true;
  }
  return false;
}

}  // namespace bustub
