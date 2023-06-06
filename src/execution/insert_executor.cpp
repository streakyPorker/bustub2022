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
      insert_rst_schema_(std::vector{Column("rows", TypeId::BIGINT)}) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    if (inserted_rows_ == 0) {
      *tuple = {std::vector{Value(TypeId::BIGINT, 0)}, &insert_rst_schema_};
    }
    return false;
  }

  BUSTUB_ENSURE(table_info_->oid_ == plan_->table_oid_, "change table oid during insert executor");

  Tuple child_tuple{};
  const auto &indices = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  while (true) {
    /*
create table t1(v1 int, v2 varchar(128), v3 int);
insert into t1 values (0, '🥰', 10), (1, '🥰🥰', 11), (2, '🥰🥰🥰', 12), (3,
'🥰🥰🥰🥰', 13), (4, '🥰🥰🥰🥰🥰', 14); create table t2(v1 int, v2 varchar(128),
v3 int); insert into t2 select * from t1;
     */

    if (!child_executor_->Next(&child_tuple, rid)) {
      break;
    }
    if (!table_info_->table_->InsertTuple(child_tuple, rid, exec_ctx_->GetTransaction())) {
      break;
    }
    for (IndexInfo *index_info : indices) {
      Tuple key_tuple = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                 index_info->index_->GetKeyAttrs());

      index_info->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
    inserted_rows_++;
  }

  if (table_info_ != nullptr) {
    *tuple = {std::vector{Value(TypeId::BIGINT, inserted_rows_)}, &insert_rst_schema_};
    table_info_ = nullptr;
    return true;
  }
  return false;
}

}  // namespace bustub
