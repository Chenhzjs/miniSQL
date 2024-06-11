//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  Row row;
  RowId rid;
  int tot = 0;
  while (child_executor_->Next(&row, &rid)) tot ++;
//  cout << "tot" << tot << endl;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), index_info_);
  txn_ = exec_ctx_->GetTransaction();
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  static int tot = 0;
  if (child_executor_->Next(row, rid)) {
//    cout << rid->Get() << endl;
//    cout << ++tot << endl;
    if (!table_info_->GetTableHeap()->MarkDelete(*rid, txn_)) {
//      cout << "1" << endl;
      return false;
    }
    Row key_row;
    for (auto info : index_info_) {  // 更新索引
//      cout << "2" << endl;
      row->GetKeyFromRow(table_info_->GetSchema(), info->GetIndexKeySchema(), key_row);
//      cout << "3" << endl;
      info->GetIndex()->RemoveEntry(key_row, *rid, txn_);
//      cout << "4" << endl;
    }
//    cout << "5" << endl;
    return true;
  }
//  cout << "out" << endl;
  return false;
}