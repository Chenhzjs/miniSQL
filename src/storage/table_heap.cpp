#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  page_id_t page_id = GetFirstPageId();
  page_id_t pre_page_id = INVALID_PAGE_ID;
  // if page can be inserted currently
  while (page_id != INVALID_PAGE_ID) {
//    LOG(INFO) << "maybe here segmetation fault 1";
    auto table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
//    LOG(INFO) << "maybe here segmetation fault0";
    if(table_page == nullptr) LOG(ERROR) << "table_page is nullptr!";
    if (table_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      // insert data, which makes this page dirty
      buffer_pool_manager_->UnpinPage(page_id, true);
      return true;
    }
//    LOG(INFO) << "maybe here segmetation fault";
    buffer_pool_manager_->UnpinPage(page_id, false);
    pre_page_id = page_id;
    page_id = table_page->GetNextPageId();
  }
//  LOG(INFO) << "maybe here segmetation fault 2";
  // no page can be inserted currently
  page_id_t new_page_id;
  auto new_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id)); // here !!!!!! page_id
  new_table_page->Init(new_page_id, pre_page_id, log_manager_, txn);
  bool insert_check = new_table_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  auto pre_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pre_page_id));
  pre_table_page->SetNextPageId(new_page_id);
  buffer_pool_manager_->UnpinPage(pre_page_id, true);
  if (!insert_check) return false;
//  LOG(INFO) << "maybe here segmetation fault 3";
  return true;
}


bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, RowId &rid, Txn *txn) {
  page_id_t update_page_id = rid.GetPageId();
  auto update_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(update_page_id));
  if (update_page == nullptr) {
    return false;
  }
  Row *old_row =  new Row(rid);
  bool update_check = update_page->UpdateTuple(row, old_row, schema_, txn, lock_manager_, log_manager_);
  if (update_check) {
    buffer_pool_manager_->UnpinPage(update_page_id, true);
    delete old_row;
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(update_page_id, false);
    delete old_row;
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  page_id_t delete_page_id = rid.GetPageId();
  auto delete_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(delete_page_id));
  if (delete_page == nullptr) {
    buffer_pool_manager_->UnpinPage(delete_page_id, false);
    return;
  }
  delete_page->ApplyDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(delete_page_id, true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  RowId rid = row->GetRowId();
  page_id_t page_id = rid.GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
//  cout << row->GetRowId().Get() << " " << row->GetFieldCount() << endl;
  bool check = page->GetTuple(row, schema_, txn, lock_manager_);
  if (check) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return false;
  }
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t page_id = first_page_id_;
  // get first rid
  if (page_id == INVALID_PAGE_ID) {
    return End();
  }
  RowId first_rid;
  while (page_id != INVALID_PAGE_ID) {
    auto table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (table_page->GetFirstTupleRid(&first_rid)) {
      buffer_pool_manager_->UnpinPage(page_id, false);
      break;
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = table_page->GetNextPageId();
  }
  // get
  if (page_id != INVALID_PAGE_ID) {
    Row *row = new Row(first_rid);
    auto table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    table_page->GetTuple(row, schema_, txn, lock_manager_);
    return TableIterator(this, row, txn);
  }
  // not get
  return End();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(nullptr, INVALID_ROWID, nullptr);
}
