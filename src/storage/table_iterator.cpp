#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, Row *row, Txn *txn) {
  row_ = row;
  table_heap_ = table_heap;
  txn_ = txn;
}
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  row_ = new Row(rid);
  table_heap_ = table_heap;
  txn_ = txn;
}
TableIterator::TableIterator(const TableIterator &other) {
  row_ = new Row(*(other.row_));
  table_heap_ = other.table_heap_;
  txn_ = other.txn_;
}

TableIterator::~TableIterator() {
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return row_->GetRowId() == itr.row_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  ASSERT(row_ != nullptr, "End() iter have no rows.");
  return *row_;
}

Row *TableIterator::operator->() {
  ASSERT(row_ != nullptr, "End() iter have no rows.");
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (row_ == nullptr) {
    row_ = new Row(*(itr.row_));
  } else {
    *row_ = *(itr.row_);
  }
  table_heap_ = itr.table_heap_;
  txn_ = itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  ASSERT(row_ != nullptr, "It is End() iter now.");
  auto table_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_->GetRowId().GetPageId()));
  if (table_page == nullptr) {
    row_->SetRowId(RowId(INVALID_PAGE_ID, 0));
    LOG_ASSERT("The FetchPage may get ruined.");
  } else {
   RowId next_rid;
   // check on this page
   if (table_page->GetNextTupleRid(row_->GetRowId(), &next_rid)) {
//     cout << row_->GetRowId().Get() << " " << row_->GetFieldCount() << endl;
     row_->SetRowId(next_rid);
//     cout << row_->GetRowId().Get() << " " << row_->GetFieldCount() << endl;
     table_heap_->GetTuple(row_, txn_);
     table_heap_->buffer_pool_manager_->UnpinPage(table_page->GetTablePageId(), false);
     return *this;
   }
   // check on other page
   while (table_page->GetNextPageId() != INVALID_PAGE_ID) {
     table_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(table_page->GetNextPageId()));
     if (table_page->GetFirstTupleRid(&next_rid)) {
       row_->SetRowId(next_rid);
       table_heap_->GetTuple(row_, txn_);
       table_heap_->buffer_pool_manager_->UnpinPage(table_page->GetTablePageId(), false);
       return *this;
     }
   }
   // no rows;
   row_->SetRowId(RowId(INVALID_PAGE_ID, 0));
   return *this;
  }
}

// iter++
TableIterator TableIterator::operator++(int) {
  Row *row = new Row(*(row_));
  TableHeap *table_heap = table_heap_;
  Txn *txn = txn_;
  ++(*this);
  return TableIterator(table_heap, row, txn);
}
