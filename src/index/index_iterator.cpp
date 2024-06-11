#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
}
// ++Iterator
IndexIterator &IndexIterator::operator++() {
  item_index ++;
  if (item_index < page->GetSize()) return *this;
  // go next page
  page_id_t old_page_id = current_page_id;
  page_id_t next_page_id = page->GetNextPageId();
  if (next_page_id != INVALID_PAGE_ID) {
    auto next_page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(next_page_id)->GetData());
    page = next_page;
    item_index = 0;
    current_page_id = next_page_id;
  } else { // no next page, End()
    current_page_id = INVALID_PAGE_ID;
    page = nullptr;
    item_index = 0;
  }
  buffer_pool_manager->UnpinPage(old_page_id, true);
  return *this;
}


bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}