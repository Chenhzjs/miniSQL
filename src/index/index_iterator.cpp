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
  if (item_index < page->GetMaxSize()) return *this;
  // go next page
  if (page->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = page->GetNextPageId();
    auto next_page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(next_page_id)->GetData());
    current_page_id = next_page_id;
    page = next_page;
    item_index = 0;
    buffer_pool_manager->UnpinPage(next_page_id, false);
    return *this;
  } else { // no next page, End()
    *this = IndexIterator();
    return *this;
  }
}


bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}