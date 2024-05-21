#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  // 1.
  auto iter = page_table_.find(page_id);
  // 1.1
  if (iter != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &(pages_[frame_id]);
    replacer_->Pin(frame_id);
    page->pin_count_ ++;
    latch_.unlock();
    return page;
  }
  // 1.2
  frame_id_t frame_id = -1;
  bool find_victim = false;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    find_victim = true;
  } else {
    find_victim = replacer_->Victim(&frame_id);
  }
  // 1.2 :not found
  if (find_victim == false) {
    latch_.unlock();
    return nullptr;
  }
  Page *page = &(pages_[frame_id]);
  // 2.
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  // 3.
  page_table_.erase(page->page_id_);
  if (page_id != INVALID_PAGE_ID) {
    page_table_.insert(make_pair(page_id, frame_id));
  }
  page->ResetMemory();
  page->page_id_ = page_id;
  // 4.
  disk_manager_->ReadPage(page->page_id_, page->data_);
  replacer_->Pin(frame_id);
  page->pin_count_ = 1;
  latch_.unlock();
  return page;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  frame_id_t frame_id = -1;
  bool find_victim = false;
  // 2.
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    find_victim = true;
  } else {
    find_victim = replacer_->Victim(&frame_id);
  }
  // 1.
  if (find_victim == false) {
    latch_.unlock();
    return nullptr;
  }
  // 3,4.
  page_id = disk_manager_->AllocatePage();
  Page *page = &(pages_[frame_id]);
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  page_table_.erase(page->page_id_);
  if (page_id != INVALID_PAGE_ID) {
    page_table_.insert(make_pair(page_id, frame_id));
  }
  page->ResetMemory();
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page->page_id_, page->data_);
  replacer_->Pin(frame_id);
  page->pin_count_ = 1;
  latch_.unlock();
  return page;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  auto iter = page_table_.find(page_id);
  // 1.
  if (iter == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  // 2.
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &(pages_[frame_id]);
  ASSERT(page_id == page->page_id_, "the unordered_map may get ruined!");
  if (page->GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }
  // 3.
  disk_manager_->DeAllocatePage(page->page_id_);
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  page_table_.erase(page->page_id_);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);
  latch_.unlock();
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &(pages_[frame_id]);
  ASSERT(page_id == page->page_id_, "the unordered_map may get ruined!");
  if (page->GetPinCount() == 0) {
    latch_.unlock();
    return false;
  }
  page->pin_count_ --;
  if (page->GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  latch_.unlock();
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  latch_.lock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &(pages_[frame_id]);
  ASSERT(page_id == page->page_id_, "the unordered_map may get ruined!");
  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;
  latch_.unlock();
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}