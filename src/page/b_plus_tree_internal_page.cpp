#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size){
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetKeySize(key_size);
  this->SetMaxSize(max_size);
  this->SetSize(0);
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
//  if (this->GetSize() == 0 || this->GetSize() == 1) return 0;
  int left = 1; // 0 is INVALID
  int right = this->GetSize() - 1;
  int ans = 0;
//  LOG(INFO) << "left = " << left << " right = " << right;
  while (left <= right) {
    int mid = (left + right) / 2;
//    LOG(INFO) << "mid = " << mid;
    int check = KM.CompareKeys(key, KeyAt(mid));
//    LOG(INFO) << "check = " << check;
    if (check == 0) {
      ans = mid;
      break;
    } else if (check == -1) {
      right = mid - 1;
    } else {
      ans = mid;
      left = mid + 1;
    }
  }
//  LOG(INFO) << "left = " << left << " right = " << right;
//  LOG(INFO) << "ans = " << ans;
  return this->ValueAt(ans);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  this->SetSize(2);
  this->SetValueAt(0, old_value);
  this->SetKeyAt(1, new_key);
  this->SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // something like insertion sort
  int pre_size = this->GetSize();
  int old_value_pos = ValueIndex(old_value);
  // actually, here can use PairCopy, but i forgot.
  for (int i = pre_size; i > old_value_pos + 1; i --) {
    this->SetKeyAt(i, KeyAt(i - 1));
    this->SetValueAt(i, ValueAt(i - 1));
  }
  this->SetKeyAt(old_value_pos + 1, new_key);
  this->SetValueAt(old_value_pos + 1, new_value);
  this->SetSize(pre_size + 1);
  return pre_size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = this->GetSize();
  recipient->CopyNFrom(this->PairPtrAt(size - size / 2), size / 2, buffer_pool_manager);
  this->SetSize(size - size / 2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  void *dest = this->PairPtrAt(this->GetSize());
  this->PairCopy(dest, src, size);
  for (int i = 0; i < size; i ++) {
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(ValueAt(i + GetSize()))->GetData());
    child_page->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(ValueAt(i + GetSize()), true);
  }
  this->SetSize(this->GetSize() + size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  int size = this->GetSize();
  if (index >= 0 && index < size) {
    void *dest = this->PairPtrAt(index);
    void *src = this->PairPtrAt(index + 1);
    this->PairCopy(dest, src, size - index - 1);
    this->SetSize(size - 1);
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  ASSERT(this->GetSize() == 1, "To use RemoveAndReturnOnlyChild, the size must be 1");
  page_id_t value = ValueAt(0);
  SetSize(0);
  return value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
//  this->SetKeyAt(0, middle_key);
  recipient->CopyNFrom(this->PairPtrAt(0), this->GetSize(), buffer_pool_manager);
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
//  this->SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0), buffer_pool_manager);
  this->Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int size = this->GetSize();
  this->SetKeyAt(size, key);
  this->SetValueAt(size, value);
  auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
  child_page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
  this->SetSize(size + 1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom(ValueAt(this->GetSize() - 1), buffer_pool_manager);
  recipient->SetKeyAt(0, KeyAt(this->GetSize() - 1));
  this->SetSize(this->GetSize() - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  PairCopy(PairPtrAt(1), PairPtrAt(0), GetSize());
  SetValueAt(0, value);
  auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
  child_page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
  this->SetSize(this->GetSize() + 1);
}