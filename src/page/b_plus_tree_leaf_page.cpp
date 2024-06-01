#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetMaxSize(max_size);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetKeySize(key_size);
  this->SetNextPageId(INVALID_PAGE_ID);
  this->SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  if (this->GetSize() == 0) return 0;
  int left = 0;
  int right = this->GetSize() - 1;
  int ans = this->GetSize();
//  LOG(INFO) << "left = " << left << " right = " << right;
  while (left <= right) {
    int mid = (left + right) / 2;
//    LOG(INFO) << "mid = " << mid;
//    LOG(INFO) << "mid_key = " << this->KeyAt(mid) << " mid_value = " << this->ValueAt(mid).GetPageId();
    int check = KM.CompareKeys(key, this->KeyAt(mid));
//    LOG(INFO) << "check = " << check;
    if (check == 0) {
      ans = mid;
      break;
    } else if (check == -1) {
      ans = mid;
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
//  LOG(INFO) << "left = " << left << " right = " << right;
//  LOG(INFO) << "ans = " << ans;
  return ans;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int key_index = this->KeyIndex(key, KM); // pair_[key_index].first >= key
  this->PairCopy(this->PairPtrAt(key_index + 1), this->PairPtrAt(key_index), this->GetSize() - key_index);
  this->SetKeyAt(key_index, key);
  this->SetValueAt(key_index, value);
  this->SetSize(this->GetSize() + 1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int size = this->GetSize();
  recipient->CopyNFrom(this->PairPtrAt(size - size / 2), size / 2);
  this->SetSize(size - size / 2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  void *dest = this->PairPtrAt(this->GetSize());
  this->PairCopy(dest, src, size);
  this->SetSize(this->GetSize() + size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
//  LOG(INFO) << "Lookup";
  int key_index = this->KeyIndex(key, KM);
//  LOG(INFO) << "KeyIndex right";
  if (key_index < this->GetSize() && KM.CompareKeys(key, this->KeyAt(key_index)) == 0) {
//    LOG(INFO) << "KeyAt right";
    value = this->ValueAt(key_index);
//    LOG(INFO) << "ValueAt right";
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int key_index = this->KeyIndex(key, KM);
  if (key_index < this->GetSize() && KM.CompareKeys(key, this->KeyAt(key_index)) == 0) {
    this->PairCopy(this->PairPtrAt(key_index), this->PairPtrAt(key_index + 1), this->GetSize() - key_index - 1);
    this->SetSize(this->GetSize() - 1);
    return this->GetSize();
  }
  return this->GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(this->PairPtrAt(0), this->GetSize());
  recipient->SetNextPageId(this->GetNextPageId());
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  ASSERT(this->GetSize() > 0, "Need at least one pair for MoveFirstToEndOf()");
  GenericKey *first_key = this->KeyAt(0);
  RowId first_value = this->ValueAt(0);
  recipient->CopyLastFrom(first_key, first_value);
  this->PairCopy(PairPtrAt(0), PairPtrAt(1), this->GetSize() - 1);
  this->SetSize(this->GetSize() - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int size = this->GetSize();
  this->SetKeyAt(size, key);
  this->SetValueAt(size, value);
  this->SetSize(size + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  ASSERT(this->GetSize() > 0, "Need at least one pair for MoveFirstToEndOf()");
  int size = this->GetSize();
  GenericKey *last_key = this->KeyAt(size - 1);
  RowId last_value = this->ValueAt(size - 1);
  recipient->CopyFirstFrom(last_key, last_value);
  this->SetSize(this->GetSize() - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int size = this->GetSize();
  this->PairCopy(PairPtrAt(1), PairPtrAt(0), this->GetSize());
  this->SetKeyAt(0, key);
  this->SetValueAt(0, value);
  this->SetSize(size + 1);
}