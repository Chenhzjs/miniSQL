#include "index/b_plus_tree.h"

#include <string>
#include <config.h>
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {


  auto index_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (!index_page->GetRootId(index_id_, &root_page_id_)) {root_page_id_ = INVALID_PAGE_ID;};
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
//  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}
// Destroy the B+ tree recursively
void BPlusTree::Destroy(page_id_t current_page_id) {
//  LOG(INFO) << "Destroy page! " << current_page_id;
  if(this->IsEmpty()) return;
  // if current_page_id is invalid, then it is the root page
  if (current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
  }
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
  // if it is leaf page, then delete it
  if (page->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(current_page_id, false);
    buffer_pool_manager_->DeletePage(current_page_id);
    return ;
  }
  auto internal_page = reinterpret_cast<InternalPage *>(page);
  for (int i = 0; i < internal_page->GetSize(); i ++) {
    Destroy(internal_page->ValueAt(i));
  }

}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if (root_page_id_ == INVALID_PAGE_ID) return true;
  else return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if (this->IsEmpty()) return false;
  Page *page = this->FindLeafPage(key);
  if (page == nullptr) return false;
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  RowId get_value;
  if (leaf_page->Lookup(key, get_value, processor_)) {
    result.push_back(get_value);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (this->IsEmpty()) {
    // LOG(INFO) << "new";
    StartNewTree(key, value);
    return true;
  } else {
    // LOG(INFO) << "insert";
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t new_page_id;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    // Can i use throw ? where is the try-catch.
    LOG(INFO) << "out of memory" << endl;
    return ;
  }
  root_page_id_ = new_page_id;
  auto leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  int internal_max_size = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;;
  int leaf_max_size = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;  // LOG(INFO) << leaf_max_size_;
  internal_max_size_ = max(internal_max_size, leaf_max_size);
  leaf_max_size_ = max(internal_max_size, leaf_max_size);
  // LOG(INFO) << "leaf_max_size_ = " << leaf_max_size_;
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  leaf_page->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  // LOG(INFO) << "0";
  Page *page = this->FindLeafPage(key);
  // LOG(INFO) << "FindLeafPage(key) success";
  if (page == nullptr) return false;
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // LOG(INFO) << "size = " << leaf_page->GetSize() << " id = " << leaf_page->GetPageId();
  RowId not_used_value;
  // if exist
  if (leaf_page->Lookup(key, not_used_value, processor_)) {
    // LOG(INFO) << "if true";
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  } else { // not exist
    // LOG(INFO) << "if false";
    leaf_page->Insert(key, value, processor_);
    // if needing to split
    // LOG(INFO) << "insert success";
    // LOG(INFO) << "leaf_page->GetSize() = " << leaf_page->GetSize() << " leaf_page->GetMaxSize() = " << leaf_page->GetMaxSize();
    if (leaf_page->GetSize() >= leaf_page->GetMaxSize()) {
      auto split_leaf_page = this->Split(leaf_page, transaction);
      split_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
      leaf_page->SetNextPageId(split_leaf_page->GetPageId());
      this->InsertIntoParent(leaf_page, split_leaf_page->KeyAt(0), split_leaf_page, transaction);
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(split_leaf_page->GetPageId(), true);
//      LOG(INFO) << "split_size = " << split_leaf_page->GetSize() << " size = " << leaf_page->GetSize();
      return true;
    } else {
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      return true;
    }
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
// pairs-->> old_half_pairs | new_half_pairs
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    LOG(INFO) << "out of memory" << endl;
    return nullptr;
  }
  auto new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_internal_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
//  LOG(INFO) << "new:" << new_internal_page->GetKeySize() << " node : " << node->GetKeySize();
  node->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  return new_internal_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    LOG(INFO) << "out of memory" << endl;
    return nullptr;
  }
  auto new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
//  LOG(INFO) << "new:" << new_leaf_page->GetKeySize() << " node : " << node->GetKeySize();
  node->MoveHalfTo(new_leaf_page);
//  LOG(INFO) << " new_page_id = " << new_page_id << " " << new_leaf_page->GetPageId();
  return new_leaf_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  // if no internal_node, that is old_node is root page
  if (old_node->IsRootPage()) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_page == nullptr) {
      LOG(INFO) << "out of memory" << endl;
      return ;
    }
    auto new_internal_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_internal_page->Init(new_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_internal_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    root_page_id_ = new_page_id;
    UpdateRootPageId(0);
    return ;
  } else { // has internal_node, then insert and check if it needs to be split.
    page_id_t internal_page_id = old_node->GetParentPageId();
    auto internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internal_page_id)->GetData());
    internal_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    // split
    if (internal_page->GetSize() >= internal_page->GetMaxSize()) {
      auto split_internal_page = this->Split(internal_page, transaction);
      this->InsertIntoParent(internal_page, split_internal_page->KeyAt(0), split_internal_page, transaction);
      buffer_pool_manager_->UnpinPage(split_internal_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), true);
    return ;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (this->IsEmpty()) return ;
//  cout << "1" << endl;
  Page* page = this->FindLeafPage(key);
//  cout << "2" << endl;
  if (page == nullptr) return ;
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // need to compare the size to check if the deletion is successful
  int size = leaf_page->GetSize();
  // if it is not successful
  if (leaf_page->RemoveAndDeleteRecord(key, processor_) == size) {
//    cout << "3" << endl;
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  } else {
//    cout << "4" << endl;
    bool is_dirty = CoalesceOrRedistribute(leaf_page, transaction);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), is_dirty);
  }
  return ;
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if (node->IsRootPage()) {
    return this->AdjustRoot(node);
  }
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  page_id_t parent_id = node->GetParentPageId();
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
  int index = parent_page->ValueIndex(node->GetPageId());
  int neighbor_index = (index == 0) ? 1 : index - 1;
  auto page = buffer_pool_manager_->FetchPage(parent_page->ValueAt(neighbor_index));
  // redistribute
  if(node->IsLeafPage() == true) {
    auto neighbor_page = reinterpret_cast<LeafPage *>(page->GetData());
    auto leaf_node = reinterpret_cast<LeafPage *>(node);
    if (neighbor_page->GetSize() + leaf_node->GetSize() > node->GetMaxSize() - 1) {
      Redistribute(neighbor_page, leaf_node, index);
      buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return true;
    } else {  // merge
      Coalesce(neighbor_page, leaf_node, parent_page, index, transaction);
      buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return true;
    }
  } else {
    auto neighbor_page = reinterpret_cast<InternalPage *>(page->GetData());
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    if (neighbor_page->GetSize() + internal_node->GetSize() > node->GetMaxSize()) {
      Redistribute(neighbor_page, internal_node, index);
      buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return true;
    } else {  // merge
      Coalesce(neighbor_page, internal_node, parent_page, index, transaction);
      buffer_pool_manager_->UnpinPage(neighbor_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return true;
    }
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  int neighbor_index = (index == 0) ? 1 : index - 1;
  if (neighbor_index < index) {
    node->MoveAllTo(neighbor_node);
    neighbor_node->SetNextPageId(node->GetNextPageId());
    parent->Remove(index);
  } else {
    neighbor_node->MoveAllTo(node);
    node->SetNextPageId(neighbor_node->GetNextPageId());
    parent->Remove(neighbor_index);
  }
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  int neighbor_index = (index == 0) ? 1 : index - 1;
  if (neighbor_index < index) {
    node->MoveAllTo(neighbor_node, parent->KeyAt(index - 1), buffer_pool_manager_);
    parent->Remove(index);
  } else {
    neighbor_node->MoveAllTo(node, parent->KeyAt(0), buffer_pool_manager_);
    parent->Remove(neighbor_index);
  }
  return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  // index == 0 means node before neightbor_node while index != 0 means neighbor_node before node
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node);
    parent_page->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  // index == 0 means node before neightbor_node while index != 0 means neighbor_node before node
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, parent_page->KeyAt(0), buffer_pool_manager_);
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node, parent_page->KeyAt(index), buffer_pool_manager_);
    parent_page->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto internel_page = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = internel_page->RemoveAndReturnOnlyChild();
    this->UpdateRootPageId(0);
    auto leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    leaf_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->UnpinPage(internel_page->GetPageId(), true);
    return true;
  } else if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    this->UpdateRootPageId(0);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  auto leaf_page = reinterpret_cast<LeafPage *>(this->FindLeafPage(nullptr, INVALID_PAGE_ID, true)->GetData());
  page_id_t leaf_page_id = leaf_page->GetPageId();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return IndexIterator(leaf_page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  auto leaf_page = reinterpret_cast<LeafPage *>(this->FindLeafPage(key)->GetData());
  page_id_t leaf_page_id = leaf_page->GetPageId();
  int leaf_index = leaf_page->KeyIndex(key, processor_);
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return IndexIterator(leaf_page_id, buffer_pool_manager_, leaf_index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (root_page_id_ == INVALID_PAGE_ID) return nullptr;
  if (page_id == INVALID_PAGE_ID) page_id = root_page_id_;
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  while (!page->IsLeafPage()) {
//    cout << "10" << endl;
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    BPlusTreePage *next_page;
    page_id_t next_page_id;
//    cout << "1" << endl;
    if (key != nullptr) next_page_id = internal_page->Lookup(key, processor_);
//    LOG(INFO) << "lookup success";
//    cout << "2" << endl;
    page_id_t left_most_id = internal_page->ValueAt(0);
    if (leftMost) {
      next_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(left_most_id)->GetData());
    } else {
      next_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
    }
//    cout << "3" << endl;
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    if (next_page == nullptr) break;
    page = next_page;
  }
  // the page now is pinned, so you must unpin after calling it.
  return reinterpret_cast<Page *>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
// delete?
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto index_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  // false(0): update, true(1): insert
  if (insert_record == 0) {
    index_page->Update(index_id_, root_page_id_);
  } else if (insert_record == 1) {
    index_page->Insert(index_id_, root_page_id_);
  } else {
    index_page->Delete(index_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}