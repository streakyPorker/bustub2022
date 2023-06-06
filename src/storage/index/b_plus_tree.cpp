#include <string>

#include <utility>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

#define chrono_diff(start) \
  (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - start).count())

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BUSTUB_ASSERT(page != nullptr, "bpt op failed:wrong root page");
  //  page->RLatch();
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  BUSTUB_ASSERT(node->GetPageType() != IndexPageType::INVALID_INDEX_PAGE, "bpt op failed:wrong root page");
  bool rst = !node->IsRootPage() || node->GetSize() == 0;
  //  page->RUnlatch();
  return rst;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // if not invalid,then must not empty
  rw_latch_.lock_shared();
  if (IsEmpty()) {
    rw_latch_.unlock_shared();
    return false;
  }

  std::deque<std::pair<LockType, Page *>> locked_pages;
  auto node = ParsePageToGeneralNode(root_page_id_, locked_pages, LockType::READ, transaction);
  BUSTUB_ASSERT(node != nullptr && node->IsRootPage(), "invalid root page id");

  LeafPage *leaf = SeekToLeaf(node, key, locked_pages, LockType::READ, SafeType::READ, transaction);
  if (leaf == nullptr) {
    ClearLockDeque(locked_pages, transaction, false, 0);
    return false;
  }
  bool found;
  int rst = leaf->IndexOfKey(comparator_, key, &found);
  if (!found) {
    // not found in the leaf node
    ClearLockDeque(locked_pages, transaction, false, 0);
    return false;
  }
  result->push_back(leaf->ValueAt(rst));
  ClearLockDeque(locked_pages, transaction, false, 0);
  return true;
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
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  std::scoped_lock<std::mutex> scoped_lock(wlatch_);
  rw_latch_.lock();
  // root_page_id_ will never be invalid once created
  if (root_page_id_ == INVALID_PAGE_ID) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "bpt creation failed : can`t allocate page");
    auto root_page = reinterpret_cast<LeafPage *>(page->GetData());
    root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }
  std::deque<std::pair<LockType, Page *>> locked_pages;
  // optimistic mode first
  auto node = ParsePageToGeneralNode(root_page_id_, locked_pages, LockType::WRITE, transaction);
  LeafPage *leaf = SeekToLeaf(node, key, locked_pages, LockType::WRITE, SafeType::INSERT, transaction);
  BUSTUB_ASSERT(leaf != nullptr, "bpt op failed:internal search error");
  bool rst = InsertIntoLeafNode(leaf, key, value, locked_pages, transaction);
  return rst;
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
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  std::scoped_lock<std::mutex> scoped_lock(wlatch_);
  rw_latch_.lock();
  if (IsEmpty()) {
    rw_latch_.unlock();
    return;
  }
  std::deque<std::pair<LockType, Page *>> locked_pages;
  auto node = ParsePageToGeneralNode(root_page_id_, locked_pages, LockType::WRITE, transaction);
  LeafPage *leaf = SeekToLeaf(node, key, locked_pages, LockType::WRITE, SafeType::DELETE, transaction);
  BUSTUB_ASSERT(leaf != nullptr, "bpt op failed:internal search error");
  DeleteFromLeafNode(leaf, key, locked_pages, transaction);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  rw_latch_.lock_shared();
  if (IsEmpty()) {
    rw_latch_.unlock_shared();
    return INDEXITERATOR_TYPE();
  }
  std::deque<std::pair<LockType, Page *>> locked_pages;
  auto root = ParsePageToGeneralNode(root_page_id_, locked_pages, LockType::READ, nullptr);
  LeafPage *leaf = SeekToStart(root, locked_pages);
  ClearLockDeque(locked_pages, nullptr, false, 0);
  return INDEXITERATOR_TYPE(leaf->GetPageId(), buffer_pool_manager_, 0);  // holding the rlock of the leaf
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  rw_latch_.lock_shared();
  if (IsEmpty()) {
    rw_latch_.unlock_shared();
    return INDEXITERATOR_TYPE();
  }
  std::deque<std::pair<LockType, Page *>> locked_pages;
  auto root = ParsePageToGeneralNode(root_page_id_, locked_pages, LockType::READ, nullptr);
  LeafPage *leaf = SeekToLeaf(root, key, locked_pages, LockType::READ, SafeType::READ, nullptr);
  bool found;
  int pos = leaf->IndexOfKey(comparator_, key, &found);
  ClearLockDeque(locked_pages, nullptr, false, 0);
  if (!found && pos == leaf->GetSize()) {
    return INDEXITERATOR_TYPE();
  }
  return INDEXITERATOR_TYPE(leaf->GetPageId(), buffer_pool_manager_, found ? pos : pos + 1);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/**
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  std::scoped_lock<std::shared_mutex> scopedLock(rw_latch_);
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
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
      ToGraph(child_page, bpm, out);
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
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
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
    if (internal->GetPageType() == IndexPageType::INVALID_INDEX_PAGE) {
      LOG_ERROR("hgere12333333333333333");
    }

    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ParsePageToGeneralNode(page_id_t page_id, std::deque<std::pair<LockType, Page *>> &deque,
                                            LockType type, Transaction *txn) -> BPlusTreePage * {
  Page *page = nullptr;
  // if the page is already locked, check the lock type and parse it directly
  for (auto iter = deque.begin(); iter != deque.end(); iter++) {
    if (iter->second->GetPageId() == page_id) {
      assert(type == iter->first);
      page = iter->second;
      break;
    }
  }

  if (page == nullptr) {
    page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr || page->GetData() == nullptr) {
      return nullptr;
    }
    if (type == LockType::READ) {
      page->RLatch();
    } else {
      page->WLatch();
      // only writes can be added into the transactions set
      if (txn != nullptr) {
        txn->AddIntoPageSet(page);
      }
    }

    deque.emplace_back(type, page);
  }

  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SeekToLeaf(BPlusTreePage *node, const KeyType &key, std::deque<std::pair<LockType, Page *>> &deque,
                                LockType lock_type, SafeType safe_type, Transaction *txn) -> BPlusTree::LeafPage * {
  if (node->IsLeafPage()) {
    return static_cast<LeafPage *>(ProcessNodeByStrategy(node, deque, lock_type, safe_type, txn));
  }
  LeafPage *leaf = nullptr;
  auto *internal = reinterpret_cast<InternalPage *>(node);

  while (leaf == nullptr) {
    int pos = internal->IndexOfKey(comparator_, key);
    node = ParsePageToGeneralNode(internal->ValueAt(pos), deque, lock_type, txn);
    BUSTUB_ASSERT(node != nullptr, "bpt op failed:wrong pointer");
    assert(ProcessNodeByStrategy(node, deque, lock_type, safe_type, txn) != nullptr);
    if (node->IsLeafPage()) {
      leaf = reinterpret_cast<LeafPage *>(node);
    } else {
      internal = reinterpret_cast<InternalPage *>(node);
    }
  }
  return leaf;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternalNode(InternalPage *internal, const KeyType &key, const page_id_t &old_node,
                                            const page_id_t &new_node, std::deque<std::pair<LockType, Page *>> &deque,
                                            Transaction *txn) {
  if (internal->GetSize() == 0) {
    internal->SetValueAt(0, old_node);
    internal->SetKVAt(1, key, new_node);
    internal->SetSize(2);
    ClearLockDeque(deque, txn, true, 0);  // the nodes are modified after all
    return;
  }

  int insert_pos = internal->IndexOfKey(comparator_, key);

  if (internal->GetSize() < internal_max_size_) {
    for (int ri = internal->GetSize() - 1; ri > insert_pos; ri--) {
      internal->SetKVAt(ri + 1, internal->KeyAt(ri), internal->ValueAt(ri));
    }
    // the first entry will never change on insert, so insert_pos can`t be -1
    internal->SetKVAt(insert_pos + 1, key, new_node);
    //    if (internal->ValueAt(insert_pos) != old_node) {
    //      //      Draw(buffer_pool_manager_, "11111111111111.dot");
    //      Print(buffer_pool_manager_);
    //      LOG_ERROR("insert pos:%d, supposed:%d,actual:%d", insert_pos, old_node, internal->ValueAt(insert_pos));
    //    }
    BUSTUB_ASSERT(internal->ValueAt(insert_pos) == old_node, "bpt op failed:wrong internal structure");
    internal->IncreaseSize(1);
    ClearLockDeque(deque, txn, true, 0);
    return;
  }

  page_id_t new_internal_page_id;
  buffer_pool_manager_->NewPage(&new_internal_page_id);
  BUSTUB_ASSERT(new_internal_page_id != INVALID_PAGE_ID, "bpt op failed : can`t allocate page");
  buffer_pool_manager_->UnpinPage(new_internal_page_id, false);

  // this is necessary, since we have to
  // 1. pin this page
  // 2. wlock it
  // 3. push it to the queue before the root
  auto new_internal =
      static_cast<InternalPage *>(ParsePageToGeneralNode(new_internal_page_id, deque, LockType::WRITE, txn));
  new_internal->Init(new_internal_page_id, internal->GetParentPageId(), internal_max_size_);

  int lift_pos = (internal_max_size_ + 1) / 2;
  // transfer the right half to new_internal
  if (insert_pos < lift_pos - 1) {  // the inserted entry stays at the left half
    for (int i = lift_pos - 1, j = 0; i < internal->GetSize(); i++, j++) {
      new_internal->SetKVAt(j, internal->KeyAt(i), internal->ValueAt(i));
    }
    // rearrange the internal
    for (int ri = lift_pos - 2; ri > insert_pos; ri--) {
      internal->SetKVAt(ri + 1, internal->KeyAt(ri), internal->ValueAt(ri));
    }
    // do insertion
    internal->SetKVAt(insert_pos + 1, key, new_node);

  } else {  // the inserted entry stays at the right half
    int j = 0;
    for (int i = lift_pos; i < insert_pos + 1; i++) {  // entries < insert key
      new_internal->SetKVAt(j++, internal->KeyAt(i), internal->ValueAt(i));
    }
    new_internal->SetKVAt(j++, key, new_node);                    // insert key
    for (int i = insert_pos + 1; i < internal->GetSize(); i++) {  // entries > insert key
      new_internal->SetKVAt(j++, internal->KeyAt(i), internal->ValueAt(i));
    }
  }
  KeyType lift_key = new_internal->KeyAt(1);
  internal->SetSize(lift_pos);
  new_internal->SetSize(new_internal->GetMinSize() + 1);

  // need to alter every child`s parent for the new internal
  for (int i = 0; i < new_internal->GetSize(); i++) {
    auto node = ParsePageToGeneralNode(new_internal->ValueAt(i), deque, LockType::WRITE, txn);
    node->SetParentPageId(new_internal_page_id);
  }

  InternalPage *parent;
  if (internal->IsRootPage()) {
    // no need to double-check,
    // since we`ve already wlocked the current node,which is the only cause of spawning a new root
    Page *new_root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "bpt op failed : can`t allocate page");
    new_root_page->WLatch();

    txn->AddIntoPageSet(new_root_page);
    // push it to the front instead of the back to maintain the same order of the existing parent
    deque.emplace_front(LockType::WRITE, new_root_page);
    parent = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    parent->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    internal->SetParentPageId(root_page_id_);
    UpdateRootPageId();
  } else {
    parent =
        static_cast<InternalPage *>(ParsePageToGeneralNode(internal->GetParentPageId(), deque, LockType::WRITE, txn));
  }
  new_internal->SetParentPageId(parent->GetPageId());
  InsertIntoInternalNode(parent, lift_key, internal->GetPageId(), new_internal->GetPageId(), deque, txn);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeafNode(BPlusTree::LeafPage *leaf, const KeyType &key, const ValueType &value,
                                        std::deque<std::pair<LockType, Page *>> &deque, Transaction *txn) -> bool {
  bool found;
  int insert_pos = leaf->IndexOfKey(comparator_, key, &found);
  if (found) {
    ClearLockDeque(deque, txn, false, 0);
    return false;
  }

  if (leaf->GetSize() < leaf_max_size_) {
    for (int ri = leaf->GetSize() - 1; ri > insert_pos; ri--) {
      leaf->SetKVAt(ri + 1, leaf->KeyAt(ri), leaf->ValueAt(ri));
    }
    leaf->SetKVAt(insert_pos + 1, key, value);
    leaf->IncreaseSize(1);
    ClearLockDeque(deque, txn, true, 0);  // release all lock
    return true;
  }
  page_id_t new_leaf_page_id;
  buffer_pool_manager_->NewPage(&new_leaf_page_id);
  BUSTUB_ASSERT(new_leaf_page_id != INVALID_PAGE_ID, "bpt op failed : can`t allocate page");
  buffer_pool_manager_->UnpinPage(new_leaf_page_id, false);

  // this is necessary, since we have to
  // 1. pin this page
  // 2. wlock it
  // 3. push it to the queue before the root
  auto new_leaf = static_cast<LeafPage *>(ParsePageToGeneralNode(new_leaf_page_id, deque, LockType::WRITE, txn));
  new_leaf->Init(new_leaf_page_id, leaf->GetParentPageId(), leaf_max_size_);
  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(new_leaf_page_id);

  int lift_pos = (leaf_max_size_ + 1) / 2;

  if (insert_pos < lift_pos - 1) {  // the inserted entry stays at the left leaf

    // transfer the right half to new_leaf
    for (int i = lift_pos - 1, j = 0; i < leaf->GetSize(); i++, j++) {
      new_leaf->SetKVAt(j, leaf->KeyAt(i), leaf->ValueAt(i));
    }
    // rearrange the leaf
    for (int ri = lift_pos - 2; ri > insert_pos; ri--) {
      leaf->SetKVAt(ri + 1, leaf->KeyAt(ri), leaf->ValueAt(ri));
    }
    leaf->SetKVAt(insert_pos + 1, key, value);
  } else {  // the inserted entry stays at the right leaf
    int j = 0;
    for (int i = lift_pos; i < insert_pos + 1; i++) {  // entries < insert key
      new_leaf->SetKVAt(j++, leaf->KeyAt(i), leaf->ValueAt(i));
    }
    new_leaf->SetKVAt(j++, key, value);                       // insert key
    for (int i = insert_pos + 1; i < leaf->GetSize(); i++) {  // entries > insert key
      new_leaf->SetKVAt(j++, leaf->KeyAt(i), leaf->ValueAt(i));
    }
  }
  KeyType lift_key = new_leaf->KeyAt(0);
  leaf->SetSize(lift_pos);
  new_leaf->SetSize(new_leaf->GetMinSize() + 1);

  InternalPage *parent;

  if (leaf->IsRootPage()) {
    // we`ve already wlocked the current node,which is the only cause of spawning a new root
    Page *new_root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "bpt op failed : can`t allocate page");

    new_root_page->WLatch();
    txn->AddIntoPageSet(new_root_page);
    // push it to the front instead of the back to maintain the same order of the existing parent
    deque.emplace_front(LockType::WRITE, new_root_page);
    parent = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    parent->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    leaf->SetParentPageId(root_page_id_);
    UpdateRootPageId();
  } else {
    parent = static_cast<InternalPage *>(ParsePageToGeneralNode(leaf->GetParentPageId(), deque, LockType::WRITE, txn));
  }
  new_leaf->SetParentPageId(parent->GetPageId());
  // no need to release the locks on these nodes, since no r/w can reach here
  // as long as the parent is wlocked
  InsertIntoInternalNode(parent, lift_key, leaf->GetPageId(), new_leaf->GetPageId(), deque, txn);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ClearLockDeque(std::deque<std::pair<LockType, Page *>> &deque, Transaction *txn, bool is_dirty,
                                    size_t remain_size) {
  Page *page;
  while (remain_size < deque.size()) {
    page = deque.front().second;
    if (deque.front().first == LockType::READ) {
      page->RUnlatch();
      if (page->GetPageId() == root_page_id_) {
        rw_latch_.unlock_shared();
      }
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      page->WUnlatch();
      if (page->GetPageId() == root_page_id_) {
        rw_latch_.unlock();
      }
      buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
    }
    deque.pop_front();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteFromInternalNode(BPlusTree::InternalPage *internal, int index,
                                            std::deque<std::pair<LockType, Page *>> &deque, Transaction *txn) {
  BUSTUB_ASSERT(index > 0, "bpt op failed:first child of an internal node should never be deleted");
  for (int i = index; i < internal->GetSize() - 2; i++) {
    internal->SetKVAt(i, internal->KeyAt(i + 1), internal->ValueAt(i + 1));
  }
  internal->IncreaseSize(-1);
  if (internal->GetSize() >= internal->GetMinSize()) {
    ClearLockDeque(deque, txn, true, 0);
    return;
  }

  // root has only 1 child, should switch the root to this child
  // at this time, root_page_id_ should be wlocked
  if (internal->IsRootPage() && internal->GetSize() == 1) {
    root_page_id_ = internal->ValueAt(0);
    auto new_root = ParsePageToGeneralNode(root_page_id_, deque, LockType::WRITE, txn);
    new_root->SetParentPageId(INVALID_PAGE_ID);
    txn->AddIntoDeletedPageSet(internal->GetPageId());
    UpdateRootPageId();
    ClearLockDeque(deque, txn, true, 0);
    return;
  }

  auto *parent =
      static_cast<InternalPage *>(ParsePageToGeneralNode(internal->GetParentPageId(), deque, LockType::WRITE, txn));
  if (parent == nullptr) {
    ClearLockDeque(deque, txn, true, 0);
    return;
  }
  InternalPage *first;
  InternalPage *second;
  int maybe_delete_index = parent->IndexOfKey(comparator_, internal->KeyAt(1));

  if (maybe_delete_index == 0) {
    maybe_delete_index++;
    first = internal;
    second = static_cast<InternalPage *>(
        ParsePageToGeneralNode(parent->ValueAt(maybe_delete_index), deque, LockType::WRITE, txn));
  } else {
    first = static_cast<InternalPage *>(
        ParsePageToGeneralNode(parent->ValueAt(maybe_delete_index - 1), deque, LockType::WRITE, txn));
    second = internal;
  }
  // now we can always have the structure:
  //                     parent
  //                  /          \(maybe_delete_index)   ...
  //                 v            v
  //               first         second
  int total_size = first->GetSize() + second->GetSize();

  // assign second node`s first key to proceed merge
  auto second_node_first_child = ParsePageToGeneralNode(second->ValueAt(0), deque, LockType::WRITE, txn);
  if (second->IsLeafPage()) {
    second->SetKeyAt(0, static_cast<LeafPage *>(second_node_first_child)->KeyAt(0));
  } else {
    second->SetKeyAt(0, static_cast<InternalPage *>(second_node_first_child)->KeyAt(1));
  }

  if (total_size < internal_max_size_) {
    for (int i = first->GetSize(), j = 0; j < second->GetSize(); j++) {
      first->SetKVAt(i, second->KeyAt(j), second->ValueAt(j));
      auto parent_changed_node = ParsePageToGeneralNode(second->ValueAt(j), deque, LockType::WRITE, txn);
      parent_changed_node->SetParentPageId(first->GetPageId());
    }
    first->SetSize(total_size);
    // declare deletion of the eaten page
    txn->AddIntoDeletedPageSet(second->GetPageId());
    DeleteFromInternalNode(parent, maybe_delete_index, deque, txn);
  } else {
    int diff;
    if (first->GetSize() > total_size / 2) {  // first -> second
      diff = first->GetSize() - total_size / 2;
      for (int rj = second->GetSize() - 1; rj >= 0; rj--) {  // shift right
        second->SetKVAt(rj + diff, second->KeyAt(rj), second->ValueAt(rj));
      }
      for (int i = total_size / 2, j = 0; i < first->GetSize(); i++, j++) {  // fill
        second->SetKVAt(j, first->KeyAt(i), first->ValueAt(i));
        auto parent_changed_node = ParsePageToGeneralNode(first->ValueAt(i), deque, LockType::WRITE, txn);
        parent_changed_node->SetParentPageId(second->GetPageId());
      }
    } else {  // second->first
      diff = total_size / 2 - first->GetSize();
      for (int i = first->GetSize(), j = 0; i < total_size / 2; i++, j++) {  // append
        first->SetKVAt(i, second->KeyAt(j), second->ValueAt(j));
        auto parent_changed_node = ParsePageToGeneralNode(second->ValueAt(j), deque, LockType::WRITE, txn);
        parent_changed_node->SetParentPageId(first->GetPageId());
      }
      for (int j = 0; j < total_size - first->GetSize(); j++) {  // shift left
        second->SetKVAt(j, second->KeyAt(j + diff), second->ValueAt(j + diff));
      }
    }
  }
  first->SetSize(total_size / 2);
  second->SetSize(total_size - first->GetSize());
  UpdateInternalNode(parent, maybe_delete_index, second->KeyAt(1), deque, txn);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteFromLeafNode(BPlusTree::LeafPage *leaf, const KeyType &key,
                                        std::deque<std::pair<LockType, Page *>> &deque, Transaction *txn) -> bool {
  bool found;
  int rst = leaf->IndexOfKey(comparator_, key, &found);
  if (!found) {
    ClearLockDeque(deque, txn, true, 0);
    return false;
  }
  for (int i = rst; i < leaf->GetSize() - 1; i++) {
    leaf->SetKVAt(i, leaf->KeyAt(i + 1), leaf->ValueAt(i + 1));
  }
  leaf->IncreaseSize(-1);
  if (leaf->IsRootPage() || leaf->GetSize() >= leaf->GetMinSize()) {
    ClearLockDeque(deque, txn, true, 0);
    return true;
  }

  auto *parent =
      static_cast<InternalPage *>(ParsePageToGeneralNode(leaf->GetParentPageId(), deque, LockType::WRITE, txn));
  if (parent == nullptr) {  // only one node
    ClearLockDeque(deque, txn, true, 0);
    return true;
  }
  LeafPage *first;
  LeafPage *second;
  // always try to find the prev brother to merge to reduce copy cost
  int maybe_delete_index = parent->IndexOfKey(comparator_, leaf->KeyAt(0));

  if (maybe_delete_index == 0) {  // no prev brother
    maybe_delete_index++;
    first = leaf;
    second = static_cast<LeafPage *>(ParsePageToGeneralNode(leaf->GetNextPageId(), deque, LockType::WRITE, txn));
  } else {
    first = static_cast<LeafPage *>(
        ParsePageToGeneralNode(parent->ValueAt(maybe_delete_index - 1), deque, LockType::WRITE, txn));
    second = leaf;
  }
  // now we can always have the structure:
  //                     parent
  //                  /          \(maybe_delete_index)   ...
  //                 v            v
  //               first         second
  int total_size = first->GetSize() + second->GetSize();
  if (total_size < leaf_max_size_) {  // can be fit in 1 node, let first eat second
    for (int i = first->GetSize(), j = 0; j < second->GetSize(); j++) {
      first->SetKVAt(i, second->KeyAt(j), second->ValueAt(j));
    }
    first->SetSize(total_size);
    first->SetNextPageId(second->GetNextPageId());
    // declare deletion of the eaten page
    txn->AddIntoDeletedPageSet(second->GetPageId());
    DeleteFromInternalNode(parent, maybe_delete_index, deque, txn);
  } else {  // make moves and shifts
    int diff;
    if (first->GetSize() > total_size / 2) {  // first -> second
      diff = first->GetSize() - total_size / 2;
      for (int rj = second->GetSize() - 1; rj >= 0; rj--) {
        second->SetKVAt(rj + diff, second->KeyAt(rj), second->ValueAt(rj));
      }
      for (int i = total_size / 2, j = 0; i < first->GetSize(); i++, j++) {
        second->SetKVAt(j, first->KeyAt(i), first->ValueAt(i));
      }
    } else {  // second->first
      diff = total_size / 2 - first->GetSize();
      for (int i = first->GetSize(), j = 0; i < total_size / 2; i++, j++) {
        first->SetKVAt(i, second->KeyAt(j), second->ValueAt(j));
      }
      for (int j = 0; j < total_size - first->GetSize(); j++) {
        second->SetKVAt(j, second->KeyAt(j + diff), second->ValueAt(j + diff));
      }
    }
    first->SetSize(total_size / 2);
    second->SetSize(total_size - first->GetSize());
    UpdateInternalNode(parent, maybe_delete_index, second->KeyAt(0), deque, txn);
  }
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateInternalNode(InternalPage *internal, int index, const KeyType &new_key,
                                        std::deque<std::pair<LockType, Page *>> &deque, Transaction *txn) {
  internal->SetKeyAt(index, new_key);
  assert(index != 0);
  ClearLockDeque(deque, txn, true, 0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SeekToStart(BPlusTreePage *node, std::deque<std::pair<LockType, Page *>> &deque)
    -> BPlusTree::LeafPage * {
  if (node->IsLeafPage()) {
    return reinterpret_cast<LeafPage *>(node);
  }
  auto *internal = reinterpret_cast<InternalPage *>(node);
  LeafPage *leaf = nullptr;
  while (leaf == nullptr) {
    node = ParsePageToGeneralNode(internal->ValueAt(0), deque, LockType::READ, nullptr);
    assert(node != internal && node->GetPageId() != internal->GetPageId());
    ClearLockDeque(deque, nullptr, false, 1);
    if (node->IsLeafPage()) {
      leaf = reinterpret_cast<LeafPage *>(node);
    } else {
      internal = reinterpret_cast<InternalPage *>(node);
    }
  }
  return leaf;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ProcessNodeByStrategy(BPlusTreePage *const node, std::deque<std::pair<LockType, Page *>> &deque,
                                           LockType strategy, SafeType safe_type, Transaction *txn) -> BPlusTreePage * {
  switch (strategy) {
    case LockType::READ:  // simply release prev lock
      ClearLockDeque(deque, txn, false, 1);
      break;
    case LockType::WRITE:
      if (node->IsSafe(safe_type)) {
        // release all prev wlock
        ClearLockDeque(deque, txn, false, 1);
      }  // else hold the lock
      break;
  }
  return node;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
