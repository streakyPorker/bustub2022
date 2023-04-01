#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

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
  if (page == nullptr) {
    return true;
  }
  page->RLatch();
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  //  BUSTUB_ASSERT(node->IsRootPage(), "invalid root");
  bool rst = !node->IsRootPage() || node->GetSize() == 0;
  page->RUnlatch();
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
  if (IsEmpty()) {
    return false;
  }
  std::deque<std::pair<LockType, Page *>> locked_pages;
  auto node = ParsePageToGeneralNode(root_page_id_, locked_pages, LockType::READ);
  BUSTUB_ASSERT(node != nullptr && node->IsRootPage(), "invalid root page id");

  LeafPage *leaf = SearchToLeaf(node, key, locked_pages, LockStrategy::READ_LOCK, SafeType::READ);
  if (leaf == nullptr) {
    return false;
  }
  int l = 0;
  int r = leaf->GetSize() - 1;
  int mid;
  while (l < r) {
    mid = (l + r) << 1;
    int rst = comparator_(key, leaf->KeyAt(mid));
    if (rst == 0) {
      result->push_back(leaf->ValueAt(mid));
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
      return true;
    }
    if (rst < 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  // not found in the leaf node
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
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
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // double check to create root node
  if (IsEmpty()) {
    std::scoped_lock<std::mutex> lock_scope(latch_);
    if (IsEmpty()) {
      Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
      BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "bpt creation failed : can`t allocate page");
      page->WLatch();
      auto root_page = reinterpret_cast<LeafPage *>(page->GetData());
      root_page->Init(root_page_id_);
      page->WUnlatch();
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
  }
  std::deque<std::pair<LockType, Page *>> locked_pages;

  // optimistic mode first
  auto node = ParsePageToGeneralNode(root_page_id_, locked_pages, LockType::READ);
  LeafPage *leaf = SearchToLeaf(node, key, locked_pages, LockStrategy::OPTIM_WRITE_LOCK, SafeType::INSERT);

  if (leaf == nullptr) {
    // at this time,all lock should be released
    BUSTUB_ASSERT(locked_pages.empty(), "bpt op failed:unreleased lock");
    // turn to pessimistic way
    node = ParsePageToGeneralNode(root_page_id_, locked_pages, LockType::READ);
    leaf = SearchToLeaf(node, key, locked_pages, LockStrategy::PESSI_WRITE_LOCK, SafeType::INSERT);
  }
  BUSTUB_ASSERT(leaf != nullptr, "bpt op failed:internal search error");

  InsertIntoLeafNode(leaf, key, value, locked_pages);

  return true;
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
  // TODO: remove the record in the header file when the index is completely deleted?
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
                                            LockType type) -> BPlusTreePage * {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr || page->GetData() == nullptr) {
    return nullptr;
  }
  if (type == LockType::READ) {
    page->RLatch();
  } else {
    page->WLatch();
  }
  deque.emplace_back(type, page);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SearchToLeaf(BPlusTreePage *node, const KeyType &key,
                                  std::deque<std::pair<LockType, Page *>> &deque, LockStrategy strategy,
                                  SafeType safe_type) -> BPlusTree::LeafPage * {
  InternalPage *internal = nullptr;
  LeafPage *leaf = nullptr;
  if (node->IsLeafPage()) {
    leaf = reinterpret_cast<LeafPage *>(node);
  } else {
    internal = reinterpret_cast<InternalPage *>(node);
  }
  int l;
  int r;
  int mid;
  std::pair<LockType, Page *> lock_pair;
  LockType node_lock_type = strategy != LockStrategy::PESSI_WRITE_LOCK ? LockType::READ : LockType::WRITE;
  while (leaf == nullptr) {
    l = 1;
    r = internal->GetSize() - 1;
    node = nullptr;
    while (l < r) {
      mid = (l + r) << 1;
      if (comparator_(key, internal->KeyAt(mid)) < 0) {
        // less than the l pos,reach the boundary
        if (mid <= l) {
          node = ParsePageToGeneralNode(internal->ValueAt(mid), deque, node_lock_type);
          //          buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
          break;
        }
        r = mid - 1;
      } else if (mid == r || comparator_(key, internal->KeyAt(mid + 1)) < 0) {
        // get to the r or the match pos
        node = ParsePageToGeneralNode(internal->ValueAt(mid), deque, node_lock_type);
        //        buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);

        break;
      } else {
        l = mid + 1;
      }
    }
    // node==nullptr when  key<r and no data is there
    if (node == nullptr) {
      // unlock all the page
      ClearLockDeque(deque);
      return nullptr;
    }

    switch (strategy) {
      case LockStrategy::OPTIM_WRITE_LOCK:
        if (!node->IsLeafPage()) {  // non-leaf,same as READ_LOCK
          while (deque.size() > 1) {
            buffer_pool_manager_->UnpinPage(deque.front().second->GetPageId(), false);
            deque.front().second->RUnlatch();
            deque.pop_front();
          }
        } else {
          // 1. upgrade the leaf latch to write mode at optim mode
          // 2. check safe state, return nullptr if failed
          Page *leaf_page = deque.back().second;
          deque.pop_back();
          leaf_page->RUnlatch();
          leaf_page->WLatch();
          deque.emplace_back(LockType::WRITE, leaf_page);

          if (node->IsSafe(safe_type == SafeType::INSERT ? WType::INSERT : WType::DELETE)) {
            // unlock all the rlock, start writing
            while (deque.size() > 1) {
              buffer_pool_manager_->UnpinPage(deque.front().second->GetPageId(), false);
              deque.front().second->RUnlatch();
              deque.pop_front();
            }
          } else {
            // release all the lock, return null to start over
            buffer_pool_manager_->UnpinPage(deque.back().second->GetPageId(), false);  // no change,no dirty
            deque.back().second->WUnlatch();
            deque.pop_back();
            while (!deque.empty()) {
              buffer_pool_manager_->UnpinPage(deque.front().second->GetPageId(), false);
              deque.front().second->RUnlatch();
              deque.pop_front();
            }
            return nullptr;
          }
        }
        break;
      case LockStrategy::READ_LOCK:  // simply release prev lock
        while (deque.size() > 1) {
          buffer_pool_manager_->UnpinPage(deque.front().second->GetPageId(), false);
          deque.front().second->RUnlatch();
          deque.pop_front();
        }
        break;
      case LockStrategy::PESSI_WRITE_LOCK:
        if (node->IsSafe(safe_type == SafeType::INSERT ? WType::INSERT : WType::DELETE)) {
          // release all prev wlock
          while (deque.size() > 1) {
            buffer_pool_manager_->UnpinPage(deque.front().second->GetPageId(), false);
            deque.front().second->WUnlatch();
            deque.pop_front();
          }
        }  // else hold the lock
        break;
    }

    if (node->IsLeafPage()) {
      leaf = reinterpret_cast<LeafPage *>(node);
    } else {
      internal = reinterpret_cast<InternalPage *>(node);
    }
  }
  return leaf;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternalNode(BPlusTree::InternalPage *internal, const KeyType &key,
                                            const page_id_t &value, std::deque<std::pair<LockType, Page *>> &deque) {}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeafNode(BPlusTree::LeafPage *leaf, const KeyType &key, const ValueType &value,
                                        std::deque<std::pair<LockType, Page *>> &deque) -> bool {
  int l = 0;
  int r = leaf->GetSize() - 1;
  int mid;
  bool need_split = leaf->GetSize() == leaf->GetMaxSize();  // TODO: need check
  while (l <= r) {
    mid = (l + r) << 1;
    int rst = comparator_(key, leaf->KeyAt(mid));
    if (rst == 0) {
      // might change,since metadata might be edited
      ClearLockDeque(deque);
      return false;
    }
    if (rst < 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  int insert_pos = std::min(l, r);

  if (!need_split) {
    for (int ri = leaf->GetSize() - 1; ri > insert_pos; ri--) {
      leaf->SetKVAt(ri + 1, leaf->KeyAt(ri), leaf->ValueAt(ri));
    }
    leaf->SetKVAt(insert_pos + 1, key, value);
    leaf->IncreaseSize(1);
  } else {
    page_id_t new_leaf_page_id;
    buffer_pool_manager_->NewPage(&new_leaf_page_id);
    BUSTUB_ASSERT(new_leaf_page_id != INVALID_PAGE_ID, "bpt op failed : can`t allocate page");
    buffer_pool_manager_->UnpinPage(root_page_id_, true);

    // this is necessary, since we have to
    // 1. pin this page
    // 2. wlock it
    // 3. push it to the queue before the root
    auto new_leaf = static_cast<LeafPage *>(ParsePageToGeneralNode(new_leaf_page_id, deque, LockType::WRITE));
    new_leaf->Init(new_leaf_page_id, leaf->GetParentPageId());
    new_leaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_leaf_page_id);

    // TODO: maybe try a easier way to impl?
    int lift_pos = (leaf->GetMaxSize() + 1) << 1;
    KeyType lift_key;
    if (insert_pos == lift_pos - 1) {  // shift the inserted entry
      lift_key = key;
      for (int i = insert_pos + 1, j = 0; i < leaf->GetSize(); i++, j++) {
        new_leaf->SetKVAt(j, leaf->KeyAt(i), leaf->ValueAt(i));
      }
    } else if (insert_pos < lift_pos - 1) {  // shift the right part
      lift_key = leaf->KeyAt(leaf->GetMaxSize() / 2 - 1);
      // transfer the right half to new_leaf
      for (int i = leaf->GetMaxSize() / 2, j = 0; i < leaf->GetSize(); i++, j++) {
        new_leaf->SetKVAt(j, leaf->KeyAt(i), leaf->ValueAt(i));
      }

      // rearrange the leaf
      for (int ri = leaf->GetMaxSize() / 2 - 1; ri > insert_pos; ri--) {
        leaf->SetKVAt(ri + 1, leaf->KeyAt(ri), leaf->ValueAt(ri));
      }
      leaf->SetKVAt(insert_pos + 1, key, value);

    } else {
      lift_key = key;
    }

    //    leaf->SetSize(lift_pos);
    //    new_leaf->SetSize(leaf->GetMaxSize() / 2);

    InternalPage *parent;
    // no need to double-check,
    // since we`ve already wlocked leaf,which is the only cause of spawning a new root
    if (leaf->IsRootPage()) {
      std::scoped_lock<std::mutex> scoped_lock(latch_);
      buffer_pool_manager_->NewPage(&root_page_id_);
      BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "bpt op failed : can`t allocate page");
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      parent = static_cast<InternalPage *>(ParsePageToGeneralNode(root_page_id_, deque, LockType::WRITE));
      parent->Init(root_page_id_);
      leaf->SetParentPageId(root_page_id_);
      UpdateRootPageId();
    } else {
      parent = static_cast<InternalPage *>(ParsePageToGeneralNode(leaf->GetParentPageId(), deque, LockType::WRITE));
    }
    new_leaf->SetParentPageId(parent->GetPageId());
    InsertIntoInternalNode(parent, lift_key, new_leaf->GetPageId(), deque);
  }

  return true;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
