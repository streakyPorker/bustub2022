//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <deque>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  /**
   * self def
   */

  /**
   * search the deque to find the page ,if not found, parse a page_id to a BPlusTreePage,after
   *    1. fetch the page from bpm(which means pinning it)
   *    2. lock it according to the type param
   *    3. append it to the lock queue
   * @attention remember we need to pin the page,so DONT call it right after
   * bpm->NewPage without unpinning it
   * @param page_id
   * @param deque lock queue
   * @param type lock type
   * @return parsed page
   */
  auto ParsePageToGeneralNode(page_id_t page_id, std::deque<std::pair<LockType, Page *>> &deque, LockType type,
                              Transaction *txn) -> BPlusTreePage *;

  auto SeekToLeaf(BPlusTreePage *root_node, const KeyType &key, std::deque<std::pair<LockType, Page *>> &deque,
                  LockStrategy strategy, SafeType safe_type, Transaction *txn) -> LeafPage *;

  void InsertIntoInternalNode(InternalPage *internal, const KeyType &key, const page_id_t &old_node,
                              const page_id_t &new_node, std::deque<std::pair<LockType, Page *>> &deque,
                              Transaction *txn);

  auto InsertIntoLeafNode(LeafPage *leaf, const KeyType &key, const ValueType &value,
                          std::deque<std::pair<LockType, Page *>> &deque, Transaction *txn) -> bool;

  void DeleteFromInternalNode(InternalPage *internal, int index, std::deque<std::pair<LockType, Page *>> &deque,
                              Transaction *txn);

  void UpdateInternalNode(InternalPage *internal, int index, const KeyType &new_key,
                          std::deque<std::pair<LockType, Page *>> &deque, Transaction *txn);

  auto DeleteFromLeafNode(LeafPage *leaf, const KeyType &key, std::deque<std::pair<LockType, Page *>> &deque,
                          Transaction *txn) -> bool;

  void ClearLockDeque(std::deque<std::pair<LockType, Page *>> &deque, Transaction *txn = nullptr, bool is_dirty = false,
                      size_t remain_size = 0);

  auto SeekToStart(BPlusTreePage *node, std::deque<std::pair<LockType, Page *>> &deque) -> LeafPage *;

  auto SeekToEnd(BPlusTreePage *node, std::deque<std::pair<LockType, Page *>> &deque) -> LeafPage * {
    if (node->IsLeafPage()) {
      return reinterpret_cast<LeafPage *>(node);
    }
    auto *internal = reinterpret_cast<InternalPage *>(node);
    LeafPage *leaf = nullptr;
    while (leaf == nullptr) {
      node = ParsePageToGeneralNode(internal->ValueAt(internal->GetSize() - 1), deque, LockType::READ, nullptr);
      ClearLockDeque(deque, nullptr, false, 1);
      if (node->IsLeafPage()) {
        leaf = reinterpret_cast<LeafPage *>(node);
      } else {
        internal = reinterpret_cast<InternalPage *>(node);
      }
    }
    return leaf;
  };

  auto ProcessNodeByStrategy(BPlusTreePage *const node, std::deque<std::pair<LockType, Page *>> &deque,
                             LockStrategy strategy, SafeType safe_type, Transaction *txn) -> BPlusTreePage * {
    switch (strategy) {
      case LockStrategy::OPTIM_WRITE_LOCK:
        if (!node->IsLeafPage()) {  // non-leaf,same as READ_LOCK
          ClearLockDeque(deque, txn, false, 1);
        } else {
          if (node->IsSafe(safe_type)) {
            // 1. upgrade the leaf latch to write mode at optim mode
            // 2. check safe state, return nullptr(to switch lock mode) if failed
            Page *leaf_page = deque.back().second;
            deque.pop_back();
            leaf_page->RUnlatch();
            leaf_page->WLatch();
            if (txn != nullptr) {
              txn->AddIntoPageSet(leaf_page);
            }
            deque.emplace_back(LockType::WRITE, leaf_page);

            // unlock all the previous rlock, start writing
            ClearLockDeque(deque, txn, false, 1);
          } else {  // not safe, optim write not available, release all the lock, return null to start over
            ClearLockDeque(deque, txn, false, 0);
            return nullptr;
          }
        }
        break;
      case LockStrategy::READ_LOCK:  // simply release prev lock
        ClearLockDeque(deque, txn, false, 1);
        break;
      case LockStrategy::PESSI_WRITE_LOCK:
        if (node->IsSafe(safe_type)) {
          // release all prev wlock
          ClearLockDeque(deque, txn, false, 1);
        }  // else hold the lock
        break;
    }
    return node;
  }

  // member variable

  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;

  // protect the root_page_id_ field, not the root page
  std::shared_timed_mutex rw_latch_;
};

}  // namespace bustub
