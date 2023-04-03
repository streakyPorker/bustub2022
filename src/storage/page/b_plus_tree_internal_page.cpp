//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(index >= 0 && index < this->GetSize(), "invalid query : index out of bound");
  KeyType key{};
  // ignore the index 0
  if (index != 0) {
    key = array_[index].first;
  }
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(index >= 0 && index < this->GetMaxSize(), "invalid modification : index out of bound");
  if (index != 0) {
    array_[index].first = key;
  }
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(index >= 0 && index < this->GetSize(), "invalid query : index out of bound");
  return array_[index].second;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(index >= 0 && index < this->GetMaxSize(), "invalid modification : index out of bound");
  if (index != 0) {
    array_[index].second = value;
  }
}
template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::IndexOfKey(const KeyComparator &comp,
                                                                           const KeyType &key) -> int {
  int l = 1;
  int r = GetSize() - 1;
  int mid;
  int rst;
  while (l <= r) {
    mid = (l + r) << 1;
    rst = comp(key, KeyAt(mid));
    if (rst < 0) {
      r = mid - 1;
    } else if (rst > 0) {
      l = mid + 1;
    } else {
      return mid;
    }
  }
  return std::min(l, r);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
