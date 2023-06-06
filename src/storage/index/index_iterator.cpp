/**
 * index_iterator.cpp
 */

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : bpm_(nullptr), leaf_(nullptr), page_(nullptr), index_(0) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t leaf_page, BufferPoolManager *bpm, int index)
    : bpm_(bpm), leaf_(nullptr), index_(index) {
  page_ = bpm_->FetchPage(leaf_page);
  if (page_ != nullptr) {
    page_->RLatch();
    leaf_ = reinterpret_cast<LeafPage *>(page_->GetData());
  } else {
    bpm_ = nullptr;
    page_ = nullptr;
    index_ = 0;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_ != nullptr) {
    page_->RUnlatch();
    bpm_->UnpinPage(page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  return leaf_->KVAt(index_);

  //  throw std::runtime_error("unimplemented");
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  if (index_ == leaf_->GetSize()) {
    index_ = 0;
    if (leaf_->GetNextPageId() == INVALID_PAGE_ID) {
      bpm_ = nullptr;
      page_ = nullptr;
      leaf_ = nullptr;
      return *this;
    }
    Page *new_page = bpm_->FetchPage(leaf_->GetNextPageId());
    if (new_page != nullptr) {
      new_page->RLatch();
      page_->RUnlatch();
      bpm_->UnpinPage(page_->GetPageId(), false);
      leaf_ = reinterpret_cast<LeafPage *>(new_page->GetData());
      page_ = new_page;
    } else {
      page_->RUnlatch();
      bpm_->UnpinPage(page_->GetPageId(), false);
      bpm_ = nullptr;
      leaf_ = nullptr;
      page_ = nullptr;
      index_ = 0;
    }
  }
  return *this;

  //  throw std::runtime_error("unimplemented");
}
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  if (leaf_ == nullptr || itr.leaf_ == nullptr) {
    return leaf_ == itr.leaf_;
  }
  return leaf_->GetPageId() == itr.leaf_->GetPageId() && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  if (leaf_ == nullptr || itr.leaf_ == nullptr) {
    return leaf_ != itr.leaf_;
  }
  return leaf_->GetPageId() != itr.leaf_->GetPageId() || index_ != itr.index_;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
