/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() : leaf_(nullptr), index_(0){};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(IndexIterator::LeafPage *leaf, BufferPoolManager *bpm, int index)
    : leaf_(leaf), bpm_(bpm), index_(index) {
  page_ = bpm_->FetchPage(leaf_->GetPageId());
  if (page_ != nullptr) {
    page_->RLatch();  // test for reachability
    bpm_->UnpinPage(page_->GetPageId(), false);
    page_->RUnlatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){

};  // NOLINT

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
      page_ = nullptr;
      leaf_ = nullptr;
      return *this;
    }
    Page *new_page = bpm_->FetchPage(leaf_->GetNextPageId());
    if (new_page != nullptr) {
      new_page->RLatch();
      bpm_->UnpinPage(page_->GetPageId(), false);
      new_page->RUnlatch();
      leaf_ = reinterpret_cast<LeafPage *>(new_page->GetData());
      page_ = new_page;
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
