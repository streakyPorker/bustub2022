////===----------------------------------------------------------------------===//
////
////                         BusTub
////
//// buffer_pool_manager_instance.cpp
////
//// Identification: src/buffer/buffer_pool_manager.cpp
////
//// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
////
////===----------------------------------------------------------------------===//
//
//#include "buffer/buffer_pool_manager_instance.h"
//
//#include "common/exception.h"
//#include "common/logger.h"
//#include "common/macros.h"
//
//namespace bustub {
//
//BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
//                                                     LogManager *log_manager)
//    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
//  // we allocate a consecutive memory space for the buffer pool
//  pages_ = new Page[pool_size_];
//  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
//  replacer_ = new LRUKReplacer(pool_size, replacer_k);
//
//  // Initially, every page is in the free list.
//  for (size_t i = 0; i < pool_size_; ++i) {
//    free_list_.emplace_back(static_cast<int>(i));
//  }
//}
//
//BufferPoolManagerInstance::~BufferPoolManagerInstance() {
//  delete[] pages_;
//  delete page_table_;
//  delete replacer_;
//}
//
//auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
//  std::scoped_lock<std::mutex> lock(latch_);
//  frame_id_t frame_id;
//  if (!free_list_.empty()) {
//    frame_id = free_list_.front();
//    free_list_.pop_front();
//  } else if (!replacer_->Evict(&frame_id)) {
//    LOG_WARN("unable to allocate new page");
//    return nullptr;  // can`t get free page
//  }
//  Page *page = pages_ + frame_id;
//
//  // clean step
//  if (page->IsDirty()) {
//    // expensive disk write/read should release the latch_
//    latch_.unlock();
//    disk_manager_->WritePage(page->GetPageId(), page->GetData());
//    latch_.lock();
//    page->is_dirty_ = false;
//  }
//  BUSTUB_ASSERT(page->pin_count_ == 0, "invalid pin count");
//  page->ResetMemory();
//  if (page->page_id_ != INVALID_PAGE_ID) {
//    page_table_->Remove(page->page_id_);  // remove old page entry
//  }
//  page_id_t new_page_id = AllocatePage();
//  *page_id = new_page_id;
//  page->page_id_ = new_page_id;
//  PinPageInternal(page, frame_id);
//  page_table_->Insert(new_page_id, frame_id);
//  return page;
//}
//
//auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
//  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "invalid page id");
//  std::scoped_lock<std::mutex> lock(latch_);
//  frame_id_t frame_id;
//  Page *page;
//  if (page_table_->Find(page_id, frame_id)) {
//    page = pages_ + frame_id;
//  } else if (replacer_->Evict(&frame_id)) {
//    page = pages_ + frame_id;
//    if (page->IsDirty()) {
//      // expensive disk write/read should release the latch_
//      latch_.unlock();
//
//      disk_manager_->WritePage(page->GetPageId(), page->GetData());
//      latch_.lock();
//      page->is_dirty_ = false;
//    }
//    page->page_id_ = page_id;
//
//    // expensive disk write/read should release the latch_
//    latch_.unlock();
//    disk_manager_->ReadPage(page_id, page->GetData());
//    latch_.lock();
//    page_table_->Insert(page_id, frame_id);
//  } else {
//    return nullptr;
//  }
//  PinPageInternal(page, frame_id);
//  return page;
//}
//
//auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
//  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "invalid page id");
//  std::scoped_lock<std::mutex> lock(latch_);
//  frame_id_t frame_id;
//  Page *page;
//  if (page_table_->Find(page_id, frame_id)) {
//    page = &pages_[frame_id];
//    page->is_dirty_ = is_dirty;
//    if (page->GetPinCount() > 0) {
//      --page->pin_count_;
//      if (page->pin_count_ == 0) {
//        replacer_->SetEvictable(frame_id, true);
//      }
//      return true;
//    }
//  }
//  return false;
//}
//
//auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
//  std::scoped_lock<std::mutex> lock(latch_);
//  frame_id_t frame_id;
//  Page *page;
//  if (page_table_->Find(page_id, frame_id)) {
//    page = &pages_[frame_id];
//
//    // expensive disk write/read should release the latch_
//    latch_.unlock();
//    disk_manager_->WritePage(page_id, page->GetData());
//    latch_.lock();
//
//    page->is_dirty_ = false;
//    return true;
//  }
//  return false;
//}
//
//void BufferPoolManagerInstance::FlushAllPgsImp() {
//  Page *page;
//  for (size_t i = 0; i < pool_size_; i++) {
//    page = &pages_[i];
//    if (page->GetPageId() != INVALID_PAGE_ID) {
//      // expensive disk write/read should release the latch_
//      latch_.unlock();
//      disk_manager_->WritePage(page->GetPageId(), page->GetData());
//      latch_.lock();
//      page->is_dirty_ = false;
//    }
//  }
//}
//
//auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
//  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "invalid page id");
//  frame_id_t frame_id;
//  Page *page;
//  std::scoped_lock<std::mutex> lock(latch_);
//  if (!page_table_->Find(page_id, frame_id)) {
//    return true;
//  }
//  page = &pages_[frame_id];
//  if (page->GetPinCount() > 0) {
//    return false;
//  }
//
//  page_table_->Remove(page_id);
//  replacer_->Remove(frame_id);
//  free_list_.push_back(frame_id);
//  page->ResetMemory();
//  page->page_id_ = INVALID_PAGE_ID;
//  page->is_dirty_ = false;
//  DeallocatePage(page_id);
//  return true;
//}
//
//auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }
//
//auto BufferPoolManagerInstance::PinPageInternal(Page *page, frame_id_t frame_id) -> int {
//  replacer_->RecordAccess(frame_id);
//  replacer_->SetEvictable(frame_id, false);
//  return ++page->pin_count_;
//}
//
//}  // namespace bustub


//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock lock(latch_);
  frame_id_t frame_id;
  /* Find a free frame. If found, `FindAvailableFrame` will preprocess
     the page data and metadata in . */
  if (!FindAvailableFrame(frame_id)) {
    return nullptr;
  }

  /* Preprocess the frame we are going to use */
  Page *page = pages_ + frame_id;

  /* Allocate new page id */
  page_id_t new_page_id = AllocatePage();
  *page_id = new_page_id;

  /* Maintain page */
  page->page_id_ = new_page_id;
  page->pin_count_++;

  /* Maintain replacer */
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  /* Maintain page table */
  page_table_->Insert(new_page_id, frame_id);

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock lock(latch_);

  ExaminePageId(page_id);

  bool in_pool = false;
  frame_id_t frame_id;
  Page *page = FindPage(page_id, frame_id);
  // not found the page in pool
  if (page == nullptr) {
    // we have to evict one page (if we can)
    if (!FindAvailableFrame(frame_id)) {
      return nullptr;
    }
    // then record it
    page_table_->Insert(page_id, frame_id);
    // Maintain page
    page = pages_ + frame_id;
    page->page_id_ = page_id;
  } else {  // found the page in pool
    in_pool = true;
  }
  assert(page->GetPageId() == page_id);
  page->pin_count_++;
  // Read the page from disk
  if (!in_pool) {
    disk_manager_->ReadPage(page->GetPageId(), page->GetData());
  }
  // Maintain replacer
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock lock(latch_);
  ExaminePageId(page_id);
  frame_id_t frame_id;
  Page *page = FindPage(page_id, frame_id);
  if (page == nullptr) {
    return false;
  }
  if (is_dirty) {
    page->is_dirty_ = true;
  }
  if (page->GetPinCount() == 0) {
    return false;
  }
  if (--(page->pin_count_) == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);

  ExaminePageId(page_id);

  frame_id_t frame_id;
  Page *page = FindPage(page_id, frame_id);
  if (page == nullptr) {
    return false;
  }

  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page = pages_ + i;
    if (page->GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  ExaminePageId(page_id);
  frame_id_t frame_id;
  Page *page = FindPage(page_id, frame_id);
  if (page == nullptr) {
    return true;
  }
  if (page->GetPinCount() != 0) {
    return false;
  }
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  ResetPageData(page);

  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

void BufferPoolManagerInstance::ResetPageData(Page *page) {
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
}

auto BufferPoolManagerInstance::FindPage(page_id_t page_id, frame_id_t &frame_id) -> Page * {
  ExaminePageId(page_id);
  if (!page_table_->Find(page_id, frame_id)) {
    return nullptr;
  }
  Page *page = pages_ + frame_id;
  assert(page->GetPageId() == page_id);
  return page;
}

auto BufferPoolManagerInstance::FindAvailableFrame(frame_id_t &frame_id) -> bool {
  if (free_list_.empty()) {
    // There is no evictable frame
    if (!replacer_->Evict(&frame_id)) {
      return false;
    }
    Page *page = pages_ + frame_id;
    assert(page->GetPageId() != INVALID_PAGE_ID);
    assert(page_table_->Remove(page->GetPageId()));
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    ResetPageData(page);
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
    assert(pages_[frame_id].GetPageId() == INVALID_PAGE_ID);
  }
  return true;
}

void BufferPoolManagerInstance::ExaminePageId(page_id_t page_id) {
  assert(page_id != INVALID_PAGE_ID);
  assert(page_id < next_page_id_);
}

}