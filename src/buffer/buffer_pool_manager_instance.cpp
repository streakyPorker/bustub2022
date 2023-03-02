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
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr;  // can`t get free page
  }
  Page *page = pages_ + frame_id;

  // clean step
  if (page->IsDirty()) {

    // expensive disk write/read should release the latch_
    page->RLatch();
    latch_.unlock();
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->RUnlatch();
    latch_.lock();

    page->is_dirty_ = false;
  }
  BUSTUB_ASSERT(page->pin_count_ == 0, "invalid pin count");
  page->ResetMemory();
  if(page->page_id_!=INVALID_PAGE_ID){
    page_table_->Remove(page->page_id_);  // remove old page entry
  }
  page_id_t new_page_id = AllocatePage();
  *page_id = new_page_id;
  page->page_id_ = new_page_id;
  PinPageInternal(page, frame_id);
  page_table_->Insert(new_page_id, frame_id);
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "invalid page id");
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  Page *page;
  if (page_table_->Find(page_id, frame_id)) {
    page = pages_ + frame_id;
    page->pin_count_++;
  } else if (replacer_->Evict(&frame_id)) {
    page = pages_ + frame_id;
    if (page->IsDirty()) {

      // expensive disk write/read should release the latch_
      page->RLatch();
      latch_.unlock();
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->RUnlatch();
      latch_.lock();

      page->is_dirty_ = false;
    }

    page->page_id_ = page_id;

    // expensive disk write/read should release the latch_
    page->WLatch();
    latch_.unlock();
    disk_manager_->ReadPage(page_id, page->GetData());
    page->WUnlatch();
    latch_.lock();

    page_table_->Insert(page_id, frame_id);
  } else {
    return nullptr;
  }
  PinPageInternal(page, frame_id);
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "invalid page id");
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  Page *page;
  if (page_table_->Find(page_id, frame_id)) {
    page = &pages_[frame_id];
    page->is_dirty_ = is_dirty;
    if (page->GetPinCount() > 0) {
      --page->pin_count_;
      if (page->pin_count_ == 0) {
        replacer_->SetEvictable(frame_id, true);
      }
      return true;
    }
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  Page *page;
  if (page_table_->Find(page_id, frame_id)) {
    page = &pages_[frame_id];

    // expensive disk write/read should release the latch_
    page->RLatch();
    latch_.unlock();
    disk_manager_->WritePage(page_id, page->GetData());
    page->RUnlatch();
    latch_.lock();

    page->is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  Page *page;
  for (size_t i = 0; i < pool_size_; i++) {
    page = &pages_[i];
    if (page->GetPageId() != INVALID_PAGE_ID) {
      // expensive disk write/read should release the latch_
      page->RLatch();
      latch_.unlock();
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->RUnlatch();
      latch_.lock();
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "invalid page id");
  frame_id_t frame_id;
  Page *page;
  std::scoped_lock<std::mutex> lock(latch_);
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }

  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::PinPageInternal(Page *page, frame_id_t frame_id) -> int {
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return ++page->pin_count_;
}

}  // namespace bustub
