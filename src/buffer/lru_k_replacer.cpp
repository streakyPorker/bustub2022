//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  for (BiListNode *node = non_cache_list_.head_; node != nullptr; node = node->next) {
    if (node->evictable) {
      *frame_id = node->frame_id_;
      non_cache_list_.remove(node);
      non_cache_map_.erase(node->frame_id_);
      delete node;
      curr_size_--;
      return true;
    }
  }

  for (BiListNode *node = cache_list_.head_; node != nullptr; node = node->next) {
    if (node->evictable) {
      *frame_id = node->frame_id_;
      cache_list_.remove(node);
      cache_map_.erase(node->frame_id_);
      delete node;
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < (int)replacer_size_, "invalid frame id");
  std::scoped_lock<std::mutex> lock(latch_);
  BiListNode *node;
  if (non_cache_map_.count(frame_id) != 0) {
    node = non_cache_map_[frame_id];
    non_cache_list_.remove(node);

    if (node->access(current_timestamp_) != SIZE_MAX) {  // promote to cache_list_
      non_cache_map_.erase(frame_id);
      InsertToCacheList(cache_list_.head_, node);
      cache_map_[frame_id] = node;
    } else {  // still here, but the last to evict
      non_cache_list_.push(node);
    }
  } else if (cache_map_.count(frame_id) != 0) {
    node = cache_map_[frame_id];
    node->access(current_timestamp_);
    BiListNode *next = node->next;
    cache_list_.remove(node);
    InsertToCacheList(next, node);

  } else {  // need to add a new node and incr size
    node = new BiListNode(k_, frame_id, current_timestamp_);
    non_cache_map_[frame_id] = node;
    non_cache_list_.push(node);
    curr_size_++;
  }

  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < (int)replacer_size_, "invalid frame id");
  std::scoped_lock<std::mutex> lock(latch_);
  BiListNode *node;
  if (non_cache_map_.count(frame_id) != 0) {
    node = non_cache_map_[frame_id];
  } else if (cache_map_.count(frame_id) != 0) {
    node = cache_map_[frame_id];
  } else {
    return;
  }
  if (node->evictable && !set_evictable) {  // true -> false
    curr_size_--;
  } else if (!node->evictable && set_evictable) {  // false -> true
    curr_size_++;
  }
  node->evictable = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < (int)replacer_size_, "invalid frame id");
  std::scoped_lock<std::mutex> lock(latch_);
  BiListNode *node;
  if (non_cache_map_.count(frame_id) != 0) {
    node = non_cache_map_[frame_id];
    BUSTUB_ASSERT(node->evictable, "removing non-evictable frame");
    non_cache_list_.remove(node);
    non_cache_map_.erase(frame_id);
  } else if (cache_map_.count(frame_id) != 0) {
    node = cache_map_[frame_id];
    BUSTUB_ASSERT(node->evictable, "removing non-evictable frame");
    cache_list_.remove(node);
    cache_map_.erase(frame_id);
  } else {
    return;
  }
  delete node;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

LRUKReplacer::~LRUKReplacer() {
  for (auto iter : cache_map_) {
    delete iter.second;
  }
  for (auto iter : non_cache_map_) {
    delete iter.second;
  }
}

/**
 * insert node into cache list
 * @param iter
 * @param node
 * @require node need to be deleted first
 */
void LRUKReplacer::InsertToCacheList(LRUKReplacer::BiListNode *iter, LRUKReplacer::BiListNode *node) {
  while (iter != nullptr && node->better(iter)) {
    iter = iter->next;
  }
  if (iter == nullptr) {  // add to the end (push)
    cache_list_.push(node);
  } else {  // add to iter`s front
    node->next = iter;
    node->prev = iter->prev;
    if (node->prev != nullptr) {
      node->prev->next = node;
    }else{
      cache_list_.head_ = node;
    }
    iter->prev = node;
  }
}

}  // namespace bustub
