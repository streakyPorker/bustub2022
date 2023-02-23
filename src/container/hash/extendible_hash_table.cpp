//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "murmur3/MurmurHash3.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t dir_index = IndexOf(key);
  return dir_[dir_index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t bucket_index = IndexOf(key);
  while (!dir_[bucket_index]->Insert(key, value)) {  // insert fail
    std::shared_ptr<Bucket> bucket = dir_[bucket_index];
    assert(bucket->GetDepth() <= global_depth_);
    if (bucket->GetDepth() == global_depth_) {  // need to expand
      ExpandHashtable();
    }
    RedistributeBucket(bucket, bucket_index);
    bucket_index = IndexOf(key);  // recalculate the bucket_index to find the right place
  }
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, size_t bucket_idx) -> void {
  bucket->IncrementDepth();  // incr the local depth
  int new_depth = bucket->GetDepth();
  size_t twin_bucket_idx = bucket_idx | (1 << (new_depth - 1));
  assert(dir_[twin_bucket_idx] == bucket);

  // split the bucket
  dir_[twin_bucket_idx] = std::make_shared<Bucket>(bucket_size_, new_depth);
  num_buckets_++;

  auto &items = bucket->GetItems();
  std::list<std::pair<K, V>> tmp(items);
  items.clear();
  int mod = (1 << new_depth) - 1;
  for (const std::pair<K, V> &item : tmp) {
    // prev key should hash into the prev bucket or its twin
    // these insertions shouldn`t fail
    assert(dir_[std::hash<K>()(item.first) & mod]->Insert(item.first, item.second));
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::ExpandHashtable() -> void {
  assert(dir_.size() == (size_t)((1 << global_depth_)));
  for (int i = 0; i < (1 << global_depth_); i++) {
    dir_.push_back(dir_[i]);
  }
  global_depth_++;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(bucket_latch_);
  for (const auto &iter : list_) {
    if (iter.first == key) {
      value = iter.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(bucket_latch_);
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      list_.remove(*iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  std::scoped_lock<std::mutex> lock(bucket_latch_);
  for (auto &iter : list_) {
    if (iter.first == key) {  // overwrite
      iter.second = value;
      return true;
    }
  }
  if (!IsFull()) {
    list_.push_back(std::make_pair(key, value));
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
