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
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0),
      bucket_size_(bucket_size),
      num_buckets_(1),
      dir_(1, std::make_shared<Bucket>(bucket_size_, 0)) {}

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
  return FindInternal(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::FindInternal(const K &key, V &value) -> bool {
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  return RemoveInternal(key);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RemoveInternal(const K &key) -> bool {
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  InsertInternal(key, value);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  auto index = IndexOf(key);
  auto bucket = dir_[index];
  while (!bucket->Insert(key, value)) {
    // If the local depth of the bucket is equal to the global depth,
    // increment the global depth and double the size of the directory.
    if (bucket->GetDepth() == global_depth_) {
      global_depth_++;
      size_t dir_old_size = dir_.size();
      dir_.resize(dir_old_size * 2);
      for (size_t i = dir_old_size; i < dir_.size(); i++) {
        dir_[i] = dir_[i - dir_old_size];
      }
    }
    // Increment the local depth of the bucket.
    bucket->IncrementDepth();
    // Split the bucket and redistribute directory pointers.
    auto new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
    num_buckets_++;
    size_t mask = (1 << bucket->GetDepth()) - 1;
    for (size_t i = 0; i < dir_.size(); i++) {
      if ((index & mask) == (i & mask)) {
        dir_[i] = new_bucket;
      }
    }
    // Redistribute the kv pairs in the bucket.
    auto bucket_items = bucket->GetItems();
    for (auto it = bucket_items.begin(); it != bucket_items.end(); it++) {
      if ((index & mask) == (IndexOf(it->first) & mask)) {
        new_bucket->Insert(it->first, it->second);
        bucket->Remove(it->first);
      }
    }

    // Get the new index and bucket for retrying
    index = IndexOf(key);
    bucket = dir_[index];
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key) -> decltype(list_.begin()) {
  return std::find_if(list_.begin(), list_.end(), [&key](const auto &iter) { return iter.first == key; });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto result = Find(key);
  return result == list_.end() ? false : (value = result->second, true);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto result = Find(key);
  if (result == list_.end()) {
    return false;
  }
  list_.erase(result);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto result = Find(key);
  if (result == list_.end()) {
    return IsFull() ? false : (list_.emplace_back(std::make_pair(key, value)), true);
  }
  result->second = value;
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
