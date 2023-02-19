
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
  // Find the first evictable frame in the ordered list access_history.
  auto it = std::find_if(access_history_.cbegin(), access_history_.cend(),
                         [this](const auto &it) { return this->evictable_map_.count(it.first) != 0U; });
  // No frames can be evicted.
  if (it == access_history_.cend()) {
    return false;
  }
  // Else, evict the frame.
  *frame_id = it->first;
  evictable_map_.erase(it->first);
  access_history_.erase(it);
  // A frame is evicted successfully.
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // Invalid frame id.
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_,
                "Frame id is invalid (ie. larger than replacer_size_).");
  // Find the frame id in the map.
  auto map_iter = evictable_map_.find(frame_id);
  if (map_iter == evictable_map_.end()) {
    map_iter = non_evictable_map_.find(frame_id);
  }
  // Create a new entry for access history if frame id has not been seen before.
  if (map_iter == non_evictable_map_.end()) {
    auto access_history_insert_iter = std::find_if(access_history_.begin(), access_history_.end(),
                                                   [](const auto &it) { return it.second.back() != 0; });
    auto list_iter = access_history_.emplace(access_history_insert_iter, frame_id, std::vector<size_t>(k_ + 1, 0));
    auto result = non_evictable_map_.emplace(frame_id, list_iter);
    map_iter = result.first;
  }
  auto list_iter = map_iter->second;
  // The given frame id is accessed at current timestamp.
  auto &record_access_of_frame_id = list_iter->second;
  for (int i = k_; i > 1; i--) {
    record_access_of_frame_id[i] = record_access_of_frame_id[i - 1];
  }
  current_timestamp_++;
  record_access_of_frame_id[1] = current_timestamp_;
  // If k—distance is +inf，resort as the earliest timestamp overall.
  if (record_access_of_frame_id[k_] != 0) {
    auto after_new_pos = list_iter;
    while ((++after_new_pos) != access_history_.end() &&
           (after_new_pos->second[k_] == 0 || after_new_pos->second[k_] < list_iter->second[k_])) {
    }
    access_history_.splice(after_new_pos, access_history_, list_iter);
  }
  //   // For debug.
  //   PrintAccessHistory();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // Invalid frame id.
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_,
                "Frame id is invalid (ie. larger than replacer_size_).");
  if (set_evictable && evictable_map_.count(frame_id) == 0U && non_evictable_map_.count(frame_id) != 0) {
    evictable_map_[frame_id] = non_evictable_map_[frame_id];
    non_evictable_map_.erase(frame_id);
  } else if (!set_evictable && evictable_map_.count(frame_id) != 0U && non_evictable_map_.count(frame_id) == 0U) {
    non_evictable_map_[frame_id] = evictable_map_[frame_id];
    evictable_map_.erase(frame_id);
  }
  // For other scenarios, this function should terminate without modifying anything.
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // Remove is called on a non-evictable frame,
  // throw an exception or abort the process.
  if (non_evictable_map_.count(frame_id) != 0U) {
    UNREACHABLE("LRUKReplacer::Remove(frame_id_t) is called on a non-evictable frame.");
  }
  // If specified frame is not found, directly return from this function.
  if (evictable_map_.count(frame_id) == 0U) {
    return;
  }
  // Remove the evictable frame from replacer, along with its access history.
  access_history_.erase(evictable_map_[frame_id]);
  evictable_map_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return evictable_map_.size();
}
}  // namespace bustub
