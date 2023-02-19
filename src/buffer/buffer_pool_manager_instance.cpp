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
  // Find replacement frame from the free list first
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {  // Else, find from the replacer
    // Set page_id to nullptr if all frames are currently in use and not evictable (in another word, pinned).
    if (!replacer_->Evict(&frame_id)) {
      page_id = nullptr;
      return nullptr;
    }
    auto &old_page = pages_[frame_id];
    page_id_t old_page_pageid = old_page.GetPageId();
    // If the old page is dirty, write it back to disk first.
    if (old_page.IsDirty()) {
      disk_manager_->WritePage(old_page_pageid, old_page.GetData());
      // Unset the dirty flag of the page after flushing.
      old_page.is_dirty_ = false;
    }
    // Update the page table.
    page_table_->Remove(old_page_pageid);
    // Update the metadata of the new page.
    ResetPageWithFrameId(frame_id);
  }
  // Call the AllocatePage() method to get a new page id.
  *page_id = AllocatePage();
  // Update the page table and replacer.
  page_table_->Insert(*page_id, frame_id);

  // Record the access history of the frame in the replacer.
  replacer_->RecordAccess(frame_id);
  // Set new page id to the page.
  pages_[frame_id].page_id_ = *page_id;
  // "Pin" the frame.
  pages_[frame_id].pin_count_++;
  replacer_->SetEvictable(frame_id, false);
  return &(pages_[frame_id]);
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // First search for page_id in the buffer pool.
  if (!page_table_->Find(page_id, frame_id)) {
    // If not found, pick a replacement frame from either the free list or the replacer
    // (always find from the free list first).
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {                               // Find from the replacer
      if (!replacer_->Evict(&frame_id)) {  // Already remove from the replacer
        return nullptr;
      }
      auto &old_page = pages_[frame_id];
      page_id_t old_page_pageid = old_page.GetPageId();
      // If the old page is dirty, write it back to disk.
      if (old_page.IsDirty()) {
        disk_manager_->WritePage(old_page_pageid, old_page.GetData());
        // Unset the dirty flag of the page after flushing.
        old_page.is_dirty_ = false;
      }
      // Update the page table.
      page_table_->Remove(old_page_pageid);
      // Update the metadata of the new page.
      ResetPageWithFrameId(frame_id);
    }
    // Set new page id to the page.
    pages_[frame_id].page_id_ = page_id;
    // Update the page table and replacer.
    page_table_->Insert(page_id, frame_id);
    // Read the page from disk and replace the old page in the frame.
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  }
  // Disable eviction and record the access history of the frame.
  replacer_->RecordAccess(frame_id);  // Record first to insert a new record.
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;
  return &(pages_[frame_id]);
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // If page_id is not in the buffer pool, return true !!!!!!!!!!!!!!!!
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  // If its pin count is already 0, return false !!!!!
  if (pages_[frame_id].GetPinCount() == 0) {
    return false;
  }
  auto &page = pages_[frame_id];
  // Decrement the pin count of a page.
  page.pin_count_--;
  // If the pin count reaches 0, the frame should be evictable by the replacer.
  if (page.GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  // Set the dirty flag on the page to indicate if the page was modified.
  if (is_dirty) {
    page.is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // Return false if the page could not be found in the page table or is INVALID_PAGE_ID.
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, frame_id)) {
    return false;
  }
  auto &page = pages_[frame_id];
  // Flush the page to disk.
  disk_manager_->WritePage(page_id, page.GetData());
  // Unset the dirty flag of the page after flushing.
  page.is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  // Flush all the pages in the buffer pool to disk.
  for (size_t i = 0; i < pool_size_; i++) {
    auto frame_id = static_cast<frame_id_t>(i);
    auto &page = pages_[frame_id];
    page_id_t page_id = page.GetPageId();
    if (page_id == INVALID_PAGE_ID) {
      continue;
    }
    disk_manager_->WritePage(page_id, page.GetData());
    page.is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  // If page_id is not in the buffer pool, do nothing and return true.
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  // If the page is pinned and cannot be deleted, return false immediately.
  if (pages_[frame_id].GetPinCount() != 0) {
    return false;
  }
  // Delete the page from the page table.
  page_table_->Remove(page_id);
  // Stop tracking the frame in the replacer.
  replacer_->Remove(frame_id);
  // Add the frame back to the free list.
  free_list_.push_back(frame_id);
  // Reset the page's memory and metadata.
  ResetPageWithFrameId(frame_id);
  // Finally, call DeallocatePage() to imitate freeing the page on the disk.
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
