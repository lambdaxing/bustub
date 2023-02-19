
/**
 * index_iterator.cpp
 */
#include <cassert>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *page, int index, BufferPoolManager *buffer_pool_manager)
    : page_(page), index_(index), buffer_pool_manager_(buffer_pool_manager) {
  if (page_ != nullptr) {
    leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_ != nullptr) {
    Destroy();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BUSTUB_ASSERT(!IsEnd(), " ");
  return leaf_->GetArray()[index_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BUSTUB_ASSERT(!IsEnd(), " ");
  index_++;
  if (index_ == leaf_->GetSize()) {
    auto leaf_next_page_id = leaf_->GetNextPageId();
    Destroy();
    if (leaf_next_page_id != INVALID_PAGE_ID) {
      Construct(leaf_next_page_id);
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::Destroy() {
  auto leaf_page_id = leaf_->GetPageId();
  page_->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  page_ = nullptr;
  leaf_ = nullptr;
  index_ = 0;
}

INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::Construct(page_id_t page_id) {
  page_ = buffer_pool_manager_->FetchPage(page_id);
  page_->RLatch();  // May be deadlock !!!
  leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
  index_ = 0;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
