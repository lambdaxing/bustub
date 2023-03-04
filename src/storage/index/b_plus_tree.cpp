#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // Get the leaf contains the key.
  LeafPage *leaf = FindLeafPage(key, BPlusTreeOpType::B_PLUS_OP_FIND, transaction);
  if (leaf == nullptr) {
    ReleasePagesInTransaction(false, transaction, INVALID_PAGE_ID);
    return false;
  }
  ValueType value;
  bool exist = leaf->Find(key, value, comparator_);
  if (exist) {
    result->push_back(value);
  }
  // Release transaction OR leaf page.
  ReleasePagesInTransaction(false, transaction, leaf->GetPageId());
  return exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  bool exclusive = true;
  LeafPage *leaf = FindLeafPage(key, BPlusTreeOpType::B_PLUS_OP_INSERT, transaction);
  // If tree is empty, create a new empty leaf, and it's the root.
  if (leaf == nullptr) {
    auto page = CrabbingProtocolNewPage(&root_page_id_, transaction);
    leaf = reinterpret_cast<LeafPage *>(page->GetData());
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    UpdateRootPageId(1);
  }
  bool result = leaf->Insert(key, value, comparator_);
  if (result) {
    // Splitting condition: number of key/value pairs AFTER insertion equals to max_size + 1 for leaf nodes.
    if (leaf->GetSize() == leaf_max_size_ + 1) {
      // Create new leaf node.
      page_id_t page_id;
      auto page = CrabbingProtocolNewPage(&page_id, transaction);
      auto new_leaf = reinterpret_cast<LeafPage *>(page->GetData());
      new_leaf->Init(page_id, leaf->GetParentPageId(), leaf_max_size_);
      // Split leaf and insert new entry to parent.
      SplitPage(leaf, new_leaf);
      InsertInParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
    }
  }
  ReleasePagesInTransaction(exclusive, transaction);
  return result;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LeafPage *leaf = FindLeafPage(key, BPlusTreeOpType::B_PLUS_OP_REMOVE, transaction);
  if (leaf != nullptr) {
    Remove(leaf, key, transaction, ++(transaction->GetPageSet()->crbegin()));
  }
  ReleasePagesInTransaction(true, transaction);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto leaf = FindLeafPage(KeyType(), BPlusTreeOpType::B_PLUS_OP_FIND, nullptr, true);
  if (leaf == nullptr) {
    ReleasePagesInTransaction(false, nullptr, INVALID_PAGE_ID);
    return End();
  }
  auto page = buffer_pool_manager_->FetchPage(leaf->GetPageId());
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  // Page read locked and pincout equals 1.
  return INDEXITERATOR_TYPE(page, 0, buffer_pool_manager_);
}
/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto leaf = FindLeafPage(key, BPlusTreeOpType::B_PLUS_OP_FIND, nullptr);
  if (leaf == nullptr) {
    ReleasePagesInTransaction(false, nullptr, INVALID_PAGE_ID);
    return End();
  }
  auto page = buffer_pool_manager_->FetchPage(leaf->GetPageId());
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  int index = leaf->FindIndex(key, comparator_);
  if (index == leaf->GetSize()) {
    ReleasePagesInTransaction(false, nullptr, leaf->GetPageId());
    return End();
  }
  // Page read locked and pincout equals 1.
  return INDEXITERATOR_TYPE(page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/**
 * @brief 将 middle_key 和 new_page 的 page id 作为一个条目插入到 old_page 的父结点中。
 * @param old_page
 * @param middle_key
 * @param new_page
 * @param transaction
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *old_page, const KeyType &middle_key, BPlusTreePage *new_page,
                                    Transaction *transaction) {
  // If old page is root of the tree.
  if (old_page->IsRootPage()) {
    // Create node contains old_page id、key、new_page id
    auto page = CrabbingProtocolNewPage(&root_page_id_, transaction);
    auto root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    old_page->SetParentPageId(root_page_id_);
    new_page->SetParentPageId(root_page_id_);
    root->SetValueAt(0, old_page->GetPageId());  // No key in 0 of internal page
    root->SetKeyAt(1, middle_key);
    root->SetValueAt(1, new_page->GetPageId());
    root->IncreaseSize(2);
    UpdateRootPageId(0);
  } else {
    page_id_t page_id = new_page->GetPageId();
    auto page = GetParentAndReleaseChildren(transaction);
    auto parent_page = reinterpret_cast<InternalPage *>(page->GetData());
    // Insert key&new_page after old_page
    parent_page->InsertToRight(middle_key, page_id, comparator_);
    if (parent_page->GetSize() == internal_max_size_ + 1) {  // Split parent_page
      auto page = CrabbingProtocolNewPage(&page_id, transaction);
      auto new_internal_page = reinterpret_cast<InternalPage *>(page->GetData());
      new_internal_page->Init(page_id, parent_page->GetParentPageId(), internal_max_size_);
      SplitPage(parent_page, new_internal_page);
      InsertInParent(parent_page, new_internal_page->KeyAt(0), new_internal_page, transaction);
    }
  }
}

/**
 * @brief 分裂 old_page 为 new_page。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitPage(BPlusTreePage *old_page, BPlusTreePage *new_page) {
  int entry_size;
  int max_size;
  if (old_page->IsLeafPage()) {
    entry_size = sizeof(MappingType);
    max_size = leaf_max_size_;
    // Update next page ids in the old page and new page.
    static_cast<LeafPage *>(new_page)->SetNextPageId(static_cast<LeafPage *>(old_page)->GetNextPageId());
    static_cast<LeafPage *>(old_page)->SetNextPageId(new_page->GetPageId());
  } else {
    entry_size = sizeof(std::pair<KeyType, page_id_t>);
    max_size = internal_max_size_;
  }
  // Move end min size entries to new leaf page.
  int rest_size = old_page->GetMinSize();
  int move_size = max_size + 1 - rest_size;
  memmove(GetArrayAddr(new_page), GetArrayAddr(old_page) + (rest_size * entry_size), move_size * entry_size);
  old_page->SetSize(rest_size);
  new_page->SetSize(move_size);
}

/**
 * @brief 从 B+ tree 的 b_plus_page 开始删除 key。
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(BPlusTreePage *b_plus_page, const KeyType &key, Transaction *transaction,
                            decltype(transaction->GetPageSet()->crbegin()) transaction_riter) {
  // Remove key from b_plus_page.
  if (!RemoveKey(b_plus_page, key)) {
    // Remove failed, return.
    return;
  }
  page_id_t page_id = b_plus_page->GetPageId();
  // If b_plus_page is root.
  if (b_plus_page->IsRootPage()) {
    if (b_plus_page->IsLeafPage() && b_plus_page->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(-1);
      transaction->AddIntoDeletedPageSet(page_id);
    } else if (b_plus_page->IsInternalPage() && b_plus_page->GetSize() == 1) {
      root_page_id_ = static_cast<InternalPage *>(b_plus_page)->ValueAt(0);
      // Now, new root page must be write locked.
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData())
          ->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      UpdateRootPageId(0);
      transaction->AddIntoDeletedPageSet(page_id);
    } else {
      // The root can have 2 ~ n pairs (internal node) or 1~n pairs (leaf node).
    }
    // Else if, size of node (not root) is too small.
  } else if (b_plus_page->GetSize() < b_plus_page->GetMinSize()) {
    auto page = *transaction_riter;
    transaction_riter++;
    auto parent = reinterpret_cast<InternalPage *>(page->GetData());
    bool brother_is_right;
    KeyType middle_key;
    auto brother = GetBrother(parent, page_id, transaction, &brother_is_right, middle_key);
    int max_size = b_plus_page->IsLeafPage() ? leaf_max_size_ : internal_max_size_;
    // Merge right node to left node, for updating the next page id easily.
    if (b_plus_page->GetSize() + brother->GetSize() <= max_size) {
      if (brother_is_right) {
        std::swap(brother, b_plus_page);
      }
      // Move right to left.
      MergeRightToLeft(brother, b_plus_page, middle_key, transaction);
      // Remove middle_key from parent.
      Remove(parent, middle_key, transaction, transaction_riter);
      // Redistribution: borrow an index item from brother
    } else {
      BorrowFromBrother(parent, b_plus_page, brother, brother_is_right, middle_key);
    }
  } else {
    // The B+ tree property is still satisfied after delete. Do nothing!
  }
}

/**
 * @brief 从 b_plus_page 中删除 key
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveKey(BPlusTreePage *b_plus_page, const KeyType &key) -> bool {
  if (b_plus_page->IsLeafPage()) {
    return static_cast<LeafPage *>(b_plus_page)->Remove(key, comparator_);
  }
  bool assert_value = static_cast<InternalPage *>(b_plus_page)->Remove(key, comparator_);
  assert(assert_value);
  return assert_value;
}

/**
 * @brief 获取兄弟结点。
 * @param parent 用于更新父结点
 * @param page_id 结点 page id
 * @param transaction 将兄弟结点加入 page set  统一处理
 * @param brother_is_right 兄弟结点是否为右兄弟
 * @param middle_key 与兄弟结点在父结点中的分隔（中间）key 值
 * @return BPlusTreePage* 兄弟结点页的指针
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBrother(InternalPage *parent, page_id_t page_id, Transaction *transaction,
                                bool *brother_is_right, KeyType &middle_key) -> BPlusTreePage * {
  page_id_t brother_page_id = INVALID_PAGE_ID;
  // Find brother page id and middle key
  int i;
  for (i = 0; i < parent->GetSize(); i++) {
    if (parent->ValueAt(i) == page_id) {
      break;
    }
  }
  assert(i != parent->GetSize());
  if (i == parent->GetSize() - 1) {
    *brother_is_right = false;
    brother_page_id = parent->ValueAt(i - 1);
    middle_key = parent->KeyAt(i);
  } else {
    *brother_is_right = true;
    brother_page_id = parent->ValueAt(i + 1);
    middle_key = parent->KeyAt(i + 1);
  }
  assert(brother_page_id != INVALID_PAGE_ID);
  auto page = buffer_pool_manager_->FetchPage(brother_page_id);
  page->WLatch();
  transaction->AddIntoPageSet(page);
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

/**
 * @brief Move all key&value pairs of right to left.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeRightToLeft(BPlusTreePage *left, BPlusTreePage *right, const KeyType &middle_key,
                                      Transaction *transaction) {
  int entry_size = left->IsLeafPage() ? sizeof(MappingType) : sizeof(std::pair<KeyType, page_id_t>);
  memmove(GetArrayAddr(left) + (left->GetSize() * entry_size), GetArrayAddr(right), entry_size * right->GetSize());
  if (left->IsLeafPage()) {
    static_cast<LeafPage *>(left)->SetNextPageId(static_cast<LeafPage *>(right)->GetNextPageId());
    left->IncreaseSize(right->GetSize());
  } else {
    static_cast<InternalPage *>(left)->SetKeyAt(left->GetSize(), middle_key);
    left->IncreaseSize(right->GetSize());
  }
  transaction->AddIntoDeletedPageSet(right->GetPageId());
}

/**
 * @brief Borrow an index item from brother.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromBrother(InternalPage *parent, BPlusTreePage *b_plus_page, BPlusTreePage *brother,
                                       bool brother_is_right, const KeyType &middle_key) {
  int entry_size = b_plus_page->IsLeafPage() ? sizeof(MappingType) : sizeof(std::pair<KeyType, page_id_t>);
  KeyType key;
  if (brother_is_right) {
    memmove(GetArrayAddr(b_plus_page) + (b_plus_page->GetSize() * entry_size), GetArrayAddr(brother), entry_size);
    memmove(GetArrayAddr(brother), GetArrayAddr(brother) + entry_size, (brother->GetSize() - 1) * entry_size);
    b_plus_page->IncreaseSize(1);
    brother->DecreaseSize(1);
    if (b_plus_page->IsLeafPage()) {
      key = static_cast<LeafPage *>(brother)->KeyAt(0);
    } else {
      static_cast<InternalPage *>(b_plus_page)->SetKeyAt(b_plus_page->GetSize() - 1, middle_key);
      key = static_cast<InternalPage *>(brother)->KeyAt(0);
    }
  } else {
    memmove(GetArrayAddr(b_plus_page) + entry_size, GetArrayAddr(b_plus_page), b_plus_page->GetSize() * entry_size);
    memmove(GetArrayAddr(b_plus_page), GetArrayAddr(brother) + (brother->GetSize() - 1) * entry_size, entry_size);
    b_plus_page->IncreaseSize(1);
    brother->DecreaseSize(1);
    if (b_plus_page->IsLeafPage()) {
      key = static_cast<LeafPage *>(b_plus_page)->KeyAt(0);
    } else {
      static_cast<InternalPage *>(b_plus_page)->SetKeyAt(1, middle_key);
      key = static_cast<InternalPage *>(b_plus_page)->KeyAt(0);
    }
  }
  parent->SetKeyAt(parent->FindIndex(middle_key, comparator_), key);
}

/**
 * @brief 找到 key 所在的叶结点，根据操作类型 op 的不同选择不同的加锁策略和类型，transaction
 * 用于保存查找过程中不安全的结点， 安全的定义依不同的操作而不同。默认只有 GetValue 和 Begin 操作的 transaction 等于
 * nullptr，它们加的锁类型是共享锁，且对 安全的定义很宽泛（直接释放父结点），可以不需要 transaction
 * 参数保存不安全结点，因为根本没有不安全的结点。对于所有操作，都将 保护 root_page_id，无论 transaction 参数是否为
 * nullptr。该函数返回后，如果 transaction != nullptr，所有未经解锁和 unpin 的页都保存在 transaction 中；如果
 * transaction == nullptr，仅有最后一个获得的页未解锁未 unpin，其余的父页包括 root_page_id_ 都已经解锁。该函数返回
 * nullptr 时，所有的锁都已被释放。
 * @param key 查找的 key
 * @param op 操作类型
 * @param transaction 或者为 nullptr，或者用于保存不安全页集合
 * @return LeafPage* 指向叶结点的指针，当 B+ Tree 为空时，返回 nullptr。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, BPlusTreeOpType op, Transaction *transaction, bool find_min)
    -> LeafPage * {
  bool exclusive = IsExclusive(op);
  LockRootPageId(exclusive, transaction);
  if (IsEmpty()) {
    return nullptr;
  }
  page_id_t previous = INVALID_PAGE_ID;
  auto current = root_page_id_;
  auto b_plus_tree_page = CrabbingProtocolFetchPage(current, op, previous, transaction);
  // Search down until find the leaf page
  while (!b_plus_tree_page->IsLeafPage()) {
    auto internal_page = static_cast<InternalPage *>(b_plus_tree_page);
    previous = current;
    if (find_min) {
      current = internal_page->ValueAt(0);
    } else {
      current = internal_page->FindChild(key, comparator_);
    }
    b_plus_tree_page = CrabbingProtocolFetchPage(current, op, previous, transaction);
  }
  // Reinterpret cast a leaf page
  return reinterpret_cast<LeafPage *>(b_plus_tree_page);
}

/**
 * @brief 获取页面并以 Crabbing Protocol 的方式加锁，依据操作不同加不同的锁，应用不用的加锁策略。
 * @param page_id 获取页面的 page id
 * @param op 操作类型
 * @param previous 加锁策略中的父结点页面 page id
 * @param transaction 或者为 nullptr，或者用于保存不安全页集合，以便在安全时释放该集合中的页
 * @return BPlusTreePage* 获得的 B Plus Page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CrabbingProtocolFetchPage(page_id_t page_id, BPlusTreeOpType op, page_id_t previous,
                                               Transaction *transaction) -> BPlusTreePage * {
  bool exclusive = IsExclusive(op);
  auto page = buffer_pool_manager_->FetchPage(page_id);
  if (exclusive) {
    page->WLatch();
  } else {
    page->RLatch();
  }
  auto b_plus_tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (!exclusive || IsSafe(b_plus_tree_page, op)) {
    // Release all locks on ancestors.
    ReleasePagesInTransaction(exclusive, transaction, previous);
  }
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  return b_plus_tree_page;
}

/**
 * @brief ReleasePagesInTransaction 在 transaction != nullptr 时负责处理 transaction 里 pages 的锁释放和页删除（包括
 * root_page_id 的锁释放。当 transaction == nullptr 时负责处理 previous 的锁释放以及 root_page_id 的锁释放。
 * 注意，默认只有 B+ Tree 的 GetValue 、Begin 操作时 transaction 可能等于 nullptr，Insert 和 Remove 默认 transaction !=
 * nullptr 。
 * @param exclusive 加的锁类型是否为排他锁
 * @param transaction 或者为 nullptr，或者保存了需要释放锁或删除的页面集合
 * @param previous 在 transaction == nullptr 时负责释放 previous 的锁，若为 INVALID_PAGE_ID 表示释放 root_page_id_ 的锁
 * @return INDEX_TEMPLATE_ARGUMENTS
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleasePagesInTransaction(bool exclusive, Transaction *transaction, page_id_t previous) {
  if (transaction == nullptr) {
    if (previous == INVALID_PAGE_ID) {
      UnLockRootPageId(exclusive);
    } else {
      auto page = buffer_pool_manager_->FetchPage(previous);
      if (exclusive) {
        page->WUnlatch();
      } else {
        page->RUnlatch();
      }
      buffer_pool_manager_->UnpinPage(previous, exclusive);
      buffer_pool_manager_->UnpinPage(previous, false);
    }
    return;
  }

  for (Page *page : *(transaction->GetPageSet())) {
    if (page == nullptr) {
      // Release root page id
      UnLockRootPageId(exclusive);
      continue;
    }
    if (exclusive) {
      page->WUnlatch();
    } else {
      page->RUnlatch();
    }
    auto page_id = page->GetPageId();
    buffer_pool_manager_->UnpinPage(page_id, exclusive);
    if (transaction->GetDeletedPageSet()->find(page_id) != transaction->GetDeletedPageSet()->end()) {
      while (!buffer_pool_manager_->DeletePage(page_id)) {
      }
      transaction->GetDeletedPageSet()->erase(page_id);
    }
  }
  transaction->GetPageSet()->clear();
}

/**
 * @brief 获取一个新的 page，并加锁，加入 transaction。之后适时释放或统一释放。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CrabbingProtocolNewPage(page_id_t *page_id, Transaction *transaction) -> Page * {
  auto page = buffer_pool_manager_->NewPage(page_id);
  page->WLatch();
  transaction->AddIntoPageSet(page);
  return page;
}

/**
 * @brief 锁住 root_page_id_
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::LockRootPageId(bool exclusive, Transaction *transaction) {
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(nullptr);
  }
  if (exclusive) {
    rwlatch_.WLock();
  } else {
    rwlatch_.RLock();
  }
}

/**
 * @brief 解锁 root_page_id_
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnLockRootPageId(bool exclusive) {
  if (exclusive) {
    rwlatch_.WUnlock();
  } else {
    rwlatch_.RUnlock();
  }
}

/**
 * @brief 判断在当前结点（页面 page ）进行 op 操作是否安全。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(BPlusTreePage *page, BPlusTreeOpType op) -> bool {
  if (op == BPlusTreeOpType::B_PLUS_OP_INSERT) {
    return page->GetSize() < page->GetMaxSize();
  }
  if (op == BPlusTreeOpType::B_PLUS_OP_REMOVE) {
    return page->GetSize() > page->GetMinSize();
  }
  assert(false);
}

/**
 * @brief 判断操作是否排他。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsExclusive(BPlusTreeOpType op) -> bool { return op != BPlusTreeOpType::B_PLUS_OP_FIND; }

/**
 * @brief 获得 page 中 array_ 的开始地址。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetArrayAddr(BPlusTreePage *page) -> u_int8_t * {
  if (page->IsLeafPage()) {
    return reinterpret_cast<u_int8_t *>(page) + LEAF_PAGE_HEADER_SIZE;
  }
  if (page->IsInternalPage()) {
    return reinterpret_cast<u_int8_t *>(page) + INTERNAL_PAGE_HEADER_SIZE;
  }
  assert(false);
}

/**
 * @brief 从 transaction 中释放两个孩子页面并 unpin，返回父页面。
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetParentAndReleaseChildren(Transaction *transaction) -> Page * {
  auto pages = transaction->GetPageSet();
  auto page1 = pages->back();
  page1->WUnlatch();
  buffer_pool_manager_->UnpinPage(page1->GetPageId(), true);
  pages->pop_back();
  auto page2 = pages->back();
  page2->WUnlatch();
  buffer_pool_manager_->UnpinPage(page2->GetPageId(), true);
  pages->pop_back();
  return pages->back();
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record == 1) {
    // create a new record<index_name + root_page_id> in header_page
    assert(header_page->InsertRecord(index_name_, root_page_id_));
  } else if (insert_record == 0) {
    // update root_page_id in header_page
    assert(header_page->UpdateRecord(index_name_, root_page_id_));
  } else if (insert_record == -1) {
    assert(header_page->DeleteRecord(index_name_));
  } else {
    assert(false);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}

/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
