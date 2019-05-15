/*
 * Copyright (C) 2017-2019 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of our NVM-based Data Structures repository.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef DBIS_wBPTree_hpp_
#define DBIS_wBPTree_hpp_

#include <array>
#include <bitset>
#include <iostream>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include "config.h"
#include "utils/ElementOfRankK.hpp"
#include "utils/PersistEmulation.hpp"

namespace dbis::wbptree {

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;
template<typename Object>
using pptr = persistent_ptr<Object>;

/**
 * A persistent memory implementation of a wBPTree.
 *
 * @tparam KeyType the data type of the key
 * @tparam ValueType the data type of the values associated with the key
 * @tparam N the maximum number of keys on a branch node
 * @tparam M the maximum number of keys on a leaf node
 */
template<typename KeyType, typename ValueType, int N, int M>
class wBPTree {
  /// we need at least two keys on a branch node to be able to split
  static_assert(N > 2, "number of branch keys has to be >2.");
  /// we need at least one key on a leaf node
  static_assert(M > 0, "number of leaf keys should be >0.");

#ifndef UNIT_TESTS
  private:
#else
  public:
#endif

  /// Forward declarations
  struct LeafNode;
  struct BranchNode;

  struct Node {
    Node() : tag(BLANK) {};

    Node(pptr<LeafNode> leaf_) : tag(LEAF), leaf(leaf_) {};

    Node(pptr<BranchNode> branch_) : tag(BRANCH), branch(branch_) {};

    Node(const Node &other) { copy(other); };

    void copy(const Node &other) throw() {
      tag = other.tag;

      switch (tag) {
        case LEAF: {
          leaf = other.leaf;
          break;
        }
        case BRANCH: {
          branch = other.branch;
          break;
        }
        default: break;
      }
    }

    Node &operator=(Node other) {
      copy(other);
      return *this;
    }

    enum NodeType {
      BLANK, LEAF, BRANCH
    } tag;
    union {
      pptr<LeafNode> leaf;
      pptr<BranchNode> branch;
    };
  };

  template<size_t E>
  struct alignas(64) Search {
    std::array<uint8_t, E+1> slot; ///< slot array for indirection, first = num
    std::bitset<E> b;              ///< bitset for valid entries

    unsigned int getFreeZero() const {
      unsigned int idx = 0;
      while (idx < E && b.test(idx)) ++idx;
      return idx;
    }
  };

  /**
   * A structure for representing a leaf node of a B+ tree.
   */
  struct LeafNode {
    /**
     * Constructor for creating a new empty leaf node.
     */
    LeafNode() : nextLeaf(nullptr), prevLeaf(nullptr) {
      search.get_rw().slot[0] = 0;
    }

    p<Search<M>>                search; ///< helper structure for faster searches
    pptr<LeafNode>            nextLeaf; ///< pointer to the subsequent sibling
    pptr<LeafNode>            prevLeaf; ///< pointer to the preceeding sibling
    p<std::array<KeyType, M>>     keys; ///< the actual keys
    p<std::array<ValueType, M>> values; ///< the actual values
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode() { search.get_rw().slot[0] = 0; }

    p<Search<N>>                 search; ///< helper structure for faster searches
    p<std::array<KeyType, N>>      keys; ///< the actual keys
    p<std::array<Node, N + 1>> children; ///< pointers to child nodes (BranchNode or LeafNode)
  };

  /**
   * Create a new empty leaf node
   */
  pptr<LeafNode> newLeafNode() {
    auto pop = pmem::obj::pool_by_vptr(this);
    pptr<LeafNode> newNode = nullptr;
    transaction::run(pop, [&] { newNode = make_persistent<LeafNode>(); });
    return newNode;
  }

  pptr<LeafNode> newLeafNode(const pptr<LeafNode> &other) {
    auto pop = pmem::obj::pool_by_vptr(this);
    pptr<LeafNode> newNode = nullptr;
    transaction::run(pop, [&] { newNode = make_persistent<LeafNode>(*other); });
    return newNode;
  }

  void deleteLeafNode(const pptr<LeafNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] { delete_persistent<LeafNode>(node); });
  }

  /**
   * Create a new empty branch node
   */
  pptr<BranchNode> newBranchNode() {
    auto pop = pmem::obj::pool_by_vptr(this);
    pptr<BranchNode> newNode = nullptr;
    transaction::run(pop, [&] { newNode = make_persistent<BranchNode>(); });
    return newNode;
  }

  void deleteBranchNode(const pptr<BranchNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] { delete_persistent<BranchNode>(node); });
  }

  /**
   * A structure for passing information about a node split to the caller.
   */
  struct SplitInfo {
    KeyType key;     ///< the key at which the node was split
    Node leftChild;  ///< the resulting lhs child node
    Node rightChild; ///< the resulting rhs child node
  };

  p<unsigned int> depth; /**< the depth of the tree, i.e. the number of levels
                              (0 => rootNode is LeafNode) */
  Node rootNode; ///< pointer to the root node

 public:

  /**
   * Typedef for a function passed to the scan method.
   */
  using ScanFunc = std::function<void(const KeyType &key, const ValueType &val)>;

  /**
   * Iterator for iterating over the leaf nodes
   */
  class iterator {
    pptr<LeafNode> currentNode;
    size_t currentPosition;

    public:
    iterator() : currentNode(nullptr), currentPosition(0) {}
    iterator(const Node &root, size_t d) {
      /// traverse to left-most key
      auto node = root;
      while (d-- > 0) {
        auto n = node.branch;
        node = n->children.get_ro()[0];
      }
      currentNode = node.leaf;
      currentPosition = 0;
      const auto &nodeRef = *currentNode;
      /// Can not overflow as there are at least M/2 entries
      while(!nodeRef.search.get_ro().b.test(currentPosition)) ++currentPosition;
    }

    iterator& operator++() {
      if (currentPosition >= M-1) {
        currentNode = currentNode->nextLeaf;
        currentPosition = 0;
        if (currentNode == nullptr) return *this;
        const auto &nodeRef = *currentNode;
        while(!nodeRef.search.get_ro().b.test(currentPosition)) ++currentPosition;
      } else if (!currentNode->search.get_ro().b.test(++currentPosition)) ++(*this);
      return *this;
    }
    iterator operator++(int) {iterator retval = *this; ++(*this); return retval;}

    bool operator==(iterator other) const {
      return (currentNode == other.currentNode && currentPosition == other.currentPosition);
    }

    bool operator!=(iterator other) const { return !(*this == other); }

    std::pair<KeyType, ValueType> operator*() {
      return std::make_pair(currentNode->keys.get_ro()[currentPosition],
                            currentNode->values.get_ro()[currentPosition]);
    }

    /// iterator traits
    using difference_type = long;
    using value_type = std::pair<KeyType, ValueType>;
    using pointer = const std::pair<KeyType, ValueType>*;
    using reference = const std::pair<KeyType, ValueType>&;
    using iterator_category = std::forward_iterator_tag;
  };
  iterator begin() { return iterator(rootNode, depth); }
  iterator end() { return iterator(); }

  /**
   * Constructor for creating a new  tree.
   */
  wBPTree() : depth(0) {
    rootNode = newLeafNode();
    LOG("created new wBPTree with sizeof(BranchNode) = " << sizeof(BranchNode) <<
        ", sizeof(LeafNode) = " << sizeof(LeafNode));
  }

  /**
   * Destructor for the tree. Should delete all allocated nodes.
   */
  ~wBPTree() {}

  /**
   * Insert an element (a key-value pair) into the tree. If the key @c key already exists, the
   * corresponding value is replaced by @c val.
   *
   * @param key the key of the element to be inserted
   * @param val the value that is associated with the key
   */
  void insert(const KeyType &key, const ValueType &val) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {
      SplitInfo splitInfo;

      bool wasSplit = false;
      if (depth == 0) {
        /// the root node is a leaf node
        auto n = rootNode.leaf;
        wasSplit = insertInLeafNode(n, key, val, &splitInfo);
      } else {
        /// the root node is a branch node
        auto n = rootNode.branch;
        wasSplit = insertInBranchNode(n, depth, key, val, &splitInfo);
      }
      if (wasSplit) {
        /// we had an overflow in the node and therefore the node is split
        const auto root = newBranchNode();
        auto &rootRef = *root;
        rootRef.keys.get_rw()[0] = splitInfo.key;
        rootRef.children.get_rw()[0] = splitInfo.leftChild;
        rootRef.children.get_rw()[N] = splitInfo.rightChild;
        rootRef.search.get_rw().slot[1] = 0;
        rootRef.search.get_rw().b.set(0);
        rootRef.search.get_rw().slot[0] = 1;
        rootNode.branch = root;
        depth.get_rw() = depth.get_ro() + 1;
      }
    });
  }

  /**
   * Find the given @c key in the  tree and if found return the corresponding value.
   *
   * @param key the key we are looking for
   * @param[out] val a pointer to memory where the value is stored if the key was found
   * @return true if the key was found, false otherwise
   */
  bool lookup(const KeyType &key, ValueType *val)  {
    assert(val != nullptr);
    const auto leaf = findLeafNode(key);
    const auto pos = lookupPositionInLeafNode(leaf, key);
    const auto &leafRef = *leaf;
    if (pos <= leafRef.search.get_ro().slot[0] &&
        leafRef.keys.get_ro()[leafRef.search.get_ro().slot[pos]] == key) {
      /// we found it!
      *val = leafRef.values.get_ro()[leafRef.search.get_ro().slot[pos]];
      return true;
    }
    return false;
  }

  /**
   * Delete the entry with the given key @c key from the tree.
   *
   * @param key the key of the entry to be deleted
   * @return true if the key was found and deleted
   */
  bool erase(const KeyType &key) {
    bool deleted = false;
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {
      if (depth.get_ro() == 0) {
        /// special case: the root node is a leaf node and there is no need to handle underflow
        auto node = rootNode.leaf;
        assert(node != nullptr);
        deleted = eraseFromLeafNode(node, key);
      } else {
        auto node = rootNode.branch;
        assert(node != nullptr);
        deleted = eraseFromBranchNode(node, depth, key);
      }
    });
    return deleted;
  }

  /**
   * Print the structure and content of the tree to stdout.
   */
  void print() const {
    if (depth == 0) printLeafNode(0, rootNode.leaf);
    else printBranchNode(0u, rootNode.branch);
  }

  /**
   * Perform a scan over all key-value pairs stored in the tree.
   * For each entry the given function @func is called.
   *
   * @param func the function called for each entry
   */
  void scan(ScanFunc func) const {
    /// we traverse to the leftmost leaf node
    auto node = rootNode;
    auto d = depth.get_ro();
    while ( d-- > 0) node = node.branch->children.get_ro()[0];
    auto leaf = *node.leaf;
    auto &leafRef = *leaf;
    while (leaf != nullptr) {
      /// for each key-value pair call func
      for (auto i = 0u; i < M; i++) {
        if (!leafRef.search.get_ro().b.test(i)) continue;
        const auto &key = leafRef.keys.get_ro()[i];
        const auto &val = leafRef.values.get_ro()[i];
        func(key, val);
      }
      /// move to the next leaf node
      leaf = leafRef.nextLeaf;
    }
  }

  /**
   * Perform a range scan over all elements within the range [minKey, maxKey] and for each element
   * call the given function @c func.
   *
   * @param minKey the lower boundary of the range
   * @param maxKey the upper boundary of the range
   * @param func the function called for each entry
   */
  void scan(const KeyType &minKey, const KeyType &maxKey, ScanFunc func) const {
    const auto leaf = findLeafNode(minKey);
    auto &leafRef = *leaf;

    bool higherThanMax = false;
    while (!higherThanMax && leaf != nullptr) {
      /// for each key-value pair within the range call func
      for (auto i = 0u; i < M; i++) {
        if (!leafRef.search.get_ro().b.test(i)) continue;
        auto &key = leafRef.keys.get_ro()[i];
        if (key < minKey) continue;
        if (key > maxKey) { higherThanMax = true; continue; }

        auto &val = leafRef.values.get_ro()[i];
        func(key, val);
      }
      /// move to the next leaf node
      leaf = leafRef.nextLeaf;
    }
  }

#ifndef UNIT_TESTS
  private:
#endif

  /**
   * Insert a (key, value) pair into the corresponding leaf node. It is the responsibility of the
   * caller to make sure that the node @c node is the correct node. The key is inserted at the
   * correct position.
   *
   * @param node the node where the key-value pair is inserted.
   * @param key the key to be inserted
   * @param val the value associated with the key
   * @param splitInfo information about a possible split of the node
   */
  bool insertInLeafNode(const pptr<LeafNode> &node, const KeyType &key, const ValueType &val,
                        SplitInfo *splitInfo) {
    auto &nodeRef = *node;
    bool split = false;
    const auto slotPos = lookupPositionInLeafNode(node, key);
    const auto &slotArray = nodeRef.search.get_ro().slot;
    if (slotPos <= slotArray[0] && nodeRef.keys.get_ro()[slotArray[slotPos]] == key) {
      /// handle insert of duplicates 
      nodeRef.values.get_rw()[slotArray[slotPos]] = val;
      return false;
    }

    if (slotArray[0] == M) {
      /// split the node 
      splitLeafNode(node, splitInfo);
      auto &splitRef = *splitInfo;

      /// insert the new entry 
      if (slotPos < (M + 1) / 2)
        insertInLeafNodeAtPosition(splitRef.leftChild.leaf, slotPos, key, val);
      else
        insertInLeafNodeAtPosition(splitRef.rightChild.leaf,
                                   lookupPositionInLeafNode(splitRef.rightChild.leaf, key),
                                   key, val);

      /// inform the caller about the split 
      splitRef.key =
        splitRef.rightChild.leaf->keys.get_ro()[splitRef.rightChild.leaf->search.get_ro().slot[1]];
      split = true;
    } else {
      /// otherwise, we can simply insert the new entry at the given position 
      insertInLeafNodeAtPosition(node, slotPos, key, val);
    }
    return split;
  }

  void splitLeafNode(const pptr<LeafNode> &node, SplitInfo *splitInfo) {
      auto &nodeRef = *node;
      const auto &slotArray = nodeRef.search.get_ro().slot;
      /// determine the split position 
      constexpr auto middle = (M + 1) / 2;

      /// move all entries behind this position to a new sibling node 
      /*
      pptr<LeafNode> sibling = newLeafNode();
      auto &sibRef = *sibling;
      auto &sibSlots = sibRef.search.get_rw().slot;
      sibSlots[0] = slotArray[0] - middle;
      nodeRef.search.get_rw().slot[0] = middle;
      for (auto i = 1u; i < sibSlots[0] + 1; i++) {
        sibSlots[i] = i - 1;
        sibRef.search.get_rw().b.set(sibSlots[i]);
        sibRef.keys.get_rw()[sibSlots[i]] = nodeRef.keys.get_ro()[slotArray[i + middle]];
        sibRef.values.get_rw()[sibSlots[i]] = nodeRef.values.get_ro()[slotArray[i + middle]];
      }
      for (auto i = middle; i < M; i++)
        nodeRef.search.get_rw().b.reset(slotArray[i+1]);
      //PersistEmulation::writeBytes(((M-middle+ sibSlots[0]  +7)>>3) +
      //(sizeof(KeyType)+sizeof(ValueType)+1)*sibSlots[0] + 2); //bits ceiled + entries/slots + numKeys
      */

      /// Alternative: copy node, inverse bitmap and shift slots 
      pptr<LeafNode> sibling = newLeafNode(node);
      auto &sibRef = *sibling;
      auto &sibSlots = sibRef.search.get_rw().slot;
      sibSlots[0] = slotArray[0] - middle;
      nodeRef.search.get_rw().slot[0] = middle;
      for (auto i = middle; i < M; i++) {
        nodeRef.search.get_rw().b.reset(slotArray[i+1]);
      }
      auto b = nodeRef.search.get_ro().b;
      sibRef.search.get_rw().b = b.flip();
      for (auto i = 1u; i < sibSlots[0] + 1; i++)
        sibSlots[i] = sibSlots[slotArray[0]+i];
      //std::copy(std::begin(sibSlots)+slotArray[0], std::end(sibSlots), std::begin(sibSlots)+1);
      //PersistEmulation::writeBytes(sizeof(LeafNode) +
      //((M-middle+M+7)>>3) + sibSlots[0] + 2); //copy leaf + bits ceiled + slots + numKeys

      /// setup the list of leaf nodes 
      if (nodeRef.nextLeaf != nullptr) {
        sibRef.nextLeaf = nodeRef.nextLeaf;
        nodeRef.nextLeaf->prevLeaf = sibling;
      }
      nodeRef.nextLeaf = sibling;
      sibRef.prevLeaf = node;
      //PersistEmulation::writeBytes(16*2);

      auto &splitRef = *splitInfo;
      splitRef.leftChild = node;
      splitRef.rightChild = sibling;
      splitRef.key = sibRef.keys.get_ro()[sibSlots[1]];
  }

  /**
   * Insert a (key, value) pair at the given position @c pos into the leaf node @c node. The caller
   * has to ensure that
   * - there is enough space to insert the element
   * - the key is inserted at the correct position according to the order of keys
   *
   * @oaram node the leaf node where the element is to be inserted
   * @param pos the position in the leaf node (0 <= pos <= numKeys < M)
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   */
  void insertInLeafNodeAtPosition(const pptr<LeafNode> &node, const unsigned int pos,
                                  const KeyType &key, const ValueType &val) {
    assert(pos <= M);
    auto &nodeRef = *node;
    auto &search = nodeRef.search;
    const auto &slots = nodeRef.search.get_ro().slot;
    const auto u = search.get_ro().getFreeZero(); ///< unused Entry

    /// insert the new entry at unused position 
    nodeRef.keys.get_rw()[u] = key;
    nodeRef.values.get_rw()[u] = val;
    //PersistEmulation::writeBytes(sizeof(KeyType) + sizeof(ValueType));
    //PersistEmulation::persistStall();

    /// adapt slot array 
    for (auto j = slots[0]; j >= pos; j--)
      search.get_rw().slot[j+1] = slots[j];
    //PersistEmulation::writeBytes(slots[0]-pos+1);
    search.get_rw().slot[pos] = u;
    search.get_rw().slot[0] = slots[0] + 1;
    search.get_rw().b.set(u);
    //PersistEmulation::writeBytes(3);
    //constexpr auto noCL = (sizeof(Search<M>)+63)/64;
    //for (auto j = noCL; j > 0; j--)
      //PersistEmulation::persistStall();
  }

  /**
   * Insert a (key, value) pair into the tree recursively by following the path down to the leaf
   * level starting at node @c node at depth @c depth.
   *
   * @param node the starting node for the insert
   * @param depth the current depth of the tree (0 == leaf level)
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   * @param splitInfo information about the split
   * @return true if a split was performed
   */
  bool insertInBranchNode(const pptr<BranchNode> &node, const unsigned int depth,
                          const KeyType &key, const ValueType &val, SplitInfo *splitInfo) {
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;
    auto &nodeRef = *node;

    auto pos = lookupPositionInBranchNode(node, key);
    if (depth - 1 == 0) {
      /// case #1: our children are leaf node 
      pptr<LeafNode> child;
      if (pos == nodeRef.search.get_ro().slot[0] + 1)
        child = nodeRef.children.get_ro()[N].leaf;
      else
        child = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos]].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      /// case #2: our children are branch nodes 
      pptr<BranchNode> child;
      if (pos == nodeRef.search.get_ro().slot[0] + 1)
        child = nodeRef.children.get_ro()[N].branch;
      else
        child = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos]].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }

    if (hasSplit) {
      auto host = node;
      /// the child node was split, thus we have to add a new entry to our branch node
      if (nodeRef.search.get_ro().slot[0] == N) {
        /// this node is also full and needs to be split 
        splitBranchNode(node, childSplitInfo.key, splitInfo);
        const auto &splitRef = *splitInfo;
        host = (key < splitRef.key ? splitRef.leftChild : splitRef.rightChild).branch;
        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
      auto &hostRef = *host;
      /// Insert new key and children 
      const auto u = hostRef.search.get_ro().getFreeZero();
      hostRef.keys.get_rw()[u] = childSplitInfo.key;
      hostRef.children.get_rw()[u] = childSplitInfo.leftChild;

      /// adapt slot array 
      if (pos <= hostRef.search.get_ro().slot[0]) {
        /// if the child isn't inserted at the rightmost position then we have to make space for it
        for(auto j = hostRef.search.get_ro().slot[0]; j >= pos; j--)
          hostRef.search.get_rw().slot[j+1] = hostRef.search.get_ro().slot[j];
        hostRef.children.get_rw()[hostRef.search.get_ro().slot[pos+1]] = childSplitInfo.rightChild;
      } else {
        //hostRef.children.get_rw()[hostRef.search.get_ro().slot[pos]] = hostRef.children.get_ro()[N];
        hostRef.children.get_rw()[N] = childSplitInfo.rightChild;
      }
      hostRef.search.get_rw().slot[pos] = u;
      hostRef.search.get_rw().slot[0] = hostRef.search.get_ro().slot[0] + 1;
      hostRef.search.get_rw().b.set(u);
    }
    return split;
  }

  /**
   * Split the given branch node @c node in the middle and move half of the keys/children to the new
   * sibling node.
   *
   * @param node the branch node to be split
   * @param splitKey the key on which the split of the child occured
   * @param splitInfo information about the split
   */
  void splitBranchNode(const pptr<BranchNode> &node, const KeyType &splitKey,
                       SplitInfo *splitInfo) {
    auto &nodeRef = *node;
    /// determine the split position 
    auto middle = (N + 1) / 2;
    if (splitKey > nodeRef.keys.get_ro()[nodeRef.search.get_ro().slot[middle]]) middle++;

    /// move all entries behind this position to a new sibling node 
    auto sibling = newBranchNode();
    auto &sibRef = *sibling;
    sibRef.search.get_rw().slot[0] = nodeRef.search.get_rw().slot[0] - middle;
    for (auto i = 0u; i < sibRef.search.get_rw().slot[0]; i++) {
      sibRef.search.get_rw().slot[i+1] = i;  ///< set slot
      sibRef.search.get_rw().b.set(i);       ///< set bit
      /// set key and children 
      sibRef.keys.get_rw()[i] = nodeRef.keys.get_ro()[nodeRef.search.get_ro().slot[middle + i + 1]];
      sibRef.children.get_rw()[i] =
        nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[middle + i + 1]];
    }
    for (auto i = middle; i <= N; i++)
      nodeRef.search.get_rw().b.reset(nodeRef.search.get_ro().slot[i]);
    nodeRef.search.get_rw().slot[0] = middle - 1;

    /// set new most right children 
    sibRef.children.get_rw()[N] = nodeRef.children.get_ro()[N];
    nodeRef.children.get_rw()[N] = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[middle]];

    /// set split information
    auto &splitRef = *splitInfo;
    splitRef.key = nodeRef.keys.get_ro()[nodeRef.search.get_ro().slot[middle]];
    splitRef.leftChild = node;
    splitRef.rightChild = sibling;
  }

  /**
   * Traverse the tree starting at the root until the leaf node is found that could contain the
   * given @key. Note, that always a leaf node is returned even if the key doesn't exist on this
   * node.
   *
   * @param key the key we are looking for
   * @return the leaf node that would store the key
   */
  pptr<LeafNode> findLeafNode(const KeyType &key) const {
    auto node = rootNode;
    auto d = depth.get_ro();
    while (d-- > 0) {
      auto n = node.branch;
      auto pos = lookupPositionInBranchNode(n, key);
      node = n->children.get_ro()[pos];
    }
    return node.leaf;
  }
  /**
   * Lookup the search key @c key in the given leaf node and return the position.
   *
   * @param node the leaf node where we search
   * @param key the search key
   * @return the position of the key  (or @c M if not found)
   */
  auto lookupPositionInLeafNode(const pptr<LeafNode> &node, const KeyType &key) const {
    auto pos = 1u;
    auto &nodeRef = *node;
    const auto &slotArray = nodeRef.search.get_ro().slot;
    const auto &keys = nodeRef.keys.get_ro();
    auto l = 1;
    auto r = slotArray[0]; ///< num keys
    while (l <= r) {
      pos = (l + r) / 2;
      if (keys[slotArray[pos]] == key) return pos;
      if (keys[slotArray[pos]] < key) l = ++pos;
      else r = pos - 1;
    }
    return pos;
  }

  /**
   * Lookup the search key @c key in the given branch node and return the position which is the
   * position in the list of keys + 1. in this way, the position corresponds to the position of the
   * child pointer in the array @children.
   * If the search key is less than the smallest key, then @c 0 is returned.
   * If the key is greater than the largest key, then @c numKeys is returned.
   *
   * @param node the branch node where we search
   * @param key the search key
   * @return the position of the key + 1 (or 0 or @c numKey)
   */
  auto lookupPositionInBranchNode(const pptr<BranchNode> &node, const KeyType &key) const {
    auto pos = 1u;
    auto &nodeRef = *node;
    const auto &slotArray = nodeRef.search.get_ro().slot;
    const auto &keys = nodeRef.keys.get_ro();
    auto l = 1;
    auto r = slotArray[0]; ///< num keys
    while (l <= r) {
      pos = (l + r) / 2;
      if (keys[slotArray[pos]] == key) return ++pos;
      if (keys[slotArray[pos]] < key) l = ++pos;
      else r = pos - 1;
    }
    return pos;
  }

  /**
   * Delete the element with the given key from the given leaf node.
   *
   * @param node the leaf node from which the element is deleted
   * @param key the key of the element to be deleted
   * @return true of the element was deleted
   */
  bool eraseFromLeafNode(const pptr<LeafNode> &node, const KeyType &key) {
    auto pos = lookupPositionInLeafNode(node, key);
    auto &nodeRef = *node;
    if (nodeRef.keys.get_ro()[nodeRef.search.get_ro().slot[pos]] == key) {
      nodeRef.search.get_rw().b.reset(pos);
      for (auto i = pos; i < nodeRef.search.get_ro().slot[0] + 1; i++)
        nodeRef.search.get_rw().slot[i] =
          nodeRef.search.get_rw().slot[i + 1];
      nodeRef.search.get_rw().slot[0] = nodeRef.search.get_ro().slot[0] - 1;
      return true;
    }
    return false;
  }

  /**
   * Delete an entry from the tree by recursively going down to the leaf level and handling the
   * underflows.
   *
   * @param node the current branch node
   * @param d the current depth of the traversal
   * @param key the key to be deleted
   * @return true if the entry was deleted
   */
  bool eraseFromBranchNode(const pptr<BranchNode> &node, const unsigned int d, const KeyType &key) {
    assert(d >= 1);
    bool deleted = false;
    const auto &nodeRef = *node;
    /// try to find the branch 
    auto pos = lookupPositionInBranchNode(node, key);
    if (d == 1) {
      /// the next level is the leaf level 
      pptr<LeafNode> leaf;
      if (pos == nodeRef.search.get_ro().slot[0] + 1) leaf = nodeRef.children.get_ro()[N].leaf;
      else leaf = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos]].leaf;
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      constexpr auto middle = (M + 1) / 2;
      /// handle possible underflow 
      if (leaf->search.get_ro().slot[0] < middle)
        underflowAtLeafLevel(node, pos, leaf);
    } else {
      pptr<BranchNode> child;
      if (pos == nodeRef.search.get_ro().slot[0] + 1)
        child = nodeRef.children.get_ro()[N].branch;
      else
        child = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos]].branch;
      deleted = eraseFromBranchNode(child, d - 1, key);
      pos = lookupPositionInBranchNode(node, key);
      auto middle = (N + 1) / 2;

      /// handle possible underflow 
      if (child->search.get_ro().slot[0] < middle) {
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth.get_ro() && nodeRef.search.get_ro().slot[0] == 0) {
          /// special case: the root node is empty now
          rootNode = child;
          depth.get_rw() = depth.get_ro() - 1;
        }
      }
    }
    return deleted;
  }

  /**
   * Handle the case that during a delete operation a underflow at node @c leaf occured. If possible
   * this is handled
   * (1) by rebalancing the elements among the leaf node and one of its siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position in the slot array containing the position in turn of the key from the
   *            left child node @leaf in the @c children array of the branch node
   * @param leaf the node at which the underflow occured
   */
  void underflowAtLeafLevel(const pptr<BranchNode> &node, unsigned int pos,
                            const pptr<LeafNode> &leaf) {
      auto &nodeRef = *node;
      auto &leafRef = *leaf;
      assert(pos <= nodeRef.search.get_ro().slot[0] + 1);
      constexpr auto middle = (M + 1) / 2;
      /// 1. we check whether we can rebalance with one of the siblings but only if both nodes have
      ///    the same direct parent
      if (pos > 1 && leafRef.prevLeaf->search.get_ro().slot[0] > middle) {
        /// we have a sibling at the left for rebalancing the keys 
        balanceLeafNodes(leafRef.prevLeaf, leaf);
        if (pos == nodeRef.search.get_ro().slot[0] + 1) ///< rightmost leaf
          nodeRef.keys.get_rw()[nodeRef.search.get_ro().slot[nodeRef.search.get_ro().slot[0]]] =
            leafRef.keys.get_ro()[nodeRef.search.get_ro().slot[1]];
        else
          nodeRef.keys.get_rw()[nodeRef.search.get_ro().slot[pos]] =
            leafRef.keys.get_ro()[nodeRef.search.get_ro().slot[1]];
      }
      else if (pos <= nodeRef.search.get_ro().slot[0] && leafRef.nextLeaf->search.get_ro().slot[0] > middle) {
        /// we have a sibling at the right for rebalancing the keys 
        balanceLeafNodes(leafRef.nextLeaf, leaf);
        nodeRef.keys.get_rw()[nodeRef.search.get_ro().slot[pos+1]] =
          leafRef.nextLeaf->keys.get_ro()[nodeRef.search.get_ro().slot[1]];
      }
      /// 2. if this fails we have to merge two leaf nodes but only if both nodes have the same
      ///    direct parent
      else {
        pptr<LeafNode> survivor = nullptr;
        if (pos > 1 && leafRef.prevLeaf->search.get_ro().slot[0] <= middle) {
          survivor = mergeLeafNodes(leafRef.prevLeaf, leaf);
          deleteLeafNode(leaf);
          --pos;
        } else if (pos <= nodeRef.search.get_ro().slot[0] &&
            leafRef.nextLeaf->search.get_ro().slot[0] <= middle) {
          /// because we update the pointers in mergeLeafNodes we keep it here 
          auto l = leafRef.nextLeaf;
          survivor = mergeLeafNodes(leaf, l);
          deleteLeafNode(l);
        } else {
          /// this shouldn't happen?! 
          assert(false);
        }
        if (nodeRef.search.get_ro().slot[0] > 1) {
          /// just remove the child node from the current branch node 
          nodeRef.search.get_rw().b.reset(nodeRef.search.get_ro().slot[pos]);
          for (auto i = pos; i < nodeRef.search.get_ro().slot[0]; i++) {
            nodeRef.search.get_rw().slot[i] =
              nodeRef.search.get_rw().slot[i + 1];
          }
          if (pos == nodeRef.search.get_ro().slot[0]) ///< merged with rightmost
            nodeRef.children.get_rw()[N] = survivor;
          else
            nodeRef.children.get_rw()[nodeRef.search.get_ro().slot[pos]] = survivor;
          nodeRef.search.get_rw().slot[0] = nodeRef.search.get_ro().slot[0] - 1;
        } else {
          /// This is a special case that happens only if the current node is the root node. Now, we
          /// have to replace the branch root node by a leaf node.
          rootNode = survivor;
          depth.get_rw() = depth.get_ro() - 1;
        }
      }
    }

  /**
   * Handle the case that during a delete operation a underflow at node @c child occured where @c
   * node is the parent node. If possible this is handled
   * (1) by rebalancing the elements among the node @c child and one of its siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position of the child node @child in the @c children array of the branch node
   * @param child the node at which the underflow occured
   * @return the (possibly new) child node (in case of a merge)
   */
  pptr<BranchNode> underflowAtBranchLevel(const pptr<BranchNode> &node, const unsigned int pos,
                                          const pptr<BranchNode> &child) {
    assert(node != nullptr);
    assert(child != nullptr);
    auto &nodeRef = *node;
    auto newChild = child;
    constexpr auto middle = (N + 1) / 2;
    /// 1. we check whether we can rebalance with one of the siblings 
    if (pos > 1 &&
        nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos-1]].branch->
        search.get_ro().slot[0] > middle) {
      /// we have a sibling at the left for rebalancing the keys 
      auto sibling = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos-1]].branch;
      balanceBranchNodes(sibling, child, node, pos-1);
      return newChild;
    } else if (pos < nodeRef.search.get_ro().slot[0] &&
        nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos + 1]].branch->
        search.get_ro().slot[0] > middle) {
      /// we have a sibling at the right for rebalancing the keys 
      auto sibling = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos+1]].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    } else if (pos == nodeRef.search.get_ro().slot[0] &&
        nodeRef.children.get_ro()[N].branch->search.get_ro().slot[0] > middle) {
      auto sibling = nodeRef.children.get_ro()[N].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    }
    /// 2. if this fails we have to merge two branch nodes 
    else {
      pptr<BranchNode> lSibling = nullptr, rSibling = nullptr;
      auto prevKeys = 0u, nextKeys = 0u;

      if (pos > 1) {
        lSibling = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos - 1]].branch;
        prevKeys = lSibling->search.get_ro().slot[0];
      }
      if (pos < nodeRef.search.get_ro().slot[0]) {
        rSibling = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos + 1]].branch;
        nextKeys = rSibling->search.get_ro().slot[0];
      }
      if (pos == nodeRef.search.get_ro().slot[0]) {
        rSibling = nodeRef.children.get_ro()[N].branch;
        nextKeys = rSibling->search.get_ro().slot[0];
      }

      pptr<BranchNode> witnessNode = nullptr;
      auto ppos = pos;
      if (prevKeys > 0) {
        mergeBranchNodes(lSibling, nodeRef.keys.get_ro()[nodeRef.search.get_ro().slot[pos - 1]],
                         child);
        ppos = pos - 1;
        witnessNode = child;
        newChild = lSibling;
      } else if (nextKeys > 0) {
        mergeBranchNodes(child, nodeRef.keys.get_ro()[nodeRef.search.get_ro().slot[pos]], rSibling);
        witnessNode = rSibling;
      } else ///< shouldn't happen
        assert(false);

      /// remove key/children ppos from node 
      for (auto i = ppos; i < nodeRef.search.get_ro().slot[0] - 1; i++)
        nodeRef.search.get_rw().slot[i] = nodeRef.search.get_rw().slot[i + 1];

      if (pos == nodeRef.search.get_ro().slot[0])
        nodeRef.children.get_rw()[N] = child; ///< new rightmost children

      nodeRef.search.get_rw().slot[0] = nodeRef.search.get_rw().slot[0] - 1;
      deleteBranchNode(witnessNode);
      return newChild;
    }
  }

  /**
   * Redistribute (key, value) pairs from the leaf node @c donor to the leaf node @c receiver such
   * that both nodes have approx. the same number of elements. This method is used in case of an
   * underflow situation of a leaf node.
   *
   * @param donor the leaf node from which the elements are taken
   * @param receiver the sibling leaf node getting the elements from @c donor
   */
  void balanceLeafNodes(const pptr<LeafNode> &donor, const pptr<LeafNode> &receiver) {
    auto &donorRef = *donor;
    auto &receiverRef = *receiver;
    const auto dNumKeys = donorRef.search.get_ro().slot[0];
    const auto rNumKeys = receiverRef.search.get_ro().slot[0];
    assert(dNumKeys > rNumKeys);
    auto balancedNum = (dNumKeys + rNumKeys) / 2;
    auto toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    if (donorRef.keys.get_ro()[donorRef.search.get_ro().slot[1]] <
        receiverRef.keys.get_ro()[receiverRef.search.get_ro().slot[1]]) {
      /// move to a node with larger keys 
      auto i = 0u, j = 1u;
      /// reserve space 
      for (i = rNumKeys; i > 0; i--)
        receiverRef.search.get_rw().slot[i + toMove] =
          receiverRef.search.get_ro().slot[i];
      /// move from donor to receiver 
      for (i = balancedNum+1; i <= dNumKeys; i++, j++) {
        const auto u = receiverRef.search.get_ro().getFreeZero();
        const auto dPos = donorRef.search.get_ro().slot[i];
        receiverRef.keys.get_rw()[u] = donorRef.keys.get_ro()[dPos];
        receiverRef.values.get_rw()[u] = donorRef.values.get_ro()[dPos];
        receiverRef.search.get_rw().slot[j] = u;
        receiverRef.search.get_rw().b.set(u);
        donorRef.search.get_rw().b.reset(dPos);
      }
    } else {
      /// move to a node with smaller keys 
      for (auto i = 1u; i < toMove + 1; i++) {
        const auto u = receiverRef.search.get_ro().getFreeZero();
        const auto dPos = donorRef.search.get_ro().slot[i];
        receiverRef.keys.get_rw()[u] = donorRef.keys.get_ro()[dPos];
        receiverRef.values.get_rw()[u] = donorRef.values.get_ro()[dPos];
        receiverRef.search.get_rw().slot[rNumKeys + i] = u;
        receiverRef.search.get_rw().b.set(u);
        donorRef.search.get_rw().b.reset(dPos);
      }
      /// move to left on donor node 
      for (auto i = 1; i < dNumKeys - toMove + 1; i++) {
        donorRef.search.get_rw().slot[i] =
          donorRef.search.get_ro().slot[toMove + i];
      }
    }
    donorRef.search.get_rw().slot[0] = dNumKeys - toMove;
    receiverRef.search.get_rw().slot[0] = rNumKeys + toMove;
  }

  /**
   * Rebalance two branch nodes by moving some key-children pairs from the node @c donor to the node
   * @receiver via the parent node @parent. The position of the key between the two nodes is denoted
   * by @c pos.
   *
   * @param donor the branch node from which the elements are taken
   * @param receiver the sibling branch node getting the elements from @c donor
   * @param parent the parent node of @c donor and @c receiver
   * @param pos the position of the key in node @c parent that lies between @c donor and @c receiver
   */
  void balanceBranchNodes(const pptr<BranchNode> &donor, const pptr<BranchNode> &receiver,
                          const pptr<BranchNode> &parent, unsigned int pos) {
    auto &donorRef = *donor;
    auto &receiverRef = *receiver;
    auto &parentRef = *parent;
    const auto dNumKeys = donorRef.search.get_ro().slot[0];
    const auto rNumKeys = receiverRef.search.get_ro().slot[0];
    assert(dNumKeys > rNumKeys);

    unsigned int balancedNum = (dNumKeys + rNumKeys) / 2;
    unsigned int toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    /// 1. move from one node to a node with larger keys 
    if (donorRef.keys.get_ro()[donorRef.search.get_ro().slot[1]]
        < receiverRef.keys.get_ro()[receiverRef.search.get_ro().slot[1]]) {
      /// 1.1. make room 
      for (auto i = rNumKeys; i > 0; i--) {
        receiverRef.search.get_rw().slot[i + toMove] =
          receiverRef.search.get_rw().slot[i];
      }
      /// 1.2. move toMove keys/children from donor to receiver the most right child first 
      const auto u = receiverRef.search.get_ro().getFreeZero();
      receiverRef.keys.get_rw()[u] = parentRef.keys.get_ro()[parentRef.search.get_ro().slot[pos]];
      receiverRef.children.get_rw()[u] = donorRef.children.get_ro()[N];
      receiverRef.search.get_rw().slot[toMove] = u;
      receiverRef.search.get_rw().b.set(u);
      /// now the rest 
      for (auto i = 2u; i <= toMove; i++) {
        const auto u2 = receiverRef.search.get_ro().getFreeZero();
        const auto dPos = donorRef.search.get_ro().slot[balancedNum + i];
        receiverRef.keys.get_rw()[u2] = donorRef.keys.get_ro()[dPos];
        receiverRef.children.get_rw()[u2] = donorRef.children.get_ro()[dPos];
        receiverRef.search.get_rw().slot[i - 1] = u2;
        receiverRef.search.get_rw().b.set(u2);
        donorRef.search.get_rw().b.reset(dPos);
      }
      /// 1.3 set donors new rightmost child and new parent key 
      donorRef.children.get_rw()[N] =
        donorRef.children.get_ro()[donorRef.search.get_ro().slot[balancedNum + 1]];
      parentRef.keys.get_rw()[parentRef.search.get_ro().slot[pos]] =
        donorRef.keys.get_ro()[donorRef.search.get_ro().slot[balancedNum + 1]];
    }
    /// 2. move from one node to a node with smaller keys 
    else {
      /// 2.1. copy parent key and rightmost child of receiver 
      const auto u = receiverRef.search.get_ro().getFreeZero();
      receiverRef.keys.get_rw()[u] = parentRef.keys.get_ro()[parentRef.search.get_ro().slot[pos]];
      receiverRef.children.get_rw()[u] = receiverRef.children.get_ro()[N];
      receiverRef.search.get_rw().slot[rNumKeys + 1] = u;
      receiverRef.search.get_rw().b.set(u);

      /// 2.2. move toMove keys/children from donor to receiver 
      for (auto i = 2u; i <= toMove; i++) {
        const auto u2 = receiverRef.search.get_ro().getFreeZero();
        const auto dPos = donorRef.search.get_ro().slot[i - 1];
        receiverRef.keys.get_rw()[u2] = donorRef.keys.get_ro()[dPos];
        receiverRef.children.get_rw()[u2] = donorRef.children.get_ro()[dPos];
        receiverRef.search.get_rw().slot[rNumKeys + i] = u2;
        receiverRef.search.get_rw().b.set(u2);
        donorRef.search.get_rw().b.reset(dPos);
      }
      /// 2.3. set receivers new rightmost child and new parent key 
      receiverRef.children.get_rw()[N] = donorRef.children.get_ro()[donorRef.search.get_ro().slot[toMove]];
      parentRef.keys.get_rw()[parentRef.search.get_ro().slot[pos]] =
        donorRef.keys.get_ro()[donorRef.search.get_ro().slot[toMove]];

      /// 2.4. on donor node move all keys and values to the left 
      for (auto i = 1u; i <= dNumKeys - toMove; i++) {
        donorRef.search.get_rw().slot[i] = donorRef.search.get_rw().slot[toMove + i];
      }
    }
    receiverRef.search.get_rw().slot[0] = rNumKeys + toMove;
    donorRef.search.get_rw().slot[0] = dNumKeys - toMove;
  }

  /**
   * Merge two leaf nodes by moving all elements from @c node2 to @c node1.
   *
   * @param node1 the target node of the merge
   * @param node2 the source node
   * @return the merged node (always @c node1)
   */
  pptr<LeafNode> mergeLeafNodes(const pptr<LeafNode> &node1, const pptr<LeafNode> &node2) {
    assert(node1 != nullptr);
    assert(node2 != nullptr);
    auto &node1Ref = *node1;
    auto &node2Ref = *node2;
    const auto n1NumKeys = node1Ref.search.get_ro().slot[0];
    const auto n2NumKeys = node2Ref.search.get_ro().slot[0];
    assert(n1NumKeys + n2NumKeys <= M);

    /// we move all keys/values from node2 to node1
    for (auto i = 1u; i < n2NumKeys + 1; i++) {
        const auto u = node1Ref.search.get_ro().getFreeZero();
        node1Ref.keys.get_rw()[u] = node2Ref.keys.get_ro()[node2Ref.search.get_ro().slot[i]];
        node1Ref.values.get_rw()[u] = node2Ref.values.get_ro()[node2Ref.search.get_ro().slot[i]];
        node1Ref.search.get_rw().slot[n1NumKeys + i] = u;
        node1Ref.search.get_rw().b.set(u);
    }
    node1Ref.search.get_rw().slot[0] = n1NumKeys + n2NumKeys;
    node1Ref.nextLeaf = node2Ref.nextLeaf;
    if (node2Ref.nextLeaf != nullptr) node2Ref.nextLeaf->prevLeaf = node1;
    return node1;
  }

  /**
   * Merge two branch nodes by moving all keys/children from @c node to @c sibling and put the key
   * @c key from the parent node in the middle. The node @c node should be deleted by the caller.
   *
   * @param sibling the left sibling node which receives all keys/children
   * @param key the key from the parent node that is between sibling and node
   * @param node the node from which we move all keys/children
   */
  void mergeBranchNodes(const pptr<BranchNode> &sibling, const KeyType &key,
                        const pptr<BranchNode> &node) {
    assert(sibling != nullptr);
    assert(node != nullptr);
    auto &nodeRef = *node;
    auto &sibRef = *sibling;
    const auto sNumKeys = sibRef.search.get_ro().slot[0];
    assert(key <= nodeRef.keys.get_ro()[0]);
    assert(sibRef.keys.get_ro()[sibRef.search.get_ro().slot[sNumKeys]] < key);

    /// merge parent key and drag rightmost child forward 
    auto u = sibRef.search.get_ro().getFreeZero();
    sibRef.keys.get_rw()[u] = key;
    sibRef.children.get_rw()[u] = sibRef.children.get_ro()[N];
    sibRef.search.get_rw().b.set(u);
    sibRef.search.get_rw().slot[sNumKeys+1] = u;

    /// merge node 
    for (auto i = 1u; i < nodeRef.search.get_ro().slot[0] + 1; i++) {
      u = sibRef.search.get_ro().getFreeZero();
      sibRef.keys.get_rw()[u] = nodeRef.keys.get_ro()[nodeRef.search.get_ro().slot[i]];
      sibRef.children.get_rw()[u] = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[i]];
      sibRef.search.get_rw().b.set(u);
      sibRef.search.get_rw().slot[sNumKeys + i + 1] = u;
    }
    /// set new rightmost child and counter 
    sibRef.children.get_rw()[N] = nodeRef.children.get_ro()[N];
    sibRef.search.get_rw().slot[0] = sibRef.search.get_ro().slot[0] +
                                     nodeRef.search.get_rw().slot[0] + 1;
  }

  /**
   * Print the given leaf node @c node to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printLeafNode(const unsigned int d, const pptr<LeafNode> &node) const {
    const auto &nodeRef = *node;
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << "[\033[1m" << std::hex << node << std::dec << "\033[0m #"
      << (int)nodeRef.search.get_ro().slot[0] << ": ";
    for (auto i = 1u; i <= nodeRef.search.get_ro().slot[0]; i++) {
      if (i > 1) std::cout << ", ";
      std::cout << "{(" << nodeRef.search.get_ro().b[nodeRef.search.get_ro().slot[i]] << ")"
                << nodeRef.keys.get_ro()[nodeRef.search.get_ro().slot[i]] << "}";
    }
    std::cout << "]" << std::endl;
  }

  /**
   * Print the given branch node @c node and all its children to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printBranchNode(const unsigned int d, const pptr<BranchNode> &node) const {
    const auto &nodeRef = *node;
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << d << " BN { ["<<node<<"] #" << (int)nodeRef.search.get_ro().slot[0] << ": ";
    for (auto k = 1u; k <= nodeRef.search.get_ro().slot[0]; k++) {
      if (k > 1) std::cout << ", ";
      std::cout << "(" << nodeRef.search.get_ro().b[nodeRef.search.get_ro().slot[k]] << ")"
                << nodeRef.keys.get_ro()[nodeRef.search.get_ro().slot[k]];
    }
    std::cout << " }" << std::endl;
    for (auto k = 1u; k <= nodeRef.search.get_ro().slot[0] + 1; k++) {
      const auto pos = (k == nodeRef.search.get_ro().slot[0] + 1)? N : nodeRef.search.get_ro().slot[k];
      if (d + 1 < depth) {
        auto child = nodeRef.children.get_ro()[pos].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto leaf = nodeRef.children.get_ro()[pos].leaf;
        printLeafNode(d + 1, leaf);
      }
    }
  }

}; /* end class wBPTree */
} /* namespace dbis::wbptree */

#endif /* DBIS_wBPTree_hpp_ */
