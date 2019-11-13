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
#include "utils/BitOperations.hpp"
#include "utils/PersistEmulation.hpp"
#include "utils/SearchFunctions.hpp"

namespace dbis::pbptrees {

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
  /// we need an even order for branch nodes to be able to merge
  static_assert(N % 2 == 0, "order of branch nodes must be even.");
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
  };

  /**
   * A structure for representing a leaf node of a B+ tree.
   */
  struct alignas(64) LeafNode {
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
  struct alignas(64) BranchNode {
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

  void deleteLeafNode(pptr<LeafNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] { delete_persistent<LeafNode>(node); });
    node = nullptr;
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

  void deleteBranchNode(pptr<BranchNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] { delete_persistent<BranchNode>(node); });
    node = nullptr;
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
      const auto &nodeBits = currentNode->search.get_ro().b;
      /// Can not overflow as there are at least M/2 entries
      while(!nodeBits.test(currentPosition)) ++currentPosition;
    }

    iterator& operator++() {
      if (currentPosition >= M-1) {
        currentNode = currentNode->nextLeaf;
        currentPosition = 0;
        if (currentNode == nullptr) return *this;
        const auto &nodeBits = currentNode->search.get_ro().b;
        while(!nodeBits.test(currentPosition)) ++currentPosition;
      } else if (!currentNode->search.get_ro().b.test(++currentPosition)) ++(*this);
      return *this;
    }
    iterator operator++(int) {iterator retval = *this; ++(*this); return retval;}

    bool operator==(iterator other) const {
      return (currentNode == other.currentNode && currentPosition == other.currentPosition);
    }

    bool operator!=(iterator other) const { return !(*this == other); }

    std::pair<KeyType, ValueType> operator*() {
      const auto &nodeRef = *currentNode;
      return std::make_pair(nodeRef.keys.get_ro()[currentPosition],
                            nodeRef.values.get_ro()[currentPosition]);
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
    LOG("created new wBPTree with sizeof(BranchNode) = " << sizeof(BranchNode)
                              << ", sizeof(LeafNode) = " << sizeof(LeafNode));
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
      auto &rootChilds = rootRef.children.get_rw();
      auto &rootSearch = rootRef.search.get_rw();
      rootRef.keys.get_rw()[0] = splitInfo.key;
      rootChilds[0] = splitInfo.leftChild;
      rootChilds[N] = splitInfo.rightChild;
      rootSearch.slot[1] = 0;
      rootSearch.b.set(0);
      rootSearch.slot[0] = 1;
      rootNode.branch = root;
      ++depth.get_rw();
    }
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
    const auto &leafSearch = leafRef.search.get_ro();
    if (pos <= leafSearch.slot[0] && leafRef.keys.get_ro()[leafSearch.slot[pos]] == key) {
      /// we found it!
      *val = leafRef.values.get_ro()[leafSearch.slot[pos]];
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
    auto leaf = node.leaf;
    while (leaf != nullptr) {
      auto &leafRef = *leaf;
      /// for each key-value pair call func
      const auto &bits = leafRef.search.get_ro().b;
      const auto &keys = leafRef.keys.get_ro();
      const auto &vals = leafRef.values.get_ro();
      for (auto i = 0u; i < M; ++i) {
        if (!bits.test(i)) continue;
        const auto &key = keys[i];
        const auto &val = vals[i];
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
    auto leaf = findLeafNode(minKey);

    bool higherThanMax = false;
    while (!higherThanMax && leaf != nullptr) {
      const auto &leafRef = *leaf;
      /// for each key-value pair within the range call func
      const auto &bits = leafRef.search.get_ro().b;
      const auto &keys = leafRef.keys.get_ro();
      const auto &vals = leafRef.values.get_ro();
      for (auto i = 0u; i < M; ++i) {
        if (!bits.test(i)) continue;
        const auto &key = keys[i];
        if (key < minKey) continue;
        if (key > maxKey) { higherThanMax = true; continue; }
        const auto &val = vals[i];
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

  /**
   * Split the given leaf node @c node in the middle and move half of the keys/children to the new
   * sibling node.
   *
   * @param node the leaf node to be split
   * @param splitInfo[out] information about the split
   */
  void splitLeafNode(const pptr<LeafNode> &node, SplitInfo *splitInfo) {
    auto &nodeRef = *node;
    /// determine the split position
    constexpr auto middle = (M + 1) / 2;

    /// move all entries behind this position to a new sibling node
    const auto sibling = newLeafNode();
    auto &sibRef = *sibling;
    auto &sibSearch = sibRef.search.get_rw();
    auto &sibKeys = sibRef.keys.get_rw();
    auto &sibVals = sibRef.values.get_rw();
    auto &nodeSearch = nodeRef.search.get_rw();
    const auto &nodeKeys = nodeRef.keys.get_ro();
    const auto &nodeVals = nodeRef.values.get_ro();
    sibSearch.slot[0] = nodeSearch.slot[0] - middle;
    nodeSearch.slot[0] = middle;
    for (auto i = 1u; i < sibSearch.slot[0] + 1; ++i) {
      sibSearch.slot[i] = i - 1;
      sibSearch.b.set(sibSearch.slot[i]);
      sibKeys[sibSearch.slot[i]] = nodeKeys[nodeSearch.slot[i + middle]];
      sibVals[sibSearch.slot[i]] = nodeVals[nodeSearch.slot[i + middle]];
    }
    for (auto i = middle; i < M; ++i)
      nodeSearch.b.reset(nodeSearch.slot[i+1]);
    PersistEmulation::writeBytes(((M-middle+ sibSearch.slot[0]  +7)>>3) +
        (sizeof(KeyType)+sizeof(ValueType)+1)*sibSearch.slot[0] + 2); //bits ceiled + entries/slots + numKeys

    /// Alternative: copy node, inverse bitmap and shift slots
    /*
       const auto sibling = newLeafNode(node);
       auto &sibRef = *sibling;
       auto &sibSearch = sibRef.search.get_rw();
       auto &nodeSearch = nodeRef.search.get_rw();
       sibSearch.slot[0] = sibSearch.slot[0] - middle;
       nodeSearch.slot[0] = middle;
       for (auto i = middle; i < M; ++i)
       nodeSearch.b.reset(sibSearch.slot[i+1]);
       auto b = nodeSearch.b;
       sibSearch.b = b.flip();
       for (auto i = 1u; i < sibSearch.slot[0] + 1; ++i)
       sibSearch.slot[i] = sibSearch.slot[sibSearch.slot[0]+i];
    //std::copy(std::begin(sibSlots)+sibSearch.slot[0], std::end(sibSlots), std::begin(sibSlots)+1);
    PersistEmulation::writeBytes(sizeof(LeafNode) +
    ((M-middle+M+7)>>3) + sibSearch.slot[0] + 2); //copy leaf + bits ceiled + slots + numKeys
    */

    /// setup the list of leaf nodes
    if (nodeRef.nextLeaf != nullptr) {
      sibRef.nextLeaf = nodeRef.nextLeaf;
      nodeRef.nextLeaf->prevLeaf = sibling;
      PersistEmulation::writeBytes<16*2>();
    }
    nodeRef.nextLeaf = sibling;
    sibRef.prevLeaf = node;
    PersistEmulation::writeBytes<16*2>();

    auto &splitRef = *splitInfo;
    splitRef.leftChild = node;
    splitRef.rightChild = sibling;
    splitRef.key = sibKeys[sibSearch.slot[1]];
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
    auto &search = nodeRef.search.get_rw();
    const auto u = BitOperations::getFreeZero(search.b); ///< unused Entry

    /// insert the new entry at unused position
    nodeRef.keys.get_rw()[u] = key;
    nodeRef.values.get_rw()[u] = val;
    PersistEmulation::writeBytes<sizeof(KeyType) + sizeof(ValueType)>();

    /// adapt slot array
    for (auto j = search.slot[0]; j >= pos; --j)
      search.slot[j+1] = search.slot[j];
    PersistEmulation::writeBytes(search.slot[0]-pos+1);
    search.slot[pos] = u;
    search.b.set(u);
    ++search.slot[0];
    PersistEmulation::writeBytes<3>();
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
    const auto &nodeSearch = nodeRef.search.get_ro();
    const auto &nodeChilds = nodeRef.children.get_ro();

    auto pos = lookupPositionInBranchNode(node, key);
    if (depth == 1) {
      /// case #1: our children are leaf node
      auto child = (pos == nodeSearch.slot[0] + 1) ? nodeChilds[N].leaf
                                                   : nodeChilds[nodeSearch.slot[pos]].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      /// case #2: our children are branch nodes
      auto child = (pos == nodeSearch.slot[0] + 1) ? nodeChilds[N].branch
                                                   : nodeChilds[nodeSearch.slot[pos]].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }

    if (hasSplit) {
      auto host = node;
      /// the child node was split, thus we have to add a new entry to our branch node
      if (nodeSearch.slot[0] == N) {
        /// this node is also full and needs to be split
        splitBranchNode(node, childSplitInfo.key, splitInfo);
        const auto &splitRef = *splitInfo;
        host = (key < splitRef.key ? splitRef.leftChild : splitRef.rightChild).branch;
        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
      auto &hostRef = *host;
      auto &hostKeys = hostRef.keys.get_rw();
      auto &hostChilds = hostRef.children.get_rw();
      auto &hostSearch = hostRef.search.get_rw();
      /// Insert new key and children
      const auto u = BitOperations::getFreeZero(hostSearch.b);
      hostKeys[u] = childSplitInfo.key;
      hostChilds[u] = childSplitInfo.leftChild;

      /// adapt slot array
      if (pos <= hostSearch.slot[0]) {
        /// if the child isn't inserted at the rightmost position then we have to make space for it
        for(auto j = hostSearch.slot[0]; j >= pos; --j)
          hostSearch.slot[j+1] = hostSearch.slot[j];
        hostChilds[hostSearch.slot[pos+1]] = childSplitInfo.rightChild;
      } else {
        hostChilds[N] = childSplitInfo.rightChild;
      }
      hostSearch.slot[pos] = u;
      hostSearch.b.set(u);
      ++hostSearch.slot[0];
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
    const auto &nodeKeys = nodeRef.keys.get_ro();
    auto &nodeSearch = nodeRef.search.get_rw();
    auto &nodeChilds = nodeRef.children.get_rw();

    /// determine the split position
    auto middle = (N + 1) / 2;
    if (splitKey > nodeKeys[nodeSearch.slot[middle]]) ++middle;

    /// move all entries behind this position to a new sibling node
    auto sibling = newBranchNode();
    auto &sibRef = *sibling;
    auto &sibKeys = sibRef.keys.get_rw();
    auto &sibChilds = sibRef.children.get_rw();
    auto &sibSearch = sibRef.search.get_rw();
    sibSearch.slot[0] = nodeSearch.slot[0] - middle;
    for (auto i = 0u; i < sibSearch.slot[0]; ++i) {
      sibSearch.slot[i+1] = i;  ///< set slot
      sibSearch.b.set(i);       ///< set bit
      /// set key and children
      sibKeys[i] = nodeKeys[nodeSearch.slot[middle + i + 1]];
      sibChilds[i] = nodeChilds[nodeSearch.slot[middle + i + 1]];
    }
    for (auto i = middle; i <= N; ++i)
      nodeSearch.b.reset(nodeSearch.slot[i]);
    nodeSearch.slot[0] = middle - 1;

    /// set new most right children
    sibChilds[N] = nodeChilds[N];
    nodeChilds[N] = nodeChilds[nodeSearch.slot[middle]];

    /// set split information
    auto &splitRef = *splitInfo;
    splitRef.key = nodeKeys[nodeSearch.slot[middle]];
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
      auto pos = lookupPositionInBranchNode(node.branch, key);
      auto &nodeRef = *node.branch;
      node = nodeRef.children.get_ro()[nodeRef.search.get_ro().slot[pos]];
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
    const auto &nodeRef = *node;
    const auto &keys = nodeRef.keys.get_ro();
    const auto &slots = nodeRef.search.get_ro().slot;
    return binarySearch<false>(keys, slots, 1, slots[0], key);
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
    const auto &nodeRef = *node;
    const auto &keys = nodeRef.keys.get_ro();
    const auto &slots = nodeRef.search.get_ro().slot;
    return binarySearch<true>(keys, slots, 1, slots[0], key);
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
    auto &nodeSearch = nodeRef.search.get_rw();
    if (nodeRef.keys.get_ro()[nodeSearch.slot[pos]] == key) {
      nodeSearch.b.reset(nodeSearch.slot[pos]);
      for (auto i = pos; i < nodeSearch.slot[0] + 1; ++i) {
        nodeSearch.slot[i] = nodeSearch.slot[i + 1];
      }
      --nodeSearch.slot[0];
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
    const auto &nodeChilds = nodeRef.children.get_ro();
    const auto &nodeSearch = nodeRef.search.get_ro();
    /// try to find the branch
    auto pos = lookupPositionInBranchNode(node, key);
    if (d == 1) {
      /// the next level is the leaf level
      auto leaf = (pos == nodeSearch.slot[0] + 1) ? nodeChilds[N].leaf :
                                                    nodeChilds[nodeSearch.slot[pos]].leaf;
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      constexpr auto middle = (M + 1) / 2;
      /// handle possible underflow
      if (leaf->search.get_ro().slot[0] < middle)
        underflowAtLeafLevel(node, pos, leaf);
    } else {
      auto child = (pos == nodeSearch.slot[0] + 1) ? nodeChilds[N].branch :
                                                     nodeChilds[nodeSearch.slot[pos]].branch;
      deleted = eraseFromBranchNode(child, d - 1, key);
      pos = lookupPositionInBranchNode(node, key);
      constexpr auto middle = (N + 1) / 2;
      /// handle possible underflow
      if (child->search.get_ro().slot[0] < middle) {
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth.get_ro() && nodeSearch.slot[0] == 0) {
          /// special case: the root node is empty now
          rootNode = child;
          --depth.get_rw();
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
  void underflowAtLeafLevel(const pptr<BranchNode> &node, unsigned int pos, pptr<LeafNode> &leaf) {
      auto &nodeRef = *node;
      auto &leafRef = *leaf;
      const auto &nodeSearch = nodeRef.search.get_ro();
      auto &nodeKeys = nodeRef.keys.get_rw();
      auto prevNumKeys = 0u, nextNumKeys = 0u;
      assert(pos <= nodeSearch.slot[0] + 1);
      constexpr auto middle = (M + 1) / 2;
      /// 1. we check whether we can rebalance with one of the siblings but only if both nodes have
      ///    the same direct parent
      if (pos > 1 && (prevNumKeys = leafRef.prevLeaf->search.get_ro().slot[0]) > middle) {
        /// we have a sibling at the left for rebalancing the keys
        balanceLeafNodes(leafRef.prevLeaf, leaf);
        const auto newKey = leafRef.keys.get_ro()[leafRef.search.get_ro().slot[1]];
        const auto slotPos = (pos == nodeSearch.slot[0] + 1) ? nodeSearch.slot[0] : pos;
        nodeKeys[nodeSearch.slot[slotPos]] = newKey;
      }
      else if (pos <= nodeSearch.slot[0] &&
              (nextNumKeys = leafRef.nextLeaf->search.get_ro().slot[0]) > middle) {
        /// we have a sibling at the right for rebalancing the keys
        balanceLeafNodes(leafRef.nextLeaf, leaf);
        const auto &nextLeaf = *leafRef.nextLeaf;
        nodeKeys[nodeSearch.slot[pos+1]] = nextLeaf.keys.get_ro()[nextLeaf.search.get_ro().slot[1]];
      }
      /// 2. if this fails we have to merge two leaf nodes but only if both nodes have the same
      ///    direct parent
      else {
        pptr<LeafNode> survivor = nullptr;
        if (pos > 1 && prevNumKeys <= middle) {
          survivor = mergeLeafNodes(leafRef.prevLeaf, leaf);
          deleteLeafNode(leaf);
          --pos;
        } else if (pos <= nodeSearch.slot[0] && nextNumKeys <= middle) {
          /// because we update the pointers in mergeLeafNodes we keep it here
          auto l = leafRef.nextLeaf;
          survivor = mergeLeafNodes(leaf, l);
          deleteLeafNode(l);
        } else assert(false); ///< this shouldn't happen?!

        if (nodeSearch.slot[0] > 1) {
          /// just remove the child node from the current branch node
          auto &nodeSearchW = nodeRef.search.get_rw();
          nodeSearchW.b.reset(nodeSearch.slot[pos]);
          for (auto i = pos; i < nodeSearch.slot[0]; ++i) {
            nodeSearchW.slot[i] = nodeSearch.slot[i + 1];
          }
          const auto surPos = (pos == nodeSearch.slot[0]) ? N : nodeSearch.slot[pos];
          nodeRef.children.get_rw()[surPos] = survivor;
          --nodeSearchW.slot[0];
        } else {
          /// This is a special case that happens only if the current node is the root node. Now, we
          /// have to replace the branch root node by a leaf node.
          rootNode = survivor;
          --depth.get_rw();
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
                                          pptr<BranchNode> &child) {
    assert(node != nullptr);
    assert(child != nullptr);
    auto &nodeRef = *node;
    const auto &nodeKeys = nodeRef.keys.get_ro();
    const auto &nodeChilds = nodeRef.children.get_ro();
    const auto &nodeSearch = nodeRef.search.get_ro();
    auto prevNumKeys = 0u, nextNumKeys = 0u;
    constexpr auto middle = (N + 1) / 2;
    /// 1. we check whether we can rebalance with one of the siblings
    if (pos > 1 &&  (prevNumKeys =
        nodeChilds[nodeSearch.slot[pos-1]].branch->search.get_ro().slot[0]) > middle) {
      /// we have a sibling at the left for rebalancing the keys
      const auto sibling = nodeChilds[nodeSearch.slot[pos-1]].branch;
      balanceBranchNodes(sibling, child, node, pos-1);
      return child;
    } else if (pos < nodeSearch.slot[0] && (nextNumKeys =
               nodeChilds[nodeSearch.slot[pos + 1]].branch->search.get_ro().slot[0]) > middle) {
      /// we have a sibling at the right for rebalancing the keys
      const auto sibling = nodeRef.children.get_ro()[nodeSearch.slot[pos+1]].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return child;
    } else if (pos == nodeSearch.slot[0] && (nextNumKeys =
        nodeChilds[N].branch->search.get_ro().slot[0]) > middle) {
      /// we have a sibling at the most right for rebalancing the keys
      auto sibling = nodeChilds[N].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return child;
    }
    /// 2. if this fails we have to merge two branch nodes
    else {
      auto newChild = child;
      auto ppos = pos;
      if (prevNumKeys > 0) {
        auto &lSibling = nodeChilds[nodeSearch.slot[pos-1]].branch;
        mergeBranchNodes(lSibling, nodeRef.keys.get_ro()[nodeSearch.slot[pos - 1]], child);
        ppos = pos - 1;
        deleteBranchNode(child);
        newChild = lSibling;
      } else if (nextNumKeys > 0) {
        auto &nodeChildsW = nodeRef.children.get_rw();
        auto rPos = (pos == nodeSearch.slot[0]) ? N : nodeSearch.slot[pos + 1];
        auto &rSibling = nodeChildsW[rPos].branch;
        mergeBranchNodes(child, nodeRef.keys.get_ro()[nodeSearch.slot[pos]], rSibling);
        if (pos == nodeSearch.slot[0])
          nodeChildsW[N] = child; ///< new rightmost children
        deleteBranchNode(rSibling);
      } else assert(false); ///< shouldn't happen

      /// remove key/children ppos from node
      auto &nodeSearchW = nodeRef.search.get_rw();
      for (auto i = ppos; i < nodeSearch.slot[0] - 1; ++i) {
        nodeSearchW.slot[i] = nodeSearch.slot[i + 1];
      }
      --nodeSearchW.slot[0];
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
    auto &receiverSearch = receiverRef.search.get_rw();
    auto &donorSearch = donorRef.search.get_rw();
    assert(donorSearch.slot[0] > receiverSearch.slot[0]);

    auto balancedNum = (donorSearch.slot[0] + receiverSearch.slot[0]) / 2;
    auto toMove = donorSearch.slot[0] - balancedNum;
    if (toMove == 0) return;

    auto &receiverKeys = receiverRef.keys.get_rw();
    auto &receiverValues = receiverRef.values.get_rw();
    const auto &donorKeys = donorRef.keys.get_ro();
    const auto &donorValues = donorRef.values.get_ro();
    if (donorKeys[donorSearch.slot[1]] < receiverKeys[receiverSearch.slot[1]]) {
      /// move to a node with larger keys
      auto i = 0u, j = 1u;
      /// reserve space
      for (i = receiverSearch.slot[0]; i > 0; --i)
        receiverSearch.slot[i + toMove] = receiverSearch.slot[i];
      /// move from donor to receiver
      for (i = balancedNum + 1; i <= donorSearch.slot[0]; i++, ++j) {
        const auto u = BitOperations::getFreeZero(receiverSearch.b);
        receiverKeys[u] = donorKeys[donorSearch.slot[i]];
        receiverValues[u] = donorValues[donorSearch.slot[i]];
        receiverSearch.slot[j] = u;
        receiverSearch.b.set(u);
        donorSearch.b.reset(donorSearch.slot[i]);
      }
    } else {
      /// move to a node with smaller keys
      for (auto i = 1u; i < toMove + 1; ++i) {
        const auto u = BitOperations::getFreeZero(receiverSearch.b);
        receiverKeys[u] = donorKeys[donorSearch.slot[i]];
        receiverValues[u] = donorValues[donorSearch.slot[i]];
        receiverSearch.slot[receiverSearch.slot[0] + i] = u;
        receiverSearch.b.set(u);
        donorSearch.b.reset(donorSearch.slot[i]);
      }
      /// move to left on donor node
      for (auto i = 1; i < donorSearch.slot[0] - toMove + 1; ++i) {
        donorSearch.slot[i] = donorSearch.slot[toMove + i];
      }
    }
    donorSearch.slot[0] -= toMove;
    receiverSearch.slot[0] += toMove;
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
    auto &receiverSearch = receiverRef.search.get_rw();
    auto &donorSearch = donorRef.search.get_rw();
    assert(donorSearch.slot[0] > receiverSearch.slot[0]);

    auto balancedNum = (donorSearch.slot[0] + receiverSearch.slot[0]) / 2;
    auto toMove = donorSearch.slot[0] - balancedNum;
    if (toMove == 0) return;

    auto &receiverKeys = receiverRef.keys.get_rw();
    auto &receiverChilds = receiverRef.children.get_rw();
    const auto &donorKeys = donorRef.keys.get_ro();
    const auto &donorChilds = donorRef.children.get_ro();
    auto &parentKeys = parentRef.keys.get_rw();
    const auto &parentSearch = parentRef.search.get_ro();
    /// 1. move from one node to a node with larger keys
    if (donorKeys[donorSearch.slot[1]] < receiverKeys[receiverSearch.slot[1]]) {
      /// 1.1. make room
      for (auto i = receiverSearch.slot[0]; i > 0; --i) {
        receiverSearch.slot[i + toMove] = receiverSearch.slot[i];
      }
      /// 1.2. move toMove keys/children from donor to receiver the most right child first
      const auto u = BitOperations::getFreeZero(receiverSearch.b);
      receiverKeys[u] = parentKeys[parentSearch.slot[pos]];
      receiverChilds[u] = donorChilds[N];
      receiverSearch.slot[toMove] = u;
      receiverSearch.b.set(u);
      /// now the rest
      for (auto i = 2u; i <= toMove; ++i) {
        const auto u2 = BitOperations::getFreeZero(receiverSearch.b);
        const auto dPos = donorSearch.slot[balancedNum + i];
        receiverKeys[u2] = donorKeys[dPos];
        receiverChilds[u2] = donorChilds[dPos];
        receiverSearch.slot[i - 1] = u2;
        receiverSearch.b.set(u2);
        donorSearch.b.reset(dPos);
      }
      /// 1.3 set donors new rightmost child and new parent key
      donorRef.children.get_rw()[N] = donorChilds[donorSearch.slot[balancedNum + 1]];
      parentKeys[parentSearch.slot[pos]] = donorKeys[donorSearch.slot[balancedNum + 1]];
    }
    /// 2. move from one node to a node with smaller keys
    else {
      /// 2.1. copy parent key and rightmost child of receiver
      const auto u = BitOperations::getFreeZero(receiverSearch.b);
      receiverKeys[u] = parentKeys[parentSearch.slot[pos]];
      receiverChilds[u] = receiverChilds[N];
      receiverSearch.slot[receiverSearch.slot[0] + 1] = u;
      receiverSearch.b.set(u);

      /// 2.2. move toMove keys/children from donor to receiver
      for (auto i = 2u; i <= toMove; ++i) {
        const auto u2 = BitOperations::getFreeZero(receiverSearch.b);
        const auto dPos = donorSearch.slot[i - 1];
        receiverKeys[u2] = donorKeys[dPos];
        receiverChilds[u2] = donorChilds[dPos];
        receiverSearch.slot[receiverSearch.slot[0] + i] = u2;
        receiverSearch.b.set(u2);
        donorSearch.b.reset(dPos);
      }
      /// 2.3. set receivers new rightmost child and new parent key
      receiverChilds[N] = donorChilds[donorSearch.slot[toMove]];
      parentKeys[parentSearch.slot[pos]] = donorKeys[donorSearch.slot[toMove]];

      /// 2.4. on donor node move all keys and values to the left
      for (auto i = 1u; i <= donorSearch.slot[0] - toMove; ++i) {
        donorSearch.slot[i] = donorSearch.slot[toMove + i];
      }
    }
    receiverSearch.slot[0] += toMove;
    donorSearch.slot[0] -= toMove;
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
    auto &node1Keys = node1Ref.keys.get_rw();
    auto &node1Vals = node1Ref.values.get_rw();
    auto &node1Search = node1Ref.search.get_rw();
    const auto &node2Keys = node2Ref.keys.get_rw();
    const auto &node2Vals = node2Ref.values.get_rw();
    const auto &node2Search = node2Ref.search.get_rw();
    assert(node1Search.slot[0] + node2Search.slot[0] <= M);

    /// we move all keys/values from node2 to node1
    for (auto i = 1u; i < node2Search.slot[0] + 1; ++i) {
      const auto u = BitOperations::getFreeZero(node1Search.b);
      node1Keys[u] = node2Keys[node2Search.slot[i]];
      node1Vals[u] = node2Vals[node2Search.slot[i]];
      node1Search.slot[node1Search.slot[0] + i] = u;
      node1Search.b.set(u);
    }
    node1Search.slot[0] += node2Search.slot[0];
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
    const auto &nodeKeys = nodeRef.keys.get_ro();
    const auto &nodeChilds = nodeRef.children.get_ro();
    const auto &nodeSearch = nodeRef.search.get_ro();
    auto &sibKeys = sibRef.keys.get_rw();
    auto &sibChilds = sibRef.children.get_rw();
    auto &sibSearch = sibRef.search.get_rw();
    assert(key <= nodeKeys[nodeSearch.slot[1]]);
    assert(sibKeys[sibSearch.slot[sibSearch.slot[0]]] < key);

    /// merge parent key and drag rightmost child forward
    auto u = BitOperations::getFreeZero(sibSearch.b);
    sibKeys[u] = key;
    sibChilds[u] = sibChilds[N];
    sibSearch.b.set(u);
    sibSearch.slot[sibSearch.slot[0]+1] = u;

    /// merge node
    for (auto i = 1u; i < nodeSearch.slot[0] + 1; ++i) {
      u = BitOperations::getFreeZero(sibSearch.b);
      sibKeys[u] = nodeKeys[nodeSearch.slot[i]];
      sibChilds[u] = nodeChilds[nodeSearch.slot[i]];
      sibSearch.b.set(u);
      sibSearch.slot[sibSearch.slot[0] + i + 1] = u;
    }
    /// set new rightmost child and counter
    sibChilds[N] = nodeChilds[N];
    sibSearch.slot[0] += nodeSearch.slot[0] + 1;
  }

  /**
   * Print the given leaf node @c node to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printLeafNode(const unsigned int d, const pptr<LeafNode> &node) const {
    const auto &nodeRef = *node;
    const auto &nodeKeys = nodeRef.keys.get_ro();
    const auto &nodeSearch = nodeRef.search.get_ro();
    for (auto i = 0u; i < d; ++i) std::cout << "  ";
    std::cout << "[\033[1m" << std::hex << node << std::dec << "\033[0m #"
      << (int)nodeSearch.slot[0] << ": ";
    for (auto i = 1u; i <= nodeSearch.slot[0]; ++i) {
      if (i > 1) std::cout << ", ";
      std::cout << "{(" << nodeSearch.b[nodeSearch.slot[i]] << ")"
                        << nodeKeys[nodeSearch.slot[i]] << "}";
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
    const auto &nodeKeys = nodeRef.keys.get_ro();
    const auto &nodeChilds = nodeRef.children.get_ro();
    const auto &nodeSearch = nodeRef.search.get_ro();
    for (auto i = 0u; i < d; ++i) std::cout << "  ";
    std::cout << d << " BN { ["<<node<<"] #" << (int)nodeSearch.slot[0] << ": ";
    for (auto k = 1u; k <= nodeSearch.slot[0]; ++k) {
      if (k > 1) std::cout << ", ";
      std::cout << "(" << nodeSearch.b[nodeSearch.slot[k]] << ")"
                       << nodeKeys[nodeSearch.slot[k]];
    }
    std::cout << " }" << std::endl;
    for (auto k = 1u; k <= nodeSearch.slot[0] + 1; ++k) {
      const auto pos = (k == nodeSearch.slot[0] + 1)? N : nodeSearch.slot[k];
      if (d + 1 < depth) {
        auto child = nodeChilds[pos].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto leaf = nodeChilds[pos].leaf;
        printLeafNode(d + 1, leaf);
      }
    }
  }

}; /* end class wBPTree */
} /* namespace dbis::pbptrees */

#endif /* DBIS_wBPTree_hpp_ */
