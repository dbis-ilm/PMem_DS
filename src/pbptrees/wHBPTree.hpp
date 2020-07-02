/*
 * Copyright (C) 2017-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#include <libpmemobj/ctl.h>

#include <array>
#include <cmath>
#include <iostream>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include "config.h"
#include "utils/Bitmap.hpp"
#include "utils/PersistEmulation.hpp"
#include "utils/SearchFunctions.hpp"

namespace dbis::pbptrees {

using pmem::obj::allocation_flag;
using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;
template<typename Object>
using pptr = persistent_ptr<Object>;

/**
 * A persistent memory implementation of a FPTree.
 *
 * @tparam KeyType the data type of the key
 * @tparam ValueType the data type of the values associated with the key
 * @tparam N the maximum number of keys on a branch node
 * @tparam M the maximum number of keys on a leaf node
 */
template<typename KeyType, typename ValueType, int N, int M>
class wHBPTree {
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

    explicit Node(pptr<LeafNode> leaf_) : tag(LEAF), leaf(leaf_) {};
    explicit Node(BranchNode branch_) : tag(BRANCH), branch(branch_) {};
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

    Node &operator=(const Node &other) {
      copy(other);
      return *this;
    }

    Node &operator=(BranchNode *branch_) {
      tag = BRANCH;
      branch = branch_;
      return *this;
    }

    Node &operator=(const pptr<LeafNode> &leaf_) {
      tag = LEAF;
      leaf = leaf_;
      return *this;
    }

    enum NodeType {
      BLANK, LEAF, BRANCH
    } tag;
    union {
      pptr<LeafNode> leaf;
      BranchNode *branch;
    };
  };

  /**
   * A structure for representing a leaf node of a B+ tree.
   */
  struct alignas(64) LeafNode {
    /**
     * Constructor for creating a new empty leaf node.
     */
    LeafNode() : nextLeaf(nullptr), prevLeaf(nullptr) { slot.get_rw()[0] = 0; }

    static constexpr auto SlotSize = ((M + 1 + 7) / 8) * 8;  ///< round to 8 Byte
    static constexpr auto BitsetSize = ((M + 63) / 64) * 8;  ///< number * size of words
    static constexpr auto SearchSize = SlotSize + BitsetSize + 32;
    static constexpr auto PaddingSize = (64 - SearchSize % 64) % 64;

    p<std::array<uint8_t, M + 1>> slot;  ///< slot array for indirection, first = num
    p<dbis::Bitmap<M>> bits;             ///< bitmap for valid entries
    pptr<LeafNode> nextLeaf;             ///< pointer to the subsequent sibling
    pptr<LeafNode> prevLeaf;             ///< pointer to the preceeding sibling
    char padding[PaddingSize];           ///< padding to align keys to 64 bytes
    p<std::array<KeyType, M>> keys;      ///< the actual keys
    p<std::array<ValueType, M>> values;  ///< the actual values
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct alignas(64) BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode() { slot[0] = 0; }

    static constexpr auto SlotSize = ((N + 1 + 7) / 8) * 8;  ///< round to 8 Byte
    static constexpr auto BitsetSize = ((N + 63) / 64) * 8;  ///< number * size of words
    static constexpr auto SearchSize = SlotSize + BitsetSize;
    static constexpr auto PaddingSize = (64 - SearchSize % 64) % 64;

    std::array<uint8_t, N + 1> slot;   ///< slot array for indirection, first = num
    dbis::Bitmap<N> bits;              ///< bitmap for valid entries
    char padding[PaddingSize];         ///< padding to align keys to 64 bytes
    std::array<KeyType, N> keys;       ///< the actual keys
    std::array<Node, N + 1> children;  ///< pointers to child nodes (BranchNode or LeafNode)
  };

  /**
   * Create a new empty leaf node
   */
  pptr<LeafNode> newLeafNode() {
    auto pop = pmem::obj::pool_by_vptr(this);
    pptr<LeafNode> newNode = nullptr;
    transaction::run(pop, [&] {
      newNode = make_persistent<LeafNode>(allocation_flag::class_id(alloc_class.class_id));
    });
    return newNode;
  }

  pptr<LeafNode> newLeafNode(const pptr<LeafNode> &other) {
    auto pop = pmem::obj::pool_by_vptr(this);
    pptr<LeafNode> newNode = nullptr;
    transaction::run(pop, [&] {
      newNode = make_persistent<LeafNode>(allocation_flag::class_id(alloc_class.class_id), *other);
    });
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
  BranchNode *newBranchNode() {
    return new BranchNode();
  }

  BranchNode *newBranchNode(const BranchNode *other) {
    return new BranchNode(*other);
  }

  void deleteBranchNode(BranchNode *&node) {
    delete node;
    node = nullptr;
  }

  /**
   * A structure for passing information about a node split to
   * the caller.
   */
  struct SplitInfo {
    KeyType     key; ///< the key at which the node was split
    Node  leftChild; ///< the resulting lhs child node
    Node rightChild; ///< the resulting rhs child node
  };

  static constexpr pobj_alloc_class_desc AllocClass{256, 64, 1, POBJ_HEADER_COMPACT};
  pobj_alloc_class_desc alloc_class;
  unsigned int      depth; ///< the depth of the tree, i.e. the number of levels (0 => rootNode is LeafNode)
  Node           rootNode; ///< pointer to the root node
  pptr<LeafNode> leafList; ///< pointer to the most left leaf node. Necessary for recovery

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
    std::size_t currentPosition;

    public:
    iterator() : currentNode(nullptr), currentPosition(0) {}
    iterator(const Node &root, std::size_t d) {
      /// traverse to left-most key
      auto node = root;
      while (d-- > 0) {
        auto n = node.branch;
        node = n->children[0];
      }
      currentNode = node.leaf;
      currentPosition = 0;
      const auto &nodeBits = currentNode->bits.get_ro();
      /// Can not overflow as there are at least M/2 entries
      while(!nodeBits.test(currentPosition)) ++currentPosition;
    }

    iterator& operator++() {
      if (currentPosition >= M-1) {
        currentNode = currentNode->nextLeaf;
        currentPosition = 0;
        if (currentNode == nullptr) return *this;
        const auto &nodeBits = currentNode->bits.get_ro();
        while(!nodeBits.test(currentPosition)) ++currentPosition;
      } else if (!currentNode->bits.get_ro().test(++currentPosition)) ++(*this);
      return *this;
    }
    iterator operator++(int) {iterator retval = *this; ++(*this); return retval;}

    bool operator==(iterator other) const {
      return (currentNode == other.currentNode &&
              currentPosition == other.currentPosition);
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
  explicit wHBPTree(struct pobj_alloc_class_desc _alloc) : depth(0), alloc_class(_alloc) {
    // wHBPTree() : depth(0){
    rootNode = newLeafNode();
    leafList = rootNode.leaf;
    LOG("created new wHBPTree with sizeof(BranchNode) = "
        << sizeof(BranchNode) << ", sizeof(LeafNode) = " << sizeof(LeafNode));
  }

  /**
   * Destructor for the tree. Should delete all allocated nodes.
   */
  ~wHBPTree() {
    /// Nodes are deleted automatically by releasing leafPool and branchPool.
  }

  /**
   * Insert an element (a key-value pair) into the tree. If the key @c key
   * already exists, the corresponding value is replaced by @c val.
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
      /* we had an overflow in the node and therefore the node is split */
      const auto root = newBranchNode();
      auto &rootRef = *root;
      rootRef.keys[0] = splitInfo.key;
      rootRef.children[0] = splitInfo.leftChild;
      rootRef.children[N] = splitInfo.rightChild;
      rootRef.slot[1] = 0;
      rootRef.bits.set(0);
      rootRef.slot[0] = 1;
      rootNode.branch = root;
      ++depth;
    }
  }

  /**
   * Find the given @c key in the  tree and if found return the
   * corresponding value.
   *
   * @param key the key we are looking for
   * @param[out] val a pointer to memory where the value is stored
   *                 if the key was found
   * @return true if the key was found, false otherwise
   */
  bool lookup(const KeyType &key, ValueType *val)  {
    assert(val != nullptr);
    const auto leaf = findLeafNode(key);
    const auto pos = lookupPositionInLeafNode(leaf, key);
    const auto &leafRef = *leaf;
    const auto &leafSlots = leafRef.slot.get_ro();
    if (pos <= leafSlots[0] && leafRef.keys.get_ro()[leafSlots[pos]] == key) {
      /// we found it!
      *val = leafRef.values.get_ro()[leafSlots[pos]];
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
      if (depth == 0) {
        /* special case: the root node is a leaf node and there is no need to
         * handle underflow */
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
  * Recover the wBPTree by iterating over the LeafList and using the recoveryInsert method.
  */
  void recover() {
    LOG("Starting RECOVERY of wHBPTree");
    pptr<LeafNode> currentLeaf = leafList;
    if (leafList == nullptr) {
      LOG("No data to recover wHBPTree");
      return;
    }
    /* counting leafs */
    auto leafs = 0u;
    while(currentLeaf != nullptr) {
      ++leafs;
      currentLeaf = currentLeaf->nextLeaf;
    }
    float x = std::log(leafs)/std::log1p(N);
    assert(x == int(x) && "Not supported for this amount of leafs, yet");

    /* actual recovery */
    currentLeaf = leafList;
    if (leafList->nextLeaf == nullptr) {
      /// The index has only one node, so the leaf node becomes the root node
      rootNode = leafList;
      depth = 0;
    } else {
      rootNode = newBranchNode();
      depth = 1;
      rootNode.branch->children[0] = currentLeaf;
      currentLeaf = currentLeaf->nextLeaf;
      while (currentLeaf != nullptr) {
        recoveryInsert(currentLeaf);
        currentLeaf = currentLeaf->nextLeaf;
      }
    }
    LOG("RECOVERY Done")
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
    auto d = depth;
    while (d-- > 0) node = node.branch->children[0];
    auto leaf = node.leaf;
    while (leaf != nullptr) {
      const auto &leafRef = *leaf;
      /// for each key-value pair call func
      const auto &bits = leafRef.bits.get_ro();
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
   * Perform a range scan over all elements within the range [minKey, maxKey]
   * and for each element call the given function @c func.
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
      const auto &bits = leafRef.bits.get_ro();
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
   * Insert a (key, value) pair into the corresponding leaf node. It is the
   * responsibility of the caller to make sure that the node @c node is
   * the correct node. The key is inserted at the correct position.
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
    const auto slotArray = nodeRef.slot.get_ro();
    if (slotPos <= slotArray[0] && nodeRef.keys.get_ro()[slotArray[slotPos]] == key) {
      /// handle insert of duplicates
      nodeRef.values.get_rw()[slotArray[slotPos]] = val;
      return false;
    }

    if (slotArray[0] == M) {
      /// the node is full, so we must split it
      splitLeafNode(node, splitInfo);
      auto &splitRef = *splitInfo;

      /// insert the new entry
      if (slotPos < (M + 1) / 2)
        insertInLeafNodeAtPosition(splitRef.leftChild.leaf, slotPos, key, val);
      else
        insertInLeafNodeAtPosition(splitRef.rightChild.leaf,
            lookupPositionInLeafNode(splitRef.rightChild.leaf, key), key, val);

      /// inform the caller about the split
      splitRef.key =
        splitRef.rightChild.leaf->keys.get_ro()[splitRef.rightChild.leaf->slot.get_ro()[1]];
      split = true;
    } else {
      /// otherwise, we can simply insert the new entry at the given position
      insertInLeafNodeAtPosition(node, slotPos, key, val);
    }
    return split;
  }

  /**
   * Split the given leaf node @c node in the middle and move half of the
   * sibling node.
   *
   * @param node the leaf node to be split
   * @param splitInfo[out] information about the split
   */
  void splitLeafNode(const pptr<LeafNode> &node, SplitInfo *splitInfo) {
    auto &nodeRef = *node;
    /// determine the split position by finding median in unsorted array of keys
    constexpr auto middle = (M + 1) / 2;

    /// move all entries behind this position to a new sibling node
    // /*
    const auto sibling = newLeafNode();
    auto &sibRef = *sibling;
    auto &sibSlots = sibRef.slot.get_rw();
    auto &sibBits = sibRef.bits.get_rw();
    auto &sibKeys = sibRef.keys.get_rw();
    auto &sibVals = sibRef.values.get_rw();
    auto &nodeSlots = nodeRef.slot.get_rw();
    auto &nodeBits = nodeRef.bits.get_rw();
    const auto &nodeKeys = nodeRef.keys.get_ro();
    const auto &nodeVals = nodeRef.values.get_ro();
    sibSlots[0] = nodeSlots[0] - middle;
    nodeSlots[0] = middle;
    for (auto i = 1u; i < sibSlots[0] + 1; ++i) {
      sibSlots[i] = i - 1;
      sibBits.set(sibSlots[i]);
      sibKeys[sibSlots[i]] = nodeKeys[nodeSlots[i + middle]];
      sibVals[sibSlots[i]] = nodeVals[nodeSlots[i + middle]];
    }
    for (auto i = middle; i < M; ++i) nodeBits.reset(nodeSlots[i + 1]);
    PersistEmulation::writeBytes(((M - middle + sibSlots[0] + 7) >> 3) +
                                 (sizeof(KeyType) + sizeof(ValueType) + 1) * sibSlots[0] +
                                 2);  // bits ceiled + entries/slots + numKeys
    // */

    /// Alternative: copy node, inverse bitmap and shift slots
    /*
    const auto sibling = newLeafNode(node);
    auto &sibRef = *sibling;
    auto &sibSlots = sibRef.slot.get_rw();
    auto &sibBits = sibRef.bits.get_rw();
    auto &nodeSlots = nodeRef.slot.get_rw();
    auto &nodeBits = nodeRef.bits.get_rw();
    sibSlots[0] = sibSlots[0] - middle;
    nodeSlots[0] = middle;
    for (auto i = middle; i < M; ++i) nodeBits.reset(sibSlots[i + 1]);
    sibBits = nodeBits;
    sibBits.flip();
    for (auto i = 1u; i < sibSlots[0] + 1; ++i) sibSlots[i] = sibSlots[middle + i];
    PersistEmulation::writeBytes(sizeof(LeafNode) + ((M - middle + M + 7) >> 3) + sibSlots[0] +
                                 2);  // copy leaf + bits ceiled + slots + numKeys
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
    splitRef.key = sibRef.keys.get_ro()[sibSlots[1]];
  }

  /**
   * Insert a (key, value) pair at the given position @c pos into the leaf node @c node.
   * The caller has to ensure that
   * - there is enough space to insert the element
   * - the key is inserted at the correct position according to the order of keys
   *
   * @param node the leaf node where the element is to be inserted
   * @param pos the position in the leaf node (0 <= pos <= numKeys < M)
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   */
  void insertInLeafNodeAtPosition(const pptr<LeafNode> &node, unsigned int pos, const KeyType &key,
                                  const ValueType &val) {
    assert(pos <= M);
    auto &nodeRef = *node;
    auto &slots = nodeRef.slot.get_rw();
    auto &bits = nodeRef.bits.get_rw();
    const auto u = bits.getFreeZero();  ///< unused Entry

    /* insert the new entry at unused position */
    nodeRef.keys.get_rw()[u] = key;
    nodeRef.values.get_rw()[u] = val;

    /* adapt slot array */
    for (auto j = slots[0]; j >= pos; --j) slots[j + 1] = slots[j];
    PersistEmulation::writeBytes(slots[0] - pos + 1);
    slots[pos] = u;
    ++slots[0];
    bits.set(u);
    PersistEmulation::writeBytes<3>();
  }

  /**
   * Insert a (key, value) pair into the tree recursively by following the path
   * down to the leaf level starting at node @c node at depth @c depth.
   *
   * @param node the starting node for the insert
   * @param depth the current depth of the tree (0 == leaf level)
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   * @param splitInfo information about the split
   * @return true if a split was performed
   */
  bool insertInBranchNode(BranchNode *node, unsigned int depth, const KeyType &key,
                          const ValueType &val, SplitInfo *splitInfo) {
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;
    auto &nodeRef = *node;

    auto pos = lookupPositionInBranchNode(node, key);
    if (depth == 1) {
      /* case #1: our children are leaf node */
      auto child = (pos == nodeRef.slot[0] + 1) ? nodeRef.children[N].leaf
                                : nodeRef.children[nodeRef.slot[pos]].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      /* case #2: our children are branch nodes */
      auto child = (pos == nodeRef.slot[0] + 1) ? nodeRef.children[N].branch
                                : nodeRef.children[nodeRef.slot[pos]].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }

    if (hasSplit) {
      auto host = node;
      /// the child node was split, thus we have to add a new entry to our branch node
      if (nodeRef.slot[0] == N) {
        /// this node is also full and needs to be split
        splitBranchNode(node, childSplitInfo.key, splitInfo);
        const auto &splitRef = *splitInfo;
        host = (key < splitRef.key ? splitRef.leftChild : splitRef.rightChild).branch;
        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
      /// Insert new key and children
      auto &hostRef = *host;
      const auto u = hostRef.bits.getFreeZero();
      hostRef.keys[u] = childSplitInfo.key;
      hostRef.children[u] = childSplitInfo.leftChild;

      /// adapt slot array
      if (pos <= hostRef.slot[0]) {
        /// if the child isn't inserted at the rightmost position then we have to make space for it
        for(auto j = hostRef.slot[0]; j >= pos; --j)
          hostRef.slot[j+1] = hostRef.slot[j];
        hostRef.children[hostRef.slot[pos+1]] = childSplitInfo.rightChild;
      } else {
        hostRef.children[N] = childSplitInfo.rightChild;
      }
      hostRef.slot[pos] = u;
      hostRef.slot[0] = hostRef.slot[0] + 1;
      hostRef.bits.set(u);
    }
    return split;
  }

  /**
   * Split the given branch node @c node in the middle and move
   * half of the keys/children to the new sibling node.
   *
   * @param node the branch node to be split
   * @param splitKey the key on which the split of the child occured
   * @param splitInfo information about the split
   */
  void splitBranchNode(BranchNode *node, const KeyType &splitKey, SplitInfo *splitInfo) {
    /// determine the split position
    auto middle = (N + 1) / 2;
    auto &nodeRef = *node;
    if (splitKey > nodeRef.keys[nodeRef.slot[middle]]) ++middle;

    /// move all entries behind this position to a new sibling node
    const auto sibling = newBranchNode();
    auto &sibRef = *sibling;
    sibRef.slot[0] = nodeRef.slot[0] - middle;
    for (auto i = 0u; i < sibRef.slot[0]; ++i) {
      sibRef.slot[i + 1] = i;  ///< set slot
      sibRef.bits.set(i);  ///< set bit
      /// set key and children
      sibRef.keys[i] = nodeRef.keys[nodeRef.slot[middle + i + 1]];
      sibRef.children[i] = nodeRef.children[nodeRef.slot[middle + i + 1]];
    }
    for (auto i = middle; i <= N; ++i) nodeRef.bits.reset(nodeRef.slot[i]);
    nodeRef.slot[0] = middle - 1;

    /// set new most right children
    sibRef.children[N] = nodeRef.children[N];
    nodeRef.children[N] = nodeRef.children[nodeRef.slot[middle]];

    /// set split information
    auto &splitRef = *splitInfo;
    splitRef.key = nodeRef.keys[nodeRef.slot[middle]];
    splitRef.leftChild = node;
    splitRef.rightChild = sibling;
  }

  /**
   * Traverse the tree starting at the root until the leaf node is found that
   * could contain the given @key. Note, that always a leaf node is returned
   * even if the key doesn't exist on this node.
   *
   * @param key the key we are looking for
   * @return the leaf node that would store the key
   */
  pptr<LeafNode> findLeafNode(const KeyType &key) const {
    auto node = rootNode;
    auto d = depth;
    while (d-- > 0) {
      auto pos = lookupPositionInBranchNode(node.branch, key);
      node = node.branch->children[node.branch->slot[pos]];
    }
    return node.leaf;
  }
  /**
   * Lookup the search key @c key in the given leaf node and return the
   * position.
   *
   * @param node the leaf node where we search
   * @param key the search key
   * @return the position of the key  (or @c M if not found)
   */
  auto lookupPositionInLeafNode(const pptr<LeafNode> &node, const KeyType &key) const {
    const auto &nodeRef = *node;
    const auto &keys = nodeRef.keys.get_ro();
    const auto &slots = nodeRef.slot.get_ro();
    return binarySearch<false>(keys, slots, 1, slots[0], key);
  }

  /**
   * Lookup the search key @c key in the given branch node and return the
   * position which is the position in the list of keys + 1. in this way, the
   * position corresponds to the position of the child pointer in the
   * array @children.
   * If the search key is less than the smallest key, then @c 0 is returned.
   * If the key is greater than the largest key, then @c numKeys is returned.
   *
   * @param node the branch node where we search
   * @param key the search key
   * @return the position of the key + 1 (or 0 or @c numKey)
   */
  auto lookupPositionInBranchNode(const BranchNode *node, const KeyType &key) const {
    const auto &nodeRef = *node;
    const auto &keys = nodeRef.keys;
    const auto &slots = nodeRef.slot;
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
    return eraseFromLeafNodeAtPosition(node, pos, key);
  }

  /**
   * Delete the element with the given position and key from the given leaf node.
   *
   * @param node the leaf node from which the element is deleted
   * @param pos the position of the key in the node
   * @param key the key of the element to be deleted
   * @return true of the element was deleted
   */
  bool eraseFromLeafNodeAtPosition(const pptr<LeafNode> &node, const unsigned int pos,
                                   const KeyType &key) {
    auto &nodeRef = *node;
    auto &nodeSlots = nodeRef.slot.get_rw();
    auto &nodeBits = nodeRef.bits.get_rw();
    if (nodeRef.keys.get_ro()[nodeSlots[pos]] == key) {
      nodeBits.reset(nodeSlots[pos]);
      for (auto i = pos; i < nodeSlots[0] + 1; ++i) {
        nodeSlots[i] = nodeSlots[i + 1];
      }
      --nodeSlots[0];
      PersistEmulation::writeBytes(1 + (nodeSlots[0] - pos) + 1);
      return true;
    }
    return false;
  }

  /**
   * Delete an entry from the tree by recursively going down to the leaf level
   * and handling the underflows.
   *
   * @param node the current branch node
   * @param d the current depth of the traversal
   * @param key the key to be deleted
   * @return true if the entry was deleted
   */
  bool eraseFromBranchNode(BranchNode *const node, const unsigned int d, const KeyType &key) {
    assert(d >= 1);
    bool deleted = false;
    const auto &nodeRef = *node;
    /* try to find the branch */
    auto pos = lookupPositionInBranchNode(node, key);
    if (d == 1) {
      /* the next level is the leaf level */
      auto leaf = (pos == nodeRef.slot[0] + 1) ? nodeRef.children[N].leaf
                                               : nodeRef.children[nodeRef.slot[pos]].leaf;
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      constexpr auto middle = (M + 1) / 2;
      /// handle possible underflow
      if (leaf->slot.get_ro()[0] < middle) underflowAtLeafLevel(node, pos, leaf);
    } else {
      auto child = (pos == nodeRef.slot[0] + 1) ? nodeRef.children[N].branch
                                                : nodeRef.children[nodeRef.slot[pos]].branch;
      deleted = eraseFromBranchNode(child, d - 1, key);
      pos = lookupPositionInBranchNode(node, key);
      constexpr auto middle = (N + 1) / 2;

      /// handle possible underflow
      if (child->slot[0] < middle) {
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth && nodeRef.slot[0] == 0) {
          /// special case: the root node is empty now
          rootNode = child;
          --depth;
        }
      }
    }
    return deleted;
  }

  /**
   * Handle the case that during a delete operation a underflow at node @c leaf
   * occured. If possible this is handled
   * (1) by rebalancing the elements among the leaf node and one of its siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position in the slot array containing the position in turn
   *  of the key from the left child node @leaf in the @c children array of the branch node
   * @param leaf the node at which the underflow occured
   */
  void underflowAtLeafLevel(BranchNode *node, unsigned int pos, pptr<LeafNode> &leaf) {
    auto &nodeRef = *node;
    auto &leafRef = *leaf;
    assert(pos <= nodeRef.slot[0] + 1);
    constexpr auto middle = (M + 1) / 2;
    /* 1. we check whether we can rebalance with one of the siblings but only
     *    if both nodes have the same direct parent */
    if (pos > 1 && leafRef.prevLeaf->slot.get_ro()[0] > middle) {
      /* we have a sibling at the left for rebalancing the keys */
      balanceLeafNodes(leafRef.prevLeaf, leaf);
      const auto keyPos = (pos == nodeRef.slot[0] + 1)
                              ? nodeRef.slot[nodeRef.slot[0]]
                              : nodeRef.slot[pos];
      nodeRef.keys[keyPos] = leafRef.keys.get_ro()[nodeRef.slot[1]];
    } else if (pos <= nodeRef.slot[0] &&
               leafRef.nextLeaf->slot.get_ro()[0] > middle) {
      /* we have a sibling at the right for rebalancing the keys */
      balanceLeafNodes(leafRef.nextLeaf, leaf);
      nodeRef.keys[nodeRef.slot[pos + 1]] =
          leafRef.nextLeaf->keys.get_ro()[nodeRef.slot[1]];
    }
    /* 2. if this fails we have to merge two leaf nodes but only if both nodes
     *    have the same direct parent */
    else {
      pptr<LeafNode> survivor = nullptr;
      if (pos > 1 && leafRef.prevLeaf->slot.get_ro()[0] <= middle) {
        survivor = mergeLeafNodes(leafRef.prevLeaf, leaf);
        deleteLeafNode(leaf);
        --pos;
      } else if (pos <= nodeRef.slot[0] && leafRef.nextLeaf->slot.get_ro()[0] <= middle) {
        /* because we update the pointers in mergeLeafNodes we keep it here */
        auto l = leafRef.nextLeaf;
        survivor = mergeLeafNodes(leaf, l);
        deleteLeafNode(l);
      } else
        assert(false);  ///< this shouldn't happen?!

      if (nodeRef.slot[0] > 1) {
        /* just remove the child node from the current branch node */
        nodeRef.bits.reset(nodeRef.slot[pos]);
        for (auto i = pos; i < nodeRef.slot[0]; ++i) {
          nodeRef.slot[i] = nodeRef.slot[i + 1];
        }
        const auto surPos = (pos == nodeRef.slot[0]) ? N : nodeRef.slot[pos];
        nodeRef.children[surPos] = survivor;
        --nodeRef.slot[0];
      } else {
        /* This is a special case that happens only if the current node is the
         * root node. Now, we have to replace the branch root node by a leaf
         * node. */
        rootNode = survivor;
        --depth;
      }
    }
    }

  /**
   * Handle the case that during a delete operation a underflow at node @c child
   * occured where @c node is the parent node. If possible this is handled
   * (1) by rebalancing the elements among the node @c child and one of its
   * siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position of the child node @child in the @c children array
   * of the branch node
   * @param child the node at which the underflow occured
   * @return the (possibly new) child node (in case of a merge)
   */
  BranchNode *underflowAtBranchLevel(BranchNode * const node, unsigned int pos,
                                     BranchNode *child) {
    assert(node != nullptr);
    assert(child != nullptr);
    auto &nodeRef = *node;
    auto prevKeys = 0u, nextKeys = 0u;
    constexpr auto middle = (N + 1) / 2;
    /* 1. we check whether we can rebalance with one of the siblings */
    if (pos > 1 && (prevKeys =
          nodeRef.children[nodeRef.slot[pos-1]].branch->slot[0]) > middle) {
      /* we have a sibling at the left for rebalancing the keys */
      auto sibling = nodeRef.children[nodeRef.slot[pos-1]].branch;
      balanceBranchNodes(sibling, child, node, pos-1);
      return child;
    } else if (pos < nodeRef.slot[0] && (nextKeys =
          nodeRef.children[nodeRef.slot[pos + 1]].branch->slot[0]) > middle) {
      /* we have a sibling at the right for rebalancing the keys */
      auto sibling = nodeRef.children[nodeRef.slot[pos+1]].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return child;
    } else if (pos == nodeRef.slot[0] && (nextKeys =
          nodeRef.children[N].branch->slot[0]) > middle) {
      auto sibling = nodeRef.children[N].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return child;
    }
    /* 2. if this fails we have to merge two branch nodes */
    else {
      auto newChild = child;
      auto ppos = pos;
      if (prevKeys > 0) {
        auto &lSibling = nodeRef.children[nodeRef.slot[pos - 1]].branch;
        mergeBranchNodes(lSibling, nodeRef.keys[nodeRef.slot[pos-1]], child);
        ppos = pos - 1;
        deleteBranchNode(child);
        newChild = lSibling;
      } else if (nextKeys > 0) {
        const auto rPos = (pos == nodeRef.slot[0]) ? N : nodeRef.slot[pos + 1];
        auto &rSibling = nodeRef.children[rPos].branch;
        mergeBranchNodes(child, nodeRef.keys[nodeRef.slot[pos]], rSibling);
        if (pos == nodeRef.slot[0])
          nodeRef.children[N] = child; ///< new rightmost children
        deleteBranchNode(rSibling);
      } else assert(false); ///< shouldn't happen

      /// remove key/children ppos from node
      for (auto i = ppos; i < nodeRef.slot[0] - 1; ++i) {
        nodeRef.slot[i] = nodeRef.slot[i + 1];
      }
      --nodeRef.slot[0];
      return newChild;
    }
  }

  /**
   * Redistribute (key, value) pairs from the leaf node @c donor to
   * the leaf node @c receiver such that both nodes have approx. the same
   * number of elements. This method is used in case of an underflow
   * situation of a leaf node.
   *
   * @param donor the leaf node from which the elements are taken
   * @param receiver the sibling leaf node getting the elements from @c donor
   */
  void balanceLeafNodes(const pptr<LeafNode> &donor, const pptr<LeafNode> &receiver) {
    auto &donorRef = *donor;
    auto &receiverRef = *receiver;
    auto &receiverSlots = receiverRef.slot.get_rw();
    auto &donorSlots = donorRef.slot.get_rw();
    assert(donorSlots[0] > receiverSlots[0]);

    auto balancedNum = (donorSlots[0] + receiverSlots[0]) / 2;
    auto toMove = donorSlots[0] - balancedNum;
    if (toMove == 0) return;

    auto &receiverKeys = receiverRef.keys.get_rw();
    auto &receiverValues = receiverRef.values.get_rw();
    auto &receiverBits = receiverRef.bits.get_rw();
    const auto &donorKeys = donorRef.keys.get_ro();
    const auto &donorValues = donorRef.values.get_ro();
    auto &donorBits = donorRef.bits.get_rw();
    if (donorKeys[donorSlots[1]] < receiverKeys[receiverSlots[1]]) {
      /// move to a node with larger keys
      auto i = 0u, j = 1u;
      /// reserve space
      for (i = receiverSlots[0]; i > 0; --i)
        receiverSlots[i + toMove] = receiverSlots[i];
      /// move from donor to receiver
      for (i = balancedNum + 1; i <= donorSlots[0]; i++, ++j) {
        const auto u = receiverBits.getFreeZero();
        receiverKeys[u] = donorKeys[donorSlots[i]];
        receiverValues[u] = donorValues[donorSlots[i]];
        receiverSlots[j] = u;
        receiverBits.set(u);
        donorBits.reset(donorSlots[i]);
      }
    } else {
      /// move to a node with smaller keys
      for (auto i = 1u; i < toMove + 1; ++i) {
        const auto u = receiverBits.getFreeZero();
        receiverKeys[u] = donorKeys[donorSlots[i]];
        receiverValues[u] = donorValues[donorSlots[i]];
        receiverSlots[receiverSlots[0] + i] = u;
        receiverBits.set(u);
        donorBits.reset(donorSlots[i]);
      }
      /// move to left on donor node
      for (auto i = 1; i < donorSlots[0] - toMove + 1; ++i) {
        donorSlots[i] = donorSlots[toMove + i];
      }
    }
    donorSlots[0] -= toMove;
    receiverSlots[0] += toMove;
  }

  /**
   * Rebalance two branch nodes by moving some key-children pairs from the node
   * @c donor to the node @receiver via the parent node @parent. The position of
   * the key between the two nodes is denoted by @c pos.
   *
   * @param donor the branch node from which the elements are taken
   * @param receiver the sibling branch node getting the elements from @c donor
   * @param parent the parent node of @c donor and @c receiver
   * @param pos the position of the key in node @c parent that lies between
   *      @c donor and @c receiver
   */
  void balanceBranchNodes(BranchNode  *donor, BranchNode *receiver, BranchNode *parent,
                          unsigned int pos) {
    auto &donorRef = *donor;
    auto &receiverRef = *receiver;
    assert(donorRef.slot[0] > receiverRef.slot[0]);

    const auto balancedNum = (donorRef.slot[0] + receiverRef.slot[0]) / 2;
    const auto toMove = donorRef.slot[0] - balancedNum;
    if (toMove == 0) return;

    /// 1. move from one node to a node with larger keys
    if (donorRef.keys[donorRef.slot[1]] < receiverRef.keys[receiverRef.slot[1]]) {
      /// 1.1. make room
      for (auto i = receiverRef.slot[0]; i > 0; --i) {
        receiverRef.slot[i + toMove] = receiverRef.slot[i];
      }
      /// 1.2. move toMove keys/children from donor to receiver
      /// the most right child first
      const auto u = receiverRef.bits.getFreeZero();
      receiverRef.keys[u] = parent->keys[parent->slot[pos]];
      receiverRef.children[u] = donorRef.children[N];
      receiverRef.slot[toMove] = u;
      receiverRef.bits.set(u);
      /// now the rest
      for (auto i = 2u; i <= toMove; ++i) {
        const auto u2 = receiverRef.bits.getFreeZero();
        const auto dPos = donorRef.slot[balancedNum + i];
        receiverRef.keys[u2] = donorRef.keys[dPos];
        receiverRef.children[u2] = donorRef.children[dPos];
        receiverRef.slot[i - 1] = u2;
        receiverRef.bits.set(u2);
        donorRef.bits.reset(dPos);
      }
      /// 1.3 set donors new rightmost child and new parent key
      donorRef.children[N] = donorRef.children[donorRef.slot[balancedNum + 1]];
      parent->keys[parent->slot[pos]] = donorRef.keys[donorRef.slot[balancedNum + 1]];
    }
    /// 2. move from one node to a node with smaller keys
    else {
      /// 2.1. copy parent key and rightmost child of receiver
      const auto u = receiverRef.bits.getFreeZero();
      receiverRef.keys[u] = parent->keys[parent->slot[pos]];
      receiverRef.children[u] = receiverRef.children[N];
      receiverRef.slot[receiverRef.slot[0] + 1] = u;
      receiverRef.bits.set(u);

      /// 2.2. move toMove keys/children from donor to receiver
      for (auto i = 2u; i <= toMove; ++i) {
        const auto u2 = receiverRef.bits.getFreeZero();
        const auto dPos = donorRef.slot[i - 1];
        receiverRef.keys[u2] = donorRef.keys[dPos];
        receiverRef.children[u2] = donorRef.children[dPos];
        receiverRef.slot[receiverRef.slot[0] + i] = u2;
        receiverRef.bits.set(u2);
        donorRef.bits.reset(dPos);
      }
      /// 2.3. set receivers new rightmost child and new parent key
      receiverRef.children[N] = donorRef.children[donorRef.slot[toMove]];
      parent->keys[parent->slot[pos]] = donorRef.keys[donorRef.slot[toMove]];

      /// 2.4. on donor node move all keys and values to the left
      for (auto i = 1u; i <= donorRef.slot[0] - toMove; ++i) {
        donorRef.slot[i] = donorRef.slot[toMove + i];
      }
    }
    receiverRef.slot[0] += toMove;
    donorRef.slot[0] -= toMove;
  }

  /**
   * Merge two leaf nodes by moving all elements from @c node2 to
   * @c node1.
   *
   * @param node1 the target node of the merge
   * @param node2 the source node
   * @return the merged node (always @c node1)
   */
  pptr<LeafNode> mergeLeafNodes(const pptr <LeafNode> &node1, const pptr <LeafNode> &node2) {
    assert(node1 != nullptr);
    assert(node2 != nullptr);
    auto &node1Ref = *node1;
    auto &node2Ref = *node2;
    auto &node1Keys = node1Ref.keys.get_rw();
    auto &node1Vals = node1Ref.values.get_rw();
    auto &node1Slots = node1Ref.slot.get_rw();
    auto &node1Bits = node1Ref.bits.get_rw();
    const auto &node2Keys = node2Ref.keys.get_rw();
    const auto &node2Vals = node2Ref.values.get_rw();
    const auto &node2Slots = node2Ref.slot.get_rw();
    assert(node1Slots[0] + node2Slots[0] <= M);

    /// we move all keys/values from node2 to node1
    for (auto i = 1u; i < node2Slots[0] + 1; ++i) {
      const auto u = node1Bits.getFreeZero();
      node1Keys[u] = node2Keys[node2Slots[i]];
      node1Vals[u] = node2Vals[node2Slots[i]];
      node1Slots[node1Slots[0] + i] = u;
      node1Bits.set(u);
    }
    node1Slots[0] += node2Slots[0];
    node1Ref.nextLeaf = node2Ref.nextLeaf;
    if (node2Ref.nextLeaf != nullptr) node2Ref.nextLeaf->prevLeaf = node1;
    return node1;
  }

  /**
   * Merge two branch nodes by moving all keys/children from @c node to @c
   * sibling and put the key @c key from the parent node in the middle. The node
   * @c node should be deleted by the caller.
   *
   * @param sibling the left sibling node which receives all keys/children
   * @param key the key from the parent node that is between sibling and node
   * @param node the node from which we move all keys/children
   */
  void mergeBranchNodes(BranchNode *sibling, const KeyType &key,
                        BranchNode *node) {
    assert(sibling != nullptr);
    assert(node != nullptr);
    auto &nodeRef = *node;
    auto &sibRef = *sibling;
    const auto sNumKeys = sibRef.slot[0];
    assert(key <= nodeRef.keys[0]);
    assert(sibRef.keys[sibRef.slot[sNumKeys]] < key);

    /// merge parent key and drag rightmost child forward
    auto u = sibRef.bits.getFreeZero();
    sibRef.keys[u] = key;
    sibRef.children[u] = sibRef.children[N];
    sibRef.bits.set(u);
    sibRef.slot[sNumKeys+1] = u;

    /// merge node
    for (auto i = 1u; i < nodeRef.slot[0] + 1; ++i) {
      u = sibRef.bits.getFreeZero();
      sibRef.keys[u] = nodeRef.keys[nodeRef.slot[i]];
      sibRef.children[u] = nodeRef.children[nodeRef.slot[i]];
      sibRef.bits.set(u);
      sibRef.slot[sNumKeys + i + 1] = u;
    }
    /// set new rightmost child and counter
    sibRef.children[N] = nodeRef.children[N];
    sibRef.slot[0] +=  nodeRef.slot[0] + 1;
  }

  /**
   * Insert a leaf node into the tree for recovery
   * @param leaf the leaf to insert
   */
  void recoveryInsert(pptr<LeafNode> leaf){
    assert(depth > 0);
    assert(leaf != nullptr);

    SplitInfo splitInfo;
    auto hassplit = recoveryInsertInBranchNode(rootNode.branch, depth, leaf, &splitInfo);

    //Check for split
    if (hassplit) {
      //The root node was splitted
      const auto newRoot = newBranchNode();
      auto &newRootRef = *newRoot;
      newRootRef.keys[0] = splitInfo.key;
      newRootRef.children[0] = splitInfo.leftChild;
      newRootRef.children[N] = splitInfo.rightChild;
      newRootRef.bits.set(0);
      newRootRef.slot[0] = 1;
      newRootRef.slot[1] = 0;
      rootNode = newRoot;
      ++depth;
    }
  }

  /**
   * Insert a leaf node into the tree recursively by following the path
   * down to the leaf level starting at node @c node at depth @c depth.
   *
   * @param node the starting node for the insert
   * @param depth the current depth of the tree (0 == leaf level)
   * @param leaf the leaf node to insert
   * @param splitInfo information about the split
   * @return true if a split was performed
   */
  bool recoveryInsertInBranchNode(BranchNode *node, int curr_depth, const pptr<LeafNode> &leaf,
                                  SplitInfo *splitInfo){
    bool hassplit=false;
    SplitInfo childSplitInfo;
    auto &nodeRef = *node;
    auto &leafRef = *leaf;
    auto &nSlotArray = nodeRef.slot;
    const auto &lSlotArray = leafRef.slot.get_ro();

    if (curr_depth == 1) {
      if (nSlotArray[0] == N) {
        /// we have to insert a new right child, as the keys in the leafList are sorted
        BranchNode *newNode = newBranchNode();
        newNode->slot[1] = 0;
        newNode->children[0] = leaf;
        auto &splitRef = *splitInfo;
        splitRef.key = leafRef.keys.get_ro()[lSlotArray[1]];
        splitRef.leftChild = node;
        splitRef.rightChild = newNode;
        return true;
      } else {
        nodeRef.bits.set(nSlotArray[0]);
        nodeRef.keys[nSlotArray[0]] = leafRef.keys.get_ro()[lSlotArray[1]];
        if (nSlotArray[0] > 0)
          nodeRef.children[nSlotArray[0]] = nodeRef.children[N];
        nodeRef.children[N] = leaf;
        nSlotArray[nSlotArray[0] + 1] = nSlotArray[0];
        ++nSlotArray[0];
        return false;
      }
    } else {
      hassplit = recoveryInsertInBranchNode(nodeRef.children[(nSlotArray[0]==0)?0:N].branch, curr_depth-  1, leaf, &childSplitInfo);
    }
    //Check for split
    if (hassplit) {
      if (nSlotArray[0] == N) {
        BranchNode *newNode = newBranchNode();
        newNode->slot[1] = 0;
        newNode->children[0] = childSplitInfo.rightChild;
        auto &splitRef = *splitInfo;
        splitRef.key = childSplitInfo.key;
        splitRef.leftChild = node;
        splitRef.rightChild = newNode;
        return true;
      } else {
        nodeRef.bits.set(nSlotArray[0]);
        nodeRef.keys[nSlotArray[0]] = childSplitInfo.key;
        if (nSlotArray[0] > 0)
          nodeRef.children[nSlotArray[0]] = nodeRef.children[N];
        nodeRef.children[N] = childSplitInfo.rightChild;
        nSlotArray[nSlotArray[0] + 1] = nSlotArray[0];
        ++nSlotArray[0];
        return false;
      }
    }
    return false;
  }


  /**
   * Print the given leaf node @c node to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printLeafNode(unsigned int d, const pptr<LeafNode> &node) const {
    const auto &nodeRef = *node;
    for (auto i = 0u; i < d; ++i) std::cout << "  ";
    std::cout << "[\033[1m" << std::hex << node << std::dec << "\033[0m #"
      << (int)nodeRef.slot.get_ro()[0] << ": ";
    for (auto i = 1u; i <= nodeRef.slot.get_ro()[0]; ++i) {
      if (i > 1) std::cout << ", ";
      std::cout << "{(" << nodeRef.bits.get_ro()[nodeRef.slot.get_ro()[i]] << ")"
                << nodeRef.keys.get_ro()[nodeRef.slot.get_ro()[i]] << "}";
    }
    std::cout << "]" << std::endl;
  }

  /**
   * Print the given branch node @c node and all its children
   * to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printBranchNode(unsigned int d, BranchNode *node) const {
    const auto &nodeRef = *node;
    for (auto i = 0u; i < d; ++i) std::cout << "  ";
    std::cout << d << " BN { ["<<node<<"] #" << (int)nodeRef.slot[0] << ": ";
    for (auto k = 1u; k <= nodeRef.slot[0]; ++k) {
      if (k > 1) std::cout << ", ";
      std::cout << "(" << nodeRef.search.b[nodeRef.slot[k]] << ")"
                << nodeRef.keys[nodeRef.slot[k]];
    }
    std::cout << " }" << std::endl;
    for (auto k = 1u; k <= nodeRef.slot[0] + 1; ++k) {
      const auto pos = (k == nodeRef.slot[0] + 1)? N
        : nodeRef.slot[k];
      if (d + 1 < depth) {
        auto child = nodeRef.children[pos].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto leaf = nodeRef.children[pos].leaf;
        printLeafNode(d + 1, leaf);
      }
    }
  }

}; /* end class wHBPTree */
} /* namespace dbis::pbptrees */

#endif /* DBIS_wHBPTree_hpp_ */
