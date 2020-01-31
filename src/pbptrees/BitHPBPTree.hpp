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

#ifndef DBIS_BitHPBPTree_hpp_
#define DBIS_BitHPBPTree_hpp_

#include <array>
#include <iostream>

#include <libpmemobj/ctl.h>
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

using pmem::obj::allocation_flag;
using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;
template<typename Object>
using pptr = persistent_ptr<Object>;

/**
  * A persistent memory implementation of a B+ tree.
  *
  * @tparam KeyType the data type of the key
  * @tparam ValueType the data type of the values associated with the key
  * @tparam N the maximum number of keys on a branch node
  * @tparam M the maximum number of keys on a leaf node
  */
template<typename KeyType, typename ValueType, size_t N, size_t M>
class BitHPBPTree {
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

    explicit Node(const pptr<LeafNode> &leaf_) : tag(LEAF), leaf(leaf_) {};
    explicit Node(const BranchNode *branch_) : tag(BRANCH), branch(branch_) {};
    Node(const Node &other) { copy(other); };

    void copy(const Node &other) {
      tag = other.tag;
      switch (tag) {
        case LEAF: {
          leaf = other.leaf; break;
        }
        case BRANCH: {
          branch = other.branch; break;
        }
        default:;
      }
    }

    Node &operator=(const Node &other) { copy(other); return *this; }
    Node &operator=(const pptr<LeafNode> &leaf_) {
      tag = LEAF; leaf = leaf_; return *this;
    }
    Node &operator=(BranchNode *const branch_) {
      tag = BRANCH; branch = branch_; return *this;
    }

    enum NodeType {
      BLANK, LEAF, BRANCH
    } tag;

    union {
      pptr<LeafNode> leaf;
      BranchNode *branch;
    };
  };

  /* -------------------------------------------------------------------------------------------- */

  /**
   * A structure for representing a leaf node of a B+ tree.
   */
  struct alignas(64) LeafNode {

    static constexpr auto NUM_KEYS = M;
    using KEY_TYPE = KeyType;

    /**
     * Constructor for creating a new empty leaf node.
     */
    LeafNode() : nextLeaf(nullptr), prevLeaf(nullptr) {}

    static constexpr auto BitsetSize = ((M + 63) / 64) * 8;  ///< number * size of words
    static constexpr auto PaddingSize = (64 - (BitsetSize + 32) % 64) % 64;

    p<std::bitset<M>> bits;              ///< bitset for valid entries
    pptr<LeafNode> nextLeaf;             ///< pointer to the subsequent sibling
    pptr<LeafNode> prevLeaf;             ///< pointer to the preceeding sibling
    char padding[PaddingSize];           ///< padding to align keys to 64 bytes
    p<std::array<KeyType, M>> keys;      ///< the actual keys
    p<std::array<ValueType, M>> values;  ///< the actual values
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   * The rightmost child is always at position N.
   */
  struct alignas(64) BranchNode {

    static constexpr auto NUM_KEYS = N;
    using KEY_TYPE = KeyType;

    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode(){}

    std::bitset<N>              bits; ///< bitset for valid entries
    std::array<KeyType, N>      keys; ///< the actual keys
    std::array<Node, N + 1> children; ///< pointers to child nodes (BranchNode or LeafNode)
  };

  /**
   * Create a new empty leaf node.
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

  /**
   * Remove/delete an existing leaf node.
   */
  void deleteLeafNode(pptr<LeafNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] { delete_persistent<LeafNode>(node); });
    node = nullptr;
  }

  /**
   * Create a new empty branch node.
   */
  BranchNode *newBranchNode() {
    return new BranchNode();
  }

  BranchNode *newBranchNode(const BranchNode *other) {
    return new BranchNode(*other);
  }

  /**
   * Remove/delete an existing branch node.
   */
  void deleteBranchNode(BranchNode *&node) {
    delete node;
    node = nullptr;
  }

  /* -------------------------------------------------------------------------------------------- */

  /**
   * A structure for passing information about a node split to the caller.
   */
  struct SplitInfo {
    KeyType key;     ///< the key at which the node was split
    Node leftChild;  ///< the resulting lhs child node
    Node rightChild; ///< the resulting rhs child node
  };

  static constexpr pobj_alloc_class_desc AllocClass{256, 64, 1, POBJ_HEADER_COMPACT};
  pobj_alloc_class_desc alloc_class;
  unsigned int depth; /**< the depth of the tree, i.e. the number of levels
                           (0 => rootNode is LeafNode) */
  Node rootNode;      /**< pointer to the root node (an instance of @c LeafNode or @c BranchNode).
                           This pointer is never @c nullptr. */
  pptr<LeafNode> leafList; /**< Pointer to the leaf at the most left position.
                                Neccessary for recovery */

 public:
  /**
   * Iterator for iterating over the leaf nodes.
   */
  class iterator {
    pptr<LeafNode> currentNode;
    size_t currentPosition;

    public:
    iterator() : currentNode(nullptr), currentPosition(0) {}

    iterator(const Node &root, size_t d) {
      /// traverse to left-most key
      auto node = root;
      while (d-- > 0) node = node.branch->children[0];
      currentNode = node.leaf;
      currentPosition = 0;
      const auto &nodeBits = currentNode->bits.get_ro();
      while(!nodeBits.test(currentPosition)) ++currentPosition;
    }

    iterator &operator++() {
      if (currentPosition >= M-1) {
        currentNode = currentNode->nextLeaf;
        currentPosition = 0;
        if (currentNode == nullptr) return *this;
        const auto &nodeBits = currentNode->bits.get_ro();
        while(!nodeBits.test(currentPosition)) ++currentPosition;
      } else {
        if (!currentNode->bits.get_ro().test(++currentPosition)) ++(*this);
      }
      return *this;
    }

    iterator operator++(int) {
      iterator retval = *this;
      ++(*this);
      return retval;
    }

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
    using pointer = const std::pair<KeyType, ValueType> *;
    using reference = const std::pair<KeyType, ValueType> &;
    using iterator_category = std::forward_iterator_tag;
  };

  iterator begin() { return iterator(rootNode, depth); }

  iterator end() { return iterator(); }


  /**
   * Alias for a function passed to the scan method.
   */
  using ScanFunc = std::function<void(const KeyType &key, const ValueType &val)>;

  /**
   * Constructor for creating a new B+ tree.
   */
  explicit BitHPBPTree(struct pobj_alloc_class_desc _alloc) : depth(0), alloc_class(_alloc) {
  // BitHPBPTree() : depth(0) {
    rootNode = newLeafNode();
    leafList = rootNode.leaf;
    LOG("created new tree with sizeof(BranchNode) = "
        << sizeof(BranchNode) << ", sizeof(LeafNode) = " << sizeof(LeafNode));
  }

  /**
   * Destructor for the B+ tree.
   */
  ~BitHPBPTree() {}

  /**
   * Insert an element (a key-value pair) into the B+ tree. If the key @c key already exists, the
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
      auto root = newBranchNode();
      auto &rootRef = *root;
      rootRef.keys[0] = splitInfo.key;
      rootRef.children[0] = splitInfo.leftChild;
      rootRef.children[N] = splitInfo.rightChild;
      rootRef.bits.set(0);
      rootNode.branch = root;
      ++depth;
    }
  }

  /**
   * Find the given @c key in the B+ tree and if found return the corresponding value.
   *
   * @param key the key we are looking for
   * @param[out] val a pointer to memory where the value is stored if the key was found
   * @return true if the key was found, false otherwise
   */
  bool lookup(const KeyType &key, ValueType *val) const {
    assert(val != nullptr);

    const auto leafNode = findLeafNode(key);
    const auto pos = lookupPositionInLeafNode(leafNode, key);
    if (pos < M) {
      /// we found it
      *val = leafNode->values.get_ro()[pos];
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
    bool result;
    if (depth == 0) {
      /// special case: the root node is a leaf node and there is no need to handle underflow
      auto node = rootNode.leaf;
      assert(node != nullptr);
      result = eraseFromLeafNode(node, key);
    } else {
      auto node = rootNode.branch;
      assert(node != nullptr);
      result = eraseFromBranchNode(node, depth, key);
    }
    return result;
  }

  /**
   * Recover the BitHPBPTree by iterating over the LeafList and using the recoveryInsert method.
   */
  void recover() {
    LOG("Starting RECOVERY of BitHPBPTree");
    pptr<LeafNode> currentLeaf = leafList;
    if(leafList == nullptr){
      LOG("No data to recover HPBPTree");
    }
    if(leafList->nextLeaf == nullptr){
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
   * Print the structure and content of the B+ tree to stdout.
   */
  void print() const {
    if (depth == 0) printLeafNode(0u, rootNode.leaf);
    else printBranchNode(0u, rootNode.branch);
  }


  /**
   * Perform a scan over all key-value pairs stored in the B+ tree.
   * For each entry the given function @func is called.
   *
   * @param func the function called for each entry
   */
  void scan(ScanFunc func) const {
    /// we traverse to the leftmost leaf node
    auto node = rootNode;
    auto d = depth;
    /// as long as we aren't at the leaf level we follow the path down
    while (d-- > 0) node = node.branch->children[0];
    auto leaf = node.leaf;
    while (leaf != nullptr) {
      const auto &leafRef = *leaf;
      /// for each key-value pair call func
      const auto &leafBits = leafRef.bits.get_ro();
      const auto &leafKeys = leafRef.keys.get_ro();
      const auto &leafValues = leafRef.values.get_ro();
      for (auto i = 0u; i < M; ++i) {
        if (!leafBits.test(i)) continue;
        const auto &key = leafKeys[i];
        const auto &val = leafValues[i];
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
      /// for each key-value pair within the range call func
      const auto &leafRef = *leaf;
      const auto &leafBits = leafRef.bits.get_ro();
      const auto &leafKeys = leafRef.keys.get_ro();
      const auto &leafValues = leafRef.values.get_ro();
      for (auto i = 0u; i < M; ++i) {
        if (leafBits.test(i)) continue;

        const auto &key = leafKeys[i];
        if (key < minKey) continue;
        if (key > maxKey) { higherThanMax = true; continue; };

        const auto &val = leafValues[i];
        func(key, val);
      }
      /// move to the next leaf node
      leaf = leafRef.nextLeaf;
    }
  }

#ifndef UNIT_TESTS
 private:
#endif
  /* -------------------------------------------------------------------------------------------- */
  /*                                     DELETE AT LEAF LEVEL                                     */
  /* -------------------------------------------------------------------------------------------- */

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
    if (pos < M) {
      /// simply reset bit
      node->bits.get_rw().reset(pos);
      PersistEmulation::writeBytes<1>();
      return true;
    }
    return false;
  }

  /**
   * Handle the case that during a delete operation an underflow at node @c leaf occured. If
   * possible this is handled
   * (1) by rebalancing the elements among the leaf node and one of its siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position of the child node @leaf in the @c children array of the branch node
   * @param leaf the node at which the underflow occured
   */
  void underflowAtLeafLevel(BranchNode * const node, const unsigned int pos, pptr<LeafNode> &leaf) {
      assert(pos <= N);
      auto &nodeRef = *node;
      auto &leafRef = *leaf;
      auto prevNumKeys = 0u;
      constexpr auto middle = (M + 1) / 2;

      /// 1. we check whether we can rebalance with one of the siblings but only if both nodes have
      ///    the same direct parent
      if (pos > 0 && (prevNumKeys = leafRef.prevLeaf->bits.get_ro().count()) > middle) {
        /// we have a sibling at the left for rebalancing the keys
        balanceLeafNodes(leafRef.prevLeaf, leaf);
        const auto newMin = leafRef.keys.get_ro()[findMinKey(leafRef.keys.get_ro(),
                                                             leafRef.bits.get_ro())];
        const auto prevPos = findMinKeyGreaterThan(leafRef.keys.get_ro(), leafRef.bits.get_ro(),
                                                   newMin);
        nodeRef.keys[prevPos] = newMin;
      } else if (pos < N && leafRef.nextLeaf->bits.get_ro().count() > middle) {
        /// we have a sibling at the right for rebalancing the keys
        balanceLeafNodes(leafRef.nextLeaf, leaf);
        const auto &nextLeaf = *leafRef.nextLeaf;
        nodeRef.keys[pos] =
          nextLeaf.keys.get_ro()[findMinKey(nextLeaf.keys.get_ro(), nextLeaf.bits.get_ro())];
      } else {
        /// 2. if this fails we have to merge two leaf nodes but only if both nodes have the same
        ///    direct parent
        pptr<LeafNode> survivor = nullptr;

        if (findMinKey(nodeRef.keys, nodeRef.bits) != pos && prevNumKeys <= middle) {
          /// merge left
          survivor = mergeLeafNodes(leafRef.prevLeaf, leaf);
          deleteLeafNode(leaf);
          /// move to next left slot
          const auto prevPos = (pos == N) ?
            findMaxKey(nodeRef.keys, nodeRef.bits) : ///< we need a new rightmost node
            findMaxKeySmallerThan(nodeRef.keys, nodeRef.bits, nodeRef.keys[pos]);
          nodeRef.children[pos] = nodeRef.children[prevPos];
          nodeRef.bits.reset(prevPos);
        } else if (pos < N && leafRef.nextLeaf->bits.get_ro().count() <= middle) {
          /// merge right
          survivor = mergeLeafNodes(leaf, leafRef.nextLeaf);
          deleteLeafNode(leafRef.nextLeaf);
          /// move to next right slot
          const auto nextPos = findMinKeyGreaterThan(nodeRef.keys, nodeRef.bits, nodeRef.keys[pos]);
          nodeRef.children[nextPos] = nodeRef.children[pos];
          nodeRef.bits.reset(pos);
        } else  assert(false); ///< this shouldn't happen?!

        if (nodeRef.bits.count() == 0) {
          /// This is a special case that happens only if the current node is the root node. Now, we
          /// have to replace the branch root node by a leaf node.
          rootNode = survivor;
          --depth;
        }
      }
    }

  /**
   * Merge two leaf nodes by moving all elements from @c node2 to @c node1.
   *
   * @param node1 the target node of the merge
   * @param node2 the source node
   * @return the merged node (always @c node1)
   */
  pptr<LeafNode> mergeLeafNodes(const pptr<LeafNode> &node1, const  pptr<LeafNode> &node2) {
    assert(node1 != nullptr);
    assert(node2 != nullptr);
    auto &node1Ref = *node1;
    auto &node2Ref = *node2;
    auto &node1Bits = node1Ref.bits.get_rw();
    const auto &node2Bits = node2Ref.bits.get_ro();
    assert(node1Bits.count() + node2Bits.count() <= M);

    /// we move all keys/values from node2 to node1
    auto &node1Keys = node1Ref.keys.get_rw();
    auto &node1Values = node1Ref.values.get_rw();
    const auto &node2Keys = node2Ref.keys.get_ro();
    const auto &node2Values = node2Ref.values.get_ro();
    for (auto i = 0u; i < M; ++i) {
      if (node2Bits.test(i)) {
        const auto u = BitOperations::getFreeZero(node1Bits);
        node1Keys[u] = node2Keys[i];
        node1Values[u] = node2Values[i];
        node1Bits.set(u);
      }
    }
    node1Ref.nextLeaf = node2Ref.nextLeaf;
    if (node2Ref.nextLeaf != nullptr) node2Ref.nextLeaf->prevLeaf = node1;
    return node1;
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
    const auto dNumKeys = donorRef.bits.get_ro().count();
    const auto rNumKeys = receiverRef.bits.get_ro().count();
    assert(dNumKeys > rNumKeys);

    const auto balancedNum = (dNumKeys + rNumKeys) / 2;
    const auto toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    auto &receiverKeys = receiverRef.keys.get_rw();
    auto &receiverValues = receiverRef.values.get_rw();
    auto &receiverBits = receiverRef.bits.get_rw();
    const auto &donorKeys = donorRef.keys.get_ro();
    const auto &donorValues = donorRef.values.get_ro();
    auto &donorBits = donorRef.bits.get_rw();

    if (donorKeys[0] < receiverKeys[0]) {
      /// move from one node to a node with larger keys
      for (auto i = 0u; i < toMove; ++i) {
        const auto max = findMaxKey(donorKeys, donorBits);
        const auto u = BitOperations::getFreeZero(receiverBits);
        /// move the donor's maximum key to the receiver
        receiverKeys[u] = donorKeys[max];
        receiverValues[u] = donorValues[max];
        receiverBits.set(u);
        donorBits.reset(max);
      }
    } else {
      /// move from one node to a node with smaller keys
      for (auto i = 0u; i < toMove; ++i) {
        const auto min = findMinKey(donorKeys, donorBits);
        const auto u = BitOperations::getFreeZero(receiverBits);
        /// move the donor's minimum key to the receiver
        receiverKeys[u] = donorKeys[min];
        receiverValues[u] = donorValues[min];
        receiverBits.set(u);
        donorBits.reset(min);
      }
    }
  }

  /* -------------------------------------------------------------------------------------------- */
  /*                                     DELETE AT INNER LEVEL                                    */
  /* -------------------------------------------------------------------------------------------- */
  /**
   * Delete an entry from the tree by recursively going down to the leaf level and handling the
   * underflows.
   *
   * @param node the current branch node
   * @param d the current depth of the traversal
   * @param key the key to be deleted
   * @return true if the entry was deleted
   */
  bool eraseFromBranchNode(BranchNode * const node, const unsigned int d, const KeyType &key) {
    assert(d >= 1);
    auto &nodeRef = *node;
    bool deleted = false;
    /// try to find the branch
    auto pos = lookupPositionInBranchNode(node, key);
    if (d == 1) {
      /// the next level is the leaf level
      auto &leaf = nodeRef.children[pos].leaf;
      deleted = eraseFromLeafNode(leaf, key);
      constexpr auto middle = (M + 1) / 2;
      if (leaf->bits.get_ro().count() < middle) {
        /// handle underflow
        underflowAtLeafLevel(node, pos, leaf);
      }
    } else {
      auto &child = nodeRef.children[pos].branch;
      deleted = eraseFromBranchNode(child, d - 1, key);

      pos = lookupPositionInBranchNode(node, key);
      constexpr auto middle = (N + 1) / 2;
      if (child->bits.count() < middle) {
        /// handle underflow
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth && nodeRef.bits.count() == 0) {
          /// special case: the root node is empty now
          rootNode = child;
          --depth;
        }
      }
    }
    return deleted;
  }

  /**
   * Handle the case that during a delete operation a underflow at node @c child occured where
   * @c node is the parent node. If possible this is handled
   * (1) by rebalancing the elements among the node @c child and one of its siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position of the child node @child in the @c children array of the branch node
   * @param child the node at which the underflow occured
   * @return the (possibly new) child node (in case of a merge)
   */
  BranchNode *underflowAtBranchLevel(BranchNode * const node, const unsigned int pos,
                                     BranchNode * child) {
    assert(node != nullptr);
    assert(child != nullptr);
    auto &nodeRef = *node;
    const auto nMinKeyPos = findMinKey(nodeRef.keys, nodeRef.bits);
    const auto prevPos = findMaxKeySmallerThan(nodeRef.keys, nodeRef.bits, nodeRef.keys[pos]); //could be N
    const auto prevNumKeys = nodeRef.children[prevPos].branch->bits.count();
    const auto nextPos = findMinKeyGreaterThan(nodeRef.keys, nodeRef.bits, nodeRef.keys[pos]);
    const auto nextNumKeys = nodeRef.children[nextPos].branch->bits.count();
    constexpr auto middle = (N + 1) / 2;

    /// 1. we check whether we can rebalance with one of the siblings
    if (nMinKeyPos != pos && prevNumKeys > middle) {
      /// we have a sibling at the left for rebalancing the keys
      assert(prevPos != N);
      const auto sibling = nodeRef.children[prevPos].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return child;
    } else if (pos < N && nextNumKeys > middle) {
      /// we have a sibling at the right for rebalancing the keys
      const auto sibling = nodeRef.children[nextPos].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return child;
    }
    /// 2. if this fails we have to merge two branch nodes
    if (nMinKeyPos != pos && prevNumKeys <= middle) {
      /// merge to left
      auto &lSibling = nodeRef.children[prevPos].branch;
      mergeBranchNodes(lSibling, nodeRef.keys[pos], child);
      deleteBranchNode(child);
      nodeRef.bits.reset(pos);
      if (pos == N)
        nodeRef.children[N] = child; ///< new rightmost child
      return lSibling;
    } else if (pos < N && nextNumKeys <= middle) {
      /// merge from right
      auto &rSibling = nodeRef.children[nextPos].branch;
      mergeBranchNodes(child, nodeRef.keys[pos], rSibling);
      deleteBranchNode(rSibling);
      nodeRef.bits.reset(pos);
      if (pos == findMaxKey(nodeRef.keys, nodeRef.bits))
        nodeRef.children[N] = child; ///< new rightmost child
      return child;
    } else assert(false); ///< shouldn't happen
  }

  /**
   * Merge two branch nodes by moving all keys/children from @c node to @c sibling and put the key
   * @c key from the parent node in the middle. The node @c node should be deleted by the caller.
   *
   * @param sibling the left sibling node which receives all keys/children
   * @param key the key from the parent node that is between sibling and node
   * @param node the node from which we move all keys/children
   */
  void mergeBranchNodes(BranchNode * const sibling, const KeyType &key, const BranchNode * node) {
    assert(sibling != nullptr);
    assert(node != nullptr);

    const auto &nodeRef = *node;
    auto &sibRef = *sibling;

    assert(key <= nodeRef.keys[findMinKey(nodeRef.keys, nodeRef.bits)]);
    assert(sibRef.keys[findMaxKey(sibRef.keys, sibRef.bits)] < key);

    auto u = BitOperations::getFreeZero(sibRef.bits);
    sibRef.keys[u] = key;
    sibRef.children[u] = sibRef.children[N];
    sibRef.bits.set(u);
    for (auto i = 0u; i < M; ++i) {
      if (nodeRef.bits.test(i)) {
        u = BitOperations::getFreeZero(sibRef.bits);
        sibRef.keys[u] = nodeRef.keys[i];
        sibRef.children[u] = nodeRef.children[i];
        sibRef.bits.set(u);
      }
    }
    sibRef.children[N] = nodeRef.children[N];
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
  void balanceBranchNodes(BranchNode * const donor, BranchNode * const receiver,
                          BranchNode * const parent, const unsigned int pos) {
    auto &donorRef = *donor;
    auto &receiverRef = *receiver;
    auto &parentRef = *parent;
    const auto dNumKeys = donorRef.bits.count();
    const auto rNumKeys = receiverRef.bits.count();
    assert(dNumKeys > rNumKeys);

    const auto balancedNum = (dNumKeys + rNumKeys) / 2;
    const auto toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    /// 1. move from one node to a node with larger keys
    if (donorRef.keys[BitOperations::getFirstSet(donorRef.bits)] <
        receiverRef.keys[BitOperations::getFirstSet(receiverRef.bits)]) {
      /// 1.1. copy parent key and rightmost child from donor
      auto u = BitOperations::getFreeZero(receiverRef.bits);
      receiverRef.keys[u] = parentRef.keys[pos];
      receiverRef.children[u] = donorRef.children[N];
      receiverRef.bits.set(u);
      /// 1.2. move toMove-1 keys/children from donor to receiver
      for (auto i = 1u; i < toMove; ++i) {
        const auto max = findMaxKey(donorRef.keys, donorRef.bits);
        u = BitOperations::getFreeZero(receiverRef.bits);
        receiverRef.keys[u] = donorRef.keys[max];
        receiverRef.children[u] = donorRef.children[max];
        receiverRef.bits.set(u);
        donorRef.bits.reset(max);
      }
      /// 1.3 set donors new rightmost child and new parent key
      const auto dPos = findMaxKey(donorRef.keys, donorRef.bits);
      donorRef.children[N] = donorRef.children[dPos];
      parentRef.keys[pos] = donorRef.keys[dPos];
      donorRef.bits.reset(dPos);

    /// 2. move from one node to a node with smaller keys
    } else {
      /// 2.1. copy parent key and rightmost child of receiver
      auto u = BitOperations::getFreeZero(receiverRef.bits);
      receiverRef.keys[u] = parentRef.keys[pos];
      receiverRef.children[u] = receiverRef.children[N];
      receiverRef.bits.set(u);
      /// 2.2. move toMove-1 keys/children from donor to receiver
      for (auto i = 1u; i < toMove; ++i) {
        u = BitOperations::getFreeZero(receiverRef.bits);
        const auto min = findMinKey(donorRef.keys, donorRef.bits);
        receiverRef.keys[u] = donorRef.keys[min];
        receiverRef.children[u] = donorRef.children[min];
        receiverRef.bits.set(u);
        donorRef.bits.reset(min);
      }
      /// 2.3. set receivers new rightmost child and new parent key
      const auto dPos = findMinKey(donorRef.keys, donorRef.bits);
      receiverRef.children[N] = donorRef.children[dPos];
      parentRef.keys[pos] = donorRef.keys[dPos];
      donorRef.bits.reset(dPos);
    }
  }

  /* -------------------------------------------------------------------------------------------- */
  /*                                         INSERT                                               */
  /* -------------------------------------------------------------------------------------------- */

  /**
   * Insert a (key, value) pair into the corresponding leaf node. It is the responsibility of the
   * caller to make sure that the node @c node is the correct node. The key is inserted at the last
   * position.
   *
   * @param node the node where the key-value pair is inserted.
   * @param key the key to be inserted
   * @param val the value associated with the key
   * @param splitInfo information about a possible split of the node
   * @return whether a split occured
   */
  bool insertInLeafNode(const pptr<LeafNode> &node, const KeyType &key, const ValueType &val,
                        SplitInfo *splitInfo) {
    const auto pos = lookupPositionInLeafNode(node, key);
    auto &nodeRef = *node;
    if (pos < M) {
      /// handle insert of duplicates
      nodeRef.values.get_rw()[pos] = val;
      return false;
    }
    const auto u = BitOperations::getFreeZero(nodeRef.bits.get_ro());
    if (u == M) {
      /// split the node
      splitLeafNode(node, splitInfo);
      auto &splitRef = *splitInfo;
      const auto &sibling = splitRef.rightChild.leaf;
      const auto &sibRef = *sibling;

      /// insert the new entry
      if (key > splitRef.key) {
        insertInLeafNodeAtPosition(sibling, BitOperations::getFreeZero(sibRef.bits.get_ro()), key, val);
      } else {
        if (key > findMaxKey(nodeRef.keys.get_ro(), nodeRef.bits.get_ro())) {
          /// Special case: new key would be the middle, thus must be right
          insertInLeafNodeAtPosition(sibling, BitOperations::getFreeZero(sibRef.bits.get_ro()), key, val);
          splitRef.key = key;
        } else {
          insertInLeafNodeAtPosition(node, BitOperations::getFreeZero(nodeRef.bits.get_ro()), key, val);
        }
      }
      /// inform the caller about the split
      return true;
    } else {
      /// otherwise, we can simply insert the new entry at the last position
      insertInLeafNodeAtPosition(node, u, key, val);
    }
    return false;
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

    /// determine the split position by finding the median in unsorted array of keys
    const auto data = nodeRef.keys.get_ro();
    auto [b, splitPos] = findSplitKey(data);
    const auto &splitKey = nodeRef.keys.get_ro()[splitPos];

    /// copy leaf with flipped bitmap
    const auto sibling = newLeafNode(node);
    auto &sibRef = *sibling;
    nodeRef.bits.get_rw() = b;
    sibRef.bits.get_rw() = b.flip();
    PersistEmulation::writeBytes<sizeof(LeafNode) + ((2*M+7)>>3)>();

    /// Alternative: move instead of complete copy
    /*
    const auto sibling = newLeafNode();
    auto &sibRef = *sibling;
    auto &sibKeys = sibRef.keys.get_rw();
    auto &sibValues = sibRef.values.get_rw();
    auto &sibBits = sibRef.bits.get_rw();
    auto &nodeBits = nodeRef.bits.get_rw();
    auto j = 0u;
    for(auto i = 0u; i < M; ++i) {
      if(nodeRef.keys.get_ro()[i] >= splitKey) {
        sibKeys[j] = nodeRef.keys.get_ro()[i];
        sibValues[j] = nodeRef.values.get_ro()[i];
        sibBits.set(j);
        nodeBits.reset(i);
        j++;
      }
    }
    PersistEmulation::writeBytes(j * (sizeof(KeyType) + sizeof(ValueType)) + ((j*2+7)>>3)); /// j entries + j*2 bits
    */

    /// setup the list of leaf nodes
    if(nodeRef.nextLeaf != nullptr) {
      sibRef.nextLeaf = nodeRef.nextLeaf;
      nodeRef.nextLeaf->prevLeaf = sibling;
      PersistEmulation::writeBytes<16*2>();
    }
    nodeRef.nextLeaf = sibling;
    sibRef.prevLeaf = node;
    PersistEmulation::writeBytes<16*2>();


    /// set split information
    auto &splitInfoRef = *splitInfo;
    splitInfoRef.leftChild = node;
    splitInfoRef.rightChild = sibling;
    splitInfoRef.key = splitKey;
  }

  /**
   * Insert a (key, value) pair at the first free position into the leaf node @c node. The caller
   * has to ensure that there is enough space to insert the element.
   *
   * @oaram node the leaf node where the element is to be inserted
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   */
  void insertInLeafNodeAtPosition(const pptr<LeafNode> &node, const unsigned int pos,
                                      const KeyType &key, const ValueType &val) {
    assert(pos < M);
    auto &nodeRef = *node;

    /// insert the new entry at this position
    nodeRef.keys.get_rw()[pos] = key;
    nodeRef.values.get_rw()[pos] = val;
    nodeRef.bits.get_rw().set(pos);
    PersistEmulation::writeBytes<sizeof(KeyType) + sizeof(ValueType) + 1>();
  }

  /**
   * Split the given branch node @c node in the middle and move half of the keys/children to the new
   * sibling node.
   *
   * @param node the branch node to be split
   * @param childSplitKey the key on which the split of the child occured
   * @param splitInfo information about the split
   */
  void splitBranchNode(BranchNode * const node, const KeyType &childSplitKey, SplitInfo *splitInfo) {
    auto &nodeRef = *node;

    /// determine the split position (by finding median in unsorted array of keys)
    auto data = nodeRef.keys;
    auto [b, splitPos] = findSplitKey(data);
    const auto &splitKey = nodeRef.keys[splitPos];

    /// copy node and flip bits
    const auto sibling = newBranchNode(node);
    auto &sibRef = *sibling;
    nodeRef.bits = b.reset(splitPos);
    sibRef.bits = b.flip().reset(splitPos);

    /// setup the list of nodes
    sibRef.children[N] = nodeRef.children[N];
    nodeRef.children[N] = nodeRef.children[splitPos];
    auto &splitRef = *splitInfo;
    splitRef.key = splitKey;
    splitRef.leftChild = node;
    splitRef.rightChild = sibling;
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
  bool insertInBranchNode(BranchNode * const node, const unsigned int depth,
                          const KeyType &key, const ValueType &val, SplitInfo *splitInfo) {
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;
    auto &nodeRef = *node;

    auto pos = lookupPositionInBranchNode(node, key);
    if (depth == 1) {
      /// case #1: our children are leaf nodes
      const auto child = nodeRef.children[pos].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      /// case #2: our children are branch nodes
      const auto child = nodeRef.children[pos].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }

    if (hasSplit) {
      /// the child node was split, thus we have to add a new entry to our branch node
      auto host = node;
      if (nodeRef.bits.count() == N) {
        /// this node is also full and needs to be split
        splitBranchNode(node, childSplitInfo.key, splitInfo);
        const auto &splitRef = *splitInfo;
        host = (key < splitRef.key ? splitRef.leftChild : splitRef.rightChild).branch;
        split = true;
      }
      /// Insert new key and children
      auto &hostRef = *host;
      const auto u = BitOperations::getFreeZero(hostRef.bits);
      const auto nextPos = findMinKeyGreaterThan(hostRef.keys, hostRef.bits, childSplitInfo.key);

      hostRef.keys[u] = childSplitInfo.key;
      hostRef.children[u] = childSplitInfo.leftChild;
      hostRef.children[nextPos] = childSplitInfo.rightChild;
      hostRef.bits.set(u);
    }
    return split;
  }

  /* -------------------------------------------------------------------------------------------- */
  /*                                         LOOKUP                                               */
  /* -------------------------------------------------------------------------------------------- */

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
    auto d = depth;
    while (d-- > 0) {
      /// as long as we aren't at the leaf level we follow the path down
      const auto pos = lookupPositionInBranchNode(node.branch, key);
      node = node.branch->children[pos];
    }
    return node.leaf;
  }

  /**
   * Lookup the search key @c key in the given branch node and return the position which is the
   * position in the list of keys + 1. in this way, the position corresponds to the position of the
   * child pointer in the array @children.
   * If the search key is less than the smallest key, then @c 0 is returned.
   * If the key is greater than the largest key, then @c N is returned.
   *
   * @param node the branch node where we search
   * @param key the search key
   * @return the position of the key + 1 (or 0 or @c N)
   */
  auto lookupPositionInBranchNode(const BranchNode *node, const KeyType &key) const {
    return findMinKeyGreaterThan(node->keys, node->bits, key);
  }

  /**
   * Lookup the search key @c key in the given leaf node and return the position.
   * If the search key was not found, then @c M is returned.
   *
   * @param node the leaf node where we search
   * @param key the search key
   * @return the position of the key (or @c M if not found)
   */
  auto lookupPositionInLeafNode(const pptr<LeafNode> &node, const KeyType &key) const {
    auto pos = 0u;
    const auto &nodeRef = *node;
    const auto &bitsRef = nodeRef.bits.get_ro();
    const auto &keysRef = nodeRef.keys.get_ro();

    for (; pos < M; ++pos)
      if (bitsRef.test(pos) && keysRef[pos] == key) break;
    return pos;
  }

  /* -------------------------------------------------------------------------------------------- */
  /*                                          RECOVERY                                            */
  /* -------------------------------------------------------------------------------------------- */

  /**
   * Insert a leaf node into the tree for recovery
   *
   * @param leaf the leaf to insert
   */
  void recoveryInsert(const pptr<LeafNode> &leaf){
    assert(depth > 0);
    assert(leaf != nullptr);

    SplitInfo splitInfo;
    auto hassplit = recoveryInsertInBranchNode(rootNode.branch, depth, leaf, &splitInfo);

    /// Check for split
    if (hassplit) {
      /// The root node was splitted
      const auto newRoot = newBranchNode();
      auto &newRootRef = *newRoot;
      newRootRef.keys[0] = splitInfo.key;
      newRootRef.children[0] = splitInfo.leftChild;
      newRootRef.children[N] = splitInfo.rightChild;
      newRootRef.bits.set(0);
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
    SplitInfo childSplitInfo;
    auto &nodeRef = *node;
    const auto nNumKeys = nodeRef.bits.count();
    const auto &leafRef = *leaf;
    const auto &leafBits = leafRef.bits.get_ro();

    if (curr_depth == 1) {
      if (nNumKeys == N) {
        /// we have to insert a new right child, as the keys in the leafList are sorted
        BranchNode *newNode = newBranchNode();
        newNode->children[0] = leaf;
        auto &splitRef = *splitInfo;
        splitRef.key = leafRef.keys.get_ro()[findMinKey(leafRef.keys.get_ro(),
                                                        leafRef.bits.get_ro())];
        splitRef.leftChild = node;
        splitRef.rightChild = newNode;
        return true;
      } else {
        nodeRef.bits.set(nNumKeys);
        nodeRef.keys[nNumKeys] = leafRef.keys.get_ro()[findMinKey(leafRef.keys.get_ro(),
                                                                  leafRef.bits.get_ro())];
        if (nNumKeys > 0)
          nodeRef.children[nNumKeys] = nodeRef.children[N];
        nodeRef.children[N] = leaf;
        return false;
      }
    } else {
      bool hassplit = recoveryInsertInBranchNode(nodeRef.children[(nNumKeys==0)?0:N].branch,
                                                 curr_depth-1, leaf, &childSplitInfo);
      //Check for split
      if (hassplit) {
        if (nNumKeys == N) {
          BranchNode *newNode = newBranchNode();
          newNode->children[0] = childSplitInfo.rightChild;
          auto &splitRef = *splitInfo;
          splitRef.key = childSplitInfo.key;
          splitRef.leftChild = node;
          splitRef.rightChild = newNode;
          return true;
        } else {
          nodeRef.bits.set(nNumKeys);
          nodeRef.keys[nNumKeys] = childSplitInfo.key;
          if (nNumKeys > 0)
            nodeRef.children[nNumKeys] = nodeRef.children[N];
          nodeRef.children[N] = childSplitInfo.rightChild;
          return false;
        }
      } else return false;
    }
  }

  /* -------------------------------------------------------------------------------------------- */
  /*                                          DEBUGGING                                           */
  /* -------------------------------------------------------------------------------------------- */

  /**
   * Print the given branch node @c node and all its children to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printBranchNode(const unsigned int d, const BranchNode *node) const {
    const auto &nodeRef = *node;
    const auto nNumKeys = nodeRef.bits.count();
    for (auto i = 0u; i < d; ++i) std::cout << "  ";
    std::cout << d << "BN { [" << node << "] #" << nNumKeys << ": ";
    for (auto k = 0u; k < N; ++k) {
      if (k > 0) std::cout << ", ";
      std::cout << '(' << nodeRef.bits[k] << ')' << nodeRef.keys[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= N; ++k) {
      if (k < N && !nodeRef.bits.test(k)) continue;
      if (d + 1 < depth) {
        auto child = nodeRef.children[k].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto leaf = (nodeRef.children[k]).leaf;
        printLeafNode(d + 1,leaf);
      }
    }
  }

  /**
   * Print the given leaf node @c node to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printLeafNode(const unsigned int d, const pptr<LeafNode> &node) const {
    const auto &nodeRef = *node;
    const auto nNumKeys = nodeRef.bits.get_ro().count();
    for (auto i = 0u; i < d; ++i) std::cout << "  ";
    std::cout << "[\033[1m" << std::hex << node << std::dec << "\033[0m #" << nNumKeys << ": ";
    for (auto i = 0u; i < M; ++i) {
      if (i > 0) std::cout << ", ";
      std::cout << "{(" << nodeRef.bits.get_ro()[i] << ')' << nodeRef.keys.get_ro()[i]<< "}";
    }
    std::cout << "]" << std::endl;
  }

}; /* class BitHPBPTree */

} /* namespace dbis::pbptrees */

#endif /* DBIS_BitHPBPTree_hpp_ */
