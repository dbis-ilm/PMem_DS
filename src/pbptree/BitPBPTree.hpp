/*
 * Copyright (C) 2017-2019 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of our NVM-based Data Structure Repository.
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

#ifndef DBIS_BitPBPTree_hpp_
#define DBIS_BitPBPTree_hpp_

#include <array>
#include <iostream>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>
#include "utils/ElementOfRankK.hpp"
#include "utils/PersistEmulation.hpp"
#include "config.h"

namespace dbis::pbptree {

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
class BitPBPTree {
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

    Node(const pptr<LeafNode> &leaf_) : tag(LEAF), leaf(leaf_) {};

    Node(const pptr<BranchNode> &branch_) : tag(BRANCH), branch(branch_) {};

    Node(const Node &other) { copy(other); };

    void copy(const Node &other) {
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
        default:
          break;
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

  /* -------------------------------------------------------------------------------------------- */

  /**
   * A structure for representing a leaf node of a B+ tree.
   */
  struct alignas(64) LeafNode {
    /**
     * Constructor for creating a new empty leaf node.
     */
    LeafNode() : nextLeaf(nullptr), prevLeaf(nullptr) {}

    static constexpr unsigned int NUM_KEYS = M;

    p<std::bitset<M>>             bits; ///< bitset for valid entries
    p<std::array<KeyType, M>>     keys; ///< the actual keys
    p<std::array<ValueType, M>> values; ///< the actual values
    pptr<LeafNode>            nextLeaf; ///< pointer to the subsequent sibling
    pptr<LeafNode>            prevLeaf; ///< pointer to the preceeding sibling
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   * The rightmost child is always at position N.
   */
  struct alignas(64) BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode(){}

    static constexpr unsigned int NUM_KEYS = N;

    p<std::bitset<N>>              bits; ///< bitset for valid entries
    p<std::array<KeyType, N>>      keys; ///< the actual keys
    p<std::array<Node, N + 1>> children; ///< pointers to child nodes (BranchNode or LeafNode)
  };

  /* -------------------------------------------------------------------------------------------- */

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

  Node rootNode; /**< pointer to the root node (an instance of @c LeafNode or @c BranchNode).
                      This pointer is never @c nullptr. */

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
      while (d-- > 0) {
        PROFILE_READ(1)
        node = node.branch->children.get_ro()[0];
      }
      currentNode = node.leaf;
      currentPosition = 0;
      const auto &nodeRef = *currentNode;
      while(!nodeRef.bits.get_ro().test(currentPosition)) ++currentPosition;

    }

    iterator &operator++() {
      if (currentPosition >= M-1) {
        currentNode = currentNode->nextLeaf;
        currentPosition = 0;
        if (currentNode == nullptr) return *this;
        const auto &nodeRef = *currentNode;
        while(!nodeRef.bits.get_ro().test(currentPosition)) ++currentPosition;
      } else if (!currentNode->bits.get_ro().test(++currentPosition)) ++(*this);
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
      PROFILE_READ(2)
      return std::make_pair(currentNode->keys.get_ro()[currentPosition],
                            currentNode->values.get_ro()[currentPosition]);
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

  PROFILE_DECL

  /**
   * Alias for a function passed to the scan method.
   */
  using ScanFunc = std::function<void(const KeyType &key, const ValueType &val)>;

  /**
   * Constructor for creating a new B+ tree.
   */
  BitPBPTree() : depth(0) {
    rootNode = newLeafNode();
    PROFILE_INIT
    LOG("created new tree with sizeof(BranchNode) = " << sizeof(BranchNode) <<
        ", sizeof(LeafNode) = " << sizeof(LeafNode));
  }

  /**
   * Destructor for the B+ tree.
   */
  ~BitPBPTree() {}

  /**
   * Insert an element (a key-value pair) into the B+ tree. If the key @c key already exists, the
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
          auto root = newBranchNode();
          auto &rootRef = *root;
          PROFILE_READ(1)
          PROFILE_WRITE(5)
          rootRef.keys.get_rw()[0] = splitInfo.key;
          rootRef.children.get_rw()[0] = splitInfo.leftChild;
          rootRef.children.get_rw()[N] = splitInfo.rightChild;
          rootRef.bits.get_rw().set(0);
          rootNode.branch = root;
          depth.get_rw() = depth.get_ro() + 1;
        }
    });
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
    PROFILE_READ(1)
    if (pos < M) {
      /// we found it
      PROFILE_READ(1)
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
    auto pop = pmem::obj::pool_by_vptr(this);
    bool result;
    transaction::run(pop, [&] {
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
    });
    return result;
  }

  /**
   * Nothing to do as everything is persistent and failure-atomic(?).
   */
  void recover() { return; }

  /**
   * Print the structure and content of the B+ tree to stdout.
   */
  void print() const {
    if (depth == 0) printLeafNode(0, rootNode.leaf);
    else printBranchNode(0u, rootNode.branch);
  }

  PROFILE_PRINT

  /**
   * Perform a scan over all key-value pairs stored in the B+ tree.
   * For each entry the given function @func is called.
   *
   * @param func the function called for each entry
   */
  void scan(ScanFunc func) const {
    /// we traverse to the leftmost leaf node
    auto node = rootNode;
    PROFILE_READ(1)
    auto d = depth.get_ro();
    while (d-- > 0) {
      /// as long as we aren't at the leaf level we follow the path down
      PROFILE_READ(1)
      node = node.branch->children.get_ro()[0];
    }
    auto leaf = node.leaf;
    while (leaf != nullptr) {
      auto &leafRef = *leaf;
      /// for each key-value pair call func
      for (auto i = 0u; i < M; i++) {
        if (!leafRef.bits.get_ro().test(i)) continue;
        PROFILE_READ(2)
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
    auto leaf = findLeafNode(minKey);

    bool higherThanMax = false;
    while (!higherThanMax && leaf != nullptr) {
      /// for each key-value pair within the range call func
      auto &leafRef = *leaf;
      for (auto i = 0u; i < M; i++) {
        PROFILE_READ(1)
        if (leafRef.bits.get_ro().test(i)) continue;

        PROFILE_READ(1)
        const auto &key = leafRef.keys.get_ro()[i];
        if (key < minKey) continue;
        if (key > maxKey) { higherThanMax = true; continue; };

        PROFILE_READ(1)
        const auto &val = leafRef.values.get_ro()[i];
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
    auto &nodeRef = *node;
    PROFILE_READ(1)
    if (nodeRef.keys.get_ro()[pos] == key) {
      PROFILE_READ(1)
      PROFILE_WRITE(1)
      /// simply reset bit
      nodeRef.bits.get_rw().reset(pos);
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
  void underflowAtLeafLevel(const pptr<BranchNode> &node, const unsigned int pos,
                            const pptr<LeafNode> &leaf) {
      assert(pos <= M);
      auto &nodeRef = *node;
      const auto &leafRef = *leaf;
      PROFILE_READ(1)
      const auto prevNumKeys = leafRef.prevLeaf->bits.get_ro().count();
      constexpr auto middle = (M + 1) / 2;

      /// 1. we check whether we can rebalance with one of the siblings but only if both nodes have
      //     the same direct parent
      if (pos > 0 && prevNumKeys > middle) {
        /// we have a sibling at the left for rebalancing the keys
        balanceLeafNodes(leafRef.prevLeaf, leaf);
        PROFILE_READ(1)
        PROFILE_WRITE(1)
        const auto newMin = leafRef.keys.get_ro()[findMinKeyInNode(leaf)];
        const auto prevPos = findMinKeyGreaterThan(leaf, newMin);
        nodeRef.keys.get_rw()[prevPos] = newMin;
      } else if (pos < nodeRef.bits.get_ro().count() &&
                 leafRef.nextLeaf->bits.get_ro().count() > middle) {
        /// we have a sibling at the right for rebalancing the keys
        balanceLeafNodes(leafRef.nextLeaf, leaf);
        PROFILE_READ(1)
        PROFILE_WRITE(1)
        nodeRef.keys.get_rw()[pos] =
          leafRef.nextLeaf->keys.get_ro()[findMinKeyInNode(leafRef.nextLeaf)];
      } else {
        /// 2. if this fails we have to merge two leaf nodes but only if both nodes have the same
        //     direct parent
        pptr<LeafNode> survivor = nullptr;
        const auto nNumKeys = nodeRef.bits.get_ro().count();

        if (pos > 0 && prevNumKeys <= middle) {
          /// merge left
          survivor = mergeLeafNodes(leafRef.prevLeaf, leaf);
          deleteLeafNode(leaf);
          PROFILE_READ(1)
          PROFILE_WRITE(1)
          nodeRef.children.get_rw()[pos] = survivor;
          if (pos == N) {
            PROFILE_READ(1)
            PROFILE_WRITE(2)
            const auto prevPos = findMaxKeyInNode(node);
            nodeRef.bits.get_rw().reset(prevPos);
            nodeRef.children.get_rw()[N] = nodeRef.children.get_ro()[prevPos];
          } else {
            PROFILE_WRITE(1)
            const auto prevPos = findMaxKeySmallerThan(node, nodeRef.keys.get_ro()[pos]);
            nodeRef.bits.get_rw().reset(prevPos);
          }
        } else if (pos < nNumKeys && leafRef.nextLeaf->bits.get_ro().count() <= middle) {
          /// merge right
          /// because we update the pointers in mergeLeafNodes we keep it here
          survivor = mergeLeafNodes(leaf, leafRef.nextLeaf);
          deleteLeafNode(leafRef.nextLeaf);
          PROFILE_WRITE(1)
          nodeRef.bits.get_rw().reset(pos);
        } else {
          /// this shouldn't happen?!
          assert(false);
        }
        if (nNumKeys <= 1) {
          /// This is a special case that happens only if the current node is the root node. Now, we
          /// have to replace the branch root node by a leaf node.
          rootNode = survivor;
          PROFILE_READ(1)
          PROFILE_WRITE(1)
          depth.get_rw() = depth.get_ro() - 1;
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
    assert(node1Ref.bits.get_ro().count() + node2Ref.bits.get_ro().count() <= M);

    /// we move all keys/values from node2 to node1
    for (auto i = 0u; i < M; i++) {
      PROFILE_READ(1)
      if (node2Ref.bits.get_ro().test(i)) {
        PROFILE_READ(3)
        PROFILE_WRITE(3)
        const auto u = getFreeZero(node1Ref.bits.get_ro());
        node1Ref.keys.get_rw()[u] = node2Ref.keys.get_ro()[i];
        node1Ref.values.get_rw()[u] = node2Ref.values.get_ro()[i];
        node1Ref.bits.get_rw().set(u);
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
    PROFILE_READ(2)
    const auto dNumKeys = donorRef.bits.get_ro().count();
    const auto rNumKeys = receiverRef.bits.get_ro().count();
    assert(dNumKeys > rNumKeys);

    const auto balancedNum = (dNumKeys + rNumKeys) / 2;
    const auto toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    PROFILE_READ(2)
    if (donorRef.keys.get_ro()[0] < receiverRef.keys.get_ro()[0]) {
      /// move from one node to a node with larger keys
      for (auto i = 0u; i < toMove; i++) {
        PROFILE_READ(3)
        PROFILE_WRITE(4)
        const auto max = findMaxKeyInNode(donor);
        const auto u = getFreeZero(receiverRef.bits.get_ro());
        /// move the donor's maximum key to the receiver
        receiverRef.keys.get_rw()[u] = donorRef.keys.get_ro()[max];
        receiverRef.values.get_rw()[u] = donorRef.values.get_ro()[max];
        receiverRef.bits.get_rw().set(u);
        donorRef.bits.get_rw().reset(max);
      }
    } else {
      /// move from one node to a node with smaller keys
      for (auto i = 0u; i < toMove; i++) {
        const auto min = findMinKeyInNode(donor);
        const auto u = getFreeZero(receiverRef.bits.get_ro());
        /// move the donor's minimum key to the receiver
        PROFILE_READ(3)
        PROFILE_WRITE(4)
        receiverRef.keys.get_rw()[u] = donorRef.keys.get_ro()[min];
        receiverRef.values.get_rw()[u] = donorRef.values.get_ro()[min];
        receiverRef.bits.get_rw().set(u);
        donorRef.bits.get_rw().reset(min);
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
  bool eraseFromBranchNode(const pptr<BranchNode> &node, const unsigned int d, const KeyType &key) {
    assert(d >= 1);
    const auto &nodeRef = *node;
    bool deleted = false;
    /// try to find the branch
    auto pos = lookupPositionInBranchNode(node, key);
    PROFILE_READ(1)
    const auto n = nodeRef.children.get_ro()[pos];
    if (d == 1) {
      /// the next level is the leaf level
      const auto &leaf = n.leaf;
      deleted = eraseFromLeafNode(leaf, key);
      constexpr auto middle = (M + 1) / 2;
      if (leaf->bits.get_ro().count() < middle) {
        /// handle underflow
        PROFILE_UNDERFLOW
        underflowAtLeafLevel(node, pos, leaf);
      }
    } else {
      auto child = n.branch;
      deleted = eraseFromBranchNode(child, d - 1, key);

      pos = lookupPositionInBranchNode(node, key);
      constexpr auto middle = (N + 1) / 2;
      if (child->bits.get_ro().count() < middle) {
        /// handle underflow
        PROFILE_UNDERFLOW
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth && nodeRef.bits.get_ro().count() == 0) {
          /// special case: the root node is empty now
          rootNode = child;
          PROFILE_READ(1)
          PROFILE_WRITE(1)
          depth.get_rw() = depth.get_ro() - 1;
        }
      }
    }
    return deleted;
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
    const auto &nodeRef = *node;
    auto &sibRef = *sibling;
    PROFILE_READ(2)
    assert(key <= nodeRef.keys.get_ro()[findMinKeyInNode(node)]);
    assert(sibRef.keys.get_ro()[findMaxKeyInNode(sibling)] < key);

    PROFILE_READ(3)
    PROFILE_WRITE(3)
    const auto u = getFreeZero(sibRef.bits.get_ro());
    sibRef.keys.get_rw()[u] = key;
    sibRef.children.get_rw()[u] = sibRef.children.get_ro()[N];
    sibRef.bits.get_rw().set(u);
    for (auto i = 0u; i < M; i++) {
      PROFILE_READ(1)
      if (nodeRef.bits.get_ro().test(i)) {
        PROFILE_READ(3)
        PROFILE_WRITE(3)
        const auto u = getFreeZero(sibRef.bits.get_ro());
        sibRef.keys.get_rw()[u] = nodeRef.keys.get_ro()[i];
        sibRef.children.get_rw()[u] = nodeRef.children.get_ro()[i];
        sibRef.bits.get_rw().set(u);
      }
    }
    PROFILE_READ(1)
    PROFILE_WRITE(1)
    sibRef.children.get_rw()[N] = nodeRef.children.get_ro()[N];
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
	 * TODO: I think this is not entirely correct, yet
   */
  pptr<BranchNode> underflowAtBranchLevel(const pptr<BranchNode> &node, const unsigned int pos,
                                          const pptr<BranchNode> &child) {
    assert(node != nullptr);
    assert(child != nullptr);
		auto &nodeRef = *node;
    PROFILE_READ(5)
		const auto &nNumKeys = nodeRef.bits.get_ro().count();
		const auto prevPos = findMaxKeySmallerThan(node, nodeRef.keys.get_ro()[pos]);
		const auto prevNumKeys = nodeRef.children.get_ro()[prevPos].branch->bits.get_ro().count();
		auto nextPos = 0u;
		auto nextNumKeys = 0u;
    constexpr auto middle = (N + 1) / 2;
    pptr<BranchNode> newChild = child;

		/// 1. we check whether we can rebalance with one of the siblings
    if (pos > 0 && prevNumKeys > middle) {
      /// we have a sibling at the left for rebalancing the keys
      PROFILE_READ(1)
      const auto sibling = (nodeRef.children.get_ro()[prevPos]).branch;
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    } else if (pos < nNumKeys) {
    	PROFILE_READ(3)
			nextPos = findMinKeyGreaterThan(node, nodeRef.keys.get_ro()[pos]);
			nextNumKeys = nodeRef.children.get_ro()[nextPos].branch->bits.get_ro().count();
      /// we have a sibling at the right for rebalancing the keys
			if (nextNumKeys > middle) {
      	PROFILE_READ(1)
      	auto sibling = (nodeRef.children.get_ro()[nextPos]).branch;
      	balanceBranchNodes(sibling, child, node, pos);
      	return newChild;
			}
    }
    /// 2. if this fails we have to merge two branch nodes
    pptr<BranchNode> lSibling = nullptr, rSibling = nullptr;
    auto prevKeys = 0u, nextKeys = 0u;

    if (pos > 0) {
      PROFILE_READ(1)
      lSibling = (nodeRef.children.get_ro()[prevPos]).branch;
      prevKeys = prevNumKeys;
    }
    if (pos < nNumKeys) {
      PROFILE_READ(1)
      rSibling = (nodeRef.children.get_ro()[nextPos]).branch;
      nextKeys = nextNumKeys;
    }

    pptr<BranchNode> witnessNode = nullptr;
    if (prevKeys > 0) {
      PROFILE_READ(1)
      mergeBranchNodes(lSibling, nodeRef.keys.get_ro()[pos], child);
      witnessNode = child;
      newChild = lSibling;
    } else if (nextKeys > 0) {
      PROFILE_READ(1)
      mergeBranchNodes(child, nodeRef.keys.get_ro()[pos], rSibling);
      witnessNode = rSibling;
    } else assert(false); ///< shouldn't happen

    /// cleanup node
    PROFILE_WRITE(2)
    nodeRef.bits.get_rw().reset(pos);
    if (pos == nNumKeys) nodeRef.children.get_rw()[N] = child; ///< new rightmost child

    deleteBranchNode(witnessNode);
    return newChild;
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
                          const pptr<BranchNode> &parent, const unsigned int pos) {
    auto &donorRef = *donor;
    auto &receiverRef = *receiver;
    auto &parentRef = *parent;
    PROFILE_READ(2)
    const auto dNumKeys = donorRef.bits.get_ro().count();
    const auto rNumKeys = receiverRef.bits.get_ro().count();
    assert(dNumKeys > rNumKeys);

    const auto balancedNum = (dNumKeys + rNumKeys) / 2;
    const auto toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    /// 1. move from one node to a node with larger keys
    PROFILE_READ(2)
    if (donorRef.keys.get_ro()[0] < receiverRef.keys.get_ro()[0]) { //TODO: bit check necessary?
      PROFILE_READ(3 * toMove + 2)
      PROFILE_WRITE(4 * toMove + 2)
      /// 1.1. copy parent key and rightmost child of receiver
      const auto u = getFreeZero(receiverRef.bits.get_ro());
      receiverRef.keys.get_rw()[u] = parentRef.keys.get_ro()[pos];
      receiverRef.children.get_rw()[u] = donorRef.children.get_ro()[N];
      receiverRef.bits.get_rw().set(u);
      /// 1.2. move toMove-1 keys/children from donor to receiver
      for (auto i = 1u; i < toMove; i++) {
      	const auto max = findMaxKeyInNode(donor);
        const auto u = getFreeZero(receiverRef.bits.get_ro());
        receiverRef.keys.get_rw()[u] = donorRef.keys.get_ro()[max];
        receiverRef.children.get_rw()[u] = donorRef.children.get_ro()[max];
        receiverRef.bits.get_rw().set(u);
        donorRef.bits.get_rw().reset(max);
      }
      /// 1.3 set donors new rightmost child and new parent key
      const auto dPos = findMaxKeyInNode(donor);
      donorRef.children.get_rw()[N] = donorRef.children.get_ro()[dPos];
      parentRef.keys.get_rw()[pos] = donorRef.keys.get_ro()[dPos];
      donorRef.bits.get_rw().reset(dPos);

    /// 2. move from one node to a node with smaller keys
    } else {
      PROFILE_READ(3 * toMove + 2)
      PROFILE_WRITE(4 * toMove + 2)
      /// 2.1. copy parent key and rightmost child of receiver
      const auto u = getFreeZero(receiverRef.bits.get_ro());
      receiverRef.keys.get_rw()[u] = parentRef.keys.get_ro()[pos];
      receiverRef.children.get_rw()[u] = receiverRef.children.get_ro()[N];
      receiverRef.bits.get_rw().set(u);
      /// 2.2. move toMove-1 keys/children from donor to receiver
      for (auto i = 1u; i < toMove; i++) {
        const auto u = getFreeZero(receiverRef.bits.get_ro());
        const auto min = findMinKeyInNode(donor);
        receiverRef.keys.get_rw()[u] = donorRef.keys.get_ro()[min];
        receiverRef.children.get_rw()[u] = donorRef.children.get_ro()[min];
        receiverRef.bits.get_rw().set(u);
        donorRef.bits.get_rw().reset(min);
      }
      /// 2.3. set receivers new rightmost child and new parent key
      const auto dPos = findMinKeyInNode(donor);
      receiverRef.children.get_rw()[N] = donorRef.children.get_ro()[dPos];
      parentRef.keys.get_rw()[pos] = donorRef.keys.get_ro()[dPos];
      donorRef.bits.get_rw().reset(dPos);
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
    PROFILE_READ(1)
    if (pos < M) {
      /// handle insert of duplicates
      PROFILE_WRITE(1)
      nodeRef.values.get_rw()[pos] = val;
      return false;
    }
    const auto u = getFreeZero(nodeRef.bits.get_ro());
    if (u == M) {
      /// split the node
      splitLeafNode(node, splitInfo);
      auto &splitRef = *splitInfo;
      const auto sibling = splitRef.rightChild.leaf;

      /// insert the new entry
      if (key < splitRef.key) insertInLeafNodeAtFreePosition(node, key, val);
      else insertInLeafNodeAtFreePosition(sibling, key, val);

      /// inform the caller about the split
      splitRef.key = sibling->keys.get_ro()[findMinKeyInNode(sibling)];
      return true;
    } else {
      /// otherwise, we can simply insert the new entry at the last position
      insertInLeafNodeAtFreePosition(node, key, val);
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
    PROFILE_SPLIT
    PROFILE_READ(1)
    const auto data = nodeRef.keys.get_ro();
    auto [b, splitPos] = findSplitKey(data);
    const auto &splitKey = nodeRef.keys.get_ro()[splitPos];

    /// copy leaf with flipped bitmap
    auto sibling = newLeafNode(node);
    auto &sibRef = *sibling;
    nodeRef.bits.get_rw() = b;
    sibRef.bits.get_rw() = b.flip();

    /// setup the list of leaf nodes
    if(nodeRef.nextLeaf != nullptr) {
      sibRef.nextLeaf = nodeRef.nextLeaf;
      nodeRef.nextLeaf->prevLeaf = sibling;
    }
    nodeRef.nextLeaf = sibling;
    sibRef.prevLeaf = node;

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
  void insertInLeafNodeAtFreePosition(const pptr<LeafNode> &node, const KeyType &key,
                                      const ValueType &val) {
    auto &nodeRef = *node;
    const auto pos = getFreeZero(nodeRef.bits.get_ro());
    assert(pos < M);
    /// insert the new entry at this position
    PROFILE_WRITE(3)
    nodeRef.keys.get_rw()[pos] = key;
    nodeRef.values.get_rw()[pos] = val;
    nodeRef.bits.get_rw().set(pos);
  }

  /**
   * Split the given branch node @c node in the middle and move half of the keys/children to the new
   * sibling node.
   *
   * @param node the branch node to be split
   * @param childSplitKey the key on which the split of the child occured
   * @param splitInfo information about the split
   */
  void splitBranchNode(const pptr<BranchNode> &node, const KeyType &childSplitKey,
                       SplitInfo *splitInfo) {
    auto &nodeRef = *node;
    /// determine the split position (by finding median in unsorted array of keys)
    PROFILE_READ(2)
    auto data = nodeRef.keys.get_ro();
    auto [b, splitPos] = findSplitKey(data);
    const auto &splitKey = nodeRef.keys.get_ro()[splitPos];

    /// copy node
    const auto sibling = newBranchNode(node);
    auto &sibRef = *sibling;
    PROFILE_WRITE(2)
    nodeRef.bits.get_rw() = b.reset(splitPos);
    sibRef.bits.get_rw() = b.flip().reset(splitPos);

    /// setup the list of nodes
    sibRef.children.get_rw()[N] = nodeRef.children.get_ro()[N];
    nodeRef.children.get_rw()[N] = nodeRef.children.get_ro()[splitPos];
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
  bool insertInBranchNode(const pptr<BranchNode> &node, const unsigned int depth,
                          const KeyType &key, const ValueType &val, SplitInfo *splitInfo) {
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;
    auto &nodeRef = *node;

    auto pos = lookupPositionInBranchNode(node, key);
    if (depth == 1) {
      /// case #1: our children are leaf nodes
      PROFILE_READ(1)
      const auto child = nodeRef.children.get_ro()[pos].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      /// case #2: our children are branch nodes
      PROFILE_READ(1)
      const auto child = nodeRef.children.get_ro()[pos].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }
    if (hasSplit) {
      /// the child node was split, thus we have to add a new entry to our branch node
      auto host = node;
      PROFILE_READ(1)
      if (nodeRef.bits.get_ro().count() == N) {
        /// this node is also full and needs to be split
        splitBranchNode(node, childSplitInfo.key, splitInfo);
        const auto &splitRef = *splitInfo;
        PROFILE_SPLIT
        host = (key < splitRef.key ? splitRef.leftChild : splitRef.rightChild).branch;
        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
      /// Insert new key and children
      auto &hostRef = *host;
      const auto u = getFreeZero(hostRef.bits.get_ro());
      const auto nextPos = findMinKeyGreaterThan(host, childSplitInfo.key);
      PROFILE_WRITE(4)
      hostRef.keys.get_rw()[u] = childSplitInfo.key;
      hostRef.children.get_rw()[u] = childSplitInfo.leftChild;
      hostRef.children.get_rw()[nextPos] = childSplitInfo.rightChild;
      hostRef.bits.get_rw().set(u);
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
    PROFILE_READ(1)
    auto d = depth.get_ro();
    while (d-- > 0) {
      /// as long as we aren't at the leaf level we follow the path down
      const auto n = node.branch;
      const auto pos = lookupPositionInBranchNode(n, key);
      PROFILE_READ(1)
      node = n->children.get_ro()[pos];
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
  auto lookupPositionInBranchNode(const pptr<BranchNode> &node, const KeyType &key) const {
    return findMinKeyGreaterThan(node, key);
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
    PROFILE_READ(2)
    const auto &bitsRef = nodeRef.bits.get_ro();
    const auto &keysRef = nodeRef.keys.get_ro();

    for (; pos < M; pos++)
      if (bitsRef.test(pos) && keysRef[pos] == key) break;
    return pos;
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
  void printBranchNode(const unsigned int d, const pptr<BranchNode> &node) const {
    PROFILE_READ(1)
		const auto &nodeRef = *node;
		const auto nNumKeys = nodeRef.bits.get_ro().count();
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << d << "BN { [" << node << "] #" << nNumKeys << ": ";
    for (auto k = 0u; k < N; k++) {
      if (k > 0) std::cout << ", ";
      PROFILE_READ(2)
      std::cout << '(' << nodeRef.bits.get_ro()[k] << ')' << nodeRef.keys.get_ro()[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= N; k++) {
      if (k < N && !nodeRef.bits.get_ro().test(k)) continue;
      if (d + 1 < depth) {
        PROFILE_READ(1)
        auto child = nodeRef.children.get_ro()[k].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        PROFILE_READ(1)
        auto leaf = (nodeRef.children.get_ro()[k]).leaf;
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
    PROFILE_READ(1)
		const auto &nodeRef = *node;
		const auto nNumKeys = nodeRef.bits.get_ro().count();
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << "[\033[1m" << std::hex << node << std::dec << "\033[0m #" << nNumKeys << ": ";
    for (auto i = 0u; i < M; i++) {
      if (i > 0) std::cout << ", ";
      PROFILE_READ(2)
      std::cout << "{(" << nodeRef.bits.get_ro()[i] << ')' << nodeRef.keys.get_ro()[i]<< "}";
    }
    std::cout << "]" << std::endl;
  }

	/* -------------------------------------------------------------------------------------------- */
  /*                                           HELPER                                             */
  /* -------------------------------------------------------------------------------------------- */

  /**
   * Create a new empty leaf node.
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

  /**
   * Remove/delete an existing leaf node.
   */
  void deleteLeafNode(const pptr<LeafNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] { delete_persistent<LeafNode>(node); });
  }

  /**
   * Create a new empty branch node.
   */
  pptr<BranchNode> newBranchNode() {
    auto pop = pmem::obj::pool_by_vptr(this);
    pptr<BranchNode> newNode = nullptr;
    transaction::run(pop, [&] { newNode = make_persistent<BranchNode>(); });
    return newNode;
  }

  pptr<BranchNode> newBranchNode(const pptr<BranchNode> &other) {
    auto pop = pmem::obj::pool_by_vptr(this);
    pptr<BranchNode> newNode = nullptr;
    transaction::run(pop, [&] { newNode = make_persistent<BranchNode>(*other); });
    return newNode;
  }

  /**
   * Remove/delete an existing branch node.
   */
  void deleteBranchNode(const pptr<BranchNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] { delete_persistent<BranchNode>(node); });
  }

  template<size_t E>
  auto getFreeZero(std::bitset<E> b) {
		if constexpr (E > 64) {
			auto idx = 0u;
			while (idx < E && b.test(idx)) ++idx;
			return idx;
		} else {
			const auto w = b.to_ullong();
			if (w != UINT64_MAX) {
      	/// count consecutive one bits using multiply and lookup
				static constexpr uint8_t tab64[64] = {
        	63, 0, 58, 1, 59, 47, 53, 2, 60, 39, 48, 27, 54, 33, 42, 3,
        	61, 51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4,
        	62, 57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21,
        	56, 45, 25, 31, 35, 16, 9, 12, 44, 24, 15, 8, 23, 7, 6, 5};

    		/// Applying deBruijn hash function + lookup
    		return tab64[((uint64_t) ((~w & -~w) * 0x07EDD5E59A4E28C2)) >> 58];
			}
    	/// Valid result is between 0 and 63; 64 means no free position
			return static_cast<uint8_t>(64);
		}
 	}

  /**
   * Find the minimum key in unsorted node
   *
   * @param node the leaf node to find the minimum key in
   * @return position of the minimum key
   */
  template<typename Node>
  auto findMinKeyInNode(const pptr<Node> &node) const {
    auto pos = 0u;
    const auto &nodeRef = *node;
    auto currMinKey = std::numeric_limits<KeyType>::max();
    for (auto i = 0u; i < M; i++) {
      PROFILE_READ(1)
      if (nodeRef.bits.get_ro().test(i)) {
        PROFILE_READ(1)
        const auto &key = nodeRef.keys.get_ro()[i];
        if (key < currMinKey) { currMinKey = key; pos = i; }
      }
    }
    return pos;
  }

  /**
   * Find the maximum key in unsorted node
   *
   * @param node the leaf node to find the maximum key in
   * @return position of the maximum key
   */
  template<typename Node>
  auto findMaxKeyInNode(const pptr<Node> &node) const {
    auto pos = 0u;
    const auto &nodeRef = *node;
    auto currMaxKey = std::numeric_limits<KeyType>::min();
    for(auto i = 0u; i < M; i++) {
      PROFILE_READ(1)
      if (nodeRef.bits.get_ro().test(i)) {
        PROFILE_READ(1)
        const auto &key = nodeRef.keys.get_ro()[i];
        if (key > currMaxKey) { currMaxKey = key; pos = i; }
      }
    }
    return pos;
  }

	/**
   * Searches for the next greater key than @key in @c node.
   */
  template<typename Node>
  auto findMinKeyGreaterThan(const pptr<Node> &node, const KeyType &key) const {
    auto pos = 0u;
    const auto &nodeRef = *node;
    PROFILE_READ(2)
    const auto &bitsRef = nodeRef.bits.get_ro();
    const auto &keysRef = nodeRef.keys.get_ro();
    constexpr auto MAX = std::numeric_limits<KeyType>::max();
    auto min = MAX;
    for (auto i = 0u; i < Node::NUM_KEYS; i++) {
      const auto &curKey = keysRef[i];
      if (bitsRef.test(i) && curKey < min && curKey > key) {
        min = curKey;
        pos = i;
      }
    }
    if (min == MAX) return Node::NUM_KEYS;
    return pos;
  }

  /**
   * Searches for the next smaller key than @key in @c node.
   */
  template<typename Node>
  auto findMaxKeySmallerThan(const pptr<Node> &node, const KeyType &key) {
    auto pos = 0u;
    const auto &nodeRef = *node;
    PROFILE_READ(2)
    const auto &bitsRef = nodeRef.bits.get_ro();
    const auto &keysRef = nodeRef.keys.get_ro();
    constexpr auto MIN = std::numeric_limits<KeyType>::min();
    auto max = MIN;
    for (auto i = 0u; i < Node::NUM_KEYS; i++) {
      const auto &curKey = keysRef[i];
      if (bitsRef.test(i) && curKey > max && curKey < key) {
        max = curKey;
        pos = i;
      }
    }
    return pos;
  }

}; /* class BitPBPTree */

} /* namespace dbis::pbptree */

#endif /* DBIS_BitPBPTree_hpp_ */
