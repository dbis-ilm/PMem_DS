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

#ifndef DBIS_PBPTree_hpp_
#define DBIS_PBPTree_hpp_

#include <array>
#include <iostream>

#include <libpmemobj/ctl.h>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include "config.h"
#include "utils/PersistEmulation.hpp"
#include "utils/SearchFunctions.hpp"

namespace dbis::pbptrees {

using pmem::obj::allocation_flag;
using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;
template <typename Object>
using pptr = persistent_ptr<Object>;

/**
 * A persistent memory implementation of a B+ tree.
 *
 * @tparam KeyType the data type of the key
 * @tparam ValueType the data type of the values associated with the key
 * @tparam N the maximum number of keys on a branch node
 * @tparam M the maximum number of keys on a leaf node
 */
template <typename KeyType, typename ValueType, int N, int M>
class PBPTree {
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
    Node() : tag(BLANK){};
    Node(const pptr<LeafNode> &leaf_) : tag(LEAF), leaf(leaf_){};
    Node(const pptr<BranchNode> &branch_) : tag(BRANCH), branch(branch_){};
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
        default:
          break;
      }
    }

    Node &operator=(Node other) {
      copy(other);
      return *this;
    }

    enum NodeType { BLANK, LEAF, BRANCH } tag;
    union {
      pptr<LeafNode> leaf;
      pptr<BranchNode> branch;
    };
  };

  /**
   * A structure for passing information about a node split to the caller.
   */
  struct SplitInfo {
    KeyType key;     ///< the key at which the node was split
    Node leftChild;  ///< the resulting lhs child node
    Node rightChild; ///< the resulting rhs child node
  };

  pobj_alloc_class_desc alloc_class;
  p<unsigned int> depth; /**< the depth of the tree, i.e. the number of levels
                              (0 => rootNode is LeafNode) */
  Node rootNode;         /**< pointer to the root node (an instance of @c LeafNode or
                              @c BranchNode). This pointer is never @c nullptr. */

  /* -------------------------------------------------------------------------------------------- */

 public:
  static constexpr pobj_alloc_class_desc AllocClass{256, 64, 1, POBJ_HEADER_COMPACT};

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
        node = node.branch->children.get_ro()[0];
      }
      currentNode = node.leaf;
      currentPosition = 0;
    }

    iterator& operator++() {
      const auto &nodeRef = *currentNode;
      if (currentPosition >= nodeRef.numKeys.get_ro() - 1) {
        currentNode = nodeRef.nextLeaf;
        currentPosition = 0;
      } else {
        currentPosition++;
      }
      return *this;
    }
    iterator operator++(int) {iterator retval = *this; ++(*this); return retval;}

    bool operator==(iterator other) const {return (currentNode == other.currentNode &&
                                                   currentPosition == other.currentPosition);}
    bool operator!=(iterator other) const {return !(*this == other);}

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

  /* -------------------------------------------------------------------------------------------- */


  /**
   * Typedef for a function passed to the scan method.
   */
  using ScanFunc = std::function<void(const KeyType &key, const ValueType &val)>;

  /**
   * Constructor for creating a new B+ tree.
   */
  explicit PBPTree(struct pobj_alloc_class_desc _alloc) : depth(0), alloc_class(_alloc) {
  //PBPTree() : depth(0) {
    rootNode = newLeafNode();
    LOG("created new tree with sizeof(BranchNode) = " << sizeof(BranchNode) <<
        ", sizeof(LeafNode) = " << sizeof(LeafNode));
  }

  /**
   * Destructor for the B+ tree. Should delete all allocated nodes.
   */
  ~PBPTree() {
    /// Nodes are deleted automatically by releasing leafPool and branchPool.
  }

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
    if (depth.get_ro() == 0) {
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
      auto &rootChilds = rootRef.children.get_rw();
      rootRef.keys.get_rw()[0] = splitInfo.key;
      rootChilds[0] = splitInfo.leftChild;
      rootChilds[1] = splitInfo.rightChild;
      rootRef.numKeys.get_rw() = 1;
      rootNode.branch = root;
      ++depth.get_rw();
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
    const auto &leafRef = *leafNode;
    if (pos < leafRef.numKeys.get_ro() && leafRef.keys.get_ro()[pos] == key) {
      /// we found it!
      *val = leafRef.values.get_ro()[pos];
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
    if (depth.get_ro() == 0) {
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
   * Nothing to do as everything is persistent and failure-atomic(?).
   */
  void recover() { return; }

  /**
   * Print the structure and content of the B+ tree to stdout.
   */
  void print() const {
    if (depth.get_ro() == 0) printLeafNode(0u, rootNode.leaf);
    else printBranchNode(0u, rootNode.branch);
  }


  /**
   * Perform a scan over all key-value pairs stored in the B+ tree. For each entry the given
   * function @func is called.
   *
   * @param func the function called for each entry
   */
  void scan(ScanFunc func) const {
    /// we traverse to the leftmost leaf node
    auto node = rootNode;
    auto d = depth.get_ro();
    /// as long as we aren't at the leaf level we follow the path down
    while ( d-- > 0) node = node.branch->children.get_ro()[0];
    auto leaf = node.leaf;
    while (leaf != nullptr) {
      const auto &leafRef = *leaf;
      /// for each key-value pair call func
      const auto &numKeys = leafRef.numKeys.get_ro();
      const auto &leafKeys = leafRef.keys.get_ro();
      const auto &leafValues = leafRef.values.get_ro();
      for (auto i = 0u; i < numKeys; ++i) {
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

    while (leaf != nullptr) {
      const auto &leafRef = *leaf;
      /// for each key-value pair within the range call func
      const auto &numKeys = leafRef.numKeys.get_ro();
      const auto &leafKeys = leafRef.keys.get_ro();
      const auto &leafValues = leafRef.values.get_ro();
      for (auto i = 0u; i < numKeys; ++i) {
        const auto &key = leafKeys[i];
        if (key < minKey) continue;
        if (key > maxKey) return;
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
  /* DELETE AT LEAF LEVEL                                                                         */
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
    auto &nodeRef = *node;
    if (nodeRef.keys.get_ro()[pos] == key) {
      auto &numKeys = nodeRef.numKeys.get_rw();
      auto &nodeKeys = nodeRef.keys.get_rw();
      auto &nodeValues = nodeRef.values.get_rw();
      for (auto i = pos; i < numKeys - 1; ++i) {
        nodeKeys[i] = nodeKeys[i + 1];
        nodeValues[i] = nodeValues[i + 1];
      }
      --numKeys;
      PersistEmulation::writeBytes((numKeys - pos) * (sizeof(KeyType) + sizeof(ValueType)) +
                                     sizeof(size_t));
      return true;
    }
    return false;
  }

  /**
   * Handle the case that during a delete operation a underflow at node @c leaf occured. If possible
   * this is handled
   * (1) by rebalancing the elements among the leaf node and one of its siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position of the child node @leaf in the @c children array of the branch node
   * @param leaf the node at which the underflow occured
   */
  void underflowAtLeafLevel(const pptr<BranchNode> &node, unsigned int pos, pptr<LeafNode> &leaf) {
    auto &nodeRef = *node;
    assert(pos <= nodeRef.numKeys.get_ro());
    const auto &leafRef = *leaf;
    auto &nodeKeys = nodeRef.keys.get_rw();
    const auto prevNumKeys = pos > 0 ? leafRef.prevLeaf->numKeys.get_ro() : 0;
    constexpr auto middle = (M + 1) / 2;
    /// 1. we check whether we can rebalance with one of the siblings but only if both nodes have
    ///    the same direct parent
    if (pos > 0 && prevNumKeys > middle) {
      /// we have a sibling at the left for rebalancing the keys
      balanceLeafNodes(leafRef.prevLeaf, leaf);
      nodeKeys[pos-1] = leafRef.keys.get_ro()[0];
    } else if (pos < nodeRef.numKeys.get_ro() && leafRef.nextLeaf->numKeys.get_ro() > middle) {
      /// we have a sibling at the right for rebalancing the keys
      balanceLeafNodes(leafRef.nextLeaf, leaf);
      nodeKeys[pos] = leafRef.nextLeaf->keys.get_ro()[0];
    } else {
      /// 2. if this fails we have to merge two leaf nodes but only if both nodes have the same
      ///    direct parent
      pptr<LeafNode> survivor = nullptr;
      if (pos > 0 && prevNumKeys <= middle) {
        survivor = mergeLeafNodes(leafRef.prevLeaf, leaf);
        deleteLeafNode(leaf);
      } else if (pos < nodeRef.numKeys.get_ro() && leafRef.nextLeaf->numKeys.get_ro() <= middle) {
        /// because we update the pointers in mergeLeafNodes we keep it here
        auto l = leafRef.nextLeaf;
        survivor = mergeLeafNodes(leaf, leafRef.nextLeaf);
        deleteLeafNode(l);
      } else {
        /// this shouldn't happen?!
        assert(false);
      }
      if (nodeRef.numKeys.get_ro() > 1) {
        if (pos > 0) --pos;
        /// just remove the child node from the current branch node
        auto &nodeChilds = nodeRef.children.get_rw();
        auto &nodeNumKeys = nodeRef.numKeys.get_rw();
        for (auto i = pos; i < nodeNumKeys - 1; ++i) {
          nodeKeys[i] = nodeKeys[i + 1];
          nodeChilds[i + 1] = nodeChilds[i + 2];
        }
        nodeChilds[pos] = survivor;
        --nodeNumKeys;
      } else {
        /// This is a special case that happens only if the current node is the root node. Now, we
        /// have to replace the branch root node by a leaf node.
        rootNode = survivor;
        --depth.get_rw();
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
  pptr<LeafNode> mergeLeafNodes(const pptr<LeafNode> &node1, const pptr<LeafNode> &node2) {
    assert(node1 != nullptr);
    assert(node2 != nullptr);
    auto &node1Ref = *node1;
    auto &node2Ref = *node2;
    auto &n1NumKeys = node1Ref.numKeys.get_rw();
    const auto &n2NumKeys = node2Ref.numKeys.get_ro();
    assert(n1NumKeys + n2NumKeys <= M);

    /// we move all keys/values from node2 to node1
    auto &node1Keys = node1Ref.keys.get_rw();
    auto &node1Values = node1Ref.values.get_rw();
    const auto &node2Keys = node2Ref.keys.get_ro();
    const auto &node2Values = node2Ref.values.get_ro();
    for (auto i = 0u; i < n2NumKeys; ++i) {
      node1Keys[n1NumKeys + i] = node2Keys[i];
      node1Values[n1NumKeys + i] = node2Values[i];
    }
    n1NumKeys = n1NumKeys + n2NumKeys;
    node1Ref.nextLeaf = node2Ref.nextLeaf;
    // n2NumKeys = 0; ///< node2 is deleted anyway
    if (node2Ref.nextLeaf != nullptr) {
      node2Ref.nextLeaf->prevLeaf = node1;
      PersistEmulation::writeBytes<16>();
    }

    PersistEmulation::writeBytes(
        n2NumKeys * (sizeof(KeyType) + sizeof(ValueType)) +  ///< moved keys, vals
        sizeof(unsigned int) + 16                            ///< numKeys + nextpointer
    );
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
    auto &dNumKeys = donorRef.numKeys.get_rw();
    auto &rNumKeys = receiverRef.numKeys.get_rw();
    assert(dNumKeys > rNumKeys);

    const auto balancedNum = (dNumKeys + rNumKeys) / 2;
    const auto toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    auto &receiverKeys = receiverRef.keys.get_rw();
    auto &receiverValues = receiverRef.values.get_rw();
    const auto &donorKeys = donorRef.keys.get_ro();
    const auto &donorValues = donorRef.values.get_ro();
    if (donorKeys[0] < receiverKeys[0]) {
      /// move from one node to a node with larger keys
      unsigned int i = 0, j = 0;
      for (i = rNumKeys; i > 0; --i) {
        /// reserve space on receiver side
        receiverKeys[i + toMove - 1] = receiverKeys[i - 1];
        receiverValues[i + toMove - 1] = receiverValues[i - 1];
      }
      /// move toMove keys/values from donor to receiver
      for (i = balancedNum; i < dNumKeys; ++i, ++j) {
        receiverKeys[j] = donorKeys[i];
        receiverValues[j] = donorValues[i];
        ++rNumKeys;
      }
      PersistEmulation::writeBytes((rNumKeys + dNumKeys - balancedNum) *
                                       (sizeof(KeyType) + sizeof(ValueType)) +
                                   sizeof(unsigned int));
    } else {
      /// move from one node to a node with smaller keys
      unsigned int i = 0;
      /// move toMove keys/values from donor to receiver
      for (i = 0; i < toMove; ++i) {
        receiverKeys[rNumKeys] = donorKeys[i];
        receiverValues[rNumKeys] = donorValues[i];
        ++rNumKeys;
      }
      /// on donor node move all keys and values to the left
      auto &donorKeysW = donorRef.keys.get_rw();
      auto &donorValuesW = donorRef.values.get_rw();
      for (i = 0; i < balancedNum; ++i) {
        donorKeysW[i] = donorKeys[toMove + i];
        donorValuesW[i] = donorValues[toMove + i];
      }
      PersistEmulation::writeBytes(dNumKeys * (sizeof(KeyType) + sizeof(ValueType)) +
                                   sizeof(unsigned int));
    }
    dNumKeys -= toMove;
    PersistEmulation::writeBytes<sizeof(unsigned int)>();
  }

  /* -------------------------------------------------------------------------------------------- */
  /* DELETE AT INNER LEVEL                                                                        */
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
    const auto &nodeRef = *node;
    assert(d >= 1);
    bool deleted = false;
    /// try to find the branch
    auto pos = lookupPositionInBranchNode(node, key);
    if (d == 1) {
      /// the next level is the leaf level
      auto leaf = nodeRef.children.get_ro()[pos].leaf;
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      constexpr auto middle = (M + 1) / 2;
      if (leaf->numKeys.get_ro() < middle) {
        /// handle underflow
        underflowAtLeafLevel(node, pos, leaf);
      }
    } else {
      auto child = nodeRef.children.get_ro()[pos].branch;
      deleted = eraseFromBranchNode(child, d - 1, key);

      pos = lookupPositionInBranchNode(node, key);
      constexpr auto middle = (N + 1) / 2;
      if (child->numKeys.get_ro() < middle) {
        /// handle underflow
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth && nodeRef.numKeys.get_ro() == 0) {
          /// special case: the root node is empty now
          rootNode = child;
          --depth.get_rw();
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
    assert(node != nullptr);
    assert(sibling != nullptr);
    const auto &nodeRef = *node;
    auto &sibRef = *sibling;
    const auto &nodeKeys = nodeRef.keys.get_ro();
    const auto &nodeChilds = nodeRef.children.get_ro();
    const auto &nodeNumKeys = nodeRef.numKeys.get_ro();
    auto &sibKeys = sibRef.keys.get_rw();
    auto &sibChilds = sibRef.children.get_rw();
    auto &sibNumKeys = sibRef.numKeys.get_rw();

    assert(key <= nodeKeys[0]);
    assert(sibKeys[sibNumKeys - 1] < key);

    sibKeys[sibNumKeys] = key;
    sibChilds[sibNumKeys + 1] = nodeChilds[0];
    for (auto i = 0u; i < nodeNumKeys; ++i) {
      sibKeys[sibNumKeys + i + 1] = nodeKeys[i];
      sibChilds[sibNumKeys + i + 2] = nodeChilds[i + 1];
    }
    sibNumKeys += nodeNumKeys + 1;
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
  pptr<BranchNode> underflowAtBranchLevel(const pptr<BranchNode> &node, unsigned int pos,
                                          pptr<BranchNode> &child) {
    assert(node != nullptr);
    assert(child != nullptr);
    auto &nodeRef = *node;
    const auto &nodeKeys = nodeRef.keys.get_ro();
    const auto &nodeChilds = nodeRef.children.get_ro();
    const auto &nNumKeys = nodeRef.numKeys.get_ro();
    auto prevNumKeys = 0u, nextNumKeys = 0u;
    constexpr auto middle = (N + 1) / 2;

    /// 1. we check whether we can rebalance with one of the siblings
    if (pos > 0 && (prevNumKeys = nodeChilds[pos - 1].branch->numKeys.get_ro()) > middle) {
      /// we have a sibling at the left for rebalancing the keys
      const auto sibling = nodeChilds[pos - 1].branch;
      balanceBranchNodes(sibling, child, node, pos - 1);
      return child;
    } else if (pos < nNumKeys &&
        (nextNumKeys = nodeChilds[pos + 1].branch->numKeys.get_ro()) > middle) {
      /// we have a sibling at the right for rebalancing the keys
      auto sibling = nodeChilds[pos + 1].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return child;
    } else {
      /// 2. if this fails we have to merge two branch nodes
      auto newChild = child;

      auto ppos = pos;
      if (prevNumKeys > 0) {
        auto &lSibling = nodeChilds[pos - 1].branch;
        mergeBranchNodes(lSibling, nodeKeys[pos - 1], child);
        ppos = pos - 1;
        deleteBranchNode(child);
        newChild = lSibling;
      } else if (nextNumKeys > 0) {
        auto &rSibling = nodeRef.children.get_rw()[pos + 1].branch;
        mergeBranchNodes(child, nodeKeys[pos], rSibling);
        deleteBranchNode(rSibling);
      } else assert(false); ///< shouldn't happen

      /// remove nodeKeys[pos] from node
      auto &nodeKeysW = nodeRef.keys.get_rw();
      for (auto i = ppos; i < nNumKeys - 1; ++i) {
        nodeKeysW[i] = nodeKeysW[i + 1];
      }
      if (pos == 0) ++pos;
      auto &nodeChildsW = nodeRef.children.get_rw();
      for (auto i = pos; i < nNumKeys; ++i) {
        nodeChildsW[i] = nodeChildsW[i + 1];
      }
      --nodeRef.numKeys.get_rw();

      return newChild;
    }
  }

  /* -------------------------------------------------------------------------------------------- */

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
    auto &donorKeys = donorRef.keys.get_rw();
    auto &donorChilds = donorRef.children.get_rw();
    auto &dNumKeys = donorRef.numKeys.get_rw();
    auto &receiverKeys = receiverRef.keys.get_rw();
    auto &receiverChilds = receiverRef.children.get_rw();
    auto &rNumKeys = receiverRef.numKeys.get_rw();
    auto &parentKeys = parentRef.keys.get_rw();
    assert(dNumKeys > rNumKeys);

    const auto balancedNum = (dNumKeys + rNumKeys) / 2;
    const auto toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    if (donorKeys[0] < receiverKeys[0]) {
      /// move from one node to a node with larger keys
      unsigned int i = 0;

      /// 1. make room
      receiverChilds[rNumKeys + toMove] = receiverChilds[rNumKeys];
      for (i = rNumKeys; i > 0; --i) {
        /// reserve space on receiver side
        receiverKeys[i + toMove - 1] = receiverKeys[i - 1];
        receiverChilds[i + toMove - 1] = receiverChilds[i - 1];
      }
      /// 2. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; ++i) {
        receiverChilds[i] = donorChilds[dNumKeys - toMove + 1 + i];
      }
      for (i = 0; i < toMove - 1; ++i) {
        receiverKeys[i] = donorKeys[dNumKeys - toMove + 1 + i];
      }
      receiverKeys[toMove - 1] = parentKeys[pos];
      assert(parentRef.numKeys.get_ro() > pos);
      parentKeys[pos] = donorKeys[dNumKeys - toMove];
      rNumKeys += toMove;
    } else {
      /// mode from one node to a node with smaller keys
      unsigned int i = 0, n = rNumKeys;

      /// 1. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; ++i) {
        receiverChilds[n + 1 + i] = donorChilds[i];
        receiverKeys[n + 1 + i] = donorKeys[i];
      }
      /// 2. we have to move via the parent node: take the key from
      /// parentKeys[pos]
      receiverKeys[n] = parentKeys[pos];
      rNumKeys += toMove;
      const auto key = donorKeys[toMove - 1];

      /// 3. on donor node move all keys and values to the left
      for (i = 0; i < dNumKeys - toMove; ++i) {
        donorKeys[i] = donorKeys[toMove + i];
        donorChilds[i] = donorChilds[toMove + i];
      }
      donorChilds[dNumKeys - toMove] = donorChilds[dNumKeys];
      /// and replace this key by donorKeys[0]
      assert(parentRef.numKeys.get_ro() > pos);
      parentKeys[pos] = key;
    }
    dNumKeys -= toMove;
  }

  /* -------------------------------------------------------------------------------------------- */
  /* DEBUGGING                                                                                    */
  /* -------------------------------------------------------------------------------------------- */

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
    const auto nNumKeys = nodeRef.numKeys.get_ro();

    for (auto i = 0u; i < d; ++i) std::cout << "  ";
    std::cout << d << " { ";
    for (auto k = 0u; k < nNumKeys; ++k) {
      if (k > 0) std::cout << ", ";
      std::cout << nodeKeys[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= nNumKeys; ++k) {
      if (d + 1 < depth) {
        auto child = nodeChilds[k].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto leaf = (nodeChilds[k]).leaf;
        printLeafNode(d + 1, leaf);
      }
    }
  }

  /**
   * Print the keys of the given branch node @c node to standard output.
   *
   * @param node the tree node to print
   */
  void printBranchNodeKeys(const pptr<BranchNode> &node) const {
    const auto &nodeRef = *node;
    const auto &nodeKeys = nodeRef.keys.get_ro();
    const auto nNumKeys = nodeRef.numKeys.get_ro();

    std::cout << "{ ";
    for (auto k = 0u; k < nNumKeys; ++k) {
      if (k > 0) std::cout << ", ";
      std::cout << nodeKeys[k];
    }
    std::cout << " }" << std::endl;
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
    const auto nNumKeys = nodeRef.numKeys.get_ro();

    for (auto i = 0u; i < d; ++i) std::cout << "  ";
    std::cout << "[" << std::hex << node << std::dec << " : ";
    for (auto i = 0u; i < nNumKeys; ++i) {
      if (i > 0) std::cout << ", ";
      std::cout << "{" << nodeKeys[i] /*<< " -> " << nodeRef.values.get_ro()[i]*/ << "}";
    }
    std::cout << "]" << std::endl;
  }

  /* -------------------------------------------------------------------------------------------- */
  /* INSERT                                                                                       */
  /* -------------------------------------------------------------------------------------------- */

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
    bool split = false;
    auto &nodeRef = *node;
    const auto &numKeys = nodeRef.numKeys.get_ro();
    auto pos = lookupPositionInLeafNode(node, key);
    if (pos < numKeys && nodeRef.keys.get_ro()[pos] == key) {
      /// handle insert of duplicates
      nodeRef.values.get_rw()[pos] = val;
      return false;
    }
    if (numKeys == M) {
      /// split the node
      splitLeafNode(node, splitInfo);
      auto &splitRef = *splitInfo;

      /// insert the new entry
      constexpr auto middle = (M + 1) / 2;
      if (pos < middle)
        insertInLeafNodeAtPosition(node, pos, key, val);
      else
        insertInLeafNodeAtPosition(splitRef.rightChild.leaf, pos - middle, key, val);

      /// inform the caller about the split
      splitRef.key = splitRef.rightChild.leaf->keys.get_ro()[0];
      split = true;
    } else {
      /// otherwise, we can simply insert the new entry at the given position
      insertInLeafNodeAtPosition(node, pos, key, val);
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
    /// determine the split position
    constexpr auto middle = (M + 1) / 2;

    /// move all entries behind this position to a new sibling node
    auto &nodeRef = *node;
    auto &nNumKeys = nodeRef.numKeys.get_rw();
    pptr<LeafNode> sibling = newLeafNode();
    auto &sibRef = *sibling;
    auto &sNumKeys = sibRef.numKeys.get_rw();
    sNumKeys = nNumKeys - middle;
    const auto &nKeys = nodeRef.keys.get_ro();
    const auto &nValues = nodeRef.values.get_ro();
    auto &sKeys = sibRef.keys.get_rw();
    auto &sValues = sibRef.values.get_rw();
    for (auto i = 0u; i < sNumKeys; ++i) {
      sKeys[i] = nKeys[i + middle];
      sValues[i] = nValues[i + middle];
    }
    nNumKeys = middle;

    /// setup the list of leaf nodes
    if(nodeRef.nextLeaf != nullptr) {
      sibRef.nextLeaf = nodeRef.nextLeaf;
      nodeRef.nextLeaf->prevLeaf = sibling;
      PersistEmulation::writeBytes<2*16>();
    }
    nodeRef.nextLeaf = sibling;
    sibRef.prevLeaf = node;
    /// 2 numKeys + half entries + pointers
    PersistEmulation::writeBytes(2*8 + sNumKeys*(sizeof(KeyType) + sizeof(ValueType)) + 2*16);

    auto &splitInfoRef = *splitInfo;
    splitInfoRef.leftChild = node;
    splitInfoRef.rightChild = sibling;
    splitInfoRef.key = sKeys[0];
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
    auto &nodeRef = *node;
    auto &numKeys = nodeRef.numKeys.get_rw();
    assert(pos < M);
    assert(pos <= numKeys);
    assert(numKeys < M);

    /// we move all entries behind pos by one position
    auto &keysRef = nodeRef.keys.get_rw();
    auto &valuesRef = nodeRef.values.get_rw();
    for (auto i = numKeys; i > pos; --i) {
      keysRef[i] = keysRef[i - 1];
      valuesRef[i] = valuesRef[i - 1];
    }

    /// and then insert the new entry at the given position
    keysRef[pos] = key;
    valuesRef[pos] = val;
    ++numKeys;
    PersistEmulation::writeBytes((numKeys-pos)*(sizeof(KeyType) + sizeof(ValueType)) +
                                                  sizeof(size_t));
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
    const auto &nodeChilds = nodeRef.children.get_ro();
    auto &nNumKeys = nodeRef.numKeys.get_rw();

    /// determine the split position
    auto middle = (N + 1) / 2;
    /// adjust the middle based on the key we have to insert
    if (splitKey > nodeKeys[middle]) ++middle;

    /// move all entries behind this position to a new sibling node
    const auto sibling = newBranchNode();
    auto &sibRef = *sibling;
    auto &sibKeys = sibRef.keys.get_rw();
    auto &sibChilds = sibRef.children.get_rw();
    auto &sNumKeys = sibRef.numKeys.get_rw();
    sNumKeys = nNumKeys - middle;
    for (auto i = 0u; i < sNumKeys; ++i) {
      sibKeys[i] = nodeKeys[middle + i];
      sibChilds[i] = nodeChilds[middle + i];
    }
    sibChilds[sNumKeys] = nodeChilds[nNumKeys];
    nNumKeys = middle - 1;

    auto &splitRef = *splitInfo;
    splitRef.key = nodeKeys[middle - 1];
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
    const auto &nodeRef = *node;
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;

    auto pos = lookupPositionInBranchNode(node, key);
    if (depth == 1) {
      /// case #1: our children are leaf nodes
      const auto child = nodeRef.children.get_ro()[pos].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      /// case #2: our children are branch nodes
      const auto child = nodeRef.children.get_ro()[pos].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }
    if (hasSplit) {
      auto host = node;
      /// the child node was split, thus we have to add a new entry to our branch node
      if (nodeRef.numKeys.get_ro() == N) {
        splitBranchNode(node, childSplitInfo.key, splitInfo);
        const auto &splitRef = *splitInfo;
        host = (key < splitRef.key ? splitRef.leftChild: splitRef.rightChild).branch;
        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
      auto &hostRef = *host;
      auto &hostKeys = hostRef.keys.get_rw();
      auto &hostChilds = hostRef.children.get_rw();
      auto &hNumKeys = hostRef.numKeys.get_rw();
      if (pos < hNumKeys) {
        /// if the child isn't inserted at the rightmost position then we have to make space for it
        hostChilds[hNumKeys + 1] = hostChilds[hNumKeys];
        for (auto i = hNumKeys; i > pos; --i) {
          hostChilds[i] = hostChilds[i - 1];
          hostKeys[i] = hostKeys[i - 1];
        }
      }
      /// finally, add the new entry at the given position
      hostKeys[pos] = childSplitInfo.key;
      hostChilds[pos] = childSplitInfo.leftChild;
      hostChilds[pos + 1] = childSplitInfo.rightChild;
      ++hNumKeys;
    }
    return split;
  }

  /* -------------------------------------------------------------------------------------------- */
  /* LOOKUP                                                                                       */
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
    auto d = depth.get_ro();
    while (d-- > 0) {
      /// as long as we aren't at the leaf level we follow the path down
      auto n = node.branch;
      auto pos = lookupPositionInBranchNode(n, key);
      node = n->children.get_ro()[pos];
    }
    return node.leaf;
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
  unsigned int lookupPositionInBranchNode(const pptr<BranchNode> &node, const KeyType &key) const {
    const auto &nodeRef = *node;
    const auto num = nodeRef.numKeys.get_ro();
    const auto &keys = nodeRef.keys.get_ro();
    return binarySearch<true>(keys, 0, num-1, key);
  }

  /**
   * Lookup the search key @c key in the given leaf node and return the position.
   * If the search key was not found, then @c numKeys is returned.
   *
   * @param node the leaf node where we search
   * @param key the search key
   * @return the position of the key  (or @c numKey if not found)
   */
  unsigned int lookupPositionInLeafNode(const pptr<LeafNode> &node, const KeyType &key) const {
    const auto &nodeRef = *node;
    const auto num = nodeRef.numKeys.get_ro();
    const auto &keys = nodeRef.keys.get_ro();
    return binarySearch<false>(keys, 0, num-1, key);
  }

  /* -------------------------------------------------------------------------------------------- */

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
    if (pmemobj_tx_stage() == TX_STAGE_NONE) {
      transaction::run(pop, [&] { delete_persistent<LeafNode>(node); });
    } else {
      delete_persistent<LeafNode>(node);
    }
    node = nullptr;
  }

  /**
   * Create a new empty branch node
   */
  pptr<BranchNode> newBranchNode() {
    auto pop = pmem::obj::pool_by_vptr(this);
    pptr<BranchNode> newNode = nullptr;
    if (pmemobj_tx_stage() == TX_STAGE_NONE) {
      transaction::run(pop, [&] {
        newNode = make_persistent<BranchNode>(allocation_flag::class_id(alloc_class.class_id));
      });
    } else {
        newNode = make_persistent<BranchNode>(allocation_flag::class_id(alloc_class.class_id));
    }
    return newNode;
  }

  void deleteBranchNode(pptr<BranchNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    if (pmemobj_tx_stage() == TX_STAGE_NONE) {
      transaction::run(pop, [&] { delete_persistent<BranchNode>(node); });
    } else {
      delete_persistent<BranchNode>(node);
    }
    node = nullptr;
  }

  /* -------------------------------------------------------------------------------------------- */

  /**
   * A structure for representing a leaf node of a B+ tree.
   */
  struct alignas(64) LeafNode {
    /**
     * Constructor for creating a new empty leaf node.
     */
    LeafNode() : numKeys(0), nextLeaf(nullptr), prevLeaf(nullptr) {}

    /// pmdk header 16 Byte                                                      - 0x00
    p<size_t> numKeys;             ///< the number of currently stored keys - 0x10
    pptr<LeafNode> nextLeaf;             ///< pointer to the subsequent sibling   - 0x18
    pptr<LeafNode> prevLeaf;             ///< pointer to the preceding sibling    - 0x28
    char padding[24];                     ///< padding to align keys to 64 byte    - 0x38
    p<std::array<KeyType, M>> keys;      ///< the actual keys                     - 0x40
    p<std::array<ValueType, M>> values;  ///< the actual values
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct alignas(64) BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode() : numKeys(0) {}

    p<unsigned int> numKeys;             ///< the number of currently stored keys
    p<std::array<KeyType, N>> keys;      ///< the actual keys
    p<std::array<Node, N + 1>> children; ///< pointers to child nodes (BranchNode or LeafNode)
  };

}; /* end class PBPTree */

} /* namespace dbis::pbptrees */

/* POLYMORPHIC TYPES ARE NOT SUPPORTED BY PMEM

template <typename KeyType, typename ValueType, int N, int M>
class PBPTree : public PBPTree_Base<KeyType, ValueType, N, M> {};

template <typename KeyType, int N, int M>
class PBPTree<KeyType, std::string, N, M> : public PBPTree_Base<KeyType, std::string, N, M> {
  private:
    using LeafNode = typename PBPTree_Base<KeyType, std::string, N, M>::LeafNode;
    using SplitInfo = typename PBPTree_Base<KeyType, std::string, N, M>::SplitInfo;

    bool insertInLeafNode(const pptr<LeafNode> &node, const KeyType &key,
        const std::string &val, SplitInfo *splitInfo) override {
      std::cout << "Specialization Test" << std::endl;
      return false;
    }
};
*/

#endif /* DBIS_PBPTree_hpp_ */
