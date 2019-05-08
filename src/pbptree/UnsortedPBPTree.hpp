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

#ifndef DBIS_UnsortedPBPTree_hpp_
#define DBIS_UnsortedPBPTree_hpp_

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

/**
  * A persistent memory implementation of a B+ tree.
  *
  * @tparam KeyType the data type of the key
  * @tparam ValueType the data type of the values associated with the key
  * @tparam N the maximum number of keys on a branch node
  * @tparam M the maximum number of keys on a leaf node
  */
template<typename KeyType, typename ValueType, int N, int M>
class UnsortedPBPTree {
  // we need at least two keys on a branch node to be able to split
  static_assert(N > 2, "number of branch keys has to be >2.");
  // we need at least one key on a leaf node
  static_assert(M > 0, "number of leaf keys should be >0.");

#ifndef UNIT_TESTS
  private:
#else
  public:
#endif

  // Forward declarations
  struct LeafNode;
  struct BranchNode;

  struct Node {
    Node() : tag(BLANK) {};

    Node(persistent_ptr<LeafNode> leaf_) : tag(LEAF), leaf(leaf_) {};

    Node(persistent_ptr<BranchNode> branch_) : tag(BRANCH), branch(branch_) {};

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
      persistent_ptr<LeafNode> leaf;
      persistent_ptr<BranchNode> branch;
    };
  };

  /**
   * A structure for passing information about a node split to
   * the caller.
   */
  struct SplitInfo {
    KeyType key;                  //< the key at which the node was split
    Node leftChild;   //< the resulting lhs child node
    Node rightChild;  //< the resulting rhs child node
  };

  p<unsigned int> depth;          //< the depth of the tree, i.e. the number of levels (0 => rootNode is LeafNode)

  Node rootNode;      //< pointer to the root node (an instance of @c LeafNode or
  //< @c BranchNode). This pointer is never @c nullptr.

  public:

  /**
   * Iterator for iterating over the leaf nodes
   */
  class iterator {
    persistent_ptr<LeafNode> currentNode;
    std::size_t currentPosition;

    public:
    iterator() : currentNode(nullptr), currentPosition(0) {}

    iterator(const Node &root, std::size_t d) {
      // traverse to left-most key
      auto node = root;
      while (d-- > 0) {
        auto n = node.branch;
        node = n->children.get_ro()[0];
      }
      currentNode = node.leaf;
      currentPosition = 0;
    }

    iterator &operator++() {
      if (currentPosition >= currentNode->numKeys - 1) {
        currentNode = currentNode->nextLeaf;
        currentPosition = 0;
      } else {
        currentPosition++;
      }
      return *this;
    }

    iterator operator++(int) {
      iterator retval = *this;
      ++(*this);
      return retval;
    }

    bool operator==(iterator other) const {
      return (currentNode == other.currentNode &&
          currentPosition == other.currentPosition);
    }

    bool operator!=(iterator other) const { return !(*this == other); }

    std::pair<KeyType, ValueType> operator*() {
      return std::make_pair(currentNode->keys.get_ro()[currentPosition],
          currentNode->values.get_ro()[currentPosition]);
    }

    // iterator traits
    using difference_type = long;
    using value_type = std::pair<KeyType, ValueType>;
    using pointer = const std::pair<KeyType, ValueType> *;
    using reference = const std::pair<KeyType, ValueType> &;
    using iterator_category = std::forward_iterator_tag;
  };

  iterator begin() { return iterator(rootNode, depth); }

  iterator end() { return iterator(); }


  /**
   * Typedef for a function passed to the scan method.
   */
  using ScanFunc = std::function<void(const KeyType &key, const ValueType &val)>;

  /**
   * Constructor for creating a new B+ tree.
   */
  UnsortedPBPTree() : depth(0) {
    rootNode = newLeafNode();
    LOG("created new tree with sizeof(BranchNode) = " << sizeof(BranchNode)
      << ", sizeof(LeafNode) = " << sizeof(LeafNode));
  }

  /**
   * Destructor for the B+ tree. Should delete all allocated nodes.
   */
  ~UnsortedPBPTree() {
    //TODO: Necessary in Pmem case?
    // Nodes are deleted automatically by releasing leafPool and branchPool.
  }

  /**
   * Insert an element (a key-value pair) into the B+ tree. If the key @c key
   * already exists, the corresponding value is replaced by @c val.
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
        // the root node is a leaf node
        auto n = rootNode.leaf;
        wasSplit = insertInLeafNode(n, key, val, &splitInfo);
        } else {
        // the root node is a branch node
        auto n = rootNode.branch;
        wasSplit = insertInBranchNode(n, depth, key, val, &splitInfo);
        }
        if (wasSplit) {
        // we had an overflow in the node and therefore the node is split
        auto root = newBranchNode();
        root->keys.get_rw()[0] = splitInfo.key;
        root->children.get_rw()[0] = splitInfo.leftChild;
        root->children.get_rw()[1] = splitInfo.rightChild;
        root->numKeys = root->numKeys + 1;
        rootNode.branch = root;
        depth = depth + 1;
        }
    });
  }

  /**
   * Find the given @c key in the B+ tree and if found return the
   * corresponding value.
   *
   * @param key the key we are looking for
   * @param[out] val a pointer to memory where the value is stored
   *                 if the key was found
   * @return true if the key was found, false otherwise
   */
  bool lookup(const KeyType &key, ValueType *val) const {
    assert(val != nullptr);
    bool result = false;

    auto leafNode = findLeafNode(key);
    auto pos = lookupPositionInLeafNode(leafNode, key);
    if (pos < leafNode->numKeys && leafNode->keys.get_ro()[pos] == key) {
      // we found it!
      *val = leafNode->values.get_ro()[pos];
      result = true;
    }
    return result;
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
        // special case: the root node is a leaf node and
        // there is no need to handle underflow
        auto node = rootNode.leaf;
        assert(node != nullptr);
        result= eraseFromLeafNode(node, key);
        } else {
        auto node = rootNode.branch;
        assert(node != nullptr);
        result= eraseFromBranchNode(node, depth, key);
        }
        });
    return result;
  }
  void recover(){return;}
  /**
   * Print the structure and content of the B+ tree to stdout.
   */
  void print() const {
    if (depth == 0) {
      // the trivial case
      printLeafNode(0, rootNode.leaf);
    } else {
      auto n = rootNode;
      printBranchNode(0u, n.branch);
    }
  }


  /**
   * Perform a scan over all key-value pairs stored in the B+ tree.
   * For each entry the given function @func is called.
   *
   * @param func the function called for each entry
   */
  void scan(ScanFunc func) const {
    // we traverse to the leftmost leaf node
    auto node = rootNode;
    auto d = depth.get_ro();
    while (d-- > 0) {
      // as long as we aren't at the leaf level we follow the path down
      auto n = node.branch;
      node = n->children.get_ro()[0];
    }
    auto leaf = node.leaf;
    while (leaf != nullptr) {
      // for each key-value pair call func
      for (auto i = 0u; i < leaf->numKeys; i++) {
        auto &key = leaf->keys.get_ro()[i];
        auto &val = leaf->values.get_ro()[i];
        func(key, val);
      }
      // move to the next leaf node
      leaf = leaf->nextLeaf;
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

    while (leaf != nullptr) {
      // for each key-value pair within the range call func
      for (auto i = 0u; i < leaf->numKeys; i++) {
        auto &key = leaf->keys.get_ro()[i];
        if (key > maxKey) return;
        auto &val = leaf->values.get_ro()[i];
        func(key, val);
      }
      // move to the next leaf node
      leaf = leaf->nextLeaf;
    }
  }

#ifndef UNIT_TESTS
  private:
#endif
  /* ------------------------------------------------------------------- */
  /*                        DELETE AT LEAF LEVEL                         */
  /* ------------------------------------------------------------------- */

  /**
   * Delete the element with the given key from the given leaf node.
   *
   * @param node the leaf node from which the element is deleted
   * @param key the key of the element to be deleted
   * @return true of the element was deleted
   */
  bool eraseFromLeafNode(persistent_ptr<LeafNode> node, const KeyType &key) {
    bool deleted = false;
    auto pos = lookupPositionInLeafNode(node, key);
    if (node->keys.get_rw()[pos] == key) {
      //simply copy the last object on the node to the position of the erased object
      auto last=node->numKeys.get_ro()-1;
      node->keys.get_rw()[pos]=node->keys.get_ro()[last];
      node->values.get_rw()[pos]=node->values.get_ro()[last];
      node->numKeys.get_rw() = node->numKeys.get_ro() - 1;
      deleted = true;
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
   * @param pos the position of the child node @leaf in the @c children array of
   * the branch node
   * @param leaf the node at which the underflow occured
   */
  void
    underflowAtLeafLevel(persistent_ptr<BranchNode> node, unsigned int pos, persistent_ptr<LeafNode> leaf) {
      assert(pos <= node->numKeys);

      unsigned int middle = (M + 1) / 2;
      // 1. we check whether we can rebalance with one of the siblings
      // but only if both nodes have the same direct parent
      if (pos > 0 && leaf->prevLeaf->numKeys > middle) {
        // we have a sibling at the left for rebalancing the keys
        balanceLeafNodes(leaf->prevLeaf, leaf);
        node->keys.get_rw()[pos-1] = leaf->keys.get_ro()[findMinKeyAtLeafNode(leaf)];
      } else if (pos < node->numKeys && leaf->nextLeaf->numKeys > middle) {
        // we have a sibling at the right for rebalancing the keys
        balanceLeafNodes(leaf->nextLeaf, leaf);
        node->keys.get_rw()[pos] = leaf->nextLeaf->keys.get_ro()[findMinKeyAtLeafNode(leaf->nextLeaf)];
      } else {
        // 2. if this fails we have to merge two leaf nodes
        // but only if both nodes have the same direct parent
        persistent_ptr<LeafNode> survivor = nullptr;
        if (pos > 0 && leaf->prevLeaf->numKeys <= middle) {
          survivor = mergeLeafNodes(leaf->prevLeaf, leaf);
          deleteLeafNode(leaf);
        } else if (pos < node->numKeys && leaf->nextLeaf->numKeys <= middle) {
          // because we update the pointers in mergeLeafNodes
          // we keep it here
          auto l = leaf->nextLeaf;
          survivor = mergeLeafNodes(leaf, leaf->nextLeaf);
          deleteLeafNode(l);
        } else {
          // this shouldn't happen?!
          assert(false);
        }
        if (node->numKeys > 1) {
          if (pos > 0) pos--;
          // just remove the child node from the current branch node
          for (auto i = pos; i < node->numKeys - 1; i++) {
            node->keys.get_rw()[i] = node->keys.get_ro()[i + 1];
            node->children.get_rw()[i + 1] = node->children.get_ro()[i + 2];
          }
          node->children.get_rw()[pos] = survivor;
          node->numKeys.get_rw() = node->numKeys.get_ro() - 1;
        } else {
          // This is a special case that happens only if
          // the current node is the root node. Now, we have
          // to replace the branch root node by a leaf node.
          rootNode = survivor;
          depth = depth - 1;
        }
      }
    }

  /**
   * Merge two leaf nodes by moving all elements from @c node2 to
   * @c node1.
   *
   * @param node1 the target node of the merge
   * @param node2 the source node
   * @return the merged node (always @c node1)
   */
  persistent_ptr<LeafNode> mergeLeafNodes(persistent_ptr<LeafNode> node1, persistent_ptr<LeafNode> node2) {
    assert(node1 != nullptr);
    assert(node2 != nullptr);
    assert(node1->numKeys + node2->numKeys <= M);

    // we move all keys/values from node2 to node1
    for (auto i = 0u; i < node2->numKeys; i++) {
      node1->keys.get_rw()[node1->numKeys + i] = node2->keys.get_ro()[i];
      node1->values.get_rw()[node1->numKeys + i] = node2->values.get_ro()[i];
    }
    node1->numKeys.get_rw() = node1->numKeys.get_ro() + node2->numKeys.get_ro();
    node1->nextLeaf = node2->nextLeaf;
    node2->numKeys = 0;
    if (node2->nextLeaf != nullptr) node2->nextLeaf->prevLeaf = node1;
    return node1;
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
  void balanceLeafNodes(persistent_ptr<LeafNode> donor, persistent_ptr<LeafNode> receiver) {
    assert(donor->numKeys > receiver->numKeys);

    unsigned int balancedNum = (donor->numKeys + receiver->numKeys) / 2;
    unsigned int toMove = donor->numKeys - balancedNum;
    if (toMove == 0) return;
    if (donor->keys.get_ro()[0] < receiver->keys.get_ro()[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0;

      // move toMove keys/values from donor to receiver
      for (i = 0; i < toMove; i++) {
        uint max=findMaxKeyAtLeafNode(donor);
        // move the donor's maximum key to the receiver
        receiver->keys.get_rw()[receiver->numKeys]=donor->keys.get_ro()[max];
        receiver->values.get_rw()[receiver->numKeys]=donor->values.get_ro()[max];
        receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + 1;

        // fill empty space in donor with its currently last key
        donor->keys.get_rw()[max] = donor->keys.get_ro()[donor->numKeys-1];
        donor->values.get_rw()[max] = donor->values.get_ro()[donor->numKeys-1];
        donor->numKeys.get_rw() = donor->numKeys.get_ro() - 1;
      }
    } else {
      // mode from one node to a node with smaller keys
      unsigned int i = 0;
      // move toMove keys/values from donor to receiver
      for (i = 0; i < toMove; i++) {
        uint min=findMinKeyAtLeafNode(donor);
        // move the donor's minimum key to the receiver
        receiver->keys.get_rw()[receiver->numKeys]=donor->keys.get_ro()[min];
        receiver->values.get_rw()[receiver->numKeys]=donor->values.get_ro()[min];
        receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + 1;

        // fill empty space in donor with its currently last key
        donor->keys.get_rw()[min] = donor->keys.get_ro()[donor->numKeys-1];
        donor->values.get_rw()[min] = donor->values.get_ro()[donor->numKeys-1];
        donor->numKeys.get_rw() = donor->numKeys.get_ro() - 1;
      }
    }
  }
  /**
   * Find the minimum key in unsorted leaf
   * @param node the leaf node to find the minimum key in
   * @return position of the minimum key
   */
  uint findMinKeyAtLeafNode(persistent_ptr<LeafNode> node ){
    uint pos=0;
    KeyType currMinKey=node->keys.get_ro()[0];
    for(auto i=1u; i<node->numKeys; i++){
      KeyType key=node->keys.get_ro()[i];
      if(key<currMinKey){currMinKey=key; pos=i;}
    }
    return pos;
  }
  /**
   * Find the maximum key in unsorted leaf
   * @param node the leaf node to find the maximum key in
   * @return position of the maximum key
   */
  uint findMaxKeyAtLeafNode(persistent_ptr<LeafNode> node ){
    uint pos=0;
    KeyType currMinKey=node->keys.get_ro()[0];
    for(auto i=1u; i<node->numKeys; i++){
      KeyType key=node->keys.get_ro()[i];
      if(key>currMinKey){currMinKey=key; pos=i;}
    }
    return pos;
  }
  /* ------------------------------------------------------------------- */
  /*                        DELETE AT INNER LEVEL                        */
  /* ------------------------------------------------------------------- */
  /**
   * Delete an entry from the tree by recursively going down to the leaf level
   * and handling the underflows.
   *
   * @param node the current branch node
   * @param d the current depth of the traversal
   * @param key the key to be deleted
   * @return true if the entry was deleted
   */
  bool eraseFromBranchNode(persistent_ptr<BranchNode> node, unsigned int d, const KeyType &key) {
    assert(d >= 1);
    bool deleted = false;
    // try to find the branch
    auto pos = lookupPositionInBranchNode(node, key);
    auto n = node->children.get_ro()[pos];
    if (d == 1) {
      // the next level is the leaf level
      auto leaf = n.leaf;
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      unsigned int middle = (M + 1) / 2;
      if (leaf->numKeys < middle) {
        // handle underflow
        underflowAtLeafLevel(node, pos, leaf);
      }
    } else {
      auto child = n.branch;
      deleted = eraseFromBranchNode(child, d - 1, key);

      pos = lookupPositionInBranchNode(node, key);
      unsigned int middle = (N + 1) / 2;
      if (child->numKeys < middle) {
        // handle underflow
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth && node->numKeys == 0) {
          // special case: the root node is empty now
          rootNode = child;
          depth = depth - 1;
        }
      }
    }
    return deleted;
  }

  /**
   * Merge two branch nodes my moving all keys/children from @c node to @c
   * sibling and put the key @c key from the parent node in the middle. The node
   * @c node should be deleted by the caller.
   *
   * @param sibling the left sibling node which receives all keys/children
   * @param key the key from the parent node that is between sibling and node
   * @param node the node from which we move all keys/children
   */
  void mergeBranchNodes(persistent_ptr<BranchNode> sibling, const KeyType &key,
      persistent_ptr<BranchNode> node){
    assert(key <= node->keys.get_ro()[0]);
    assert(sibling != nullptr);
    assert(node != nullptr);
    assert(sibling->keys.get_ro()[sibling->numKeys - 1] < key);

    sibling->keys.get_rw()[sibling->numKeys] = key;
    sibling->children.get_rw()[sibling->numKeys + 1] = node->children.get_ro()[0];
    for (auto i = 0u; i < node->numKeys; i++) {
      sibling->keys.get_rw()[sibling->numKeys + i + 1] = node->keys.get_ro()[i];
      sibling->children.get_rw()[sibling->numKeys + i + 2] = node->children.get_ro()[i + 1];
    }
    sibling->numKeys.get_rw() = sibling->numKeys.get_ro() + node->numKeys.get_ro() + 1;
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
  persistent_ptr<BranchNode> underflowAtBranchLevel(persistent_ptr<BranchNode> node, unsigned int pos,
      persistent_ptr<BranchNode> child) {
    assert(node != nullptr);
    assert(child != nullptr);

    persistent_ptr<BranchNode> newChild = child;
    unsigned int middle = (N + 1) / 2;
    // 1. we check whether we can rebalance with one of the siblings
    if (pos > 0 &&
        (node->children.get_ro()[pos - 1]).branch->numKeys >
        middle) {
      // we have a sibling at the left for rebalancing the keys
      persistent_ptr<BranchNode> sibling = (node->children.get_ro()[pos - 1]).branch;
      balanceBranchNodes(sibling, child, node, pos - 1);
      // node->keys.get_rw()[pos] = child->keys.get_ro()[0];
      return newChild;
    } else if (pos < node->numKeys && (node->children.get_ro()[pos + 1]).branch->numKeys > middle) {
      // we have a sibling at the right for rebalancing the keys
      auto sibling = (node->children.get_ro()[pos + 1]).branch;
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    } else {
      // 2. if this fails we have to merge two branch nodes
      persistent_ptr<BranchNode> lSibling = nullptr, rSibling = nullptr;
      unsigned int prevKeys = 0, nextKeys = 0;

      if (pos > 0) {
        lSibling = (node->children.get_ro()[pos - 1]).branch;
        prevKeys = lSibling->numKeys;
      }
      if (pos < node->numKeys) {
        rSibling = (node->children.get_ro()[pos + 1]).branch;
        nextKeys = rSibling->numKeys;
      }

      persistent_ptr<BranchNode> witnessNode = nullptr;
      auto ppos = pos;
      if (prevKeys > 0) {
        mergeBranchNodes(lSibling, node->keys.get_ro()[pos - 1], child);
        ppos = pos - 1;
        witnessNode = child;
        newChild = lSibling;
        // pos -= 1;
      } else if (nextKeys > 0) {
        mergeBranchNodes(child, node->keys.get_ro()[pos], rSibling);
        witnessNode = rSibling;
      } else
        // shouldn't happen
        assert(false);

      // remove node->keys.get_ro()[pos] from node
      for (auto i = ppos; i < node->numKeys - 1; i++) {
        node->keys.get_rw()[i] = node->keys.get_ro()[i + 1];
      }
      if (pos == 0) pos++;
      for (auto i = pos; i < node->numKeys; i++) {
        if (i + 1 <= node->numKeys){
          node->children.get_rw()[i] = node->children.get_ro()[i + 1];
        }
      }
      node->numKeys.get_rw() = node->numKeys.get_ro() - 1;

      deleteBranchNode(witnessNode);
      return newChild;
    }
  }

  /* ---------------------------------------------------------------------- */
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
  void balanceBranchNodes(persistent_ptr<BranchNode> donor, persistent_ptr<BranchNode> receiver,
      persistent_ptr<BranchNode> parent, unsigned int pos) {
    assert(donor->numKeys > receiver->numKeys);

    unsigned int balancedNum = (donor->numKeys + receiver->numKeys) / 2;
    unsigned int toMove = donor->numKeys - balancedNum;
    if (toMove == 0) return;
    if (donor->keys.get_ro()[0] < receiver->keys.get_ro()[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0;

      // 1. make room
      receiver->children.get_rw()[receiver->numKeys + toMove] =
        receiver->children.get_ro()[receiver->numKeys];
      for (i = receiver->numKeys; i > 0; i--) {
        // reserve space on receiver side
        receiver->keys.get_rw()[i + toMove - 1] = receiver->keys.get_ro()[i - 1];
        receiver->children.get_rw()[i + toMove - 1] = receiver->children.get_ro()[i - 1];
      }
      // 2. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {
        receiver->children.get_rw()[i] =
          donor->children.get_ro()[donor->numKeys - toMove + 1 + i];
      }
      for (i = 0; i < toMove - 1; i++) {
        receiver->keys.get_rw()[i] = donor->keys.get_ro()[donor->numKeys - toMove + 1 + i];
      }
      receiver->keys.get_rw()[toMove - 1] = parent->keys.get_ro()[pos];
      assert(parent->numKeys > pos);
      parent->keys.get_rw()[pos] = donor->keys.get_ro()[donor->numKeys - toMove];
      receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + toMove;
    } else {
      // mode from one node to a node with smaller keys
      unsigned int i = 0, n = receiver->numKeys;

      // 1. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {
        receiver->children.get_rw()[n + 1 + i] = donor->children.get_ro()[i];
        receiver->keys.get_rw()[n + 1 + i] = donor->keys.get_ro()[i];
      }
      // 2. we have to move via the parent node: take the key from
      // parent->keys.get_ro()[pos]
      receiver->keys.get_rw()[n] = parent->keys.get_ro()[pos];
      receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + toMove;
      KeyType key = donor->keys.get_ro()[toMove - 1];

      // 3. on donor node move all keys and values to the left
      for (i = 0; i < donor->numKeys - toMove; i++) {
        donor->keys.get_rw()[i] = donor->keys.get_ro()[toMove + i];
        donor->children.get_rw()[i] = donor->children.get_ro()[toMove + i];
      }
      donor->children.get_rw()[donor->numKeys - toMove] =
        donor->children.get_ro()[donor->numKeys];
      // and replace this key by donor->keys.get_ro()[0]
      assert(parent->numKeys > pos);
      parent->keys.get_rw()[pos] = key;
    }
    donor->numKeys.get_rw() = donor->numKeys.get_ro() - toMove;
  }

  /* ---------------------------------------------------------------------- */
  /*                                   DEBUGGING                            */
  /* ---------------------------------------------------------------------- */

  /**
   * Print the given branch node @c node and all its children
   * to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printBranchNode(unsigned int d, persistent_ptr<BranchNode> node) const {
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << d << " { ";
    for (auto k = 0u; k < node->numKeys; k++) {
      if (k > 0) std::cout << ", ";
      std::cout << node->keys.get_ro()[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= node->numKeys; k++) {
      if (d + 1 < depth) {
        auto child = node->children.get_ro()[k].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto leaf = (node->children.get_ro()[k]).leaf;
        printLeafNode(d+1,leaf);
      }
    }
  }

  /**
   * Print the keys of the given branch node @c node to standard
   * output.
   *
   * @param node the tree node to print
   */
  void printBranchNodeKeys(persistent_ptr<BranchNode> node) const {
    std::cout << "{ ";
    for (auto k = 0u; k < node->numKeys; k++) {
      if (k > 0) std::cout << ", ";
      std::cout << node->keys.get_ro()[k];
    }
    std::cout << " }" << std::endl;
  }

  /**
   * Print the given leaf node @c node to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printLeafNode(unsigned int d, persistent_ptr<LeafNode> node) const {
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << "[" << std::hex << node << std::dec << " : ";
    for (auto i = 0u; i < node->numKeys; i++) {
      if (i > 0) std::cout << ", ";
      std::cout << "{" << node->keys.get_ro()[i]<< "}";
    }
    std::cout << "]" << std::endl;
  }
  /**
   * Print the keys of the  given leaf node @c node to standard output.
   *
   * @param node the tree node to print
   */
  void printLeafNodeKeys(persistent_ptr<LeafNode> node) const {
    std::cout<<"[";
    for (auto i = 0u; i < node->numKeys; i++) {
      if (i > 0) std::cout << ", ";
      std::cout << "{" << node->keys.get_ro()[i] << "}";
    }
    std::cout << "]" << std::endl;
  }

  /* ---------------------------------------------------------------------- */
  /*                                   INSERT                               */
  /* ---------------------------------------------------------------------- */

  /**
   * Insert a (key, value) pair into the corresponding leaf node. It is the
   * responsibility of the caller to make sure that the node @c node is
   * the correct node. The key is inserted at the last position.
   *
   * @param node the node where the key-value pair is inserted.
   * @param key the key to be inserted
   * @param val the value associated with the key
   * @param splitInfo information about a possible split of the node
   */
  bool insertInLeafNode(persistent_ptr<LeafNode> node, const KeyType &key,
      const ValueType &val, SplitInfo *splitInfo) {
    bool split = false;
    auto pos = lookupPositionInLeafNode(node, key);
    auto &nodeRef = *node;
    if (pos < nodeRef.numKeys && nodeRef.keys.get_ro()[pos] == key) {
      // handle insert of duplicates
      nodeRef.values.get_rw()[pos] = val;
      return false;
    }
    if (nodeRef.numKeys == M) {
      /* split the node */
      splitLeafNode(node, splitInfo);
      auto &splitRef = *splitInfo;
      auto sibling = splitRef.rightChild.leaf;

      /* insert the new entry */
      if (key < splitRef.key)
        insertInLeafNodeAtLastPosition(node, key, val);
      else
        insertInLeafNodeAtLastPosition(sibling, key, val);

      /* inform the caller about the split */
      splitRef.key = sibling->keys.get_ro()[findMinKeyAtLeafNode(sibling)];
      split = true;
    } else {
      /* otherwise, we can simply insert the new entry at the last position */
      insertInLeafNodeAtLastPosition(node, key, val);
    }
    return split;
  }

  void splitLeafNode(persistent_ptr<LeafNode> node, SplitInfo *splitInfo) {
    auto &nodeRef = *node;

    /* determine the split position by finding the median in unsorted array of keys */
    auto data = node->keys.get_ro();
    const auto [__unused__, splitPos] = findSplitKey(data);
    const auto splitKey = data[splitPos];

    /* move all entries with greater or equal keys to a new sibling node */
    persistent_ptr<LeafNode> sibling = newLeafNode();
    auto &sibRef = *sibling;
    auto n = 0u, s = 0u; //< node/sibling entry count
    for (auto i = 0u; i < M; i++) {
      const auto &currkey = nodeRef.keys.get_ro()[i];
      if (currkey > splitKey){
        sibRef.keys.get_rw()[s] = currkey;
        sibRef.values.get_rw()[s] = nodeRef.values.get_ro()[i];
        s++;
      } else {
        nodeRef.keys.get_rw()[n] = currkey;
        nodeRef.values.get_rw()[n] = nodeRef.values.get_ro()[i];
        n++;
      }
    }
    nodeRef.numKeys.get_rw() = n;
    sibRef.numKeys.get_rw() = s;

    /* setup the list of leaf nodes */
    if(nodeRef.nextLeaf != nullptr) {
      sibRef.nextLeaf = nodeRef.nextLeaf;
      nodeRef.nextLeaf->prevLeaf = sibling;
    }
    nodeRef.nextLeaf = sibling;
    sibRef.prevLeaf = node;
    //PersistEmulation::writeBytes(M * (sizeof(KeyType) + sizeof(ValueType)) + 2*4 + 2*16); //M entries moved + 2 numKeys + 2 pointers

    auto &splitInfoRef = *splitInfo;
    splitInfoRef.leftChild = node;
    splitInfoRef.rightChild = sibling;
    splitInfoRef.key = splitKey;
  }

  /**
   * Insert a (key, value) pair at the last position into the leaf node
   * @c node. The caller has to ensure that there is enough space to insert the element
   *
   * @oaram node the leaf node where the element is to be inserted
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   */
  void insertInLeafNodeAtLastPosition(persistent_ptr<LeafNode> node,
      const KeyType &key,
      const ValueType &val) {
    const auto pos = node->numKeys.get_ro();
    assert(pos < M);
    /* insert the new entry at the currently last position */
    node->keys.get_rw()[pos] = key;
    node->values.get_rw()[pos] = val;
    //PersistEmulation::persistStall();

    node->numKeys.get_rw() = pos + 1;
    //PersistEmulation::persistStall();
    //PersistEmulation::writeBytes(sizeof(KeyType) + sizeof(ValueType) +sizeof(unsigned int));
  }

  /**
   * Split the given branch node @c node in the middle and move
   * half of the keys/children to the new sibling node.
   *
   * @param node the branch node to be split
   * @param splitKey the key on which the split of the child occured
   * @param splitInfo information about the split
   */
  void splitBranchNode(persistent_ptr<BranchNode> node, const KeyType &splitKey,
      SplitInfo *splitInfo) {
    // we have an overflow at the branch node, let's split it
    // determine the split position
    unsigned int middle = (N + 1) / 2;
    // adjust the middle based on the key we have to insert
    if (splitKey > node->keys.get_ro()[middle]) middle++;
    // move all entries behind this position to a new sibling node
    persistent_ptr<BranchNode> sibling = newBranchNode();
    sibling->numKeys = node->numKeys - middle;
    for (auto i = 0u; i < sibling->numKeys; i++) {
      sibling->keys.get_rw()[i] = node->keys.get_ro()[middle + i];
      sibling->children.get_rw()[i] = node->children.get_ro()[middle + i];
    }
    sibling->children.get_rw()[sibling->numKeys] = node->children.get_ro()[node->numKeys];
    node->numKeys = middle - 1;
    splitInfo->key = node->keys.get_ro()[middle - 1];
    splitInfo->leftChild = node;
    splitInfo->rightChild = sibling;
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
  bool insertInBranchNode(persistent_ptr<BranchNode> node, unsigned int depth,
      const KeyType &key, const ValueType &val,
      SplitInfo *splitInfo) {
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;

    auto pos = lookupPositionInBranchNode(node, key);
    if (depth - 1 == 0) {
      // case #1: our children are leaf nodes
      auto child = node->children.get_ro()[pos].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      // case #2: our children are branch nodes
      auto child = node->children.get_ro()[pos].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }
    if (hasSplit) {
      persistent_ptr<BranchNode> host = node;
      // the child node was split, thus we have to add a new entry
      // to our branch node

      if (node->numKeys == N) {

        splitBranchNode(node, childSplitInfo.key, splitInfo);
        host = (key < splitInfo->key ? splitInfo->leftChild
            : splitInfo->rightChild).branch;
        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
      if (pos < host->numKeys) {
        // if the child isn't inserted at the rightmost position
        // then we have to make space for it
        host->children.get_rw()[host->numKeys + 1] = host->children.get_ro()[host->numKeys];
        for (auto i = host->numKeys.get_ro(); i > pos; i--) {
          host->children.get_rw()[i] = host->children.get_ro()[i - 1];
          host->keys.get_rw()[i] = host->keys.get_ro()[i - 1];
        }
      }
      // finally, add the new entry at the given position
      host->keys.get_rw()[pos] = childSplitInfo.key;
      host->children.get_rw()[pos] = childSplitInfo.leftChild;
      host->children.get_rw()[pos + 1] = childSplitInfo.rightChild;
      host->numKeys = host->numKeys + 1;
    }
    return split;
  }

  /* ---------------------------------------------------------------------- */
  /*                                   LOOKUP                               */
  /* ---------------------------------------------------------------------- */

  /**
   * Traverse the tree starting at the root until the leaf node is found that
   * could contain the given @key. Note, that always a leaf node is returned
   * even if the key doesn't exist on this node.
   *
   * @param key the key we are looking for
   * @return the leaf node that would store the key
   */
  persistent_ptr<LeafNode> findLeafNode(const KeyType &key) const {
    auto node = rootNode;
    auto d = depth.get_ro();
    while (d-- > 0) {
      // as long as we aren't at the leaf level we follow the path down
      auto n = node.branch;
      auto pos = lookupPositionInBranchNode(n, key);
      node = n->children.get_ro()[pos];
    }
    return node.leaf;
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
  unsigned int lookupPositionInBranchNode(persistent_ptr<BranchNode> node,
      const KeyType &key) const {
    unsigned int pos = 0;
    const unsigned int num = node->numKeys;
    const auto &keys = node->keys.get_ro();

    for (; pos < num && keys[pos] <= key; pos++) {
    };
    return pos;
  }

  /**
   * Lookup the search key @c key in the given leaf node and return the
   * position.
   * If the search key was not found, then @c numKeys is returned.
   *
   * @param node the leaf node where we search
   * @param key the search key
   * @return the position of the key  (or @c numKey if not found)
   */
  unsigned int lookupPositionInLeafNode(persistent_ptr<LeafNode> node,
      const KeyType &key) const {
    unsigned int pos = 0;
    const unsigned int num = node->numKeys.get_ro();
    const auto &keys = node->keys.get_ro();
    for (; pos < num && keys[pos] != key; pos++);
    return pos;
  }

  /* ---------------------------------------------------------------------- */

  /**
   * Create a new empty leaf node
   */
  persistent_ptr<LeafNode> newLeafNode() {
    auto pop = pmem::obj::pool_by_vptr(this);
    persistent_ptr<LeafNode> newNode = nullptr;
    transaction::run(pop, [&] {
        newNode = make_persistent<LeafNode>();
        });
    return newNode;
  }

  void deleteLeafNode(persistent_ptr<LeafNode> node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {
        delete_persistent<LeafNode>(node);
        });
  }

  /**
   * Create a new empty branch node
   */
  persistent_ptr<BranchNode> newBranchNode() {
    auto pop = pmem::obj::pool_by_vptr(this);
    persistent_ptr<BranchNode> newNode = nullptr;
    transaction::run(pop, [&] {
        newNode = make_persistent<BranchNode>();
        });
    return newNode;
  }

  void deleteBranchNode(persistent_ptr<BranchNode> node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {
        delete_persistent<BranchNode>(node);
        });
  }
  /* -----------------------------------------------------------------------
  */

  /**
   * A structure for representing a leaf node of a B+ tree.
   */
  struct alignas(64) LeafNode {
    /**
     * Constructor for creating a new empty leaf node.
     */
    LeafNode() : numKeys(0), nextLeaf(nullptr), prevLeaf(nullptr) {}

    p<unsigned int> numKeys;             //< the number of currently stored keys
    p<std::array<KeyType, M>> keys;      //< the actual keys
    p<std::array<ValueType, M>> values;  //< the actual values
    persistent_ptr<LeafNode> nextLeaf;   //< pointer to the subsequent sibling
    persistent_ptr<LeafNode> prevLeaf;   //< pointer to the preceeding sibling
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct alignas(64) BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode() : numKeys(0) {}

    p<unsigned int> numKeys;             //< the number of currently stored keys
    p<std::array<KeyType, N>> keys;      //< the actual keys
    p<std::array<Node, N + 1>> children; //< pointers to child nodes (BranchNode or LeafNode)
  };

}; /* end class UnsortedPBPTree */

} /* namespace dbis::pbptree */

#endif
