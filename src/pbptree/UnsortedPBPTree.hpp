/*
 * Copyright (C) 2017-2019 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of our NVM-based Data Structure Repository.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
#include "ElementOfRankK.hpp"

#define BRANCH_PADDING  0
#define LEAF_PADDING    0

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
  // there is a bug that for odd numbers the tree sometimes breaks (TODO)
  static_assert(M % 2 == 0 && N % 2 == 0, "The number of keys should be even");

#ifndef UNIT_TESTS
  private:
#else
  public:
#endif

  // Forward declarations
  struct LeafNode;
  struct BranchNode;

  struct LeafOrBranchNode {
    LeafOrBranchNode() : tag(BLANK) {};

    LeafOrBranchNode(persistent_ptr<LeafNode> leaf_) : tag(LEAF), leaf(leaf_) {};

    LeafOrBranchNode(persistent_ptr<BranchNode> branch_) : tag(BRANCH), branch(branch_) {};

    LeafOrBranchNode(const LeafOrBranchNode &other) { copy(other); };

    void copy(const LeafOrBranchNode &other) throw() {
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

    LeafOrBranchNode &operator=(LeafOrBranchNode other) {
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
    LeafOrBranchNode leftChild;   //< the resulting lhs child node
    LeafOrBranchNode rightChild;  //< the resulting rhs child node
  };

  p<unsigned int> depth;          //< the depth of the tree, i.e. the number of levels (0 => rootNode is LeafNode)

  LeafOrBranchNode rootNode;      //< pointer to the root node (an instance of @c LeafNode or
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

    iterator(const LeafOrBranchNode &root, std::size_t d) {
      // traverse to left-most key
      auto node = root;
      while (d-- > 0) {
        auto n = node.branch;
        //PROFILE_READ(1)
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
      //PROFILE_READ(2)
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
  
  PROFILE_DECL
  
  /**
   * Typedef for a function passed to the scan method.
   */
  using ScanFunc = std::function<void(const KeyType &key, const ValueType &val)>;

  /**
   * Constructor for creating a new B+ tree.
   */
  UnsortedPBPTree() : depth(0) {
    rootNode = newLeafNode();
    PROFILE_INIT
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
        PROFILE_WRITE(3)
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
    PROFILE_READ()
    if (pos < leafNode->numKeys && leafNode->keys.get_ro()[pos] == key) {
      // we found it!
      PROFILE_READ()
      *val = leafNode->values.get_ro()[pos];
      result = true;
    }
    if(result==false) print();
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

  PROFILE_PRINT

  /**
   * Perform a scan over all key-value pairs stored in the B+ tree.
   * For each entry the given function @func is called.
   *
   * @param func the function called for each entry
   */
  void scan(ScanFunc func) const {
    // we traverse to the leftmost leaf node
    auto node = rootNode;
    PROFILE_READ()
    auto d = depth.get_ro();
    while (d-- > 0) {
      // as long as we aren't at the leaf level we follow the path down
      auto n = node.branch;
      PROFILE_READ()
      node = n->children.get_ro()[0];
    }
    auto leaf = node.leaf;
    while (leaf != nullptr) {
      // for each key-value pair call func
      for (auto i = 0u; i < leaf->numKeys; i++) {
        PROFILE_READ(2)
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
        PROFILE_READ()
        auto &key = leaf->keys.get_ro()[i];
        if (key > maxKey) return;
        PROFILE_READ()
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
    PROFILE_WRITE()
    if (node->keys.get_rw()[pos] == key) {
      //simply copy the last object on the node to the position of the erased object
      PROFILE_READ(3)
      PROFILE_WRITE(2)
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
        PROFILE_READ()
        PROFILE_WRITE()
        node->keys.get_rw()[pos-1] = leaf->keys.get_ro()[findMinKeyAtLeafNode(leaf)];
      } else if (pos < node->numKeys && leaf->nextLeaf->numKeys > middle) {
        // we have a sibling at the right for rebalancing the keys
        balanceLeafNodes(leaf->nextLeaf, leaf);
        PROFILE_READ()
        PROFILE_WRITE()
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
            PROFILE_READ(2)
            PROFILE_WRITE(2)
            node->keys.get_rw()[i] = node->keys.get_ro()[i + 1];
            node->children.get_rw()[i + 1] = node->children.get_ro()[i + 2];
          }
          PROFILE_WRITE()
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
      PROFILE_READ(2)
      PROFILE_WRITE(2)
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
    PROFILE_READ(2)
    if (donor->keys.get_ro()[0] < receiver->keys.get_ro()[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0;

      // move toMove keys/values from donor to receiver
      for (i = 0; i < toMove; i++) {
        uint max=findMaxKeyAtLeafNode(donor);
        // move the donor's maximum key to the receiver
        PROFILE_READ(2)
        PROFILE_WRITE(2)
        receiver->keys.get_rw()[receiver->numKeys]=donor->keys.get_ro()[max];
        receiver->values.get_rw()[receiver->numKeys]=donor->values.get_ro()[max];
        receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + 1;

        // fill empty space in donor with its currently last key
        PROFILE_READ(2)
        PROFILE_WRITE(2)
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
        PROFILE_READ(2)
        PROFILE_WRITE(2)
        receiver->keys.get_rw()[receiver->numKeys]=donor->keys.get_ro()[min];
        receiver->values.get_rw()[receiver->numKeys]=donor->values.get_ro()[min];
        receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + 1;

        // fill empty space in donor with its currently last key
        PROFILE_READ(2)
        PROFILE_WRITE(2)
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
    PROFILE_READ()
    KeyType currMinKey=node->keys.get_ro()[0];
    for(auto i=1u; i<node->numKeys; i++){
      PROFILE_READ()
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
    PROFILE_READ()
    KeyType currMinKey=node->keys.get_ro()[0];
    for(auto i=1u; i<node->numKeys; i++){
      PROFILE_READ()
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
    PROFILE_READ()
    auto n = node->children.get_ro()[pos];
    if (d == 1) {
      // the next level is the leaf level
      auto leaf = n.leaf;
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      unsigned int middle = (M + 1) / 2;
      if (leaf->numKeys < middle) {
        // handle underflow
        PROFILE_UNDERFLOW
        underflowAtLeafLevel(node, pos, leaf);
      }
    } else {
      auto child = n.branch;
      deleted = eraseFromBranchNode(child, d - 1, key);

      pos = lookupPositionInBranchNode(node, key);
      unsigned int middle = (N + 1) / 2;
      if (child->numKeys < middle) {
        // handle underflow
        PROFILE_UNDERFLOW
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
    PROFILE_READ(2)
    assert(key <= node->keys.get_ro()[0]);
    assert(sibling != nullptr);
    assert(node != nullptr);
    PROFILE_READ()
    PROFILE_WRITE()
    assert(sibling->keys.get_ro()[sibling->numKeys - 1] < key);

    sibling->keys.get_rw()[sibling->numKeys] = key;
    PROFILE_READ()
    PROFILE_WRITE()
    sibling->children.get_rw()[sibling->numKeys + 1] = node->children.get_ro()[0];
    for (auto i = 0u; i < node->numKeys; i++) {
      PROFILE_READ(2)
      PROFILE_WRITE(2)
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
    PROFILE_READ()
    if (pos > 0 &&
        (node->children.get_ro()[pos - 1]).branch->numKeys >
        middle) {
      // we have a sibling at the left for rebalancing the keys
      PROFILE_READ()
      persistent_ptr<BranchNode> sibling = (node->children.get_ro()[pos - 1]).branch;
      balanceBranchNodes(sibling, child, node, pos - 1);
      // node->keys.get_rw()[pos] = child->keys.get_ro()[0];
      return newChild;
    } else if (pos < node->numKeys && (node->children.get_ro()[pos + 1]).branch->numKeys > middle) {
      // we have a sibling at the right for rebalancing the keys
      PROFILE_READ(2)
      auto sibling = (node->children.get_ro()[pos + 1]).branch;
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    } else {
      // 2. if this fails we have to merge two branch nodes
      persistent_ptr<BranchNode> lSibling = nullptr, rSibling = nullptr;
      unsigned int prevKeys = 0, nextKeys = 0;

      if (pos > 0) {
        PROFILE_READ(2)
        lSibling = (node->children.get_ro()[pos - 1]).branch;
        prevKeys = lSibling->numKeys;
      }
      if (pos < node->numKeys) {
        PROFILE_READ()
        rSibling = (node->children.get_ro()[pos + 1]).branch;
        nextKeys = rSibling->numKeys;
      }

      persistent_ptr<BranchNode> witnessNode = nullptr;
      auto ppos = pos;
      if (prevKeys > 0) {
        PROFILE_READ()
        mergeBranchNodes(lSibling, node->keys.get_ro()[pos - 1], child);
        ppos = pos - 1;
        witnessNode = child;
        newChild = lSibling;
        // pos -= 1;
      } else if (nextKeys > 0) {
        PROFILE_READ()
        mergeBranchNodes(child, node->keys.get_ro()[pos], rSibling);
        witnessNode = rSibling;
      } else
        // shouldn't happen
        assert(false);

      // remove node->keys.get_ro()[pos] from node
      for (auto i = ppos; i < node->numKeys - 1; i++) {
        PROFILE_READ()
        PROFILE_WRITE()
        node->keys.get_rw()[i] = node->keys.get_ro()[i + 1];
      }
      if (pos == 0) pos++;
      for (auto i = pos; i < node->numKeys; i++) {
        if (i + 1 <= node->numKeys){
          PROFILE_READ()
          PROFILE_WRITE()
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
    PROFILE_READ(2)
    if (donor->keys.get_ro()[0] < receiver->keys.get_ro()[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0;

      // 1. make room
      PROFILE_READ()
      PROFILE_WRITE()
      receiver->children.get_rw()[receiver->numKeys + toMove] =
        receiver->children.get_ro()[receiver->numKeys];
      for (i = receiver->numKeys; i > 0; i--) {
        // reserve space on receiver side
        PROFILE_READ(2)
        PROFILE_WRITE(2)
        receiver->keys.get_rw()[i + toMove - 1] = receiver->keys.get_ro()[i - 1];
        receiver->children.get_rw()[i + toMove - 1] = receiver->children.get_ro()[i - 1];
      }
      // 2. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {
        PROFILE_READ()
        PROFILE_WRITE()
        receiver->children.get_rw()[i] =
          donor->children.get_ro()[donor->numKeys - toMove + 1 + i];
      }
      for (i = 0; i < toMove - 1; i++) {
        PROFILE_READ()
        PROFILE_WRITE()
        receiver->keys.get_rw()[i] = donor->keys.get_ro()[donor->numKeys - toMove + 1 + i];
      }
      PROFILE_READ()
      PROFILE_WRITE()
      receiver->keys.get_rw()[toMove - 1] = parent->keys.get_ro()[pos];
      assert(parent->numKeys > pos);
      PROFILE_READ(2)
      PROFILE_WRITE(2)
      parent->keys.get_rw()[pos] = donor->keys.get_ro()[donor->numKeys - toMove];
      receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + toMove;
    } else {
      // mode from one node to a node with smaller keys
      unsigned int i = 0, n = receiver->numKeys;

      // 1. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {
        PROFILE_READ(2)
        PROFILE_WRITE(2)
        receiver->children.get_rw()[n + 1 + i] = donor->children.get_ro()[i];
        receiver->keys.get_rw()[n + 1 + i] = donor->keys.get_ro()[i];
      }
      // 2. we have to move via the parent node: take the key from
      // parent->keys.get_ro()[pos]
      PROFILE_READ(2)
      PROFILE_WRITE(2)
      receiver->keys.get_rw()[n] = parent->keys.get_ro()[pos];
      receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + toMove;
      PROFILE_READ()
      KeyType key = donor->keys.get_ro()[toMove - 1];

      // 3. on donor node move all keys and values to the left
      for (i = 0; i < donor->numKeys - toMove; i++) {
        PROFILE_READ(2)
        PROFILE_WRITE(2)
        donor->keys.get_rw()[i] = donor->keys.get_ro()[toMove + i];
        donor->children.get_rw()[i] = donor->children.get_ro()[toMove + i];
      }
      PROFILE_READ()
      PROFILE_WRITE(2)
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
      PROFILE_READ()
      std::cout << node->keys.get_ro()[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= node->numKeys; k++) {
      if (d + 1 < depth) {
        PROFILE_READ()
        auto child = node->children.get_ro()[k].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        PROFILE_READ()
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
      PROFILE_READ()
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
      PROFILE_READ()
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
      PROFILE_READ()
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
    PROFILE_READ()
    if (pos < node->numKeys && node->keys.get_ro()[pos] == key) {
      // handle insert of duplicates
      PROFILE_WRITE()
      node->values.get_rw()[pos] = val;
      return false;
    }
    if (node->numKeys == M) {
      // the node is full, so we must split it
      // determine the split position by finding the median in unsorted array of keys
      PROFILE_SPLIT
      KeyType data[M];
      for (auto i = 0u; i < M; i++) {
        PROFILE_READ()
        data[i] = node->keys.get_ro()[i];
      }

      KeyType middle = ElementOfRank::elementOfRank((M + 1) / 2 + 1, data, M, 0);
      // move all entries with greater or equal keys to a new sibling node
      persistent_ptr<LeafNode> sibling = newLeafNode();
      auto node_filled = 0u, sibling_filled = 0u;
      for (auto i = 0u; i < node->numKeys; i++) {
        PROFILE_READ()
        KeyType currkey = node->keys.get_ro()[i];
        if (currkey>=middle){
          PROFILE_READ()
          PROFILE_WRITE(2)
          sibling->keys.get_rw()[sibling_filled] = currkey;
          sibling->values.get_rw()[sibling_filled] = node->values.get_ro()[i];
          sibling_filled++;
        } else{
          PROFILE_READ()
          PROFILE_WRITE(2)
          node->keys.get_rw()[node_filled] = currkey;
          node->values.get_rw()[node_filled] = node->values.get_ro()[i];
          node_filled++;
        }
      }
      node->numKeys = node_filled;
      sibling->numKeys = sibling_filled;
      // insert the new entry
      if (key < middle)
        insertInLeafNodeAtLastPosition(node, key, val);
      else
        insertInLeafNodeAtLastPosition(sibling, key, val);
      // setup the list of leaf nodes
      if(node->nextLeaf != nullptr) {
        sibling->nextLeaf = node->nextLeaf;
        node->nextLeaf->prevLeaf = sibling;
      }

      node->nextLeaf = sibling;
      sibling->prevLeaf = node;

      // and inform the caller about the split
      split = true;
      splitInfo->leftChild = node;
      splitInfo->rightChild = sibling;
      splitInfo->key = middle;
    } else {
      // otherwise, we can simply insert the new entry at the last position
      insertInLeafNodeAtLastPosition(node, key, val);
    }
    return split;
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
    assert(node->numKeys < M);
    PROFILE_READ(2)
    PROFILE_WRITE(3)
    int pos = node->numKeys.get_ro();
    //insert the new entry at the currently last position
    node->keys.get_rw()[pos] = key;
    node->values.get_rw()[pos] = val;
    node->numKeys.get_rw() = node->numKeys.get_ro() + 1;
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
    PROFILE_READ()
    if (splitKey > node->keys.get_ro()[middle]) middle++;
    // move all entries behind this position to a new sibling node
    persistent_ptr<BranchNode> sibling = newBranchNode();
    sibling->numKeys = node->numKeys - middle;
    for (auto i = 0u; i < sibling->numKeys; i++) {
      PROFILE_READ(2)
      PROFILE_WRITE(2)
      sibling->keys.get_rw()[i] = node->keys.get_ro()[middle + i];
      sibling->children.get_rw()[i] = node->children.get_ro()[middle + i];
    }
    PROFILE_WRITE()
    sibling->children.get_rw()[sibling->numKeys] = node->children.get_ro()[node->numKeys];
    node->numKeys = middle - 1;
    PROFILE_READ()
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
      PROFILE_READ()
      auto child = node->children.get_ro()[pos].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      // case #2: our children are branch nodes
      PROFILE_READ()
      auto child = node->children.get_ro()[pos].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }
    if (hasSplit) {
      persistent_ptr<BranchNode> host = node;
      // the child node was split, thus we have to add a new entry
      // to our branch node

      if (node->numKeys == N) {

        splitBranchNode(node, childSplitInfo.key, splitInfo);
        PROFILE_SPLIT
        host = (key < splitInfo->key ? splitInfo->leftChild
            : splitInfo->rightChild).branch;
        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
      if (pos < host->numKeys) {
        // if the child isn't inserted at the rightmost position
        // then we have to make space for it
        PROFILE_READ()
        PROFILE_WRITE()
        host->children.get_rw()[host->numKeys + 1] = host->children.get_ro()[host->numKeys];
        PROFILE_READ()
        for (auto i = host->numKeys.get_ro(); i > pos; i--) {
          PROFILE_READ(2)
          PROFILE_WRITE(2)
          host->children.get_rw()[i] = host->children.get_ro()[i - 1];
          host->keys.get_rw()[i] = host->keys.get_ro()[i - 1];
        }
      }
      // finally, add the new entry at the given position
      PROFILE_WRITE(3)
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
    PROFILE_READ()
    auto d = depth.get_ro();
    while (d-- > 0) {
      // as long as we aren't at the leaf level we follow the path down
      auto n = node.branch;
      auto pos = lookupPositionInBranchNode(n, key);
      PROFILE_READ()
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
    // we perform a simple linear search, perhaps we should try a binary
    // search instead?
    unsigned int pos = 0;
    const unsigned int num = node->numKeys;

    for (; pos < num && node->keys.get_ro()[pos] <= key; pos++) {
      PROFILE_READ()
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
    PROFILE_READ()
    const unsigned int num = node->numKeys.get_ro();
    PROFILE_READ()
    for (; pos < num && node->keys.get_ro()[pos]!=key; pos++);
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
  struct LeafNode {
    /**
     * Constructor for creating a new empty leaf node.
     */
    LeafNode() : numKeys(0), nextLeaf(nullptr), prevLeaf(nullptr) {}

    p<unsigned int> numKeys;             //< the number of currently stored keys
    p<std::array<KeyType, M>> keys;      //< the actual keys
    p<std::array<ValueType, M>> values;  //< the actual values
    persistent_ptr<LeafNode> nextLeaf;   //< pointer to the subsequent sibling
    persistent_ptr<LeafNode> prevLeaf;   //< pointer to the preceeding sibling
    p<unsigned char> pad_[LEAF_PADDING]; //<
    //<
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode() : numKeys(0) {}

    p<unsigned int> numKeys;                         //< the number of currently stored keys
    p<std::array<KeyType, N>> keys;                  //< the actual keys
    p<std::array<LeafOrBranchNode, N + 1>> children; //< pointers to child nodes (BranchNode or LeafNode)
    p<unsigned char> pad_[BRANCH_PADDING];           //<
    //<
  };

}; /* end class UnsortedPBPTree */

} /* namespace dbis::pbptree */

#endif
