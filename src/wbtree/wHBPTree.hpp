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
#include <cmath>
#include <iostream>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include "config.h"
#include "utils/ElementOfRankK.hpp"

namespace dbis::wbptree {

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;

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

    Node(BranchNode branch_) : tag(BRANCH), branch(branch_) {};

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

    Node &operator=(const persistent_ptr<LeafNode> &leaf_) {
      tag = LEAF;
      leaf = leaf_;
      return *this;
    }

    enum NodeType {
      BLANK, LEAF, BRANCH
    } tag;
    union {
      persistent_ptr<LeafNode> leaf;
      BranchNode *branch;
    };
  };

  /* By Herb Sutter */
  template<typename T>
  struct CacheLineStorage {
    alignas(64) T data;
    char pad[ 64 > sizeof(T) ? 64 - sizeof(T) : 0];
  };
  template<size_t E>
  struct Search {
    std::array<uint8_t, E+1> slot; //< slot array for indirection, first = num
    std::bitset<E> b;              //< bitset for valid entries

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
      search.get_rw().data.slot[0] = 0;
    }

    p<CacheLineStorage<Search<M>>> search; //< helper structure for faster searches
    persistent_ptr<LeafNode>  nextLeaf; //< pointer to the subsequent sibling
    persistent_ptr<LeafNode>  prevLeaf; //< pointer to the preceeding sibling
    p<std::array<KeyType, M>>     keys; //< the actual keys
    p<std::array<ValueType, M>> values; //< the actual values
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode() { search.data.slot[0] = 0; }

    CacheLineStorage<Search<N>> search; //< helper structure for faster searches
    std::array<KeyType, N>        keys; //< the actual keys
    std::array<Node, N + 1>   children; //< pointers to child nodes (BranchNode or LeafNode)
  };

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

  persistent_ptr<LeafNode> newLeafNode(const persistent_ptr<LeafNode> &other) {
    auto pop = pmem::obj::pool_by_vptr(this);
    persistent_ptr<LeafNode> newNode = nullptr;
    transaction::run(pop, [&] {
      newNode = make_persistent<LeafNode>(*other);
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
  BranchNode *newBranchNode() {
    return new BranchNode();
  }

  void deleteBranchNode(BranchNode* node) {
    delete node;
  }

  /**
   * A structure for passing information about a node split to
   * the caller.
   */
  struct SplitInfo {
    KeyType     key; //< the key at which the node was split
    Node  leftChild; //< the resulting lhs child node
    Node rightChild; //< the resulting rhs child node
  };

  unsigned int                depth; //< the depth of the tree, i.e. the number of levels (0 => rootNode is LeafNode)
  Node                     rootNode; //< pointer to the root node
  persistent_ptr<LeafNode> leafList; //< pointer to the most left leaf node. Necessary for recovery

  public:
  /**
   * Typedef for a function passed to the scan method.
   */
  using ScanFunc = std::function<void(const KeyType &key, const ValueType &val)>;
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
        node = n->children[0];
      }
      currentNode = node.leaf;
      currentPosition = 0;
      // Can not overflow as there are at least M/2 entries
      while(!currentNode->search.get_ro().data.b.test(currentPosition)) ++currentPosition;
    }

    iterator& operator++() {
      if (currentPosition >= M-1) {
        currentNode = currentNode->nextLeaf;
        currentPosition = 0;
        if (currentNode == nullptr) return *this;
        while(!currentNode->search.get_ro().data.b.test(currentPosition)) ++currentPosition;
      } else if (!currentNode->search.get_ro().data.b.test(++currentPosition)) ++(*this);
      return *this;
    }
    iterator operator++(int) {iterator retval = *this; ++(*this); return retval;}

    bool operator==(iterator other) const {return (currentNode == other.currentNode &&
        currentPosition == other.currentPosition);}
    bool operator!=(iterator other) const {return !(*this == other);}

    std::pair<KeyType, ValueType> operator*() {

      return std::make_pair(currentNode->keys.get_ro()[currentPosition], currentNode->values.get_ro()[currentPosition]);
    }

    // iterator traits
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
  wHBPTree() : depth(0) {
    rootNode = newLeafNode();
    leafList = rootNode.leaf;
    LOG("created new wHBPTree with sizeof(BranchNode) = " << sizeof(BranchNode)
                              <<  ", sizeof(LeafNode) = " << sizeof(LeafNode));
  }

  /**
   * Destructor for the tree. Should delete all allocated nodes.
   */
  ~wHBPTree() {
    // Nodes are deleted automatically by releasing leafPool and branchPool.
  }

  /**
   * Insert an element (a key-value pair) into the tree. If the key @c key
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
        /* we had an overflow in the node and therefore the node is split */
        auto root = newBranchNode();
        root->keys[0] = splitInfo.key;
        root->children[0] = splitInfo.leftChild;
        root->children[N] = splitInfo.rightChild;
        root->search.data.slot[1] = 0;
        root->search.data.b.set(0);
        root->search.data.slot[0] = 1;
        rootNode.branch = root;
        ++depth;
      }
    });
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
    auto leafNode = findLeafNode(key);
    auto pos = lookupPositionInLeafNode(leafNode, key);
    if (pos <= leafNode->search.get_ro().data.slot[0] &&
        leafNode->keys.get_ro()[leafNode->search.get_ro().data.slot[pos]] == key) {
      // we found it!
      *val = leafNode->values.get_ro()[leafNode->search.get_ro().data.slot[pos]];
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
    });
    return deleted;
  }
 /**
  * Recover the wBPTree by iterating over the LeafList and using the recoveryInsert method.
  */
  void recover() {
    LOG("Starting RECOVERY of wHBPTree");
    persistent_ptr<LeafNode> currentLeaf = leafList;
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
    float x = std::log(leafs)/std::log(N+1);
    assert(x == int(x) && "Not supported for this amount of leafs, yet");

    /* actual recovery */
    currentLeaf = leafList;
    if (leafList->nextLeaf == nullptr) {
      // The index has only one node, so the leaf node becomes the root node
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
    if (depth == 0) {
      printLeafNode(0, rootNode.leaf);
    } else {
      auto n = rootNode;
      printBranchNode(0u, n.branch);
    }
  }

  /**
   * Perform a scan over all key-value pairs stored in the tree.
   * For each entry the given function @func is called.
   *
   * @param func the function called for each entry
   */
  void scan(ScanFunc func) const {
    // we traverse to the leftmost leaf node
    auto node = rootNode;
    auto d = depth;
    while ( d-- > 0) node = node.branch->children[0];
    auto leaf = node.leaf;
    while (leaf != nullptr) {
      // for each key-value pair call func
      for (auto i = 0u; i < M; i++) {
        if (!leaf->search.get_ro().data.b.test(i)) continue;
        const auto &key = leaf->keys.get_ro()[i];
        const auto &val = leaf->values.get_ro()[i];
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

    bool higherThanMax = false;
    while (!higherThanMax && leaf != nullptr) {
      // for each key-value pair within the range call func
      for (auto i = 0u; i < M; i++) {
        if (!leaf->search.get_ro().data.b.test(i)) continue;
        auto &key = leaf->keys.get_ro()[i];
        if (key < minKey) continue;
        if (key > maxKey) { higherThanMax = true; continue; }

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
  bool insertInLeafNode(persistent_ptr<LeafNode> node, const KeyType &key,
      const ValueType &val, SplitInfo *splitInfo) {
    bool split = false;
    const auto slotPos = lookupPositionInLeafNode(node, key);
    const auto slotArray = node->search.get_ro().data.slot;
    if (slotPos <= slotArray[0] && node->keys.get_ro()[slotArray[slotPos]] == key) {
      // handle insert of duplicates
      node->values.get_rw()[slotArray[slotPos]] = val;
      return false;
    }

    if (slotArray[0] == M) {
      // the node is full, so we must split it
      // determine the split position by finding median in unsorted array of keys
      const auto middle = (M + 1) / 2;

      // move all entries behind this position to a new sibling node
      persistent_ptr<LeafNode> sibling = newLeafNode();
      auto &sibSlots = sibling->search.get_rw().data.slot;
      sibSlots[0] = slotArray[0] - middle;
      for (auto i = 1u; i < sibSlots[0] + 1; i++) {
        sibSlots[i] = i - 1;
        sibling->search.get_rw().data.b.set(sibSlots[i]);
        sibling->keys.get_rw()[sibSlots[i]] = node->keys.get_ro()[slotArray[i + middle]];
        sibling->values.get_rw()[sibSlots[i]] = node->values.get_ro()[slotArray[i + middle]];
      }
      for (auto i = middle; i < M; i++)
        node->search.get_rw().data.b.reset(slotArray[i+1]);
      node->search.get_rw().data.slot[0] = middle;

      /* insert the new entry */
      if (slotPos-1 <= middle)
        insertInLeafNodeAtPosition(node, slotPos, key, val);
      else
        insertInLeafNodeAtPosition(sibling, slotPos-middle, key, val);

      // setup the list of leaf nodes
      if (node->nextLeaf != nullptr) {
        sibling->nextLeaf = node->nextLeaf;
        node->nextLeaf->prevLeaf = sibling;
      }
      node->nextLeaf = sibling;
      sibling->prevLeaf = node;

      // and inform the caller about the split
      split = true;
      splitInfo->leftChild = node;
      splitInfo->rightChild = sibling;
      splitInfo->key = sibling->keys.get_ro()[sibSlots[1]];
    } else {
      // otherwise, we can simply insert the new entry at the given position
      insertInLeafNodeAtPosition(node, slotPos, key, val);
    }
    return split;
  }

  /**
   * Insert a (key, value) pair at the given position @c pos into the leaf node
   * @c node. The caller has to ensure that
   * - there is enough space to insert the element
   * - the key is inserted at the correct position according to the order of
   * keys
   *
   * @oaram node the leaf node where the element is to be inserted
   * @param pos the position in the leaf node (0 <= pos <= numKeys < M)
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   */
  void insertInLeafNodeAtPosition(persistent_ptr<LeafNode> node, unsigned int pos,
      const KeyType &key, const ValueType &val) {
    assert(pos <= M);
    const auto u = node->search.get_ro().data.getFreeZero(); //< unused Entry

    /* insert the new entry at unused position */
    node->keys.get_rw()[u] = key;
    node->values.get_rw()[u] = val;

    /* adapt slot array */
    for (auto j = node->search.get_ro().data.slot[0]; j >= pos; j--)
      node->search.get_rw().data.slot[j+1] = node->search.get_ro().data.slot[j];
    node->search.get_rw().data.slot[pos] = u;
    node->search.get_rw().data.slot[0] = node->search.get_ro().data.slot[0] + 1;
    node->search.get_rw().data.b.set(u);
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
  bool insertInBranchNode(BranchNode *node, unsigned int depth,
      const KeyType &key, const ValueType &val, SplitInfo *splitInfo) {
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;

    auto pos = lookupPositionInBranchNode(node, key);
    if (depth == 1) {
      /* case #1: our children are leaf node */
      persistent_ptr<LeafNode> child;
      if (pos == node->search.data.slot[0] + 1)
        child = node->children[N].leaf;
      else
        child = node->children[node->search.data.slot[pos]].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      /* case #2: our children are branch nodes */
      BranchNode *child;
      if (pos == node->search.data.slot[0] + 1)
        child = node->children[N].branch;
      else
        child = node->children[node->search.data.slot[pos]].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }

    if (hasSplit) {
      auto host = node;
      /* the child node was split, thus we have to add a new entry to our branch
       * node */
      if (node->search.data.slot[0] == N) {
        /* this node is also full and needs to be split */
        splitBranchNode(node, childSplitInfo.key, splitInfo);

        host = (key < splitInfo->key ? splitInfo->leftChild
                                     : splitInfo->rightChild).branch;
        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
      /* Insert new key and children */
      const auto u = host->search.data.getFreeZero();
      host->keys[u] = childSplitInfo.key;
      host->children[u] = childSplitInfo.leftChild;

      /* adapt slot array */
      if (pos <= host->search.data.slot[0]) {
        /* if the child isn't inserted at the rightmost position then we have to
         * make space for it */
        for(auto j = host->search.data.slot[0]; j >= pos; j--)
          host->search.data.slot[j+1] = host->search.data.slot[j];
        host->children[host->search.data.slot[pos+1]] = childSplitInfo.rightChild;
      } else {
        host->children[N] = childSplitInfo.rightChild;
      }
      host->search.data.slot[pos] = u;
      host->search.data.slot[0] = host->search.data.slot[0] + 1;
      host->search.data.b.set(u);
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
  void splitBranchNode(BranchNode *node, const KeyType &splitKey,
                       SplitInfo *splitInfo) {
    /* determine the split position */
    auto middle = (N + 1) / 2;
    if (splitKey > node->keys[node->search.data.slot[middle]])
      middle++;

    /* move all entries behind this position to a new sibling node */
    auto sibling = newBranchNode();
    sibling->search.data.slot[0] = node->search.data.slot[0] - middle;
    for (auto i = 0u; i < sibling->search.data.slot[0]; i++) {
      sibling->search.data.slot[i+1] = i;  //< set slot
      sibling->search.data.b.set(i);       //< set bit
      /* set key and children */
      sibling->keys[i] = node->keys[node->search.data.slot[middle + i + 1]];
      sibling->children[i] = node->children[node->search.data.slot[middle + i + 1]];
    }
    for (auto i = middle; i <= N; i++)
      node->search.data.b.reset(node->search.data.slot[i]);
    node->search.data.slot[0] = middle-1;

    /* set new most right children */
    sibling->children[N] = node->children[N];
    node->children[N] = node->children[node->search.data.slot[middle]];

    /* set split information */
    splitInfo->key = node->keys[node->search.data.slot[middle]];
    splitInfo->leftChild = node;
    splitInfo->rightChild = sibling;
  }

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

    auto d = depth;
    while (d-- > 0) {
      auto n = node.branch;
      auto pos = lookupPositionInBranchNode(n, key);
      node = n->children[pos];
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
  unsigned int lookupPositionInLeafNode(persistent_ptr<LeafNode> node,
      const KeyType &key) const {
    auto pos = 1u;
    const auto &slotArray = node->search.get_ro().data.slot;
    const auto &keys = node->keys.get_ro();
    auto l = 1;
    auto r = slotArray[0]; //< num keys
    while (l <= r) {
      pos = (l + r) / 2;
      if (keys[slotArray[pos]] == key) return pos;
      if (keys[slotArray[pos]] < key) l = ++pos;
      else r = pos - 1;
    }
    return pos;
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
  unsigned int lookupPositionInBranchNode(BranchNode *node,
                                          const KeyType &key) const {
    auto pos = 1u;
    const auto &slotArray = node->search.data.slot;
    const auto &keys = node->keys;
    auto l = 1;
    auto r = slotArray[0]; //< num keys
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
  bool eraseFromLeafNode(persistent_ptr <LeafNode> node, const KeyType &key) {
    auto pos = lookupPositionInLeafNode(node, key);
    if (node->keys.get_ro()[node->search.get_ro().data.slot[pos]] == key) {
      node->search.get_rw().data.b.reset(pos);
      for (auto i = pos; i < node->search.get_ro().data.slot[0] + 1; i++)
        node->search.get_rw().data.slot[i] =
          node->search.get_rw().data.slot[i + 1];
      node->search.get_rw().data.slot[0] = node->search.get_ro().data.slot[0] - 1;
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
  bool eraseFromBranchNode(BranchNode *node, unsigned int d, const KeyType &key) {
    assert(d >= 1);
    bool deleted = false;
    /* try to find the branch */
    auto pos = lookupPositionInBranchNode(node, key);
    if (d == 1) {
      /* the next level is the leaf level */
      persistent_ptr<LeafNode> leaf;
      if (pos == node->search.data.slot[0] + 1)
        leaf = node->children[N].leaf;
      else
        leaf = node->children[node->search.data.slot[pos]].leaf;
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      auto middle = (M + 1) / 2;
      /* handle possible underflow */
      if (leaf->search.get_ro().data.slot[0] < middle)
        underflowAtLeafLevel(node, pos, leaf);
    } else {
      BranchNode *child;
      if (pos == node->search.data.slot[0] + 1)
        child = node->children[N].branch;
      else
        child = node->children[node->search.data.slot[pos]].branch;
      deleted = eraseFromBranchNode(child, d - 1, key);
      pos = lookupPositionInBranchNode(node, key);
      auto middle = (N + 1) / 2;

      /* handle possible underflow */
      if (child->search.data.slot[0] < middle) {
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth && node->search.data.slot[0] == 0) {
          // special case: the root node is empty now
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
  void underflowAtLeafLevel(BranchNode *node, unsigned int pos,
                            persistent_ptr <LeafNode> leaf) {
      assert(pos <= node->search.data.slot[0] + 1);
      const auto middle = (M + 1) / 2;
      /* 1. we check whether we can rebalance with one of the siblings but only
       *    if both nodes have the same direct parent */
      if (pos > 1 && leaf->prevLeaf->search.get_ro().data.slot[0] > middle) {
        /* we have a sibling at the left for rebalancing the keys */
        balanceLeafNodes(leaf->prevLeaf, leaf);
        if (pos == node->search.data.slot[0] + 1) //< rightmost leaf
          node->keys[node->search.data.slot[node->search.data.slot[0]]] =
            leaf->keys.get_ro()[node->search.data.slot[1]];
        else
          node->keys[node->search.data.slot[pos]] =
            leaf->keys.get_ro()[node->search.data.slot[1]];
      }
      else if (pos <= node->search.data.slot[0] && leaf->nextLeaf->search.get_ro().data.slot[0] > middle) {
        /* we have a sibling at the right for rebalancing the keys */
        balanceLeafNodes(leaf->nextLeaf, leaf);
        node->keys[node->search.data.slot[pos+1]] =
          leaf->nextLeaf->keys.get_ro()[node->search.data.slot[1]];
      }
      /* 2. if this fails we have to merge two leaf nodes but only if both nodes
       *    have the same direct parent */
      else {
        persistent_ptr<LeafNode> survivor = nullptr;
        if (pos > 1 && leaf->prevLeaf->search.get_ro().data.slot[0] <= middle) {
          survivor = mergeLeafNodes(leaf->prevLeaf, leaf);
          deleteLeafNode(leaf);
          --pos;
        } else if (pos <= node->search.data.slot[0] &&
            leaf->nextLeaf->search.get_ro().data.slot[0] <= middle) {
          /* because we update the pointers in mergeLeafNodes we keep it here */
          auto l = leaf->nextLeaf;
          survivor = mergeLeafNodes(leaf, l);
          deleteLeafNode(l);
        } else {
          /* this shouldn't happen?! */
          assert(false);
        }
        if (node->search.data.slot[0] > 1) {
          /* just remove the child node from the current branch node */
          node->search.data.b.reset(node->search.data.slot[pos]);
          for (auto i = pos; i < node->search.data.slot[0]; i++) {
            node->search.data.slot[i] = node->search.data.slot[i + 1];
          }
          if (pos == node->search.data.slot[0]) //< merged with rightmost
            node->children[N] = survivor;
          else
            node->children[node->search.data.slot[pos]] = survivor;
          node->search.data.slot[0] = node->search.data.slot[0] - 1;
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
  BranchNode *underflowAtBranchLevel(BranchNode *node, unsigned int pos,
                                     BranchNode *child) {
    assert(node != nullptr);
    assert(child != nullptr);

    auto newChild = child;
    auto middle = (N + 1) / 2;
    /* 1. we check whether we can rebalance with one of the siblings */
    if (pos > 1 && node->children[node->search.data.slot[pos-1]].branch->search.data.slot[0] > middle) {
      /* we have a sibling at the left for rebalancing the keys */
      auto sibling = node->children[node->search.data.slot[pos-1]].branch;
      balanceBranchNodes(sibling, child, node, pos-1);
      return newChild;
    } else if (pos < node->search.data.slot[0] &&
        node->children[node->search.data.slot[pos + 1]].branch->search.data.slot[0] > middle) {
      /* we have a sibling at the right for rebalancing the keys */
      auto sibling = node->children[node->search.data.slot[pos+1]].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    } else if (pos == node->search.data.slot[0] &&
        node->children[N].branch->search.data.slot[0] > middle) {
      auto sibling = node->children[N].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    }
    /* 2. if this fails we have to merge two branch nodes */
    else {
      BranchNode *lSibling = nullptr, *rSibling = nullptr;
      auto prevKeys = 0u, nextKeys = 0u;

      if (pos > 1) {
        lSibling = node->children[node->search.data.slot[pos - 1]].branch;
        prevKeys = lSibling->search.data.slot[0];
      }
      if (pos < node->search.data.slot[0]) {
        rSibling = node->children[node->search.data.slot[pos + 1]].branch;
        nextKeys = rSibling->search.data.slot[0];
      }
      if (pos == node->search.data.slot[0]) {
        rSibling = node->children[N].branch;
        nextKeys = rSibling->search.data.slot[0];
      }

      BranchNode *witnessNode = nullptr;
      auto ppos = pos;
      if (prevKeys > 0) {
        mergeBranchNodes(lSibling, node->keys[node->search.data.slot[pos-1]], child);
        ppos = pos - 1;
        witnessNode = child;
        newChild = lSibling;
      } else if (nextKeys > 0) {
        mergeBranchNodes(child, node->keys[node->search.data.slot[pos]], rSibling);
        witnessNode = rSibling;
      } else //< shouldn't happen
        assert(false);

      /* remove key/children ppos from node */
      for (auto i = ppos; i < node->search.data.slot[0] - 1; i++)
        node->search.data.slot[i] = node->search.data.slot[i + 1];

      if (pos == node->search.data.slot[0])
        node->children[N] = child; //< new rightmost children

      node->search.data.slot[0] = node->search.data.slot[0] - 1;
      deleteBranchNode(witnessNode);
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
  void balanceLeafNodes(persistent_ptr<LeafNode> donor, persistent_ptr<LeafNode> receiver) {
    const auto dNumKeys = donor->search.get_ro().data.slot[0];
    const auto rNumKeys = receiver->search.get_ro().data.slot[0];
    assert(dNumKeys > rNumKeys);
    auto balancedNum = (dNumKeys + rNumKeys) / 2;
    auto toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    if (donor->keys.get_ro()[donor->search.get_ro().data.slot[1]] <
        receiver->keys.get_ro()[receiver->search.get_ro().data.slot[1]]) {
      /* move to a node with larger keys */
      auto i = 0u, j = 1u;
      /* reserve space */
      for (i = rNumKeys; i > 0; i--)
        receiver->search.get_rw().data.slot[i + toMove] =
          receiver->search.get_ro().data.slot[i];
      /* move from donor to receiver */
      for (i = balancedNum+1; i <= dNumKeys; i++, j++) {
        const auto u = receiver->search.get_ro().data.getFreeZero();
        const auto dPos = donor->search.get_ro().data.slot[i];
        receiver->keys.get_rw()[u] = donor->keys.get_ro()[dPos];
        receiver->values.get_rw()[u] = donor->values.get_ro()[dPos];
        receiver->search.get_rw().data.slot[j] = u;
        receiver->search.get_rw().data.b.set(u);
        donor->search.get_rw().data.b.reset(dPos);
      }
    } else {
      /* move to a node with smaller keys */
      for (auto i = 1u; i < toMove + 1; i++) {
        const auto u = receiver->search.get_ro().data.getFreeZero();
        const auto dPos = donor->search.get_ro().data.slot[i];
        receiver->keys.get_rw()[u] = donor->keys.get_ro()[dPos];
        receiver->values.get_rw()[u] = donor->values.get_ro()[dPos];
        receiver->search.get_rw().data.slot[rNumKeys + i] = u;
        receiver->search.get_rw().data.b.set(u);
        donor->search.get_rw().data.b.reset(dPos);
      }
      /* move to left on donor node */
      for (auto i = 1; i < dNumKeys - toMove + 1; i++) {
        donor->search.get_rw().data.slot[i] =
          donor->search.get_ro().data.slot[toMove + i];
      }
    }
    donor->search.get_rw().data.slot[0] = dNumKeys - toMove;
    receiver->search.get_rw().data.slot[0] = rNumKeys + toMove;
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
  void balanceBranchNodes(BranchNode  *donor, BranchNode *receiver,
                          BranchNode *parent, unsigned int pos) {
    const auto dNumKeys = donor->search.data.slot[0];
    const auto rNumKeys = receiver->search.data.slot[0];
    assert(dNumKeys > rNumKeys);

    unsigned int balancedNum = (dNumKeys + rNumKeys) / 2;
    unsigned int toMove = dNumKeys - balancedNum;
    if (toMove == 0) return;

    /* 1. move from one node to a node with larger keys */
    if (donor->keys[donor->search.data.slot[1]]
        < receiver->keys[receiver->search.data.slot[1]]) {
      /* 1.1. make room */
      for (auto i = rNumKeys; i > 0; i--) {
        receiver->search.data.slot[i + toMove] =
          receiver->search.data.slot[i];
      }
      /* 1.2. move toMove keys/children from donor to receiver */
      /* the most right child first */
      const auto u = receiver->search.data.getFreeZero();
      receiver->keys[u] = parent->keys[parent->search.data.slot[pos]];
      receiver->children[u] = donor->children[N];
      receiver->search.data.slot[toMove] = u;
      receiver->search.data.b.set(u);
      /* now the rest */
      for (auto i = 2u; i <= toMove; i++) {
        const auto u2 = receiver->search.data.getFreeZero();
        const auto dPos = donor->search.data.slot[balancedNum + i];
        receiver->keys[u2] = donor->keys[dPos];
        receiver->children[u2] = donor->children[dPos];
        receiver->search.data.slot[i - 1] = u2;
        receiver->search.data.b.set(u2);
        donor->search.data.b.reset(dPos);
      }
      /* 1.3 set donors new rightmost child and new parent key */
      donor->children[N] =
        donor->children[donor->search.data.slot[balancedNum + 1]];
      parent->keys[parent->search.data.slot[pos]] =
        donor->keys[donor->search.data.slot[balancedNum + 1]];
    }
    /* 2. move from one node to a node with smaller keys */
    else {
      /* 2.1. copy parent key and rightmost child of receiver */
      const auto u = receiver->search.data.getFreeZero();
      receiver->keys[u] = parent->keys[parent->search.data.slot[pos]];
      receiver->children[u] = receiver->children[N];
      receiver->search.data.slot[rNumKeys + 1] = u;
      receiver->search.data.b.set(u);

      /* 2.2. move toMove keys/children from donor to receiver */
      for (auto i = 2u; i <= toMove; i++) {
        const auto u2 = receiver->search.data.getFreeZero();
        const auto dPos = donor->search.data.slot[i - 1];
        receiver->keys[u2] = donor->keys[dPos];
        receiver->children[u2] = donor->children[dPos];
        receiver->search.data.slot[rNumKeys + i] = u2;
        receiver->search.data.b.set(u2);
        donor->search.data.b.reset(dPos);
      }
      /* 2.3. set receivers new rightmost child and new parent key */
      receiver->children[N] = donor->children[donor->search.data.slot[toMove]];
      parent->keys[parent->search.data.slot[pos]] =
        donor->keys[donor->search.data.slot[toMove]];

      /* 2.4. on donor node move all keys and values to the left */
      for (auto i = 1u; i <= dNumKeys - toMove; i++) {
        donor->search.data.slot[i] = donor->search.data.slot[toMove + i];
      }
    }
    receiver->search.data.slot[0] = rNumKeys + toMove;
    donor->search.data.slot[0] = dNumKeys - toMove;
  }

  /**
   * Merge two leaf nodes by moving all elements from @c node2 to
   * @c node1.
   *
   * @param node1 the target node of the merge
   * @param node2 the source node
   * @return the merged node (always @c node1)
   */
  persistent_ptr <LeafNode> mergeLeafNodes(persistent_ptr <LeafNode> node1, persistent_ptr <LeafNode> node2) {
    assert(node1 != nullptr);
    assert(node2 != nullptr);
    const auto n1NumKeys = node1->search.get_ro().data.slot[0];
    const auto n2NumKeys = node2->search.get_ro().data.slot[0];
    assert(n1NumKeys + n2NumKeys <= M);

    /* we move all keys/values from node2 to node1*/
    for (auto i = 1u; i < n2NumKeys + 1; i++) {
        const auto u = node1->search.get_ro().data.getFreeZero();
        node1->keys.get_rw()[u] = node2->keys.get_ro()[node2->search.get_ro().data.slot[i]];
        node1->values.get_rw()[u] = node2->values.get_ro()[node2->search.get_ro().data.slot[i]];
        node1->search.get_rw().data.slot[n1NumKeys + i] = u;
        node1->search.get_rw().data.b.set(u);
    }
    node1->search.get_rw().data.slot[0] = n1NumKeys + n2NumKeys;
    node1->nextLeaf = node2->nextLeaf;
    if (node2->nextLeaf != nullptr) node2->nextLeaf->prevLeaf = node1;
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
    const auto sNumKeys = sibling->search.data.slot[0];
    assert(key <= node->keys[0]);
    assert(sibling != nullptr);
    assert(node != nullptr);
    assert(sibling->keys[sibling->search.data.slot[sNumKeys]] < key);

    /* merge parent key and drag rightmost child forward */
    auto u = sibling->search.data.getFreeZero();
    sibling->keys[u] = key;
    sibling->children[u] = sibling->children[N];
    sibling->search.data.b.set(u);
    sibling->search.data.slot[sNumKeys+1] = u;

    /* merge node */
    for (auto i = 1u; i < node->search.data.slot[0] + 1; i++) {
      u = sibling->search.data.getFreeZero();
      sibling->keys[u] = node->keys[node->search.data.slot[i]];
      sibling->children[u] = node->children[node->search.data.slot[i]];
      sibling->search.data.b.set(u);
      sibling->search.data.slot[sNumKeys + i + 1] = u;
    }
    /* set new rightmost child and counter */
    sibling->children[N] = node->children[N];
    sibling->search.data.slot[0] +=  node->search.data.slot[0] + 1;
  }

  /**
   * Insert a leaf node into the tree for recovery
   * @param leaf the leaf to insert
   */
  void recoveryInsert(persistent_ptr<LeafNode> leaf){
    assert(depth > 0);
    assert(leaf != nullptr);

    SplitInfo splitInfo;
    auto hassplit = recoveryInsertInBranchNode(rootNode.branch, depth, leaf, &splitInfo);

    //Check for split
    if (hassplit) {
      //The root node was splitted
      auto newRoot = newBranchNode();
      newRoot->keys[0] = splitInfo.key;
      newRoot->children[0] = splitInfo.leftChild;
      newRoot->children[N] = splitInfo.rightChild;
      newRoot->search.data.b.set(0);
      newRoot->search.data.slot[0] = 1;
      newRoot->search.data.slot[1] = 0;
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
  bool recoveryInsertInBranchNode(BranchNode *node, int curr_depth, persistent_ptr<LeafNode>   leaf, SplitInfo *splitInfo){
    bool hassplit=false;
    SplitInfo childSplitInfo;
    auto &nSlotArray = node->search.data.slot;
    const auto &lSlotArray = leaf->search.get_ro().data.slot;

    if (curr_depth == 1) {
      if (nSlotArray[0] == N) {
        /* we have to insert a new right child, as the keys in the leafList are sorted */
        BranchNode *newNode = newBranchNode();
        newNode->search.data.slot[1] = 0;
        newNode->children[0] = leaf;
        splitInfo->key = leaf->keys.get_ro()[lSlotArray[1]];
        splitInfo->leftChild = node;
        splitInfo->rightChild = newNode;
        return true;
      } else {
        node->search.data.b.set(nSlotArray[0]);
        node->keys[nSlotArray[0]] = leaf->keys.get_ro()[lSlotArray[1]];
        if (nSlotArray[0] > 0)
          node->children[nSlotArray[0]] = node->children[N];
        node->children[N] = leaf;
        nSlotArray[nSlotArray[0] + 1] = nSlotArray[0];
        ++nSlotArray[0];
        return false;
      }
    } else {
      hassplit = recoveryInsertInBranchNode(node->children[(nSlotArray[0]==0)?0:N].branch, curr_depth-  1, leaf, &childSplitInfo);
    }
    //Check for split
    if (hassplit) {
      if (nSlotArray[0] == N) {
        BranchNode *newNode = newBranchNode();
        newNode->search.data.slot[1] = 0;
        newNode->children[0] = childSplitInfo.rightChild;
        splitInfo->key = childSplitInfo.key;
        splitInfo->leftChild = node;
        splitInfo->rightChild = newNode;
        return true;
      } else {
        node->search.data.b.set(nSlotArray[0]);
        node->keys[nSlotArray[0]] = childSplitInfo.key;
        if (nSlotArray[0] > 0)
          node->children[nSlotArray[0]] = node->children[N];
        node->children[N] = childSplitInfo.rightChild;
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
  void printLeafNode(unsigned int d, persistent_ptr<LeafNode> node) const {
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << "[\033[1m" << std::hex << node << std::dec << "\033[0m #"
      << (int)node->search.get_ro().data.slot[0] << ": ";
    for (auto i = 1u; i <= node->search.get_ro().data.slot[0]; i++) {
      if (i > 1) std::cout << ", ";
      std::cout << "{(" << node->search.get_ro().data.b[node->search.get_ro().data.slot[i]] << ")"
                << node->keys.get_ro()[node->search.get_ro().data.slot[i]] << "}";
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
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << d << " BN { ["<<node<<"] #" << (int)node->search.data.slot[0] << ": ";
    for (auto k = 1u; k <= node->search.data.slot[0]; k++) {
      if (k > 1) std::cout << ", ";
      std::cout << "(" << node->search.data.b[node->search.data.slot[k]] << ")"
                << node->keys[node->search.data.slot[k]];
    }
    std::cout << " }" << std::endl;
    for (auto k = 1u; k <= node->search.data.slot[0] + 1; k++) {
      const auto pos = (k == node->search.data.slot[0] + 1)? N
        : node->search.data.slot[k];
      if (d + 1 < depth) {
        auto child = node->children[pos].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto leaf = node->children[pos].leaf;
        printLeafNode(d + 1, leaf);
      }
    }
  }

}; /* end class wHBPTree */
} /* namespace dbis::wbptree */

#endif /* DBIS_wHBPTree_hpp_ */