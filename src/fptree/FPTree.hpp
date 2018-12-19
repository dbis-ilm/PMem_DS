/*
 * Copyright (C) 2017-2018 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#ifndef DBIS_FPTree_hpp_
#define DBIS_FPTree_hpp_

#include <array>
#include <iostream>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#define BRANCH_PADDING  0
#define LEAF_PADDING    0

namespace dbis::fptree {

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
class FPTree {
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
  struct LowestBranchNode;


  struct LeafOrBranchNode {
    LeafOrBranchNode() : tag(BLANK) {};

    LeafOrBranchNode(persistent_ptr<LeafNode> leaf_) : tag(LEAF), leaf(leaf_) {};

    LeafOrBranchNode(BranchNode *branch_) : tag(BRANCH), branch(branch_) {};

    LeafOrBranchNode(LowestBranchNode *lowestbranch_) : tag(LOWESTBRANCH), lowestbranch(lowestbranch_) {};

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
        case LOWESTBRANCH: {
          lowestbranch = other.lowestbranch;
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
      BLANK, LEAF, BRANCH, LOWESTBRANCH
    } tag;
    union {
      persistent_ptr<LeafNode> leaf;
      BranchNode *branch;
      LowestBranchNode *lowestbranch;
    };
  };

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
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode() : numKeys(0) {}

    unsigned int numKeys;                         //< the number of currently stored keys
    std::array<KeyType, N> keys;                  //< the actual keys
    std::array<LeafOrBranchNode, N + 1> children; //< pointers to child nodes (BranchNode or LeafNode)
    unsigned char pad_[BRANCH_PADDING];           //<
  };

  /**
   * A structure for representing a branch node of the last level before leaf level.
   */
  struct LowestBranchNode {
    /**
     * Constructor for creating a new empty lowest branch node.
     */
    LowestBranchNode() : numKeys(0) {}

    unsigned int numKeys;                         //< the number of currently stored keys
    std::array<KeyType, N> keys;                  //< the actual keys
    std::array<persistent_ptr<LeafNode>,
      N + 1> children;      //< pointers to child nodes (LeafNodes) in persistent mem
    unsigned char pad_[BRANCH_PADDING];           //<
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
    BranchNode *newNode = new BranchNode;
    return newNode;
  }

  void deleteBranchNode(BranchNode *node) {
    //TODO
  }

  /**
   * Create a new empty lowest branch node
   */
  LowestBranchNode *newLowestBranchNode() {
    LowestBranchNode *newNode = new LowestBranchNode;
    return newNode;
  }

  void deleteLowestBranchNode(LowestBranchNode *node) {
    //TODO
  }

  /**
   * A structure for passing information about a node split to
   * the caller.
   */
  struct SplitInfo {
    KeyType key;                 //< the key at which the node was split
    LeafOrBranchNode leftChild;  //< the resulting lhs child node
    LeafOrBranchNode rightChild; //< the resulting rhs child node
  };

  unsigned int depth;         //< the depth of the tree, i.e. the number of levels (0 => rootNode is LeafNode)

  LeafOrBranchNode rootNode;     //< pointer to the root node
  persistent_ptr<LeafNode> leafList; //<Pointer to the leaf at the most left position. Neccessary for recovery

  PROFILE_DECL

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
    iterator(const LeafOrBranchNode &root, std::size_t d) {
      // traverse to left-most key
      auto node = root;
      while (d-- > 0) {
        auto n = node.branch;
        node = n->children[0];
        if (d == 0) {
          auto n =node.lowestbranch;
          node=n->children[0];
        }
      }
      currentNode = node.leaf;
      currentPosition = 0;
    }

    iterator& operator++() {
      if (currentPosition >= currentNode->numKeys-1) {
        currentNode = currentNode->nextLeaf;
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
  FPTree() {
    rootNode = newLeafNode();
    leafList=rootNode.leaf;
    depth=0;
    PROFILE_INIT
  }

  /**
   * Destructor for the tree. Should delete all allocated nodes.
   */
  ~FPTree() {
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
        } else if (depth == 1) {
        //the root node is a lowest branch node
        auto n = rootNode.lowestbranch;
        wasSplit = insertInLowestBranchNode(n, key, val, &splitInfo);
        } else {
        // the root node is a branch node
        auto n = rootNode.branch;
        wasSplit = insertInBranchNode(n, depth, key, val, &splitInfo);
        }
        if (wasSplit) {
        // we had an overflow in the node and therefore the node is split
        if (depth == 0) {
        auto root = newLowestBranchNode();

        root->keys[0] = splitInfo.key;
        root->children[0] = splitInfo.leftChild.leaf;
        root->children[1] = splitInfo.rightChild.leaf;
        root->numKeys = root->numKeys + 1;
        rootNode.lowestbranch = root;
        depth = depth + 1;
        } else {
          auto root = newBranchNode();

          root->keys[0] = splitInfo.key;
          root->children[0] = splitInfo.leftChild;
          root->children[1] = splitInfo.rightChild;
          root->numKeys = root->numKeys + 1;
          rootNode.branch = root;
          depth = depth + 1;

        }
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

    //if(needRecovery()){recover();};

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
        result=eraseFromLeafNode(node, key);
        } else if (depth == 1) {
        // the root node is a lowest branch node
        auto node = rootNode.lowestbranch;
        assert(node != nullptr);
        result=eraseFromLowestBranchNode(node, key);
        } else {
        auto node = rootNode.branch;
        assert(node != nullptr);
        result=eraseFromBranchNode(node, depth, key);

        }
        });
    return result;
  }
  /**
   * Recover the FPTree by iterating over the LeafList and using the recoveryInsert method.
   */
  void recover(){
    std::cout<<"Starting RECOVERY of FPTree"<<std::endl;
    persistent_ptr<LeafNode> currentLeaf=leafList;
    if(leafList== nullptr){
      std::cout<<"No data to recover FPTree"<<std::endl;
    }
    if(leafList->nextLeaf== nullptr){
      // The index has only one node, so the leaf node becomes the root node
      rootNode=leafList;
      depth=0;
    } else {
      rootNode= newLowestBranchNode();
      depth = 1;
      rootNode.lowestbranch->children[0] = currentLeaf;
      while (currentLeaf != nullptr) {
        recoveryInsert(currentLeaf);
        currentLeaf = currentLeaf->nextLeaf;
      }
    }
    std::cout<<"RECOVERY Done"<<std::endl;
  }

  /**
   * Print the structure and content of the tree to stdout.
   */
  void print() const {
    if (depth == 0) {
      printLeafNode(0, rootNode.leaf);
    } else if (depth == 1) {
      printLowestBranchNode(0, rootNode.lowestbranch);
    } else {
      auto n = rootNode;
      printBranchNode(0u, n.branch);
    }
  }

  PROFILE_PRINT

  /**
   * Perform a scan over all key-value pairs stored in the tree.
   * For each entry the given function @func is called.
   *
   * @param func the function called for each entry
   */
  void scan(ScanFunc func) const {}

  /**
   * Perform a range scan over all elements within the range [minKey, maxKey]
   * and for each element call the given function @c func.
   *
   * @param minKey the lower boundary of the range
   * @param maxKey the upper boundary of the range
   * @param func the function called for each entry
   */
  void scan(const KeyType &minKey, const KeyType &maxKey, ScanFunc func) const {}

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
    auto pos = lookupPositionInLeafNode(node, key);

    if (pos < node->numKeys && node->keys.get_ro()[pos] == key) {
      // handle insert of duplicates
      node->values.get_rw()[pos] = val;
      return false;
    }
    if (node->numKeys == M) {
      // the node is full, so we must split it
      // determine the split position
      unsigned int middle = (M + 1) / 2;
      // move all entries behind this position to a new sibling node
      persistent_ptr<LeafNode> sibling = newLeafNode();
      sibling->numKeys = node->numKeys - middle;
      for (auto i = 0u; i < sibling->numKeys; i++) {

        sibling->keys.get_rw()[i] = node->keys.get_ro()[i + middle];
        sibling->values.get_rw()[i] = node->values.get_ro()[i + middle];
      }
      node->numKeys = middle;

      // insert the new entry
      if (pos < middle)
        insertInLeafNodeAtPosition(node, pos, key, val);
      else
        insertInLeafNodeAtPosition(sibling, pos - middle, key, val);

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

      splitInfo->key = sibling->keys.get_ro()[0];
    } else {
      // otherwise, we can simply insert the new entry at the given position
      insertInLeafNodeAtPosition(node, pos, key, val);
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
    assert(pos < M);
    assert(pos <= node->numKeys);
    assert(node->numKeys < M);
    // we move all entries behind pos by one position
    for (unsigned int i = node->numKeys; i > pos; i--) {

      node->keys.get_rw()[i] = node->keys.get_ro()[i - 1];
      node->values.get_rw()[i] = node->values.get_ro()[i - 1];
    }
    // and then insert the new entry at the given position

    node->keys.get_rw()[pos] = key;
    node->values.get_rw()[pos] = val;
    node->numKeys = node->numKeys + 1;
  }

  /**
   * Insert a (key, value) pair into the tree recursively by following the path
   * down to the leaf level starting at a lowest branch node @c at depth @c depth 1.
   *
   * @param node the starting node for the insert
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   * @param splitInfo information about the split
   * @return true if a split was performed
   */
  bool insertInLowestBranchNode(LowestBranchNode *node,
      const KeyType &key, const ValueType &val,
      SplitInfo *splitInfo) {
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;

    auto pos = lookupPositionInLowestBranchNode(node, key);

    //our children are leaf nodes
    auto child = node->children[pos];
    hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);

    if (hasSplit) {
      LowestBranchNode *host = node;
      // the child node was split, thus we have to add a new entry
      // to our lowest branch node

      if (node->numKeys == N) {
        splitLowestBranchNode(node, childSplitInfo.key, splitInfo);

        host = (key < splitInfo->key ? splitInfo->leftChild
            : splitInfo->rightChild).lowestbranch;
        split = true;
        pos = lookupPositionInLowestBranchNode(host, key);
      }
      if (pos < host->numKeys) {
        // if the child isn't inserted at the rightmost position
        // then we have to make space for it

        host->children[host->numKeys + 1] = host->children[host->numKeys];
        for (auto i = host->numKeys; i > pos; i--) {

          host->children[i] = host->children[i - 1];
          host->keys[i] = host->keys[i - 1];
        }
      }
      // finally, add the new entry at the given position

      host->keys[pos] = childSplitInfo.key;
      host->children[pos] = childSplitInfo.leftChild.leaf;
      host->children[pos + 1] = childSplitInfo.rightChild.leaf;
      host->numKeys = host->numKeys + 1;
    }
    return split;
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
      const KeyType &key, const ValueType &val,
      SplitInfo *splitInfo) {
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;

    auto pos = lookupPositionInBranchNode(node, key);
    if (depth - 1 == 1) {
      //case #1: our children are lowest branch nodes
      auto child = node->children[pos].lowestbranch;
      hasSplit = insertInLowestBranchNode(child, key, val, &childSplitInfo);
    } else {
      // case #2: our children are branch nodes

      auto child = node->children[pos].branch;
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }
    if (hasSplit) {
      BranchNode *host = node;
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

        host->children[host->numKeys + 1] = host->children[host->numKeys];
        for (auto i = host->numKeys; i > pos; i--) {

          host->children[i] = host->children[i - 1];
          host->keys[i] = host->keys[i - 1];
        }
      }
      // finally, add the new entry at the given position

      host->keys[pos] = childSplitInfo.key;
      host->children[pos] = childSplitInfo.leftChild;
      host->children[pos + 1] = childSplitInfo.rightChild;
      host->numKeys = host->numKeys + 1;
    }
    return split;
  }

  /**
   * Split the given lowest branch node @c node in the middle and move
   * half of the keys/children to the new sibling node.
   *
   * @param node the branch node to be split
   * @param splitKey the key on which the split of the child occured
   * @param splitInfo information about the split
   */
  void splitLowestBranchNode(LowestBranchNode *node, const KeyType &splitKey,
      SplitInfo *splitInfo) {
    // we have an overflow at the lowest branch node, let's split it
    // determine the split position
    unsigned int middle = (N + 1) / 2;
    // adjust the middle based on the key we have to insert

    if (splitKey > node->keys[middle]) middle++;
    // move all entries behind this position to a new sibling node
    LowestBranchNode *sibling = newLowestBranchNode();
    sibling->numKeys = node->numKeys - middle;
    for (auto i = 0u; i < sibling->numKeys; i++) {

      sibling->keys[i] = node->keys[middle + i];
      sibling->children[i] = node->children[middle + i];
    }

    sibling->children[sibling->numKeys] = node->children[node->numKeys];
    node->numKeys = middle - 1;
    splitInfo->key = node->keys[middle - 1];
    splitInfo->leftChild = node;
    splitInfo->rightChild = sibling;
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
    // we have an overflow at the branch node, let's split it
    // determine the split position
    unsigned int middle = (N + 1) / 2;
    // adjust the middle based on the key we have to insert

    if (splitKey > node->keys[middle]) middle++;
    // move all entries behind this position to a new sibling node
    BranchNode *sibling = newBranchNode();
    sibling->numKeys = node->numKeys - middle;
    for (auto i = 0u; i < sibling->numKeys; i++) {

      sibling->keys[i] = node->keys[middle + i];
      sibling->children[i] = node->children[middle + i];
    }

    sibling->children[sibling->numKeys] = node->children[node->numKeys];
    node->numKeys = middle - 1;
    splitInfo->key = node->keys[middle - 1];
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
    while (d > 1) {
      // as long as we aren't at the lowest branch level we follow the path down
      auto n = node.branch;
      auto pos = lookupPositionInBranchNode(n, key);

      node = n->children[pos];
      d--;
    }
    if(d==1){
      //after reaching the lowest leaf level, one more step is neccessary
      auto n=node.lowestbranch;
      auto pos=lookupPositionInLowestBranchNode(n, key);
      node=n->children[pos];
    }
    return node.leaf;
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
    // we perform a simple linear search, perhaps we should try a binary
    // search instead?
    unsigned int pos = 0;

    const unsigned int num = node->numKeys.get_ro();
    for (; pos < num && node->keys.get_ro()[pos] < key; pos++) {

    };
    return pos;
  }

  /**
   * Lookup the search key @c key in the given lowesst branch node and return the
   * position which is the position in the list of keys + 1. in this way, the
   * position corresponds to the position of the child pointer in the
   * array @children.
   * If the search key is less than the smallest key, then @c 0 is returned.
   * If the key is greater than the largest key, then @c numKeys is returned.
   *
   * @param node the lowest branch node where we search
   * @param key the search key
   * @return the position of the key + 1 (or 0 or @c numKey)
   */
  unsigned int lookupPositionInLowestBranchNode(LowestBranchNode *node,
      const KeyType &key) const {
    // we perform a simple linear search, perhaps we should try a binary
    // search instead?
    unsigned int pos = 0;
    const unsigned int num = node->numKeys;
    for (; pos < num && node->keys[pos] <= key; pos++) {

    };
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
    // we perform a simple linear search, perhaps we should try a binary
    // search instead?
    unsigned int pos = 0;
    const unsigned int num = node->numKeys;
    for (; pos < num && node->keys[pos] <= key; pos++) {

    };
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
    bool deleted = false;
    auto pos = lookupPositionInLeafNode(node, key);
    if (node->keys.get_ro()[pos] == key) {
      for (auto i = pos; i < node->numKeys - 1; i++) {

        node->keys.get_rw()[i] = node->keys.get_ro()[i + 1];
        node->values.get_rw()[i] = node->values.get_ro()[i + 1];
      }
      node->numKeys.get_rw() = node->numKeys.get_ro() - 1;
      deleted = true;
    }
    return deleted;
  }

  /**
   * Delete an entry from the tree by recursively going down to the leaf level
   * and handling the underflows.
   *
   * @param node the current lowest branch node
   * @param key the key to be deleted
   * @return true if the entry was deleted
   */
  bool eraseFromLowestBranchNode(LowestBranchNode *node, const KeyType &key) {
    bool deleted = false;
    // try to find the leaf
    auto pos = lookupPositionInLowestBranchNode(node, key);

    // the next level is the leaf level
    auto leaf = node->children[pos];
    assert(leaf != nullptr);
    deleted = eraseFromLeafNode(leaf, key);
    unsigned int middle = (M + 1) / 2;
    if (leaf->numKeys < middle) {
      // handle underflow
      underflowAtLeafLevel(node, pos, leaf);
    }

    return deleted;
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
    // try to find the branch
    auto pos = lookupPositionInBranchNode(node, key);
    if (d == 2) {
      // the next level is the lowest branch level

      auto lowestbranch = node->children[pos].lowestbranch;
      assert(lowestbranch != nullptr);
      deleted = eraseFromLowestBranchNode(lowestbranch, key);
      unsigned int middle = (M + 1) / 2;
      if (lowestbranch->numKeys < middle) {
        // handle underflow

        lowestbranch=underflowAtLowestBranchLevel(node, pos, lowestbranch);
        if (d == depth && node->numKeys == 0) {
          // special case: the root node is empty now
          rootNode = lowestbranch;
          depth = depth - 1;
        }
      }
    } else {

      auto child = node->children[pos].branch;
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
   * Handle the case that during a delete operation a underflow at node @c leaf
   * occured. If possible this is handled
   * (1) by rebalancing the elements among the leaf node and one of its siblings
   * (2) if not possible by merging with one of its siblings.
   *
   * @param node the parent node of the node where the underflow occured
   * @param pos the position of the child node @leaf in the @c children array of
   * the lowest branch node
   * @param leaf the node at which the underflow occured
   */
  void
    underflowAtLeafLevel(LowestBranchNode *node, unsigned int pos, persistent_ptr <LeafNode> leaf) {
      assert(pos <= node->numKeys);
      unsigned int middle = (M + 1) / 2;
      // 1. we check whether we can rebalance with one of the siblings
      // but only if both nodes have the same direct parent
      if (pos > 0 && leaf->prevLeaf->numKeys > middle) {
        // we have a sibling at the left for rebalancing the keys
        balanceLeafNodes(leaf->prevLeaf, leaf);

        node->keys[pos - 1] = leaf->keys.get_ro()[0];
      } else if (pos < node->numKeys && leaf->nextLeaf->numKeys > middle) {
        // we have a sibling at the right for rebalancing the keys
        balanceLeafNodes(leaf->nextLeaf, leaf);

        node->keys[pos] = leaf->nextLeaf->keys.get_ro()[0];
      } else {
        // 2. if this fails we have to merge two leaf nodes
        // but only if both nodes have the same direct parent
        persistent_ptr <LeafNode> survivor = nullptr;
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
          // just remove the child node from the current lowest branch node
          for (auto i = pos; i < node->numKeys - 1; i++) {

            node->keys[i] = node->keys[i + 1];
            node->children[i + 1] = node->children[i + 2];
          }

          node->children[pos] = survivor;
          node->numKeys--;
        } else {
          // This is a special case that happens only if
          // the current node is the root node. Now, we have
          // to replace the lowest branch root node by a leaf node.
          rootNode = survivor;
          depth = depth - 1;
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
   * of the lowest branch node
   * @param child the node at which the underflow occured
   * @return the (possibly new) child node (in case of a merge)
   */
  LowestBranchNode* underflowAtLowestBranchLevel(BranchNode *node, unsigned int pos,
      LowestBranchNode* child) {
    assert(node != nullptr);
    assert(child != nullptr);

    LowestBranchNode *newChild = child;
    unsigned int middle = (N + 1) / 2;
    // 1. we check whether we can rebalance with one of the siblings

    if (pos > 0 &&
        node->children[pos - 1].branch->numKeys >middle) {
      // we have a sibling at the left for rebalancing the keys

      LowestBranchNode *sibling = node->children[pos - 1].lowestbranch;
      balanceLowestBranchNodes(sibling, child, node, pos - 1);
      // node->keys.get_rw()[pos] = child->keys.get_ro()[0];
      return newChild;
    } else if (pos < node->numKeys && node->children[pos + 1].lowestbranch->numKeys > middle) {
      // we have a sibling at the right for rebalancing the keys
      auto sibling = node->children[pos + 1].lowestbranch;
      balanceLowestBranchNodes(sibling, child, node, pos);

      return newChild;
    } else {

      // 2. if this fails we have to merge two lowest branch nodes
      LowestBranchNode *lSibling = nullptr, *rSibling = nullptr;
      unsigned int prevKeys = 0, nextKeys = 0;

      if (pos > 0) {

        lSibling = node->children[pos - 1].lowestbranch;
        prevKeys = lSibling->numKeys;
      }
      if (pos < node->numKeys) {

        rSibling = node->children[pos + 1].lowestbranch;
        nextKeys = rSibling->numKeys;
      }

      LowestBranchNode *witnessNode = nullptr;
      auto ppos = pos;
      if (prevKeys > 0) {

        mergeLowestBranchNodes(lSibling, node->keys[pos - 1], child);
        ppos = pos - 1;
        witnessNode = child;
        newChild = lSibling;
        // pos -= 1;
      } else if (nextKeys > 0) {

        mergeLowestBranchNodes(child, node->keys[pos], rSibling);
        witnessNode = rSibling;
      } else
        // shouldn't happen
        assert(false);

      // remove node->keys[pos] from node
      for (auto i = ppos; i < node->numKeys - 1; i++) {

        node->keys[i] = node->keys[i + 1];
      }
      if (pos == 0) pos++;
      for (auto i = pos; i < node->numKeys; i++) {
        if (i + 1 <= node->numKeys) {


          node->children[i] = node->children[i + 1];
        }
      }
      node->numKeys--;

      deleteLowestBranchNode(witnessNode);
      return newChild;
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
  BranchNode* underflowAtBranchLevel(BranchNode *node, unsigned int pos,
      BranchNode* child) {
    assert(node != nullptr);
    assert(child != nullptr);

    BranchNode *newChild = child;
    unsigned int middle = (N + 1) / 2;
    // 1. we check whether we can rebalance with one of the siblings

    if (pos > 0 &&
        node->children[pos - 1].branch->numKeys >middle) {
      // we have a sibling at the left for rebalancing the keys

      BranchNode *sibling = node->children[pos - 1].branch;
      balanceBranchNodes(sibling, child, node, pos - 1);
      // node->keys.get_rw()[pos] = child->keys.get_ro()[0];
      return newChild;
    } else if (pos < node->numKeys && node->children[pos + 1].branch->numKeys > middle) {
      // we have a sibling at the right for rebalancing the keys
      auto sibling = node->children[pos + 1].branch;
      balanceBranchNodes(sibling, child, node, pos);

      return newChild;
    } else {

      // 2. if this fails we have to merge two branch nodes
      BranchNode *lSibling = nullptr, *rSibling = nullptr;
      unsigned int prevKeys = 0, nextKeys = 0;

      if (pos > 0) {

        lSibling = node->children[pos - 1].branch;
        prevKeys = lSibling->numKeys;
      }
      if (pos < node->numKeys) {

        rSibling = node->children[pos + 1].branch;
        nextKeys = rSibling->numKeys;
      }

      BranchNode *witnessNode = nullptr;
      auto ppos = pos;
      if (prevKeys > 0) {

        mergeBranchNodes(lSibling, node->keys[pos - 1], child);
        ppos = pos - 1;
        witnessNode = child;
        newChild = lSibling;
        // pos -= 1;
      } else if (nextKeys > 0) {

        mergeBranchNodes(child, node->keys[pos], rSibling);
        witnessNode = rSibling;
      } else
        // shouldn't happen
        assert(false);

      // remove node->keys.get_ro()[pos] from node
      for (auto i = ppos; i < node->numKeys - 1; i++) {

        node->keys[i] = node->keys[i + 1];
      }
      if (pos == 0) pos++;
      for (auto i = pos; i < node->numKeys; i++) {
        if (i + 1 <= node->numKeys) {


          node->children[i] = node->children[i + 1];
        }
      }
      node->numKeys--;

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
  void balanceLeafNodes(persistent_ptr <LeafNode> donor, persistent_ptr <LeafNode> receiver) {
    assert(donor->numKeys > receiver->numKeys);
    unsigned int balancedNum = (donor->numKeys + receiver->numKeys) / 2;
    unsigned int toMove = donor->numKeys - balancedNum;
    if (toMove == 0) return;

    if (donor->keys.get_ro()[0] < receiver->keys.get_ro()[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0, j = 0;
      for (i = receiver->numKeys; i > 0; i--) {
        // reserve space on receiver side

        receiver->keys.get_rw()[i + toMove - 1] = receiver->keys.get_ro()[i - 1];
        receiver->values.get_rw()[i + toMove - 1] = receiver->values.get_ro()[i - 1];
      }
      // move toMove keys/values from donor to receiver
      for (i = balancedNum; i < donor->numKeys; i++, j++) {

        receiver->keys.get_rw()[j] = donor->keys.get_ro()[i];
        receiver->values.get_rw()[j] = donor->values.get_ro()[i];
        receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + 1;
      }
    } else {
      // mode from one node to a node with smaller keys
      unsigned int i = 0;
      // move toMove keys/values from donor to receiver
      for (i = 0; i < toMove; i++) {

        receiver->keys.get_rw()[receiver->numKeys] = donor->keys.get_ro()[i];
        receiver->values.get_rw()[receiver->numKeys] = donor->values.get_ro()[i];
        receiver->numKeys.get_rw() = receiver->numKeys.get_ro() + 1;
      }
      // on donor node move all keys and values to the left
      for (i = 0; i < donor->numKeys - toMove; i++) {

        donor->keys.get_rw()[i] = donor->keys.get_ro()[toMove + i];
        donor->values.get_rw()[i] = donor->values.get_ro()[toMove + i];
      }
    }
    donor->numKeys.get_rw() = donor->numKeys.get_ro() - toMove;
  }

  /**
   * Rebalance two lowest branch nodes by moving some key-children pairs from the node
   * @c donor to the node @receiver via the parent node @parent. The position of
   * the key between the two nodes is denoted by @c pos.
   *
   * @param donor the lowest branch node from which the elements are taken
   * @param receiver the sibling lowest branch node getting the elements from @c donor
   * @param parent the parent node of @c donor and @c receiver
   * @param pos the position of the key in node @c parent that lies between
   *      @c donor and @c receiver
   */
  void balanceLowestBranchNodes(LowestBranchNode *donor, LowestBranchNode *receiver,
      BranchNode *parent, unsigned int pos) {
    assert(donor->numKeys > receiver->numKeys);

    unsigned int balancedNum = (donor->numKeys + receiver->numKeys) / 2;
    unsigned int toMove = donor->numKeys - balancedNum;
    if (toMove == 0) return;

    if (donor->keys[0] < receiver->keys[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0;

      // 1. make room
      receiver->children[receiver->numKeys + toMove] =
        receiver->children[receiver->numKeys];
      for (i = receiver->numKeys; i > 0; i--) {
        // reserve space on receiver side

        receiver->keys[i + toMove - 1] = receiver->keys[i - 1];
        receiver->children[i + toMove - 1] = receiver->children[i - 1];
      }
      // 2. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {

        receiver->children[i] =
          donor->children[donor->numKeys - toMove + 1 + i];
      }
      for (i = 0; i < toMove - 1; i++) {

        receiver->keys[i] = donor->keys[donor->numKeys - toMove + 1 + i];
      }

      receiver->keys[toMove - 1] = parent->keys[pos];
      assert(parent->numKeys > pos);
      parent->keys[pos] = donor->keys[donor->numKeys - toMove];
      receiver->numKeys += toMove;
    } else {
      // mode from one node to a node with smaller keys
      unsigned int i = 0, n = receiver->numKeys;

      // 1. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {

        receiver->children[n + 1 + i] = donor->children[i];
        receiver->keys[n + 1 + i] = donor->keys[i];
      }
      // 2. we have to move via the parent node: take the key from
      // parent->keys.get_ro()[pos]

      receiver->keys[n] = parent->keys[pos];
      receiver->numKeys += toMove;
      KeyType key = donor->keys[toMove - 1];

      // 3. on donor node move all keys and values to the left
      for (i = 0; i < donor->numKeys - toMove; i++) {

        donor->keys[i] = donor->keys[toMove + i];
        donor->children[i] = donor->children[toMove + i];
      }

      donor->children[donor->numKeys - toMove] =
        donor->children[donor->numKeys];
      // and replace this key by donor->keys.get_ro()[0]
      assert(parent->numKeys > pos);
      parent->keys[pos] = key;
    }
    donor->numKeys -= toMove;
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
  void balanceBranchNodes(BranchNode *donor, BranchNode *receiver,
      BranchNode *parent, unsigned int pos) {
    assert(donor->numKeys > receiver->numKeys);

    unsigned int balancedNum = (donor->numKeys + receiver->numKeys) / 2;
    unsigned int toMove = donor->numKeys - balancedNum;
    if (toMove == 0) return;

    if (donor->keys[0] < receiver->keys[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0;

      // 1. make room
      receiver->children[receiver->numKeys + toMove] =
        receiver->children[receiver->numKeys];
      for (i = receiver->numKeys; i > 0; i--) {
        // reserve space on receiver side

        receiver->keys[i + toMove - 1] = receiver->keys[i - 1];
        receiver->children[i + toMove - 1] = receiver->children[i - 1];
      }
      // 2. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {

        receiver->children[i] =
          donor->children[donor->numKeys - toMove + 1 + i];
      }
      for (i = 0; i < toMove - 1; i++) {

        receiver->keys[i] = donor->keys[donor->numKeys - toMove + 1 + i];
      }

      receiver->keys[toMove - 1] = parent->keys[pos];
      assert(parent->numKeys > pos);
      parent->keys[pos] = donor->keys[donor->numKeys - toMove];
      receiver->numKeys += toMove;
    } else {
      // mode from one node to a node with smaller keys
      unsigned int i = 0, n = receiver->numKeys;

      // 1. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {

        receiver->children[n + 1 + i] = donor->children[i];
        receiver->keys[n + 1 + i] = donor->keys[i];
      }
      // 2. we have to move via the parent node: take the key from
      // parent->keys.get_ro()[pos]

      receiver->keys[n] = parent->keys[pos];
      receiver->numKeys += toMove;
      KeyType key = donor->keys[toMove - 1];

      // 3. on donor node move all keys and values to the left
      for (i = 0; i < donor->numKeys - toMove; i++) {

        donor->keys[i] = donor->keys[toMove + i];
        donor->children[i] = donor->children[toMove + i];
      }

      donor->children[donor->numKeys - toMove] =
        donor->children[donor->numKeys];
      // and replace this key by donor->keys.get_ro()[0]
      assert(parent->numKeys > pos);
      parent->keys[pos] = key;
    }
    donor->numKeys -= toMove;
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
   * Merge two lowest branch nodes my moving all keys/children from @c node to @c
   * sibling and put the key @c key from the parent node in the middle. The node
   * @c node should be deleted by the caller.
   *
   * @param sibling the left sibling node which receives all keys/children
   * @param key the key from the parent node that is between sibling and node
   * @param node the node from which we move all keys/children
   */
  void mergeLowestBranchNodes(LowestBranchNode *sibling, const KeyType &key,
      LowestBranchNode *node) {
    assert(key <= node->keys[0]);
    assert(sibling != nullptr);
    assert(node != nullptr);
    assert(sibling->keys[sibling->numKeys - 1] < key);

    sibling->keys[sibling->numKeys] = key;
    sibling->children[sibling->numKeys + 1] = node->children[0];
    for (auto i = 0u; i < node->numKeys; i++) {

      sibling->keys[sibling->numKeys + i + 1] = node->keys[i];
      sibling->children[sibling->numKeys + i + 2] = node->children[i + 1];
    }
    sibling->numKeys += node->numKeys + 1;
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
  void mergeBranchNodes(BranchNode *sibling, const KeyType &key,
      BranchNode *node) {
    assert(key <= node->keys[0]);
    assert(sibling != nullptr);
    assert(node != nullptr);
    assert(sibling->keys[sibling->numKeys - 1] < key);

    sibling->keys[sibling->numKeys] = key;
    sibling->children[sibling->numKeys + 1] = node->children[0];
    for (auto i = 0u; i < node->numKeys; i++) {

      sibling->keys[sibling->numKeys + i + 1] = node->keys[i];
      sibling->children[sibling->numKeys + i + 2] = node->children[i + 1];
    }
    sibling->numKeys += node->numKeys + 1;
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

      std::cout << "{" << node->keys.get_ro()[i] << "}";
    }
    std::cout << "]" << std::endl;
  }

  /**
   * Insert a leaf node into the tree for recovery
   * @param leaf the leaf to insert
   */
  void recoveryInsert(persistent_ptr<LeafNode> leaf){
    assert(depth>0);
    assert(leaf!= nullptr);
    bool hassplit=false;
    SplitInfo splitInfo;
    if (depth==1){
      //The root node is a lowest branch node
      hassplit=recoveryInsertInLowestBranchNode(rootNode.lowestbranch, leaf, &splitInfo);
    } else{
      //The root node is a branch node
      hassplit=recoveryInsertInBranchNode(rootNode.branch,depth, leaf,&splitInfo);
    }

    //Check for split
    if(hassplit){
      //The root node was splitted
      auto newRoot=newBranchNode();
      newRoot->keys[0]=splitInfo.key;
      newRoot->children[0]=splitInfo.leftChild;
      newRoot->children[1]=splitInfo.rightChild;
      newRoot->numKeys=1;
      rootNode=newRoot;
      depth++;
    }
  }
  /**
   * Insert a leaf node into the tree recursively by following the path
   * down to the leaf level starting at a lowest branch node @c at depth @c depth 1.
   *
   * @param node the starting node for the insert
   * @param leaf the leaf node to insert
   * @param splitInfo information about the split
   * @return true if a split was performed
   */
  bool recoveryInsertInLowestBranchNode(LowestBranchNode *node, persistent_ptr<LeafNode> leaf, SplitInfo *splitInfo){

    if(node->numKeys==N){
      //We have to split the lowest branch node
      splitLowestBranchNode(node, leaf->keys.get_ro()[0], splitInfo);
      //we have to insert in the new right child, as the keys in the leafList are sorted
      auto done=recoveryInsertInLowestBranchNode(splitInfo->rightChild.lowestbranch,leaf,splitInfo );
      return true;
    }
    else{
      node->keys[node->numKeys]=leaf->keys.get_ro()[0];
      node->numKeys++;
      node->children[node->numKeys]=leaf;
      return false;
    }
  }
  /**
   * Insert a leaf node into the tree recursively by following the path
   * down to the leaf level starting at node @c node at depth @c depth.
   *
   * @param node the starting node for the insert
   * @param depth the current depth of the tree (0 == leaf level, 1 == lowestbranch level)
   * @param leaf the leaf node to insert
   * @param splitInfo information about the split
   * @return true if a split was performed
   */
  bool recoveryInsertInBranchNode(BranchNode *node, int curr_depth, persistent_ptr<LeafNode> leaf, SplitInfo *splitInfo){
    bool hassplit=false;
    SplitInfo childSplitInfo;
    if(curr_depth==2){
      //The next level is the lowest branch level
      //We can go down the rightmost way
      hassplit=recoveryInsertInLowestBranchNode(node->children[node->numKeys].lowestbranch, leaf,&childSplitInfo);
    }else{
      hassplit=recoveryInsertInBranchNode(node->children[node->numKeys].branch,curr_depth-1,leaf,&childSplitInfo);
    }

    //Check for split
    if(hassplit){
      if(node->numKeys==N){
        //We have to split the branch node
        splitBranchNode(node, childSplitInfo.key,splitInfo);
        BranchNode *newNode = splitInfo->rightChild.branch;
        //Insert the new child into the new branch at the rightmost position
        newNode->keys[newNode->numKeys]=childSplitInfo.key;
        newNode->children[newNode->numKeys]=childSplitInfo.leftChild;
        newNode->numKeys++;
        newNode->children[newNode->numKeys]=childSplitInfo.rightChild;
        return true;
      }
      else{
        node->keys[node->numKeys]=childSplitInfo.key;
        node->numKeys++;
        node->children[node->numKeys]=childSplitInfo.rightChild;
        return false;
      }
    }
    return false;
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
    std::cout << d << " BN { ["<<node<<"] ";
    for (auto k = 0u; k < node->numKeys; k++) {
      if (k > 0) std::cout << ", ";

      std::cout << node->keys[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= node->numKeys; k++) {
      if (d + 1 < depth - 1) {
        auto child = node->children[k].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto child = node->children[k].lowestbranch;
        if (child != nullptr) printLowestBranchNode(d + 1, child);
      }

    }
  }

  /**
   * Print the given lowest branch node @c node and all its children
   * to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printLowestBranchNode(unsigned int d, LowestBranchNode *node) const {
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << d << " LBN { ";
    for (auto k = 0u; k < node->numKeys; k++) {
      if (k > 0) std::cout << ", ";

      std::cout << node->keys[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= node->numKeys; k++) {
      auto leaf = node->children[k];
      printLeafNode(d + 1, leaf);
    }
  }

  void printLeafList(){
    persistent_ptr<LeafNode> curr_node=leafList;
    while(curr_node!= nullptr){
      printLeafNode(0,curr_node);
      curr_node=curr_node->nextLeaf;
    }
  }
};//end class FPTree
}//end namespace dbis::fptree

#endif
