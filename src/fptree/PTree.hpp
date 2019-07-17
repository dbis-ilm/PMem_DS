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

#ifndef DBIS_PTree_hpp_
#define DBIS_PTree_hpp_

#include <array>
#include <iostream>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

namespace dbis::fptree {

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;
template<class Object>
using pptr = persistent_ptr<Object>;

/**
 * A persistent memory implementation of a PTree.
 *
 * @tparam KeyType the data type of the key
 * @tparam ValueType the data type of the values associated with the key
 * @tparam N the maximum number of keys on a branch node
 * @tparam M the maximum number of keys on a leaf node
 */
template<typename KeyType, typename ValueType, int N, int M>
class PTree {
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

    Node(BranchNode *branch_) : tag(BRANCH), branch(branch_) {};

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
    LeafNode() : numKeys(0), nextLeaf(nullptr), prevLeaf(nullptr) {}

    p<unsigned int> numKeys;             ///< the number of currently stored keys
    p<std::array<KeyType, M>> keys;      ///< the actual keys
    p<std::array<ValueType, M>> values;  ///< the actual values
    pptr<LeafNode> nextLeaf;             ///< pointer to the subsequent sibling
    pptr<LeafNode> prevLeaf;             ///< pointer to the preceeding sibling
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct alignas(64) BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode() : numKeys(0) {}

    unsigned int numKeys;             ///< the number of currently stored keys
    std::array<KeyType, N> keys;      ///< the actual keys
    std::array<Node, N + 1> children; ///< pointers to child nodes (BranchNode or LeafNode)
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

  void deleteLeafNode(pptr<LeafNode> node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] { delete_persistent<LeafNode>(node); });
  }

  /**
   * Create a new empty branch node
   */
  BranchNode *newBranchNode() {
    return new BranchNode();
  }

  void deleteBranchNode(BranchNode *node) {
    delete node;
  }

  /**
   * A structure for passing information about a node split to the caller.
   */
  struct SplitInfo {
    KeyType key;     ///< the key at which the node was split
    Node leftChild;  ///< the resulting lhs child node
    Node rightChild; ///< the resulting rhs child node
  };

  unsigned int depth;            /**< the depth of the tree, i.e. the number of levels
                                      (0 => rootNode is LeafNode) */

  Node rootNode;     ///< pointer to the root node
  pptr<LeafNode> leafList;       /**< Pointer to the leaf at the most left position.
                                      Neccessary for recovery */

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
      while (d-- > 0) node = node.branch->children[0];
      currentNode = node.leaf;
      currentPosition = 0;
    }

    iterator& operator++() {
      if (currentPosition >= currentNode->numKeys-1) {
        currentNode = currentNode->nextLeaf;
        currentPosition = 0;
        if (currentNode == nullptr) return *this;
      } else ++currentPosition;
      return *this;
    }

    iterator operator++(int) {iterator retval = *this; ++(*this); return retval;}

    bool operator==(iterator other) const {
      return (currentNode == other.currentNode &&
        currentPosition == other.currentPosition);}

    bool operator!=(iterator other) const {return !(*this == other);}

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
  PTree() {
    rootNode = newLeafNode();
    leafList = rootNode.leaf;
    depth = 0;
  }

  /**
   * Destructor for the tree. Should delete all allocated nodes.
   */
  ~PTree() {
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
        rootRef.keys[0] = splitInfo.key;
        rootRef.children[0] = splitInfo.leftChild;
        rootRef.children[1] = splitInfo.rightChild;
        ++rootRef.numKeys;
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
    const auto leaf = findLeafNode(key);
    const auto &leafRef = *leaf;
    auto pos = lookupPositionInLeafNode(leaf, key);
    if (pos < leafRef.numKeys && leafRef.keys.get_ro()[pos] == key) {
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
    auto pop = pmem::obj::pool_by_vptr(this);
    bool result;
    transaction::run(pop, [&] {
      if (depth == 0) {
        /// special case: the root node is a leaf node and
        /// there is no need to handle underflow
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
   * Recover the PTree by iterating over the LeafList and using the recoveryInsert method.
   */
  void recover(){
    LOG("Starting RECOVERY of PTree");
    pptr<LeafNode> currentLeaf=leafList;
    if(leafList == nullptr){
      LOG("No data to recover PTree");
    }
    if(leafList->nextLeaf == nullptr){
      /// The index has only one node, so the leaf node becomes the root node
      rootNode = leafList;
      depth = 0;
    } else {
      rootNode = newBranchNode();
      depth = 1;
      rootNode.branch->children[0] = currentLeaf;
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
		while ( d-- > 0) node = node.branch->children[0];
		auto leaf = node.leaf;
		auto &leafRef = *leaf;
		while (leaf != nullptr) {
			/// for each key-value pair call func
			for (auto i = 0u; i < leafRef.numKeys.get_ro(); i++) {
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
			/// for each key-value pair within the range call func
			const auto leafRef = *leaf;
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
   * Insert a (key, value) pair into the corresponding leaf node. It is the
   * responsibility of the caller to make sure that the node @c node is
   * the correct node. The key is inserted at the correct position.
   *
   * @param node the node where the key-value pair is inserted.
   * @param key the key to be inserted
   * @param val the value associated with the key
   * @param splitInfo information about a possible split of the node
   */
  bool insertInLeafNode(const pptr<LeafNode> node, const KeyType &key,
      const ValueType &val, SplitInfo *splitInfo) {
		auto &nodeRef = *node;
    bool split = false;
    auto pos = lookupPositionInLeafNode(node, key);

    if (pos < nodeRef.numKeys && nodeRef.keys.get_ro()[pos] == key) {
      /// handle insert of duplicates
      nodeRef.values.get_rw()[pos] = val;
      return false;
    }
    if (nodeRef.numKeys == M) {
      /// the node is full, so we must split it
      /// determine the split position
      unsigned int middle = (M + 1) / 2;
      /// move all entries behind this position to a new sibling node
      pptr<LeafNode> sibling = newLeafNode();
			auto &sibRef = *sibling;
      sibRef.numKeys = nodeRef.numKeys - middle;
      for (auto i = 0u; i < sibRef.numKeys; i++) {

        sibRef.keys.get_rw()[i] = nodeRef.keys.get_ro()[i + middle];
        sibRef.values.get_rw()[i] = nodeRef.values.get_ro()[i + middle];
      }
      nodeRef.numKeys = middle;

      /// insert the new entry
      if (pos < middle)
        insertInLeafNodeAtPosition(node, pos, key, val);
      else
        insertInLeafNodeAtPosition(sibling, pos - middle, key, val);

      /// setup the list of leaf nodes
      if (nodeRef.nextLeaf != nullptr) {
        sibRef.nextLeaf = nodeRef.nextLeaf;
        nodeRef.nextLeaf->prevLeaf = sibling;
      }
      nodeRef.nextLeaf = sibling;
      sibRef.prevLeaf = node;

      /// and inform the caller about the split
      split = true;
			auto &splitRef = *splitInfo;
      splitRef.leftChild = node;
      splitRef.rightChild = sibling;

      splitRef.key = sibRef.keys.get_ro()[0];
    } else {
      /// otherwise, we can simply insert the new entry at the given position
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
  void insertInLeafNodeAtPosition(const pptr<LeafNode> node, unsigned int pos,
      const KeyType &key, const ValueType &val) {
    assert(pos < M);
		auto &nodeRef = *node;
    assert(pos <= nodeRef.numKeys);
    assert(nodeRef.numKeys < M);
    /// we move all entries behind pos by one position
    for (unsigned int i = nodeRef.numKeys; i > pos; i--) {

      nodeRef.keys.get_rw()[i] = nodeRef.keys.get_ro()[i - 1];
      nodeRef.values.get_rw()[i] = nodeRef.values.get_ro()[i - 1];
    }
    /// and then insert the new entry at the given position

    nodeRef.keys.get_rw()[pos] = key;
    nodeRef.values.get_rw()[pos] = val;
    nodeRef.numKeys = nodeRef.numKeys + 1;
  }

  /**
   * Insert a (key, value) pair into the tree recursively by following the path
   * down to the leaf level starting at node @c node at depth @c depth.
   *
   * @param node the starting node for the insert
   * @param d the current depth of the tree (0 == leaf level)
   * @param key the key of the element
   * @param val the actual value corresponding to the key
   * @param splitInfo information about the split
   * @return true if a split was performed
   */
  bool insertInBranchNode(BranchNode *node, unsigned int d, const KeyType &key,
													const ValueType &val, SplitInfo *splitInfo) {
		auto &nodeRef = *node;
    SplitInfo childSplitInfo;
    bool split = false, hasSplit = false;

    auto pos = lookupPositionInBranchNode(node, key);
    if (d == 1) {
      //case #1: our children are leaf nodes
      auto child = nodeRef.children[pos].leaf;
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      /// case #2: our children are branch nodes
      auto child = nodeRef.children[pos].branch;
      hasSplit = insertInBranchNode(child, d - 1, key, val, &childSplitInfo);
    }
    if (hasSplit) {
      BranchNode *host = node;
      /// the child node was split, thus we have to add a new entry
      /// to our branch node
      if (nodeRef.numKeys == N) {
        splitBranchNode(node, childSplitInfo.key, splitInfo);
				const auto &splitRef = *splitInfo;
        host = (key < splitRef.key ? splitRef.leftChild : splitRef.rightChild).branch;
        split = true;
        pos = lookupPositionInBranchNode(host, key);
      }
			auto &hostRef = *host;
      if (pos < hostRef.numKeys) {
        /// if the child isn't inserted at the rightmost position
        /// then we have to make space for it
        hostRef.children[hostRef.numKeys + 1] = hostRef.children[hostRef.numKeys];
        for (auto i = hostRef.numKeys; i > pos; i--) {
          hostRef.children[i] = hostRef.children[i - 1];
          hostRef.keys[i] = hostRef.keys[i - 1];
        }
      }
      /// finally, add the new entry at the given position
      hostRef.keys[pos] = childSplitInfo.key;
      hostRef.children[pos] = childSplitInfo.leftChild;
      hostRef.children[pos + 1] = childSplitInfo.rightChild;
      ++hostRef.numKeys;
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
		auto &nodeRef = *node;
    /// we have an overflow at the branch node, let's split it
    /// determine the split position
    auto middle = (N + 1) / 2;
    /// adjust the middle based on the key we have to insert
    if (splitKey > nodeRef.keys[middle]) middle++;

    /// move all entries behind this position to a new sibling node
    BranchNode *sibling = newBranchNode();
		auto &sibRef = *sibling;
    sibRef.numKeys = nodeRef.numKeys - middle;
    for (auto i = 0u; i < sibRef.numKeys; i++) {

      sibRef.keys[i] = nodeRef.keys[middle + i];
      sibRef.children[i] = nodeRef.children[middle + i];
    }

    sibRef.children[sibRef.numKeys] = nodeRef.children[nodeRef.numKeys];
    nodeRef.numKeys = middle - 1;
		auto &splitRef = *splitInfo;
    splitRef.key = nodeRef.keys[middle - 1];
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
      auto n = node.branch;
      auto pos = lookupPositionInBranchNode(n, key);
      node = n->children[pos];
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
  unsigned int lookupPositionInLeafNode(const pptr<LeafNode> &node, const KeyType &key) const {
    auto pos = 0u;
		const auto &nodeRef = *node;
    const auto num = nodeRef.numKeys.get_ro();
    for (; pos < num && nodeRef.keys.get_ro()[pos] < key; pos++);
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
  unsigned int lookupPositionInBranchNode(BranchNode * const node, const KeyType &key) const {
    auto pos = 0;
		const auto &nodeRef = *node;
    const auto num = nodeRef.numKeys;
    for (; pos < num && nodeRef.keys[pos] <= key; pos++) {
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
  bool eraseFromLeafNode(const pptr<LeafNode> &node, const KeyType &key) {
    auto pos = lookupPositionInLeafNode(node, key);
		auto &nodeRef = *node;
    if (nodeRef.keys.get_ro()[pos] == key) {
      for (auto i = pos; i < nodeRef.numKeys - 1; i++) {
        nodeRef.keys.get_rw()[i] = nodeRef.keys.get_ro()[i + 1];
        nodeRef.values.get_rw()[i] = nodeRef.values.get_ro()[i + 1];
      }
      nodeRef.numKeys.get_rw() = nodeRef.numKeys.get_ro() - 1;
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
  bool eraseFromBranchNode(BranchNode * const node, unsigned int d, const KeyType &key) {
    assert(d >= 1);
		auto &nodeRef = *node;
    bool deleted = false;
    /// try to find the branch
    auto pos = lookupPositionInBranchNode(node, key);
    if (d == 1) {
      auto leaf = nodeRef.children[pos].leaf;
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      constexpr auto middle = (M + 1) / 2;
      if (leaf->numKeys < middle) {
        /// handle underflow
        underflowAtLeafLevel(node, pos, leaf);
      }
    } else {
      auto child = nodeRef.children[pos].branch;
      deleted = eraseFromBranchNode(child, d - 1, key);

      pos = lookupPositionInBranchNode(node, key);
      unsigned int middle = (N + 1) / 2;
      if (child->numKeys < middle) {
        /// handle underflow
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth && nodeRef.numKeys == 0) {
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
   * @param pos the position of the child node @leaf in the @c children array of the branch node
   * @param leaf the node at which the underflow occured
   */
  void underflowAtLeafLevel(BranchNode * const node, unsigned int pos, const pptr<LeafNode> &leaf) {
			auto &nodeRef = *node;
			auto &leafRef = *leaf;
      assert(pos <= nodeRef.numKeys);
      constexpr auto middle = (M + 1) / 2;
      /// 1. we check whether we can rebalance with one of the siblings
      /// but only if both nodes have the same direct parent
      if (pos > 0 && leafRef.prevLeaf->numKeys > middle) {
        /// we have a sibling at the left for rebalancing the keys
        balanceLeafNodes(leafRef.prevLeaf, leaf);

        nodeRef.keys[pos - 1] = leafRef.keys.get_ro()[0];
      } else if (pos < nodeRef.numKeys && leafRef.nextLeaf->numKeys > middle) {
        /// we have a sibling at the right for rebalancing the keys
        balanceLeafNodes(leafRef.nextLeaf, leaf);

        nodeRef.keys[pos] = leafRef.nextLeaf->keys.get_ro()[0];
      } else {
        /// 2. if this fails we have to merge two leaf nodes
        /// but only if both nodes have the same direct parent
        pptr<LeafNode> survivor = nullptr;
        if (pos > 0 && leafRef.prevLeaf->numKeys <= middle) {
          survivor = mergeLeafNodes(leafRef.prevLeaf, leaf);
          deleteLeafNode(leaf);
        } else if (pos < nodeRef.numKeys && leafRef.nextLeaf->numKeys <= middle) {
          /// because we update the pointers in mergeLeafNodes
          /// we keep it here
          auto l = leafRef.nextLeaf;
          survivor = mergeLeafNodes(leaf, leafRef.nextLeaf);
          deleteLeafNode(l);
        } else {
          /// this shouldn't happen?!
          assert(false);
        }
        if (nodeRef.numKeys > 1) {
          if (pos > 0) pos--;
          /// just remove the child node from the current branch node
          for (auto i = pos; i < nodeRef.numKeys - 1; i++) {
            nodeRef.keys[i] = nodeRef.keys[i + 1];
            nodeRef.children[i + 1] = nodeRef.children[i + 2];
          }
          nodeRef.children[pos] = survivor;
          --nodeRef.numKeys;
        } else {
          /// This is a special case that happens only if the current node is the root node.
					/// Now, we have to replace the branch root node by a leaf node.
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
  BranchNode* underflowAtBranchLevel(BranchNode * const node, unsigned int pos,
      															 BranchNode * const child) {
    assert(node != nullptr);
    assert(child != nullptr);
		auto &nodeRef = *node;
    auto newChild = child;
    unsigned int middle = (N + 1) / 2;

    /// 1. we check whether we can rebalance with one of the siblings
    if (pos > 0 &&
        nodeRef.children[pos - 1].branch->numKeys >middle) {
      /// we have a sibling at the left for rebalancing the keys
      BranchNode *sibling = nodeRef.children[pos - 1].branch;
      balanceBranchNodes(sibling, child, node, pos - 1);
      /// nodeRef.keys.get_rw()[pos] = child->keys.get_ro()[0];
      return newChild;
    } else if (pos < nodeRef.numKeys && nodeRef.children[pos + 1].branch->numKeys > middle) {
      /// we have a sibling at the right for rebalancing the keys
      auto sibling = nodeRef.children[pos + 1].branch;
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    } else {
      /// 2. if this fails we have to merge two branch nodes
      BranchNode *lSibling = nullptr, *rSibling = nullptr;
      unsigned int prevKeys = 0, nextKeys = 0;
      if (pos > 0) {
        lSibling = nodeRef.children[pos - 1].branch;
        prevKeys = lSibling->numKeys;
      }
      if (pos < nodeRef.numKeys) {
        rSibling = nodeRef.children[pos + 1].branch;
        nextKeys = rSibling->numKeys;
      }
      BranchNode *witnessNode = nullptr;
      auto ppos = pos;
      if (prevKeys > 0) {
        mergeBranchNodes(lSibling, nodeRef.keys[pos - 1], child);
        ppos = pos - 1;
        witnessNode = child;
        newChild = lSibling;
        /// pos -= 1;
      } else if (nextKeys > 0) {
        mergeBranchNodes(child, nodeRef.keys[pos], rSibling);
        witnessNode = rSibling;
      } else
        /// shouldn't happen
        assert(false);

      /// remove nodeRef.keys.get_ro()[pos] from node
      for (auto i = ppos; i < nodeRef.numKeys - 1; i++) {
        nodeRef.keys[i] = nodeRef.keys[i + 1];
      }
      if (pos == 0) pos++;
      for (auto i = pos; i < nodeRef.numKeys; i++) {
        if (i + 1 <= nodeRef.numKeys) {
          nodeRef.children[i] = nodeRef.children[i + 1];
        }
      }
      nodeRef.numKeys--;
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
  void balanceLeafNodes(const pptr<LeafNode> &donor, const pptr<LeafNode> &receiver) {
  	auto &donorRef = *donor;
    auto &receiverRef = *receiver;
    assert(donorRef.numKeys > receiverRef.numKeys);
    unsigned int balancedNum = (donorRef.numKeys + receiverRef.numKeys) / 2;
    unsigned int toMove = donorRef.numKeys - balancedNum;
    if (toMove == 0) return;

    if (donorRef.keys.get_ro()[0] < receiverRef.keys.get_ro()[0]) {
      /// move from one node to a node with larger keys
      unsigned int i = 0, j = 0;
      for (i = receiverRef.numKeys; i > 0; i--) {
        /// reserve space on receiver side

        receiverRef.keys.get_rw()[i + toMove - 1] = receiverRef.keys.get_ro()[i - 1];
        receiverRef.values.get_rw()[i + toMove - 1] = receiverRef.values.get_ro()[i - 1];
      }
      /// move toMove keys/values from donor to receiver
      for (i = balancedNum; i < donorRef.numKeys; i++, j++) {

        receiverRef.keys.get_rw()[j] = donorRef.keys.get_ro()[i];
        receiverRef.values.get_rw()[j] = donorRef.values.get_ro()[i];
        receiverRef.numKeys.get_rw() = receiverRef.numKeys.get_ro() + 1;
      }
    } else {
      /// mode from one node to a node with smaller keys
      unsigned int i = 0;
      /// move toMove keys/values from donor to receiver
      for (i = 0; i < toMove; i++) {

        receiverRef.keys.get_rw()[receiverRef.numKeys] = donorRef.keys.get_ro()[i];
        receiverRef.values.get_rw()[receiverRef.numKeys] = donorRef.values.get_ro()[i];
        receiverRef.numKeys.get_rw() = receiverRef.numKeys.get_ro() + 1;
      }
      /// on donor node move all keys and values to the left
      for (i = 0; i < donorRef.numKeys - toMove; i++) {

        donorRef.keys.get_rw()[i] = donorRef.keys.get_ro()[toMove + i];
        donorRef.values.get_rw()[i] = donorRef.values.get_ro()[toMove + i];
      }
    }
    donorRef.numKeys.get_rw() = donorRef.numKeys.get_ro() - toMove;
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
  void balanceBranchNodes(BranchNode * const donor, BranchNode * const receiver,
													BranchNode * const parent, const unsigned int pos) {
  	auto &donorRef = *donor;
    auto &receiverRef = *receiver;
    auto &parentRef = *parent;
    assert(donorRef.numKeys > receiverRef.numKeys);

    unsigned int balancedNum = (donorRef.numKeys + receiverRef.numKeys) / 2;
    unsigned int toMove = donorRef.numKeys - balancedNum;
    if (toMove == 0) return;

    if (donorRef.keys[0] < receiverRef.keys[0]) {
      /// move from one node to a node with larger keys
      unsigned int i = 0;
      /// 1. make room
      receiverRef.children[receiverRef.numKeys + toMove] =
        receiverRef.children[receiverRef.numKeys];
      for (i = receiverRef.numKeys; i > 0; i--) {
        /// reserve space on receiver side
        receiverRef.keys[i + toMove - 1] = receiverRef.keys[i - 1];
        receiverRef.children[i + toMove - 1] = receiverRef.children[i - 1];
      }
      /// 2. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++)
        receiverRef.children[i] = donorRef.children[donorRef.numKeys - toMove + 1 + i];
      for (i = 0; i < toMove - 1; i++)
        receiverRef.keys[i] = donorRef.keys[donorRef.numKeys - toMove + 1 + i];

      receiverRef.keys[toMove - 1] = parentRef.keys[pos];
      assert(parentRef.numKeys > pos);
      parentRef.keys[pos] = donorRef.keys[donorRef.numKeys - toMove];
      receiverRef.numKeys += toMove;
    } else {
      /// mode from one node to a node with smaller keys
      unsigned int i = 0, n = receiverRef.numKeys;

      /// 1. move toMove keys/children from donor to receiver
      for (i = 0; i < toMove; i++) {

        receiverRef.children[n + 1 + i] = donorRef.children[i];
        receiverRef.keys[n + 1 + i] = donorRef.keys[i];
      }
      /// 2. we have to move via the parent node: take the key from
      /// parentRef.keys.get_ro()[pos]

      receiverRef.keys[n] = parentRef.keys[pos];
      receiverRef.numKeys += toMove;
      KeyType key = donorRef.keys[toMove - 1];

      /// 3. on donor node move all keys and values to the left
      for (i = 0; i < donorRef.numKeys - toMove; i++) {

        donorRef.keys[i] = donorRef.keys[toMove + i];
        donorRef.children[i] = donorRef.children[toMove + i];
      }

      donorRef.children[donorRef.numKeys - toMove] =
        donorRef.children[donorRef.numKeys];
      /// and replace this key by donorRef.keys.get_ro()[0]
      assert(parentRef.numKeys > pos);
      parentRef.keys[pos] = key;
    }
    donorRef.numKeys -= toMove;
  }

  /**
   * Merge two leaf nodes by moving all elements from @c node2 to
   * @c node1.
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
    assert(node1Ref.numKeys + node2Ref.numKeys <= M);

    /// we move all keys/values from node2 to node1
    for (auto i = 0u; i < node2Ref.numKeys; i++) {

      node1Ref.keys.get_rw()[node1Ref.numKeys + i] = node2Ref.keys.get_ro()[i];
      node1Ref.values.get_rw()[node1Ref.numKeys + i] = node2Ref.values.get_ro()[i];
    }
    node1Ref.numKeys.get_rw() = node1Ref.numKeys.get_ro() + node2Ref.numKeys.get_ro();
    node1Ref.nextLeaf = node2Ref.nextLeaf;
    node2Ref.numKeys = 0;
    if (node2Ref.nextLeaf != nullptr) node2Ref.nextLeaf->prevLeaf = node1;
    return node1;
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
  void mergeBranchNodes(BranchNode * const sibling, const KeyType &key,
												const BranchNode * const node) {
    assert(sibling != nullptr);
    assert(node != nullptr);
		auto &sibRef = *sibling;
		auto &nodeRef = *node;
    assert(key <= nodeRef.keys[0]);
    assert(sibRef.keys[sibRef.numKeys - 1] < key);

    sibRef.keys[sibRef.numKeys] = key;
    sibRef.children[sibRef.numKeys + 1] = nodeRef.children[0];
    for (auto i = 0u; i < nodeRef.numKeys; i++) {

      sibRef.keys[sibRef.numKeys + i + 1] = nodeRef.keys[i];
      sibRef.children[sibRef.numKeys + i + 2] = nodeRef.children[i + 1];
    }
    sibRef.numKeys += nodeRef.numKeys + 1;
  }

  /**
   * Insert a leaf node into the tree for recovery
   * @param leaf the leaf to insert
   */
  void recoveryInsert(const pptr<LeafNode> &leaf){
    assert(depth>0);
    assert(leaf!= nullptr);
    
		SplitInfo splitInfo;
    auto hassplit = recoveryInsertInBranchNode(rootNode.branch, depth, leaf, &splitInfo);

    /// Check for split
    if(hassplit){
      /// The root node was splitted
      auto newRoot=newBranchNode();
			auto &newRootRef = *newRoot;
      newRootRef.keys[0] = splitInfo.key;
      newRootRef.children[0] = splitInfo.leftChild;
      newRootRef.children[1] = splitInfo.rightChild;
      newRootRef.numKeys = 1;
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
  bool recoveryInsertInBranchNode(BranchNode * const node, const int curr_depth,
																	const pptr<LeafNode> &leaf, SplitInfo *splitInfo) {
		auto &nodeRef = *node;
		auto &leafRef = *leaf;
		auto &splitRef = *splitInfo;
    bool hassplit = false;
    SplitInfo childSplitInfo;
    if (curr_depth == 1) {
			if (nodeRef.numKeys == N) {
				/* we have to insert a new right child, as the keys in the leafList are sorted */
				auto newNode = newBranchNode();
				newNode->children[0] = leaf;
				splitRef.key = leafRef.keys.get_ro()[findMinKeyAtLeafNode(leaf)];
				splitRef.leftChild = node;
				splitRef.rightChild = newNode;
				return true;
			} else {
				nodeRef.keys[nodeRef.numKeys] = leafRef.keys.get_ro()[findMinKeyAtLeafNode(leaf)];
				++nodeRef.numKeys;
				nodeRef.children[nodeRef.numKeys] = leaf;
				return false;
			}
		} else{
      hassplit=recoveryInsertInBranchNode(nodeRef.children[nodeRef.numKeys].branch,curr_depth-1,leaf,&childSplitInfo);
    }

    //Check for split
    if (hassplit) {
      if (nodeRef.numKeys == N) {
        //We have to split the branch node
        splitBranchNode(node, childSplitInfo.key,splitInfo);
        auto newNode = splitRef.rightChild.branch;
				auto &newNodeRef = *newNode;
        //Insert the new child into the new branch at the rightmost position
        newNodeRef.keys[newNodeRef.numKeys]=childSplitInfo.key;
        newNodeRef.children[newNodeRef.numKeys]=childSplitInfo.leftChild;
        newNodeRef.numKeys++;
        newNodeRef.children[newNodeRef.numKeys]=childSplitInfo.rightChild;
        return true;
      }
      else{
        nodeRef.keys[nodeRef.numKeys]=childSplitInfo.key;
        nodeRef.numKeys++;
        nodeRef.children[nodeRef.numKeys]=childSplitInfo.rightChild;
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
		auto &nodeRef = *node;
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << d << " BN { ["<<node<<"] ";
    for (auto k = 0u; k < nodeRef.numKeys; k++) {
      if (k > 0) std::cout << ", ";

      std::cout << nodeRef.keys[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= nodeRef.numKeys; k++) {
      if (d + 1 < depth) {
        auto child = nodeRef.children[k].branch;
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto child = nodeRef.children[k].leaf;
        if (child != nullptr) printLeafNode(d + 1, child);
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
    auto &nodeRef = *node;
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << "[" << std::hex << node << std::dec << " : ";
    for (auto i = 0u; i < nodeRef.numKeys; i++) {
      if (i > 0) std::cout << ", ";

      std::cout << "{" << nodeRef.keys.get_ro()[i] << "}";
    }
    std::cout << "]" << std::endl;
  }

  void printLeafList(){
    auto curr_node = leafList;
    while (curr_node != nullptr) {
      printLeafNode(0, curr_node);
      curr_node = curr_node->nextLeaf;
    }
  }
};//end class PTree
}//end namespace dbis::fptree

#endif
