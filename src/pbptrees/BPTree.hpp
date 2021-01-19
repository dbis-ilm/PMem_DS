#ifndef BPTree_hpp_
#define BPTree_hpp_

#include <array>
#include <cassert>
#include <functional>
#include <iostream>


#define BRANCH_PADDING  0
#define LEAF_PADDING    0

#define USE_MEM_POOL        1
#define USE_NODE_ALIGNMENT  1

#if USE_MEM_POOL
#include <boost/pool/object_pool.hpp>
#endif

namespace dbis::pbptrees {
/**
 * An in-memory implementation of a B+ tree.
 *
 * @tparam KeyType the data type of the key
 * @tparam ValueType the data type of the values associated with the key
 * @tparam N the maximum number of keys on a branch node
 * @tparam M the maximum number of keys on a leaf node
 */
template <typename KeyType, typename ValueType, int N, int M, int NODE_ALIGNMENT = 64>
class BPTree {
  // we need at least two keys on a branch node to be able to split
  static_assert(N > 2, "number of branch keys has to be >2.");
  // we need at least one key on a leaf node
  static_assert(M > 0, "number of leaf keys should be >0.");
#ifndef UNIT_TESTS
 private:
#else
 public:
#endif

#if USE_NODE_ALIGNMENT
  template <unsigned ALIGNMENT>
  struct AlignedMemoryAllocator {
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    static char *malloc(const size_type bytes) {
      void *result;
      if (posix_memalign(&result, ALIGNMENT, bytes) != 0) {
        result = 0;
      }
      return reinterpret_cast<char *>(result);
    }
    static void free(char *const block) { std::free(block); }
  };
#endif

  // Forward declarations
  struct LeafNode;
  struct BranchNode;

  /**
   * A structure for passing information about a node split to
   * the caller.
   */
  struct SplitInfo {
    KeyType key;       //< the key at which the node was split
    void *leftChild;   //< the resulting lhs child node
    void *rightChild;  //< the resulting rhs child node
  };

  // Must be declared before rootNode!
#if USE_MEM_POOL
  boost::object_pool<BranchNode
 #if USE_NODE_ALIGNMENT
    , AlignedMemoryAllocator<NODE_ALIGNMENT>
 #endif
    > branchPool;
  boost::object_pool<LeafNode
 #if USE_NODE_ALIGNMENT
    , AlignedMemoryAllocator<NODE_ALIGNMENT>
 #endif
    > leafPool;
 #endif

  unsigned int depth;  //< the depth of the tree, i.e. the number of levels (0 => rootNode is LeafNode)
  void *rootNode;  //< pointer to the root node (an instance of @c LeafNode or
                   //< @c BranchNode). This pointer is never @c nullptr.

 public:

/**
* Iterator for iterating over the leaf nodes
*/
class iterator {
  LeafNode* currentNode;
  std::size_t currentPosition;

  public:
  iterator() : currentNode(nullptr), currentPosition(0) {}
  iterator(void *root, std::size_t d) {
    /// traverse to left-most key
    auto node = root;
    while (d-- > 0) {
      node = reinterpret_cast<BranchNode *>(node)->children[0];
    }
    currentNode = reinterpret_cast<LeafNode *>(node);
    currentPosition = 0;
  }
    iterator& operator++() {
      const auto &nodeRef = *currentNode;
      if (currentPosition >= nodeRef.numKeys - 1) {
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
      return std::make_pair(nodeRef.keys[currentPosition],
                            nodeRef.values[currentPosition]);
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
  BPTree() : depth(0), rootNode(newLeafNode()) {
//    std::cout << "BRANCHKEYS/LEAFKEYS: " << N << '/' << M << '\n';
//    std::cout << "sizeof(BranchNode) = " << sizeof(BranchNode)
//    << ", sizeof(LeafNode) = " << sizeof(LeafNode) << std::endl;
  }

  /**
   * Destructor for the B+ tree. Should delete all allocated nodes.
   */
  ~BPTree() {
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
    SplitInfo splitInfo;

    bool wasSplit = false;
    if (depth == 0) {
      // the root node is a leaf node
      auto n = reinterpret_cast<LeafNode *>(rootNode);
      wasSplit = insertInLeafNode(n, key, val, &splitInfo);
    } else {
      // the root node is a branch node
      auto n = reinterpret_cast<BranchNode *>(rootNode);
      wasSplit = insertInBranchNode(n, depth, key, val, &splitInfo);
    }
    if (wasSplit) {
      // we had an overflow in the node and therefore the node is split
      auto root = newBranchNode();
      root->keys[0] = splitInfo.key;
      root->children[0] = splitInfo.leftChild;
      root->children[1] = splitInfo.rightChild;
      root->numKeys = 1;
      rootNode = root;
      ++depth;
    }
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

    auto leafNode = findLeafNode(key);
    auto pos = lookupPositionInLeafNode(leafNode, key);
    if (pos < leafNode->numKeys && leafNode->keys[pos] == key) {
      // we found it!
      *val = leafNode->values[pos];
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
    if (depth == 0) {
      /// special case: the root node is a leaf node and there is no need to handle underflow
      LeafNode *node = reinterpret_cast<LeafNode *>(rootNode);
      assert(node != nullptr);
      return eraseFromLeafNode(node, key);
    } else {
      BranchNode *node = reinterpret_cast<BranchNode *>(rootNode);
      assert(node != nullptr);
      return eraseFromBranchNode(node, depth, key);
    }
  }

  /**
   * Print the structure and content of the B+ tree to stdout.
   */
  void print() const {
    if (depth == 0) {
      // the trivial case
      printLeafNode(0, reinterpret_cast<LeafNode *>(rootNode));
    } else {
      void *n = rootNode;
      printBranchNode(0u, reinterpret_cast<BranchNode *>(n));
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
    void *node = rootNode;
    auto d = depth;
    while (d-- > 0) {
      // as long as we aren't at the leaf level we follow the path down
      BranchNode *n = reinterpret_cast<BranchNode *>(node);
      node = n->children[0];
    }
    auto leaf = reinterpret_cast<LeafNode *>(node);
    while (leaf != nullptr) {
      // for each key-value pair call func
      for (auto i = 0u; i < leaf->numKeys; i++) {
        auto &key = leaf->keys[i];
        auto &val = leaf->values[i];
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
        auto &key = leaf->keys[i];
        if (key > maxKey) return;
        if (key < minKey) continue;

        auto &val = leaf->values[i];
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
  bool eraseFromLeafNode(LeafNode *node, const KeyType &key) {
    auto pos = lookupPositionInLeafNode(node, key);
    if (node->keys[pos] == key) {
      for (auto i = pos; i < node->numKeys - 1; i++) {
        node->keys[i] = node->keys[i + 1];
        node->values[i] = node->values[i + 1];
      }
      --node->numKeys;
      return true;
    }
    return false;
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
  void underflowAtLeafLevel(BranchNode *node, unsigned int pos, LeafNode *leaf) {
    assert(pos <= node->numKeys);

    constexpr auto middle = (M + 1) / 2;
    // 1. we check whether we can rebalance with one of the siblings
    // but only if both nodes have the same direct parent
    if (pos > 0 && leaf->prevLeaf->numKeys > middle) {
      // we have a sibling at the left for rebalancing the keys
      balanceLeafNodes(leaf->prevLeaf, leaf);
      node->keys[pos-1] = leaf->keys[0];
    } else if (pos < node->numKeys && leaf->nextLeaf->numKeys > middle) {
      // we have a sibling at the right for rebalancing the keys
      balanceLeafNodes(leaf->nextLeaf, leaf);
      node->keys[pos] = leaf->nextLeaf->keys[0];
    } else {
      // 2. if this fails we have to merge two leaf nodes
      // but only if both nodes have the same direct parent
      LeafNode *survivor = nullptr;
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
          node->keys[i] = node->keys[i + 1];
          node->children[i + 1] = node->children[i + 2];
        }
        node->children[pos] = survivor;
        node->numKeys--;
      } else {
        // This is a special case that happens only if
        // the current node is the root node. Now, we have
        // to replace the branch root node by a leaf node.
        rootNode = survivor;
        depth--;
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
  LeafNode *mergeLeafNodes(LeafNode *node1, LeafNode *node2) {
    assert(node1 != nullptr);
    assert(node2 != nullptr);
    assert(node1->numKeys + node2->numKeys <= M);

    // we move all keys/values from node2 to node1
    for (auto i = 0u; i < node2->numKeys; i++) {
      node1->keys[node1->numKeys + i] = node2->keys[i];
      node1->values[node1->numKeys + i] = node2->values[i];
    }
    node1->numKeys += node2->numKeys;
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
  void balanceLeafNodes(LeafNode *donor, LeafNode *receiver) {
    assert(donor->numKeys > receiver->numKeys);

    unsigned int balancedNum = (donor->numKeys + receiver->numKeys) / 2;
    unsigned int toMove = donor->numKeys - balancedNum;
    if (toMove == 0) return;

    if (donor->keys[0] < receiver->keys[0]) {
      // move from one node to a node with larger keys
      unsigned int i = 0, j = 0;
      for (i = receiver->numKeys; i > 0; i--) {
        // reserve space on receiver side
        receiver->keys[i + toMove - 1] = receiver->keys[i - 1];
        receiver->values[i + toMove - 1] = receiver->values[i - 1];
      }
      // move toMove keys/values from donor to receiver
      for (i = balancedNum; i < donor->numKeys; i++, j++) {
        receiver->keys[j] = donor->keys[i];
        receiver->values[j] = donor->values[i];
        receiver->numKeys++;
      }
    } else {
      // mode from one node to a node with smaller keys
      unsigned int i = 0;
      // move toMove keys/values from donor to receiver
      for (i = 0; i < toMove; i++) {
        receiver->keys[receiver->numKeys] = donor->keys[i];
        receiver->values[receiver->numKeys] = donor->values[i];
        receiver->numKeys++;
      }
      // on donor node move all keys and values to the left
      for (i = 0; i < donor->numKeys - toMove; i++) {
        donor->keys[i] = donor->keys[toMove + i];
        donor->values[i] = donor->values[toMove + i];
      }
    }
    donor->numKeys -= toMove;
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
  bool eraseFromBranchNode(BranchNode *node, unsigned int d, const KeyType &key) {
    assert(d >= 1);
    bool deleted = false;
    // try to find the branch
    auto pos = lookupPositionInBranchNode(node, key);
    void *n = node->children[pos];
    if (d == 1) {
      // the next level is the leaf level
      LeafNode *leaf = reinterpret_cast<LeafNode *>(n);
      assert(leaf != nullptr);
      deleted = eraseFromLeafNode(leaf, key);
      unsigned int middle = (M + 1) / 2;
      if (leaf->numKeys < middle) {
        // handle underflow
        underflowAtLeafLevel(node, pos, leaf);
      }
    } else {
      BranchNode *child = reinterpret_cast<BranchNode *>(n);
      deleted = eraseFromBranchNode(child, d - 1, key);

      pos = lookupPositionInBranchNode(node, key);
      unsigned int middle = (N + 1) / 2;
      if (child->numKeys < middle) {
        // handle underflow
        child = underflowAtBranchLevel(node, pos, child);
        if (d == depth && node->numKeys == 0) {
          // special case: the root node is empty now
          rootNode = child;
          depth--;
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

    BranchNode *newChild = child;
    unsigned int middle = (N + 1) / 2;
    // 1. we check whether we can rebalance with one of the siblings
    if (pos > 0 &&
        reinterpret_cast<BranchNode *>(node->children[pos - 1])->numKeys >
            middle) {
      // we have a sibling at the left for rebalancing the keys
      BranchNode *sibling =
          reinterpret_cast<BranchNode *>(node->children[pos - 1]);
      balanceBranchNodes(sibling, child, node, pos - 1);
      // node->keys[pos] = child->keys[0];
      return newChild;
    } else if (pos<node->numKeys &&reinterpret_cast<BranchNode *>(
                           node->children[pos + 1])
                       ->numKeys>
                   middle) {
      // we have a sibling at the right for rebalancing the keys
      BranchNode *sibling =
          reinterpret_cast<BranchNode *>(node->children[pos + 1]);
      balanceBranchNodes(sibling, child, node, pos);
      return newChild;
    } else {
      // 2. if this fails we have to merge two branch nodes
      BranchNode *lSibling = nullptr, *rSibling = nullptr;
      unsigned int prevKeys = 0, nextKeys = 0;

      if (pos > 0) {
        lSibling = reinterpret_cast<BranchNode *>(node->children[pos - 1]);
        prevKeys = lSibling->numKeys;
      }
      if (pos < node->numKeys) {
        rSibling = reinterpret_cast<BranchNode *>(node->children[pos + 1]);
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

      // remove node->keys[pos] from node
      for (auto i = ppos; i < node->numKeys - 1; i++) {
        node->keys[i] = node->keys[i + 1];
      }
      if (pos == 0) pos++;
      for (auto i = pos; i < node->numKeys; i++) {
        if (i + 1 <= node->numKeys)
          node->children[i] = node->children[i + 1];
      }
      node->numKeys--;

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
      // parent->keys[pos]
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
      // and replace this key by donor->keys[0]
      assert(parent->numKeys > pos);
      parent->keys[pos] = key;
    }
    donor->numKeys -= toMove;
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
  void printBranchNode(unsigned int d, BranchNode *node) const {
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << d << " { ";
    for (auto k = 0u; k < node->numKeys; k++) {
      if (k > 0) std::cout << ", ";
      std::cout << node->keys[k];
    }
    std::cout << " }" << std::endl;
    for (auto k = 0u; k <= node->numKeys; k++) {
      if (d + 1 < depth) {
        auto child = reinterpret_cast<BranchNode *>(node->children[k]);
        if (child != nullptr) printBranchNode(d + 1, child);
      } else {
        auto leaf = reinterpret_cast<LeafNode *>(node->children[k]);
        printLeafNode(d + 1, leaf);
      }
    }
  }

  /**
   * Print the keys of the given branch node @c node to standard
   * output.
   *
   * @param node the tree node to print
   */
  void printBranchNodeKeys(BranchNode *node) const {
    std::cout << "{ ";
    for (auto k = 0u; k < node->numKeys; k++) {
      if (k > 0) std::cout << ", ";
      std::cout << node->keys[k];
    }
    std::cout << " }" << std::endl;
  }

  /**
   * Print the given leaf node @c node to standard output.
   *
   * @param d the current depth used for indention
   * @param node the tree node to print
   */
  void printLeafNode(unsigned int d, LeafNode *node) const {
    for (auto i = 0u; i < d; i++) std::cout << "  ";
    std::cout << "[" << std::hex << node << std::dec << " : ";
    for (auto i = 0u; i < node->numKeys; i++) {
      if (i > 0) std::cout << ", ";
      std::cout << "{" << node->keys[i] << " -> " << node->values[i] << "}";
    }
    std::cout << "]" << std::endl;
  }

  /* ---------------------------------------------------------------------- */
  /*                                   INSERT                               */
  /* ---------------------------------------------------------------------- */

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
  bool insertInLeafNode(LeafNode *node, const KeyType &key,
                        const ValueType &val, SplitInfo *splitInfo) {
    bool split = false;
    auto pos = lookupPositionInLeafNode(node, key);
    if (pos < node->numKeys && node->keys[pos] == key) {
      // handle insert of duplicates
      node->values[pos] = val;
      return false;
    }
    if (node->numKeys == M) {
      // the node is full, so we must split it
      // determine the split position
      unsigned int middle = (M + 1) / 2;
      // move all entries behind this position to a new sibling node
      LeafNode *sibling = newLeafNode();
      sibling->numKeys = node->numKeys - middle;
      for (auto i = 0u; i < sibling->numKeys; i++) {
        sibling->keys[i] = node->keys[i + middle];
        sibling->values[i] = node->values[i + middle];
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
      splitInfo->key = sibling->keys[0];
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
  void insertInLeafNodeAtPosition(LeafNode *node, unsigned int pos,
                                  const KeyType &key, const ValueType &val) {
    assert(pos < M);
    assert(pos <= node->numKeys);
    assert(node->numKeys < M);
    // we move all entries behind pos by one position
    for (unsigned int i = node->numKeys; i > pos; i--) {
      node->keys[i] = node->keys[i - 1];
      node->values[i] = node->values[i - 1];
    }
    // and then insert the new entry at the given position
    node->keys[pos] = key;
    node->values[pos] = val;
    node->numKeys++;
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
    if (depth - 1 == 0) {
      // case #1: our children are leaf nodes
      auto child = reinterpret_cast<LeafNode *>(node->children[pos]);
      hasSplit = insertInLeafNode(child, key, val, &childSplitInfo);
    } else {
      // case #2: our children are branch nodes
      auto child = reinterpret_cast<BranchNode *>(node->children[pos]);
      hasSplit = insertInBranchNode(child, depth - 1, key, val, &childSplitInfo);
    }
    if (hasSplit) {
      BranchNode *host = node;
      // the child node was split, thus we have to add a new entry
      // to our branch node

      if (node->numKeys == N) {
        splitBranchNode(node, childSplitInfo.key, splitInfo);

        host = reinterpret_cast<BranchNode *>(key < splitInfo->key
                                                 ? splitInfo->leftChild
                                                 : splitInfo->rightChild);
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
      host->numKeys++;
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
  LeafNode *findLeafNode(const KeyType &key) const {
    void *node = rootNode;
    auto d = depth;
    while (d-- > 0) {
      // as long as we aren't at the leaf level we follow the path down
      BranchNode *n = reinterpret_cast<BranchNode *>(node);
      auto pos = lookupPositionInBranchNode(n, key);
      node = n->children[pos];
    }
    return reinterpret_cast<LeafNode *>(node);
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
    for (; pos < num && node->keys[pos] <= key; pos++)
      ;
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
  unsigned int lookupPositionInLeafNode(LeafNode *node,
                                        const KeyType &key) const {
    // we perform a simple linear search, perhaps we should try a binary
    // search instead?
    unsigned int pos = 0;
    const unsigned int num = node->numKeys;
    for (; pos < num && node->keys[pos] < key; pos++)
      ;
    return pos;
  }

  /* ---------------------------------------------------------------------- */

  /**
   * Create a new empty leaf node
   */
  LeafNode *newLeafNode() {
#if USE_MEM_POOL
    return leafPool.construct();
#else
    return new LeafNode();
#endif
  }

  void deleteLeafNode(LeafNode *node) {
#if USE_MEM_POOL
    leafPool.destroy(node);
#else
    delete node;
#endif
  }

  /**
   * Create a new empty branch node
   */
  BranchNode *newBranchNode() {
#if USE_MEM_POOL
    return branchPool.construct();
#else
    return new BranchNode();
#endif
  }

  void deleteBranchNode(BranchNode *node) {
#if USE_MEM_POOL
    branchPool.destroy(node);
#else
    delete node;
#endif
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
   // ~LeafNode() { std::cout << "~LeafNode: " << std::hex << this <<
   //    std::endl; }

    size_t numKeys;             //< the number of currently stored keys
    LeafNode *nextLeaf;               //< pointer to the subsequent sibling
    LeafNode *prevLeaf;               //< pointer to the preceeding sibling
    std::array<KeyType, M> keys;      //< the actual keys
    std::array<ValueType, M> values;  //< the actual values
    unsigned char pad_[LEAF_PADDING];   //<
  };

  /**
   * A structure for representing an branch node (branch node) of a B+ tree.
   */
  struct alignas(64) BranchNode {
    /**
     * Constructor for creating a new empty branch node.
     */
    BranchNode() : numKeys(0) {}
    // ~BranchNode() { std::cout << "~BranchNode: " << std::hex << this << std::dec <<
     //   std::endl; }

    size_t numKeys;         //< the number of currently stored keys
    std::array<KeyType, N> keys;  //< the actual keys
    std::array<void *, N + 1> children;  //< pointers to child nodes (BranchNode or LeafNode)
    unsigned char pad_[BRANCH_PADDING];  //<
  };
};
}
#endif
