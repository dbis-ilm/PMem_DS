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

#ifndef WOP_SKIPLIST_HPP
#define WOP_SKIPLIST_HPP

#include <libpmemobj/ctl.h>

#include <algorithm> ///< std::copy
#include <array>
#include <cstdlib>   ///< size_t
#include <iostream>  ///< std:cout
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include "utils/Bitmap.hpp"
#include "utils/Random.hpp"
#include "utils/SearchFunctions.hpp"

namespace dbis::pskiplists {

using pmem::obj::allocation_flag;
using pmem::obj::array;
using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;
template <typename Object>
using pptr = persistent_ptr<Object>;

/**
 * A persistent memory implementation of a skip list with multiple records per node.
 *
 * @tparam KeyType the data type of the key
 * @tparam ValueType the data type of the values associated with the key
 * @tparam N the bucket size of a node
 * @tparam M the maximum number of levels
 */
template<typename KeyType, typename ValueType, size_t N, size_t M>
class woPSkiplist {

  static_assert(N > 0, "A node must have at least 1 element.");

  static constexpr auto MAX_KEY = std::numeric_limits<KeyType>::max();
  static constexpr auto MIN_KEY = std::numeric_limits<KeyType>::min();

#ifndef UNIT_TESTS
  private:
#else
  public:
#endif

  /**
   * A structure for representing a node of a skip list.
   */
  struct alignas(64) SkipNode {
    static constexpr auto BitsetSize = ((N + 63) / 64) * 8;  ///< number * size of words
    static constexpr auto ForwardSize = M * 16;
    static constexpr auto PaddingSize = (64 - (BitsetSize + ForwardSize + 2 * sizeof(KeyType) +
                                               sizeof(size_t)) % 64) % 64;

    p<dbis::Bitmap<N>> bits;          ///< a bitmap indicating empty/used slots
    p<KeyType> minKey;                ///< SMA min
    p<KeyType> maxKey;                ///< SMA max
    p<size_t> nodeLevel;              ///< the height of this node
    array<pptr<SkipNode>, M> forward; ///< the forward pointers to the following nodes
    char padding[PaddingSize];        ///< padding to align keys to a multiple of 64 bytes
    array<KeyType, N> keys;           ///< the actual keys
    array<ValueType, N> values;       ///< the actual values


    /**
     * Constructor for creating a new empty node.
     */
    SkipNode() :
      minKey(MAX_KEY), maxKey(MIN_KEY), nodeLevel(0) {}

    /**
     * Constructor for creating a new empty node on a given level.
     */
    SkipNode(size_t level) :
      minKey(MAX_KEY), maxKey(MIN_KEY), nodeLevel(level) {}

    /**
     * Constructor for creating a new empty node on a given level and min/max bounds
     */
    SkipNode(const KeyType &min, const KeyType &max, size_t level) :
      minKey(min), maxKey(max), nodeLevel(level) {}

    /**
     * Constructor for creating a new node with an initial key and value.
     */
    explicit SkipNode(const KeyType &_key, const ValueType &_value) :
      bits(1), minKey(_key), maxKey(_key), nodeLevel(0), keys(_key), values(_value) {}

    /**
     * Check if the node is full.
     *
     * @return true if the node is full
     */
    inline bool isFull() const {
      return bits.get_ro().all();
    }

    /**
     * Print this node's bounds and keys.
     */
    void printNode() const {
      std::cout << "\u001b[30;1m[" << minKey << "|" << maxKey << "](" << bits.get_ro().count()
                <<  "):\u001b[0m{";
      for (auto i = 0u; i < N; ++i) {
        if (bits.get_ro().test(i))
          std::cout << keys[i] << ",";
      }
      std::cout << "} --> ";
    }

  }; /// end struct SkipNode

  /* -------------------------------------------------------------------------------------------- */

  p<size_t> level;                   ///< the current number of levels (height)
  p<size_t> nodeCount;               ///< the current number of nodes
  dbis::Random* rnd;                 ///< volatile pointer to a random number generator
  pptr<SkipNode> head;               ///< pointer to the entry node of the skip list
  pobj_alloc_class_desc alloc_class; ///< Allocation class for correct alignment

  /**
   * Create a new node within the skip list.
   *
   * @param level the level of the node within the skip list
   * @param min the initial minimum bound
   * @param max the initial maximum bound
   * @param node[out] the pointer/reference to the newly allocated node
   */
  void newSkipNode(size_t level, const KeyType &min, const KeyType &max, pptr<SkipNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {
        node = make_persistent<SkipNode>(allocation_flag::class_id(alloc_class.class_id),
            level, min, max);
    });
  }

  /**
   * Create a new node within the skip list.
   *
   * @param node[out] the pointer/reference to the newly allocated node
   */
  void newSkipNode(pptr<SkipNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {
        node = make_persistent<SkipNode>(allocation_flag::class_id(alloc_class.class_id));
    });
  }

  /**
   * Initialize a skip list with a new head and initial empty node.
   */
  void initList() {
    newSkipNode(MIN_KEY, MIN_KEY, M, head);
    pptr<SkipNode> node;
    const auto height = getRandomLevel();
    newSkipNode(MAX_KEY, MIN_KEY, height, node);  ///< first empty node
    auto &headForward = head->forward;
    for (auto i = 0; i < height; ++i) {
      headForward[i] = node;
    }
    level.get_rw() = height;
  }

  /**
   * Search for the node containing the given key.
   *
   * @param key the key to be searched for
   * @return volatile reference of the node possibly containing the key
   */
  SkipNode* searchNode(const KeyType &key) const {
    auto pred = head.get();
    for (auto i = level.get_ro();; --i) {
      auto node = pred->forward[i].get();
      while (node) {
        if (key > node->maxKey.get_ro()) {
          pred = node;
          node = node->forward[i].get();
        }
        else if (key < node->minKey.get_ro()) break;
        else return node;
      }
      if (i == 0) break;
    }
    return nullptr;
  }

  /**
   * Search for a given key within the given node.
   *
   * @param node a pointer to the node to be searched
   * @param key the key to be searched for
   * @param[out] val the value associated to this key
   * @return the true if the key was found
   */
  bool searchInNode(const SkipNode * const node, const KeyType &key, ValueType &val) const {
    for (auto i = 0u; i < N; ++i) {
      if (node->bits.get_ro().test(i) && node->keys[i] == key) {
        val = node->values[i];
        return true;
      }
    }
    return false;
  }

  /**
   * Insert a new key-value pair into this node.
   *
   * @param key the key to be inserted
   * @param value the value to be inserted
   * @return true if split was necessary
   */
  bool insertInNode(SkipNode * const node, const KeyType &key, const ValueType &value) {
    auto wasSplit = false;
    auto targetNode = node;

    /// Handle overflow
    if (node->isFull()) {
      auto splitKey = splitNode(node);
      wasSplit = true;
      if (key >= splitKey)
        targetNode = node->forward[0].get();
    }

    /// Insert
    auto &targetBits = targetNode->bits.get_rw();
    const auto slot = targetBits.getFreeZero();
    targetNode->keys[slot] = key;
    targetNode->values[slot] = value;
    targetBits.set(slot);
    if (key < targetNode->minKey) targetNode->minKey.get_rw() = key;
    if (key > targetNode->maxKey) targetNode->maxKey.get_rw() = key;
    return wasSplit;
  }

  /**
   * Split the given node and balance the records.
   *
   * @param node the node to be split
   * @return the split key
   */
  KeyType splitNode(SkipNode * const node) {
    /// Create new sibling and load fields
    pptr<SkipNode> sibling;
    newSkipNode(sibling);
    auto sib = sibling.get();
    auto &sibBits = sib->bits.get_rw();
    auto &sibMin = sib->minKey.get_rw();
    auto &sibMax = sib->maxKey.get_rw();
    auto &nodeBits = node->bits.get_rw();

    /// Balance this with the new sibling node
    auto sibPos = 0u;
    const auto [b, splitPos] = findSplitKey<KeyType, N>(node->keys.data());
    const auto &splitKey = node->keys[splitPos];
    for (auto i = 0u; i < N; ++i) {
      if (nodeBits.test(i) && node->keys[i] >= splitKey) {
        sib->keys[sibPos] = node->keys[i];
        sib->values[sibPos] = node->values[i];
        if (node->keys[i] < sibMin) sibMin = node->keys[i];
        if (node->keys[i] > sibMax) sibMax = node->keys[i];
        sibBits.set(sibPos);
        ++sibPos;
      }
    }
    nodeBits = b;

    /// Refresh bounds
    KeyType tmpMax = MIN_KEY;
    KeyType tmpMin = MAX_KEY;
    for (auto i = 0u; i < N; ++i) {
      if (nodeBits.test(i)) {
        if (node->keys[i] > tmpMax) tmpMax = node->keys[i];
        if (node->keys[i] < tmpMin) tmpMin = node->keys[i];
      }
    }
    node->maxKey.get_rw() = tmpMax;
    node->minKey.get_rw() = tmpMin;

    /// Link new Node
    sib->forward[0] = node->forward[0];
    node->forward[0] = sibling;
    return splitKey;
  }

  /**
   * Calculate a random level.
   *
   * @return the calculated level
   */
  size_t getRandomLevel() const {
    const auto rlevel = rnd->Uniform(M);
    return (rlevel ? rlevel : 1);
  }

  /* -------------------------------------------------------------------------------------------- */

 public:
  static constexpr pobj_alloc_class_desc AllocClass{256, 64, 1, POBJ_HEADER_COMPACT};

  /**
   * Constructor for creating a new skip list.
   *
   * @param _alloc an allocation class object created in the active pool
   */
  explicit woPSkiplist(struct pobj_alloc_class_desc _alloc) :
    level(0), nodeCount(1), rnd(new dbis::Random(std::time(nullptr))), alloc_class(_alloc) {
      initList();
  }

  /**
   * Destructor for the skip list; should only free volatile parts.
   */
  ~woPSkiplist() {
    delete rnd;
  }

  /**
   * Search for the value for a given key.
   *
   * @param key the key to be searched for
   * @param[out] value a reference to the value
   * @return true if found, else otherwise
   */
  bool search(const KeyType &key, ValueType &value) const {
    const auto node = searchNode(key);
    return node ? searchInNode(node, key, value) : false;
  }

  /**
   * Inserts a new key-value pair into the skip list.
   *
   * @param key the key to be inserted
   * @param value the value to be inserted
   * @return true if insert was successful, false if a value was updated
   */
  bool insert(const KeyType &key, const ValueType &value) {
    auto wasSplit = false;

    /// find target node @c node and prepare forward pointers for each level in DRAM
    std::array<SkipNode*, M> update{};
    auto node = head.get();
    for (auto i = level.get_ro(); ; --i) {
      auto next = node->forward[i].get();
      while (next != nullptr && next->maxKey < key) {
        node = next;
        next = next->forward[i].get();
      }
      update[i] = node;
      if (i == 0) break;
    }

    /// get sibling node of last level, if not tail
    const auto next = node->forward[0].get();
    if (next) node = next;

    /// handle duplicates
    for (auto i = 0u; i < N; ++i) {
      if (node->bits.get_ro().test(i) && node->keys[i] == key) {
        node->values[i] = value;
        return false;
      }
    }

    /// insert key and value into target node
    if (insertInNode(node, key, value)) {
      wasSplit = true;
      ++nodeCount.get_rw();
    }

    /// Additionally increase the height of the new node to a random level in case of a split
    if (wasSplit) {
      /// increase global height, if necessary
      const auto newLevel = getRandomLevel();
      if (newLevel > level.get_ro()) {
        for (auto i = level.get_ro() + 1; i < newLevel; ++i)
          update[i] = head.get();
        level.get_rw() = newLevel;
      }
      /// update links on all affected levels
      auto next = node->forward[0].get();
      next->nodeLevel.get_rw() = newLevel;
      for (auto i = 1; i < newLevel; ++i) {
        next->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = node->forward[0];
      }
    }
    return true;
  }

  /**
   * Recover volatile parts of the skip list.
   */
  void recover() {
    rnd = new dbis::Random(std::time(nullptr));
  }

  /**
   * Print all nodes to standard output.
   */
  void printList() const {
    std::cout << "\nLevels: " << level + 1 << ", Nodes: " << nodeCount << "\n";
    std::cout << "________\n";
    for (int i = level; i >= 0; --i) {
      auto tmp = head->forward[i];
      std::cout << "|HEAD|" << i << "| --> ";
      while (tmp) {
        tmp->printNode();
        tmp = tmp->forward[i];
      }
      std::cout << "\u001b[31mTAIL\u001b[0m\n";
    }
    std::cout << "‾‾‾‾‾‾‾‾" << std::endl;
  }

  /**
   * Print only nodes of the last level to standard output.
   */
  void printLastLevel() const {
    auto tmp = head;
    while(tmp) {
      tmp->printNode();
      tmp = tmp->forward[0];
    }
  }

}; /// end class woPSkiplist

} /// namespace dbis::pskiplists

#endif /// WOP_SKIPLIST_HPP
