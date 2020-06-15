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

#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <array>
#include <bitset>

#include <libpmemobj++/utils.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

namespace dbis::pskiplists {

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;
template <typename Object>
using pptr = persistent_ptr<Object>;

class Random {
  public:
    explicit Random(uint32_t s) : seed(s & 0x7fffffffu) {
      if (seed == 0 || seed == 2147483647L) {
        seed = 1;
      }
    }

    uint32_t Next() {
      static const uint32_t M = 2147483647L;
      static const uint64_t A = 16807;
      uint64_t product = seed * A;

      seed = static_cast<uint32_t>((product >> 31) + (product & M));
      if (seed > M) {
        seed -= M;
      }
      return seed;
    }

    uint32_t Uniform(int n) { return (Next() % n); }
    bool OneIn(int n) { return (Next() % n) == 0; }
    uint32_t Skewed(int max_log) {
      return Uniform(1 << Uniform(max_log + 1));
    }

  private:
    uint32_t seed;
};

/**
 * A persistent memory implementation of a write-optimized Skip List.
 *
 * @tparam KeyType the data type of the key
 * @tparam ValueType the data type of the values associated with the key
 * @tparam N the bucket size
 * @tparam M the maximum level
 */
template<typename KeyType, typename ValueType, int N, int M>
class woPSkiplist {

  static constexpr auto MAX_KEY = std::numeric_limits<KeyType>::max();
  static constexpr auto MIN_KEY = std::numeric_limits<KeyType>::min();

  /**
   * A structure for representing a node of a skip list.
   */
  struct alignas(64) SkipNode {
    p<std::bitset<N>> bitset;           ///< a bitmap indicating empty/used slots
    p<KeyType> minKey;                  ///< SMA min
    p<KeyType> maxKey;                  ///< SMA max
    p<size_t> nodeLevel;                ///< the level of this node
    /// TODO: padding to 64 bytes?
    p<std::array<KeyType, N>> keys;     ///< the actual keys
    p<std::array<ValueType, N>> values; ///< the actual values

    pptr<std::array<pptr<SkipNode>, M>> forward; ///< the forward pointers to the following nodes

    /**
     * Constructor for creating a new empty node.
     */
    SkipNode() : maxKey(MIN_KEY), minKey(MAX_KEY), nodeLevel(0) {}

    /**
     * Constructor for creating a new node with an initial key and value.
     */
    SkipNode(const KeyType _key, const ValueType _value) : keys(_key), values(_value) {}

    /**
     * Calculate the average key value of this node.
     *
     * @return the average
     */
    KeyType getAvg() const {
      KeyType sum = 0;
      auto btCount = 0u;
      const auto &bs = bitset.get_ro();
      const auto &ks = keys.get_ro();
      for (auto i = 0u; i < N; ++i) {
        if (bs.test(i)) {
          const auto &k = ks[i];
          if (k != minKey && k != maxKey) {
            sum += k;
            btCount++;
          }
        }
      }
      return sum / btCount;
    }

    bool isFull() {
      return bitset.get_ro().all();
    }

    bool keyBetween(KeyType newKey) {
      if(minKey == - 1 ||
          maxKey == -1)
        return true;

      return newKey >= minKey && newKey <= maxKey;
    }

    ValueType* searchKey(const KeyType searchKey) {
      for(int i=0; i<N; i++) {
        if(bitset.get_ro().test(i)) {
          if(keys.get_ro()[i] == searchKey) {
            return &values.get_rw().at(i);
          }
        }
      }
      return nullptr;
    }

    void splitNode(pptr<SkipNode> prevNode, pptr<SkipNode> nextNode, int listLevel) {
      auto pop = pmem::obj::pool_by_vptr(this);
      transaction::run(pop, [&] {
          auto newNode = make_persistent<SkipNode>();

          newNode->forward = make_persistent<std::array<pptr<SkipNode>,M>>();
          newNode->forward.get()->at(0) = nextNode;
          newNode->maxKey = this->minKey;
          newNode->minKey = this->maxKey;
          this->forward.get()->at(0) = newNode;

          int leftPos = 0;

          auto avg = getAvg();

          for(int i=0; i<N; i++) {
          if(bitset.get_ro().test(i)) {
          if(keys.get_ro()[i] >= avg) {
          newNode->keys.get_rw()[leftPos] = keys.get_ro()[i];
          newNode->values.get_rw()[leftPos] = values.get_ro()[i];
          newNode->bitset.get_rw().set(leftPos);
          bitset.get_rw().reset(i);
          leftPos++;
          if(keys.get_ro()[i] < newNode->minKey)
            newNode->minKey = keys.get_ro()[i];
          if(keys.get_ro()[i] > newNode->maxKey)
            newNode->maxKey = keys.get_ro()[i];
          }
          }
          }
          refreshBounds();
      });
    }

    void refreshBounds() {
      KeyType tmpMax = -1;
      KeyType tmpMin = -1;

      for(int i=0; i<N; i++) {
        if(bitset.get_ro().test(i)) {
          if(tmpMax == -1 || keys.get_ro()[i] > tmpMax)
            tmpMax = keys.get_ro()[i];
          if(tmpMin == -1 || keys.get_ro()[i] < tmpMin)
            tmpMin = keys.get_ro()[i];
        }
      }


      maxKey.get_rw() = tmpMax;
      minKey.get_rw() = tmpMin;
    }

    void insertInNode(pptr<SkipNode> prevNode, KeyType k, ValueType v, bool& splitInfo, int level) {
      if(isFull()) {
        splitNode(prevNode, forward.get()->at(0), level);
        //prevNode->forward[0]->key[3] = k;
        //prevNode->forward[0]->bitset.set(3);
        splitInfo = true;
        return;
      }
      int freeSlot = 0;
      while(bitset.get_ro().test(freeSlot)) {
        freeSlot++;
      }

      keys.get_rw().at(freeSlot) = k;
      values.get_rw().at(freeSlot)= v;
      bitset.get_rw().set(freeSlot);

      if(this->minKey == -1 || k < this->minKey) {
        this->minKey = k;
      } else if(this->maxKey == -1 || k > this->maxKey) {
        this->maxKey = k;
      }
    }

    void printNode() {
      std::cout << "{"<<minKey << "|" << maxKey <<"}[";
      for(int i=0; i<N; i++) {
        if(bitset.get_ro().test(i))
          std::cout << keys.get_ro()[i] << ",";
      }
      std::cout << "]->";
    }
  };

  static constexpr auto MAX_LEVEL  = M;
  pptr<SkipNode> head;
  pptr<SkipNode> tail;
  p<int> level;
  bool nl;
  p<size_t> nodeCount;
  Random rnd;

  void createNode(int level, pptr<SkipNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {
        node = make_persistent<SkipNode>();
        node->forward = make_persistent<std::array<pptr<SkipNode>,M>>();
        node->nodeLevel = level;
        node->maxKey = -1;
        node->minKey = -1;
        });
  }

  void createList(KeyType tailKey) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {

        createNode(0, tail);
        tail->maxKey = -tailKey;
        tail->minKey = tailKey;
        this->level = 0;

        createNode(MAX_LEVEL, head);
        for (int i = 0; i < MAX_LEVEL; ++i) {
        head->forward.get()->at(i) = tail.get();
        }
        nodeCount = 0;
        });
  }

  int getRandomLevel() {
    int level = static_cast<int>(rnd.Uniform(MAX_LEVEL));
    if (level == 0) {
      level = 1;
    }
    return level;
  }

  public:

  woPSkiplist() : head(nullptr), tail(nullptr), level(0), nl(false), nodeCount(0), rnd(0x12378) {}

  woPSkiplist(KeyType tailKey) : rnd(0x12378) {
    createList(tailKey);
    nl = false;
  }

  bool insert(KeyType key, ValueType value) {
    if (head == nullptr)
      createList(key);
reInsert:
    auto update = new SkipNode[MAX_LEVEL]();

    auto node = head;
    auto prev = head;
    auto last = head;

    for(int i = level; i>= 0; --i) {
      while((node->maxKey <= key)) {
        if(node->forward.get()->at(i) == nullptr)
          break;
        last = prev;
        prev = node;
        node = node->forward.get()->at(i);
      }
      if(node->maxKey > key) {
        node = prev;
        prev = last;
      }
      update[i] = *prev;
    }

    auto nodeLevel = getRandomLevel();

    bool splitInfo = false;
    if(node->forward.get()->at(0)) {
      prev = node;
      node = node->forward.get()->at(0);
    }

    node->insertInNode(prev, key, value, *&splitInfo, level);
    if(splitInfo) {
      nl = true;
      nodeCount.get_rw()++;
      goto reInsert;
    }

    if(nl) {
      if(nodeLevel > level.get_ro()) {
        nodeLevel = ++level.get_rw();
        update[nodeLevel] = *head;
      }
      auto n = &update[nodeLevel];
      for(int i = nodeLevel; i >= 1; --i) {
        update[i].forward.get()->at(i) = node;
        prev->forward[i] = update[i].forward[i];
      }
      nl = false;
    }
    return true;
  }

  pptr<SkipNode> searchNode(const KeyType key) const {
    auto pred = head;
    for(auto i = level.get_ro(); i >= 0; --i) {
      auto item = (*pred->forward)[i];
      while(item != nullptr) {
        if( key > item->maxKey) {
          pred = item;
          item = (*item->forward)[i];
        } else if(key < item->minKey) {
          break;
        } else {
          return item;
        }
      }
    }
  }

  ValueType* search(const KeyType key) const {
    const auto node = searchNode(key);
    return node ? node->searchKey(key) : nullptr;
  }

  void printElementNode() {
    std::cout << "\nLevel: " << level << std::endl;
    auto tmp = head;

    for(int i = level; i>=0; i--) {
      while(tmp != nullptr && tmp->forward) {
        tmp->printNode();
        tmp = tmp->forward.get()->at(i);
      }
      tmp = head;
      std::cout << std::endl;
    }
  }

  void printLastLevel() {
    auto tmp = head;

    while(tmp != nullptr && tmp->forward) {
      tmp->printNode();
      tmp = tmp->forward[0];
    }
  }

}; /// end class woPSkiplist

} /// namespace dbis::pbptrees

#endif /// WOP_SKIPLIST_HPP
