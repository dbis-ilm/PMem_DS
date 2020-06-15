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

#ifndef SIMPLE_PSKIPLIST_HPP
#define SIMPLE_PSKIPLIST_HPP

#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <libpmemobj++/utils.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
namespace pmemobj_exp = pmem::obj::experimental;
using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;

class Random {
  uint32_t seed;
  public:
  explicit Random(uint32_t s) : seed(s & 0x7fffffffu) {
    if (seed == 0 || seed == 2147483647L) {
      seed = 1;
    }
  }

  uint32_t Next() {
    static const uint32_t M = 2147483647L;   // 2^31-1
    static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    uint64_t product = seed * A;
    seed = static_cast<uint32_t>((product >> 31) + (product & M));
    if(seed>M) {
      seed -= M;
    }
    return seed;
  }

  uint32_t Uniform(int n) { return (Next() % n); }

  bool OneIn(int n) { return (Next() % n) == 0; }

  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }
};

template<typename KeyType, typename ValueType, int MAX_LEVEL>
class simplePSkiplist {

  struct SkipNode {
    p<KeyType> key;
    p<ValueType> value;
    persistent_ptr<persistent_ptr<SkipNode>> forward;
    p<int> nodeLevel;

    SkipNode() {}

    SkipNode(const KeyType key, const ValueType value) {
      this->key = key;
      this->value = value;
    }
  };

  persistent_ptr<SkipNode> head;
  persistent_ptr<SkipNode> tail;
  p<int> level;


  p<size_t> nodeCount;
  Random rnd;

  void createNode(int level, persistent_ptr<SkipNode> &node) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {node = make_persistent<SkipNode>();
        node->forward = make_persistent<persistent_ptr<SkipNode>>();
        for(int i=0; i<level+1;i++) {
        node->forward[i] = make_persistent<SkipNode>();
        }
        //node->forward = new SkipNode*[level+1];
        node->nodeLevel = level;
        });
  }

  void createNode(int level, persistent_ptr<SkipNode> &node, KeyType key, ValueType value) {
    auto pop = pmem::obj::pool_by_vptr(this);
    transaction::run(pop, [&] {
        node = make_persistent<SkipNode>(key, value);
        if(level > 0) {
        //node->forward = new SkipNode*[level +1];
        node->forward = make_persistent<persistent_ptr<SkipNode>>();
        for(int i=0; i<level+1; i++) {
        node->forward[i] = make_persistent<SkipNode>();
        }
        }
        node->nodeLevel = level;
        });
  }

  void createList(KeyType tailKey) {
    createNode(0, tail);
    tail->key = tailKey;
    this->level = 0;

    createNode(MAX_LEVEL, head);
    for(int i=0; i < MAX_LEVEL; ++i) {
      head->forward[i] = tail;
    }
    nodeCount = 0;
  }

  int getRandomLevel() {
    int level = static_cast<int>(rnd.Uniform(MAX_LEVEL));
    if (level == 0) {
      level = 1;
    }
    return level;
  }

  public:

  simplePSkiplist() : head(nullptr), tail(nullptr), level(0), nodeCount(0), rnd(0x12345678) {}

  simplePSkiplist(KeyType tailKey) : rnd(0x12345678) {
    createList(tailKey);
  }


  bool insert(KeyType key, ValueType value) {
    auto pop = pmem::obj::pool_by_vptr(this);
    bool ret = true;
    transaction::run(pop, [&] {
        persistent_ptr<SkipNode> update[MAX_LEVEL];

        auto node = head;

        for(int i = level; i>= 0; --i) {
        while(node->forward[i]->key < key) {
        node = node->forward[i];
        }
        update[i] = node;
        }

        node = node->forward[0];

        if(node->key == key) {
        ret = false;
        }

        auto nodeLevel = getRandomLevel();

        if(nodeLevel > level) {
          nodeLevel = ++level;
          update[nodeLevel] = head;
        }

        persistent_ptr<SkipNode> newNode;
        createNode(nodeLevel, newNode, key, value);

        for(int i = nodeLevel; i >= 0; --i) {
          node = update[i];
          newNode->forward[i] = node->forward[i];
          node->forward[i] = newNode;
        }
        ++nodeCount;
    });
    return ret;
  }

  persistent_ptr<SkipNode> search(const KeyType key) {
    auto node = head;
    for(int i = level; i >= 0; --i) {
      while(node->forward[i]->key < key) {
        node = *(node->forward + i);
      }
    }
    node = node->forward[0];
    if(node->key == key) {
      return node;
    } else {
      return nullptr;
    }
  }

  void printNodes() {
    auto tmp = head;
    while (tmp->forward[0] != tail) {
      tmp = tmp->forward[0];
      dumpNodeDetail(tmp, tmp->nodeLevel);
      std::cout << "----------------------------" << std::endl;
    }
    std::cout << std::endl;
  }

  void dumpNodeDetail(persistent_ptr<SkipNode> node, int nodeLevel) {
    if (node == nullptr) {
      return;
    }
    std::cout << "node->key:" << node->key << ",node->value:" << node->value << std::endl;
    for (int i = 0; i <= nodeLevel; ++i) {
      std::cout << "forward[" << i << "]:" << "key:" << node->forward[i]->key << ",value:" << node->forward[i]->value
        << std::endl;
    }
  }

};

#endif //SIMPLE_PSKIPLIST_HPP

