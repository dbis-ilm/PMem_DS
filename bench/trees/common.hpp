/*
 * Copyright (C) 2017-2021 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of our PMem-based Data Structures repository.
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

#ifndef DBIS_TREES_COMMON_HPP
#define DBIS_TREES_COMMON_HPP

#include <unistd.h>
#include <chrono>
#include <iostream>

#include <libpmempool.h>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <numeric>

#define UNIT_TESTS
#include "PBPTree.hpp"
#include "benchmark/benchmark.h"

using namespace dbis::pbptrees;

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;


/*=== Types and constants ===*/
/* Customization section */
using MyTuple = std::tuple<int, int, double>;
using MyKey = unsigned long long;
constexpr auto TARGET_BRANCH_SIZE = 512;
constexpr auto TARGET_LEAF_SIZE = 512;
constexpr auto TARGET_DEPTH = 1;
constexpr auto IS_HYBRID = 0;
constexpr auto MOVE_TO_RIGHT = 0;  ///< for balance direction
const std::string path = dbis::gPmemPath + "tree_bench.data";
constexpr auto POOL_SIZE = 1024 * 1024 * 1024 * 4ull;  //< 4GB
constexpr auto LAYOUT = "Tree";
constexpr auto NODE_PTR_SIZE = 24;  ///< 24 Byte (8-tag + 16-pptr)
constexpr auto L3 = 14080 * 1024;

/* wBPTree pre-calculations */
template <unsigned int KEYS>
constexpr unsigned int getBranchKeyswBPTree() {
  constexpr auto CL_h = ((KEYS + 1 + 7) / 8) * 8 + ((KEYS + 63) / 64) * 8;
  constexpr auto CL = ((CL_h + 63) / 64) * 64;  ///< rounding up
  constexpr auto SIZE = CL + KEYS * (sizeof(MyKey) + NODE_PTR_SIZE) +
                        NODE_PTR_SIZE;  ///< CacheLine structure & n keys & n+1 children
  if constexpr (SIZE <= TARGET_BRANCH_SIZE)
    return KEYS;
  else
    return getBranchKeyswBPTree<KEYS - 1>();
}

constexpr size_t getBranchKeyswBPTree() {
  constexpr auto KEYS = ((TARGET_BRANCH_SIZE - 64 - NODE_PTR_SIZE) / (sizeof(MyKey) + NODE_PTR_SIZE));
  return getBranchKeyswBPTree<KEYS>();
}

template <size_t KEYS>
constexpr size_t getLeafKeyswBPTree() {
  constexpr auto CL_h = ((KEYS + 1 + 7) / 8) * 8 + ((KEYS + 63) / 64) * 8 + 32;
  constexpr auto CL = ((CL_h + 63) / 64) * 64;
  constexpr auto SIZE = CL + KEYS * (sizeof(MyKey) + sizeof(MyTuple));
  if constexpr (SIZE <= TARGET_LEAF_SIZE)
    return KEYS;
  else
    return getLeafKeyswBPTree<KEYS - 1>();
  // return SIZE <= TARGET_LEAF_SIZE ? KEYS : getLeafKeyswBPTree<KEYS-1>();
}

constexpr size_t getLeafKeyswBPTree() {
  constexpr auto KEYS = ((TARGET_LEAF_SIZE - 64) / (sizeof(MyKey) + sizeof(MyTuple)));
  return getLeafKeyswBPTree<KEYS>();
}

/* PBPTree pre-calculations */
constexpr size_t getBranchKeysPBPTree() {
  return ((TARGET_BRANCH_SIZE - 28) / (sizeof(MyKey) + 24));
}

constexpr size_t getLeafKeysPBPTree() {
  return ((TARGET_LEAF_SIZE - 64) / (sizeof(MyKey) + sizeof(MyTuple)));
}

/* FPTree pre-calculations */
constexpr size_t getBranchKeysFPTree() {
  return ((TARGET_BRANCH_SIZE - 28) / (sizeof(MyKey) + NODE_PTR_SIZE));
}

template <size_t KEYS>
constexpr size_t getLeafKeysFPTree() {
  constexpr auto CL_h = ((KEYS + 7) / 8) * 8 + ((KEYS + 63) / 64) * 8 + 32;
  constexpr auto CL = ((CL_h + 63) / 64) * 64;
  constexpr auto SIZE = CL + KEYS * (sizeof(MyKey) + sizeof(MyTuple));
  if constexpr (SIZE <= TARGET_LEAF_SIZE)
    return KEYS;
  else
    return getLeafKeysFPTree<KEYS - 1>();
}

constexpr size_t getLeafKeysFPTree() {
  constexpr auto KEYS = ((TARGET_LEAF_SIZE - 64) / (sizeof(MyKey) + sizeof(MyTuple)));
  return getLeafKeysFPTree<KEYS>();
}

/* BitPBPTree pre-calculations */
template <size_t KEYS>
constexpr size_t getBranchKeysBitPBPTree() {
  constexpr auto wordBits = sizeof(unsigned long) * 8;
  constexpr auto BM = (KEYS + wordBits - 1) / wordBits * 8;  ///< round to necessary words
  constexpr auto SIZE = BM + KEYS * (sizeof(MyKey) + NODE_PTR_SIZE) +
                        NODE_PTR_SIZE;  ///< bitmap structure & n keys & n+1 children
  if constexpr (SIZE <= TARGET_BRANCH_SIZE)
    return KEYS;
  else
    return getBranchKeyswBPTree<KEYS - 1>();
}

constexpr size_t getBranchKeysBitPBPTree() {
  constexpr auto KEYS = (TARGET_BRANCH_SIZE - sizeof(unsigned long) - NODE_PTR_SIZE) /
                        (sizeof(MyKey) + NODE_PTR_SIZE);
  return getBranchKeysBitPBPTree<KEYS>();
}

template <size_t KEYS>
constexpr size_t getLeafKeysBitPBPTree() {
  constexpr auto CL_h = ((KEYS + 63) / 64) * 8 + 32;
  constexpr auto CL = ((CL_h + 63) / 64) * 64;
  constexpr auto SIZE = CL + KEYS * (sizeof(MyKey) + sizeof(MyTuple));
  if constexpr (SIZE <= TARGET_LEAF_SIZE)
    return KEYS;
  else
    return getLeafKeysBitPBPTree<KEYS - 1>();
}

constexpr size_t getLeafKeysBitPBPTree() {
  constexpr auto KEYS = ((TARGET_LEAF_SIZE - 64) / (sizeof(MyKey) + sizeof(MyTuple)));
  return getLeafKeysBitPBPTree<KEYS>();
}

/* Power of function */
constexpr uint64_t ipow(uint64_t base, int exp, uint64_t result = 1) {
  return exp < 1 ? result : ipow(base * base, exp / 2, (exp % 2) ? result * base : result);
}

/* Tree relevant calculated parameters*/
constexpr auto LEAFKEYS = getLeafKeysPBPTree();
constexpr auto BRANCHKEYS = getBranchKeysPBPTree() & ~1;  ///< make this one even
constexpr auto ELEMENTS = LEAFKEYS * ipow(BRANCHKEYS + 1, TARGET_DEPTH);
constexpr auto KEYPOS = 1;

using TreeType = PBPTree<MyKey, MyTuple, BRANCHKEYS, LEAFKEYS>;

/* Helper for distinction between hybrid and NVM-only structures
   http://index-of.co.uk/C++/C++%20Design%20Generic%20Programming%20and%20Design%20Patterns%20Applied.pdf
   depending on variable IS_HYBRID different code is generated during compile time
*/
template <int v>
struct Int2Type {
  enum { value = v };
};

template <typename T, size_t arrSize, bool isHybrid>
struct getTypes;
template <typename T, size_t arrSize>
struct getTypes<T, arrSize, true> {
  using bNodeType = typename T::BranchNode *;
  using iArrayType = std::array<bNodeType, arrSize>;
  using iPtrType = std::unique_ptr<iArrayType>;
};
template <typename T, size_t arrSize>
struct getTypes<T, arrSize, false> {
  using bNodeType = pptr<typename T::BranchNode>;
  using iArrayType = pmem::obj::array<bNodeType, arrSize>;
  using iPtrType = pptr<iArrayType>;
};

template <typename T, bool isHybrid>
class HybridWrapper {
  using Node = typename T::Node;
  using BNode = typename T::BranchNode;

 private:

  inline size_t getDepth(const T &tree, Int2Type<true>) const { return tree.depth; }
  inline size_t getDepth(const T &tree, Int2Type<false>) const { return tree.depth.get_ro(); }

  inline Node getChildAt(const Node &node, const size_t pos, Int2Type<true>) const{
    return node.branch->children[pos];
  }
  inline Node getChildAt(const Node &node, const size_t pos, Int2Type<false>) const {
    return node.branch->children.get_ro()[pos];
  }

  inline Node &setChildAt(BNode &node, const size_t pos, Int2Type<true>) const {
    return node.children[pos];
  }
  inline Node &setChildAt(BNode &node, const size_t pos, Int2Type<false>) const {
    return node.children.get_rw()[pos];
  }

  inline void recover(T &tree, Int2Type<true>) const { tree.recover(); }
  inline void recover(const T &tree, Int2Type<false>) const { return; }

 public:
  inline size_t getDepth(const T &tree) const { return getDepth(tree, Int2Type<isHybrid>()); }

  inline Node getFirstChild(const Node &node) const {
    return getChildAt(node, 0, Int2Type<isHybrid>());
  }

  inline Node getChildAt(const Node &node, const size_t pos) const {
    return getChildAt(node, pos, Int2Type<isHybrid>());
  }

  inline Node &setChildAt(BNode &node, const size_t pos) const {
    return setChildAt(node, pos, Int2Type<isHybrid>());
  }

  inline void recover(T &tree) const { recover(tree, Int2Type<isHybrid>()); }
};

auto hybridWrapperPtr = std::make_unique<HybridWrapper<TreeType, IS_HYBRID>>();
auto &hybridWrapper = *hybridWrapperPtr;

/*=== Insert Function ===*/
void insert(const persistent_ptr<TreeType> &tree, unsigned int target_depth = TARGET_DEPTH) {
  std::chrono::high_resolution_clock::time_point t_start, t_end;
  std::vector<typename std::chrono::duration<int64_t, std::micro>::rep> measures;

  auto insertLoopLeaf = [&](int start, bool ohterHalf = false) {
    auto end = start + (LEAFKEYS + 1) / 2 - ((LEAFKEYS % 2 == 1 && ohterHalf)? 1 : 0);
    for (auto j = start; j < end && j < ELEMENTS; ++j) {
      auto tup = MyTuple(j + 1, (j + 1) * 100, (j + 1) * 1.0);
      /*if (j % (ELEMENTS/100) == 0) {
        std::cout << "Inserting tuple: " << (j+1)*100/ELEMENTS << "%\r";
        std::cout.flush();
        }*/
      t_start = std::chrono::high_resolution_clock::now();
      tree->insert(j + 1, tup);
      t_end = std::chrono::high_resolution_clock::now();
      auto diff = std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count();
      measures.push_back(diff);
    }
  };
  std::function<void(int, int, bool)> insertLoopBranch = [&](int start, int depth, bool otherHalf) {
    auto nodeRange = LEAFKEYS * ipow(BRANCHKEYS + 1, depth);
    auto lowerRange = LEAFKEYS * ipow(BRANCHKEYS + 1, depth - 1);
    auto middle = (nodeRange + 1) / 2;
    auto firstHalf = start + middle;
    auto helper = start + nodeRange - middle;
    auto secondHalf = (BRANCHKEYS % 2 == 0) ? helper - 0 : helper - lowerRange;
    auto end = (otherHalf) ? secondHalf : firstHalf;
    for (auto i = start; i <= end; i += LEAFKEYS * ipow(BRANCHKEYS + 1, depth - 1)) {
      if (depth == 1)
        insertLoopLeaf(i);
      else
        insertLoopBranch(i, depth - 1, false);
    }
  };

  std::function<void(int)> insertLoopOtherHalf = [&](int depth) {
    // tree->printBranchNode(0, tree->rootNode.branch);
    if (depth == 0) {
      auto otherHalf = (LEAFKEYS + 1) / 2;
      for (auto i = 0; i < ipow(BRANCHKEYS + 1, target_depth - depth); ++i)
        insertLoopLeaf(otherHalf + i * LEAFKEYS, true);
    } else {
      auto nodeRange = LEAFKEYS * ipow(BRANCHKEYS + 1, depth);
      auto lowerRange = LEAFKEYS * ipow(BRANCHKEYS + 1, depth - 1);
      auto middle = (nodeRange + 1) / 2;
      auto seen = middle + lowerRange;
      auto otherHalf = (BRANCHKEYS % 2 == 0) ? seen - (lowerRange + 1) / 2 : seen;
      for (auto i = 0; i < ipow(BRANCHKEYS + 1, target_depth - depth); ++i)
        insertLoopBranch(otherHalf + i * LEAFKEYS * ipow(BRANCHKEYS + 1, depth), depth, true);
      insertLoopOtherHalf(depth - 1);
    }
  };

  if (target_depth == 0) {
    insertLoopLeaf(0);
    constexpr bool Even = LEAFKEYS % 2 == 0;
    insertLoopLeaf(Even? (LEAFKEYS + 1) / 2 : (LEAFKEYS - 1) / 2);
  } else {
    insertLoopBranch(0, target_depth, false);
    insertLoopOtherHalf(target_depth);
  }

  // auto avg = std::accumulate(measures.begin(), measures.end(), 0) / measures.size();
  // auto minmax = std::minmax_element(std::begin(measures), std::end(measures));
  // std::cout << "\nInsert Statistics in µs: "
  //           << "\n\tAvg: \t" << avg << "\n\tMin: \t" << *minmax.first << "\n\tMax: \t"
  //           << *minmax.second << std::endl;
}

#endif /* DBIS_TREES_COMMON_HPP */
