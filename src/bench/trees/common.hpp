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

#ifndef DBIS_TREES_COMMON_HPP
#define DBIS_TREES_COMMON_HPP

#include <chrono>
#include <iostream>
#include <numeric>
#include <unistd.h>
#include <experimental/filesystem>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>

#define UNIT_TESTS
#include "benchmark/benchmark.h"
#include "PBPTree.hpp"

using namespace dbis::pbptrees;

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::pool;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;

/*=== Types and constants ===*/
/* Customization section */
using MyTuple = std::tuple <int, int, double>;
using MyKey = unsigned long long;
constexpr auto TARGET_BRANCH_SIZE = 512;
constexpr auto TARGET_LEAF_SIZE = 256; //< 512B best performance
constexpr auto TARGET_DEPTH = 1;
constexpr auto IS_HYBRID = 0;
const std::string path = dbis::gPmemPath + "tree_benchSp.data";
constexpr auto POOL_SIZE = 1024 * 1024 * 1024 * 4ull; //< 1GB
constexpr auto LAYOUT = "Tree";

/* wBPTree pre-calculations */
template<unsigned int N>
constexpr unsigned int getBranchKeyswBPTree() {
  constexpr auto keys = getBranchKeyswBPTree<N-1>();
  constexpr auto CL_h = keys + ((keys + 63) / 64) * 8;
  constexpr auto CL = ((CL_h + 63) / 64) * 64;
  constexpr auto SIZE = CL + keys * (sizeof(MyKey) + 24) + 24; //< CacheLine structure & n keys & n+1 children
  return (SIZE <= TARGET_BRANCH_SIZE)? keys : ((TARGET_BRANCH_SIZE - CL - 24) / (sizeof(MyKey) + 24));
}
template<>
constexpr unsigned int getBranchKeyswBPTree<1>() {
  return ((TARGET_BRANCH_SIZE - 64 - 24) / (sizeof(MyKey) + 24));
}
template<unsigned int N>
constexpr unsigned int getLeafKeyswBPTree() {
  constexpr auto keys = getLeafKeyswBPTree<N-1>();
  constexpr auto CL_h = keys + ((keys + 63) / 64) * 8;
  constexpr auto CL = ((CL_h + 63) / 64) * 64;
  constexpr auto SIZE = CL + 32 + keys * (sizeof(MyKey) + sizeof(MyTuple));
  return (SIZE <= TARGET_LEAF_SIZE)? keys : ((TARGET_LEAF_SIZE - CL - 32) / (sizeof(MyKey) + sizeof(MyTuple)));
}
template<>
constexpr unsigned int getLeafKeyswBPTree<1>() {
  return ((TARGET_LEAF_SIZE - 64 - 32) / (sizeof(MyKey) + sizeof(MyTuple)));
}

/* PBPTree pre-calculations */
template<unsigned int N>
constexpr unsigned int getBranchKeysPBPTree() {
  return ((TARGET_BRANCH_SIZE - 28) / (sizeof(MyKey) + 24));
}
template<unsigned int N>
constexpr unsigned int getLeafKeysPBPTree() {
  return ((TARGET_LEAF_SIZE - 36) / (sizeof(MyKey) + sizeof(MyTuple)));
}

/* FPTree pre-calculations */
template<unsigned int N>
constexpr unsigned int getBranchKeysFPTree() {
  return ((TARGET_BRANCH_SIZE - 28) / (sizeof(MyKey) + 24));
}
template<unsigned int N>
constexpr unsigned int getLeafKeysFPTree() {
  constexpr auto keys = getLeafKeysFPTree<N-1>();
  constexpr auto CL_h = keys + ((keys + 63) / 64) * 8;
  constexpr auto CL = ((CL_h + 63) / 64) * 64;
  constexpr auto SIZE = CL + 32 + keys * (sizeof(MyKey) + sizeof(MyTuple));
  return (SIZE <= TARGET_LEAF_SIZE)? keys : ((TARGET_LEAF_SIZE - CL - 32) / (sizeof(MyKey) + sizeof(MyTuple)));
}
template<>
constexpr unsigned int getLeafKeysFPTree<1>() {
  return ((TARGET_LEAF_SIZE - 64 - 32) / (sizeof(MyKey) + sizeof(MyTuple)));
}

/* BitPBPTree pre-calculations */
template<unsigned int N>
constexpr unsigned int getBranchKeysBitPBPTree() {
  constexpr auto keys = getBranchKeysBitPBPTree<N-1>();
  constexpr auto word = sizeof(unsigned long) * 8;
  constexpr auto BM = (keys + word - 1) / word * 8;
  constexpr auto SIZE = BM + keys * (sizeof(MyKey) + 24) + 24; //< bitmap structure & n keys & n+1 children
  return (SIZE <= TARGET_BRANCH_SIZE)? keys : ((TARGET_BRANCH_SIZE - BM - 24) / (sizeof(MyKey) + 24));
}
template<>
constexpr unsigned int getBranchKeysBitPBPTree<1>() {
  return ((TARGET_BRANCH_SIZE - sizeof(unsigned long) - 24) / (sizeof(MyKey) + 24));
}

template<unsigned int N>
constexpr unsigned int getLeafKeysBitPBPTree() {
  constexpr auto keys = getLeafKeysBitPBPTree<N-1>();
  constexpr auto word = sizeof(unsigned long) * 8;
  constexpr auto BM = (keys + word - 1) / word * 8;
  constexpr auto SIZE = BM + 32 + keys * (sizeof(MyKey) + sizeof(MyTuple));
  return (SIZE <= TARGET_LEAF_SIZE)? keys : ((TARGET_LEAF_SIZE - BM - 32) / (sizeof(MyKey) + sizeof(MyTuple)));
}
template<>
constexpr unsigned int getLeafKeysBitPBPTree<1>() {
  return ((TARGET_LEAF_SIZE - sizeof(unsigned long) - 32) / (sizeof(MyKey) + sizeof(MyTuple)));
}

/* Power of function */
constexpr uint64_t ipow(uint64_t base, int exp, uint64_t result = 1) {
  return exp < 1 ? result : ipow(base*base, exp/2, (exp % 2) ? result*base : result);
}

/* Tree relevant calculated parameters*/
constexpr auto LEAFKEYS = getLeafKeysPBPTree<5>(); ///< 5 iterations should be enough
constexpr auto BRANCHKEYS = getBranchKeysPBPTree<5>() & ~1; ///< make this one even
constexpr auto ELEMENTS = LEAFKEYS*ipow(BRANCHKEYS+1, TARGET_DEPTH);
constexpr auto KEYPOS = 1;

using TreeType = PBPTree<MyKey, MyTuple, BRANCHKEYS, LEAFKEYS>;





/* Helper for distinction between hybrid and NVM-only structures
   http://index-of.co.uk/C++/C++%20Design%20Generic%20Programming%20and%20Design%20Patterns%20Applied.pdf
   depending on variable IS_HYBRID different code is generated during compile time
*/
template <int v>
struct Int2Type { enum { value = v }; };

template<typename T, bool isHybrid>
class HybridWrapper {
  using Node = typename T::Node;

 private:
  inline size_t getDepth(const T &tree, Int2Type<true>) const {
    return tree.depth;
  }
  inline size_t getDepth(const T &tree, Int2Type<false>) const {
    return tree.depth.get_ro();
  }

  inline Node getChildAt(const Node &node, const size_t pos, Int2Type<true>) const {
    return node.branch->children[pos];
  }
  inline Node getChildAt(const Node &node, const size_t pos, Int2Type<false>) const {
    return node.branch->children.get_ro()[pos];
  }

  inline void recover(T &tree, Int2Type<true>) const {
    tree.recover();
  }
  inline void recover(const T &tree, Int2Type<false>) const {
    return;
  }

 public:
  inline size_t getDepth(const T &tree) const {
    return getDepth(tree, Int2Type<isHybrid>());
  }

  inline Node getFirstChild(const Node &node) const {
    return getChildAt(node, 0, Int2Type<isHybrid>());
  }

  inline Node getChildAt(const Node &node, const size_t pos) const {
    return getChildAt(node, pos, Int2Type<isHybrid>());
  }

  inline void recover(T &tree) const {
    recover(tree, Int2Type<isHybrid>());
  }
};

auto hybridWrapperPtr = new HybridWrapper<TreeType, IS_HYBRID>();
auto &hybridWrapper = *hybridWrapperPtr;





/*=== Insert Function ===*/
void insert(persistent_ptr<TreeType> &tree) {
  std::chrono::high_resolution_clock::time_point t_start, t_end;
  std::vector<typename std::chrono::duration<int64_t, std::micro>::rep> measures;

  auto insertLoopLeaf = [&](int start) {
    auto end = start + (LEAFKEYS + 1) / 2;
    for (auto j = start; j < end && j < ELEMENTS; ++j) {
      auto tup = MyTuple(j + 1, (j + 1) * 100, (j + 1) * 1.0);
      /*if (i % (ELEMENTS/100) == 0) {
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
  std::function<void(int,int,bool)> insertLoopBranch = [&](int start, int depth, bool otherHalf) {
    auto nodeRange = LEAFKEYS * ipow(BRANCHKEYS+1, depth);
    auto lowerRange = LEAFKEYS * ipow(BRANCHKEYS+1, depth-1);
    auto middle = (nodeRange + 1) / 2;
    auto firstHalf = start + middle;
    auto helper = start + nodeRange - middle - 1;
    auto secondHalf = (BRANCHKEYS%2==0)? helper - 0 : helper - lowerRange;
    auto end = (otherHalf)? secondHalf : firstHalf;
    for(auto i = start; i <= end; i+= LEAFKEYS*ipow(BRANCHKEYS+1,depth-1)) {
      if (depth == 1) insertLoopLeaf(i);
      else insertLoopBranch(i, depth-1, false);
    }
  };

  std::function<void(int)> insertLoopOtherHalf = [&](int depth) {
    //tree->printBranchNode(0, tree->rootNode.branch);
    if (depth == 0) {
      auto otherHalf = (LEAFKEYS + 1) / 2;
      for(auto i = 0; i < ipow(BRANCHKEYS+1,TARGET_DEPTH-depth); ++i)
        insertLoopLeaf(otherHalf + i * LEAFKEYS);
    } else {
      auto nodeRange = LEAFKEYS * ipow(BRANCHKEYS+1, depth);
      auto lowerRange = LEAFKEYS * ipow(BRANCHKEYS+1, depth-1);
      auto middle = (nodeRange + 1) / 2;
      auto seen = middle + lowerRange;
      auto otherHalf = (BRANCHKEYS%2==0)? seen - (lowerRange+1)/2 : seen;
      for(auto i = 0; i < ipow(BRANCHKEYS+1,TARGET_DEPTH-depth); ++i)
        insertLoopBranch(otherHalf + i * LEAFKEYS * ipow(BRANCHKEYS+1, depth), depth, true);
      insertLoopOtherHalf(depth-1);
    }
  };

  insertLoopBranch(0, TARGET_DEPTH, false);
  insertLoopOtherHalf(TARGET_DEPTH);

  auto avg = std::accumulate(measures.begin(), measures.end(), 0) / measures.size();
  auto minmax = std::minmax_element(std::begin(measures), std::end(measures));
  std::cout << "\nInsert Statistics in µs: "
    << "\n\tAvg: \t" << avg
    << "\n\tMin: \t" << *minmax.first
    << "\n\tMax: \t" << *minmax.second << std::endl;
}

#endif /* DBIS_TREES_COMMON_HPP */
