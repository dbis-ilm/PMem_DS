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

#include <libpmemobj++/container/array.hpp>
#include "common.hpp"

constexpr auto L3 = 30 * 1024 * 1024;
constexpr auto ArraySize = L3 / TARGET_BRANCH_SIZE;
using myTypes = getTypes<TreeType, ArraySize, IS_HYBRID>;
using BranchPtr = typename myTypes::bNodeType;
using InnerArray = typename myTypes::iArrayType;
using InnerPtr = typename myTypes::iPtrType;

struct root {
  pptr<TreeType> tree;
  pptr<std::array<InnerPtr, TARGET_DEPTH>> branchArray;  ///< Non-Hybrid only
};

pptr<TreeType> tree;
std::array<InnerPtr, TARGET_DEPTH> *branchArray;
BranchPtr rootBranch;

void prepare(pmem::obj::pool<root> &pop, pobj_alloc_class_desc alloc_class);

/* Traverse benchmarks on Tree */
static void BM_TreeTraverse(benchmark::State &state) {
  std::cout << "BRANCHKEYS: " << BRANCHKEYS << " - " << sizeof(TreeType::BranchNode)
            << "\nLEAFKEYS: " << LEAFKEYS << " - " << sizeof(TreeType::LeafNode) << "\n";

  pool<root> pop;
  pobj_alloc_class_desc alloc_class;

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.128.desc",
                                                            TreeType::AllocClass);
    prepare(pop, alloc_class);
  } else {
    LOG("Warning: " << path << " already exists");
    if (pool<root>::check(path, LAYOUT) == 1) {
      pop = pool<root>::open(path, LAYOUT);
    } else {
      pmempool_rm(path.c_str(), 0);
      pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    }
    alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.128.desc",
                                                            TreeType::AllocClass);
    if (pop.root()->tree != nullptr) {
      tree = pop.root()->tree;
      rootBranch = tree->rootNode.branch;
      if constexpr (IS_HYBRID) {  // TODO: how to make this generic?
        // branchArray = new std::array<InnerPtr, TARGET_DEPTH>();
        // for (auto i = TARGET_DEPTH; i > 0; --i) {
        //   branchArray->at(i - 1) = std::make_unique<InnerArray>();
        // // }
        // for (auto i = TARGET_DEPTH; i > 0; --i) {
        //   auto &levelArray = branchArray->at(i - 1);
        //   auto &a = *levelArray;
        //   for (int j = 0; j < ArraySize; ++j) {
        //     a[j] = tree->newBranchNode();
        //     *a[j] = *rootBranch;  ///< copy from tree
        //     if (i < TARGET_DEPTH)
        //       hybridWrapper.setChildAt(*a[j], 0).branch = branchArray->at(i)->at(j);
        //   }
        // }
      } else {
        branchArray = &*pop.root()->branchArray;
      }
    } else {
      prepare(pop, alloc_class);
    }
  }
  pop.drain();

  auto &treeRef = *tree;

  /* BENCHMARKING */
  for (auto _ : state) {
    state.PauseTiming();
    auto d = TARGET_DEPTH;
    const auto branchNodePos = std::rand() % ArraySize;
    TreeType::Node node;
    auto branchNode = branchArray->at(0)->at(branchNodePos);
    state.ResumeTiming();

    benchmark::DoNotOptimize(*branchNode);
    while (--d > 0) {
      node = hybridWrapper.getChildAt(branchNode, 0);
      branchNode = node.branch;
      benchmark::DoNotOptimize(*branchNode);
    }
  }

  std::cout << "Elements:" << ELEMENTS << "\n";
  pop.close();
  pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_TreeTraverse);
BENCHMARK_MAIN();

void prepare(pmem::obj::pool<root> &pop, pobj_alloc_class_desc alloc_class) {
  transaction::run(pop, [&] {
    pop.root()->tree = make_persistent<TreeType>(alloc_class);
    tree = pop.root()->tree;
    if constexpr (IS_HYBRID) {  // TODO: how to make this generic?
      // branchArray = new std::array<InnerPtr, TARGET_DEPTH>();
      // for (auto i = TARGET_DEPTH; i > 0; --i) {
      //   branchArray->at(i - 1) = std::make_unique<InnerArray>();
      // }
    } else {
      pop.root()->branchArray = make_persistent<std::array<InnerPtr, TARGET_DEPTH>>();
      branchArray = &*pop.root()->branchArray;
      for (auto i = TARGET_DEPTH; i > 0; --i) {
        branchArray->at(i - 1) = make_persistent<InnerArray>();
      }
    }

    tree = pop.root()->tree;
    insert(tree, 1);
    rootBranch = tree->rootNode.branch;

    for (auto i = TARGET_DEPTH; i > 0; --i) {
      auto &levelArray = branchArray->at(i - 1);
      auto &a = *levelArray;
      for (int j = 0; j < ArraySize; ++j) {
        a[j] = tree->newBranchNode();
        *a[j] = *rootBranch;  ///< copy from tree
        if (i < TARGET_DEPTH) hybridWrapper.setChildAt(*a[j], 0).branch = branchArray->at(i)->at(j);
      }
    }
  });
}