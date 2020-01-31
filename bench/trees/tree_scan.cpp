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

#include <cstdlib>
#include <ctime>
#include <libpmemobj++/container/array.hpp>
#include "common.hpp"

constexpr auto L3 = 30 * 1024 * 1024;
constexpr auto ArraySize =
    L3 / (TARGET_LEAF_SIZE * (BRANCHKEYS + 1) + TARGET_BRANCH_SIZE) + 2;  ///< at least two

struct root {
  pptr<pmem::obj::array<pptr<TreeType>, ArraySize>> treeArray;
};

pptr<pmem::obj::array<pptr<TreeType>, ArraySize>> treeArray;

void prepare(pmem::obj::pool<root> &pop, pobj_alloc_class_desc alloc_class);


/* Scan benchmarks on Tree */
static void BM_TreeScan(benchmark::State& state) {
  std::cout << "BRANCHKEYS: " << BRANCHKEYS << " - " << sizeof(TreeType::BranchNode)
            << "\nLEAFKEYS: " << LEAFKEYS << " - " << sizeof(TreeType::LeafNode) << "\n";

  pool<root> pop;
  pobj_alloc_class_desc alloc_class;

  if (access(path.c_str(), F_OK) != 0) {
    std::cout << "Start creating trees, number: " << ArraySize << "\n";
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
    if (pop.root()->treeArray != nullptr) {
      treeArray = pop.root()->treeArray;
      for (int i = 0; i < ArraySize; ++i) {
        auto& a = *treeArray;
        hybridWrapper.recover(*a[i]);
      }
    } else {
      prepare(pop, alloc_class);
    }
  }
  pop.drain();  ///< wait for preparations to be persistent
  std::cout << "Finished creating trees, number: " << ArraySize << "\n";

  MyKey key;
  /* BENCHMARKING */
  for (auto _ : state) {
    state.PauseTiming();
    const auto treePos = std::rand() % ArraySize;
    const auto tree = (*treeArray)[treePos];
    auto& treeRef = *tree;
    state.ResumeTiming();

    benchmark::DoNotOptimize(treeRef);
    treeRef.scan([&key](const auto& k, const auto& v) { key = k; });
    benchmark::DoNotOptimize(treeRef);
  }

  // treeRef.printBranchNode(0, treeRef.rootNode.branch);
  std::cout << "Elements:" << ELEMENTS << "\n";
  pop.close();
  pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_TreeScan);
BENCHMARK_MAIN();

void prepare(pmem::obj::pool<root>& pop, pobj_alloc_class_desc alloc_class) {
  transaction::run(pop, [&] {
    pop.root()->treeArray = make_persistent<pmem::obj::array<pptr<TreeType>, ArraySize>>();
    treeArray = pop.root()->treeArray;
    for (int i = 0; i < ArraySize; ++i) {
      auto& a = *treeArray;
      a[i] = make_persistent<TreeType>(alloc_class);
      insert(a[i]);
    }
  });
}
