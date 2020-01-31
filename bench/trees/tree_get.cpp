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

#include "common.hpp"
#include <cstdlib>
#include <ctime>
#include <libpmemobj++/container/array.hpp>

constexpr auto L3 = 30 * 1024 * 1024;
constexpr auto ArraySize = L3 / TARGET_LEAF_SIZE;

/* Get benchmarks on Tree */
static void BM_TreeGet(benchmark::State &state) {
  std::cout << "BRANCHKEYS: " << BRANCHKEYS << " - " << sizeof(TreeType::BranchNode)
            << "\nLEAFKEYS: " << LEAFKEYS << " - " << sizeof(TreeType::LeafNode) << "\n";

  struct root {
    persistent_ptr<TreeType> tree;
  };

  pool<root> pop;
  pobj_alloc_class_desc alloc_class;

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class =
        pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.128.desc", TreeType::AllocClass);
    transaction::run(pop, [&] { pop.root()->tree = make_persistent<TreeType>(alloc_class); });
  } else {
    LOG("Warning: " << path << " already exists");
    pmempool_rm(path.c_str(), 0);
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class =
        pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.128.desc", TreeType::AllocClass);
    transaction::run(pop, [&] { pop.root()->tree = make_persistent<TreeType>(alloc_class); });
  }


/*
  struct alignas(64) DRAMLeafNode {
    DRAMLeafNode() : numKeys(0), nextLeaf(nullptr), prevLeaf(nullptr) {}
    DRAMLeafNode(const TreeType::LeafNode &other)
        : numKeys(other.numKeys.get_ro()),
          nextLeaf(nullptr),
          prevLeaf(nullptr),
          keys(other.keys.get_ro())
    // values(other.values.get_ro()),
    {}

    unsigned int numKeys;             ///< the number of currently stored keys - 0x10
    DRAMLeafNode *nextLeaf;               ///< pointer to the subsequent sibling   - 0x18
    DRAMLeafNode *prevLeaf;               ///< pointer to the preceding sibling    - 0x28
    char padding[44];                 ///< padding to align keys to 64 byte    - 0x38
    std::array<MyKey, LEAFKEYS> keys;      ///< the actual keys                     - 0x40
    std::array<MyTuple, LEAFKEYS> values;  ///< the actual values
  };*/

  const auto tree = pop.root()->tree;
  insert(tree, 0);
  auto &treeRef = *tree;
  auto leaf = treeRef.rootNode.leaf;

  pptr<pmem::obj::array<pptr<TreeType::LeafNode>, ArraySize>> leafArray;
  transaction::run(pop, [&] {
    leafArray = make_persistent<pmem::obj::array<pptr<TreeType::LeafNode>, ArraySize>>();
    for (int i = 0; i < ArraySize; ++i) {
      auto &a = *leafArray;
      a[i] = make_persistent<TreeType::LeafNode>(allocation_flag::class_id(alloc_class.class_id),
                                                 *leaf);
    }
  });
  pop.drain();

  /*
  std::unique_ptr<std::array<DRAMLeafNode *, ArraySize>> leafArray(
      new std::array<DRAMLeafNode *, ArraySize>);
  for (int i = 0; i < ArraySize; ++i) {
    auto &a = *leafArray;
    a[i] =  new DRAMLeafNode(*leaf);
  }*/

  std::srand(std::time(nullptr));

  /* BENCHMARKING */
  for (auto _ : state) {
    state.PauseTiming();
    const auto leafNodePos = std::rand() % ArraySize;
    const auto leafNode = (*leafArray)[leafNodePos];
    const auto key = (long long unsigned int)KEYPOS;
    state.ResumeTiming();
    benchmark::DoNotOptimize(*leafNode);
    treeRef.lookupPositionInLeafNode(leafNode, KEYPOS);
    // dbis::binarySearch<false>(leaf->keys.get_ro(), 0, leaf->numKeys.get_ro()-1, key);
    benchmark::DoNotOptimize(*leafNode);
  }

  // treeRef.printBranchNode(0, treeRef.rootNode.branch);
  std::cout << "Elements:" << ELEMENTS << '\n';

  transaction::run(pop, [&] { delete_persistent<TreeType>(tree); });
  pop.close();
  pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_TreeGet);
BENCHMARK_MAIN();
