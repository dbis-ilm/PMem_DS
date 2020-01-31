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

#include <libpmemobj++/container/array.hpp>
#include "common.hpp"
#include "utils/PersistEmulation.hpp"

void prepare(const persistent_ptr<TreeType> tree);

/* Get benchmarks on Tree */
static void BM_TreeErase(benchmark::State &state) {
  std::cout << "BRANCHKEYS: " << BRANCHKEYS << " -> " << sizeof(TreeType::BranchNode)
            << "\nLEAFKEYS: " << LEAFKEYS << " -> " << sizeof(TreeType::LeafNode) << "\n";

  struct root {
    persistent_ptr<TreeType> tree;
  };

  pool<root> pop;

  pobj_alloc_class_desc alloc_class;
  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.new.desc",
                                                            TreeType::AllocClass);
    transaction::run(pop, [&] { pop.root()->tree = make_persistent<TreeType>(alloc_class); });
  } else {
    LOG("Warning: " << path << " already exists");
    pmempool_rm(path.c_str(), 0);
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.new.desc",
                                                            TreeType::AllocClass);
    transaction::run(pop, [&] { pop.root()->tree = make_persistent<TreeType>(alloc_class); });
  }

  const auto tree = pop.root()->tree;
  prepare(tree);
  auto &treeRef = *tree;
  auto leaf = treeRef.rootNode.leaf;
  constexpr auto L3 = 30 * 1024 * 1024;
  constexpr auto ArraySize = L3 / TARGET_LEAF_SIZE;

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

  /* BENCHMARKING */
  for (auto _ : state) {
    state.PauseTiming();
    std::cout.setstate(std::ios_base::failbit);
    const auto leafNodePos = std::rand() % ArraySize;
    auto leafNode = (*leafArray)[leafNodePos];
    const auto pos = treeRef.lookupPositionInLeafNode(leafNode, KEYPOS);
    // const auto pos = dbis::BitOperations::getFreeZero(leafNode->bits.get_ro());
    auto &leafRef = *leafNode;
    dbis::PersistEmulation::getBytesWritten();
    state.ResumeTiming();
    benchmark::DoNotOptimize(*leafNode);

    // transaction::run(pop, [&] {
    treeRef.eraseFromLeafNodeAtPosition(leafNode, pos, KEYPOS);
    //});

    pop.flush(leafRef.numKeys);
    // pop.flush(leafRef.bits);
    // pop.flush(&leafRef.slot.get_ro(),
    //           sizeof(leafRef.slot.get_ro()) + sizeof(leafRef.bits.get_ro()));
    // pop.flush(&leafRef.keys.get_ro()[pos], sizeof(MyKey));
    // pop.flush(&leafRef.values.get_ro()[pos], sizeof(MyTuple));
    pop.flush(&leafRef.keys.get_ro()[pos], (LEAFKEYS - 1 - pos) * sizeof(MyKey));
    pop.flush(&leafRef.values.get_ro()[pos], (LEAFKEYS - 1 - pos) * sizeof(MyTuple));
    pop.drain();
    // leaf.persist(pop);

    benchmark::DoNotOptimize(*leaf);
    state.PauseTiming();
    *leafNode = *leaf;  ///< reset the modified node
    state.ResumeTiming();
  }

  // treeRef.printLeafNode(0, leaf);
  std::cout.clear();
  std::cout << "Writes:" << dbis::PersistEmulation::getBytesWritten() << '\n';
  std::cout << "Elements:" << ELEMENTS << '\n';

  transaction::run(pop, [&] { delete_persistent<TreeType>(tree); });
  pop.close();
  pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_TreeErase);
BENCHMARK_MAIN();

/* preparing inserts */
void prepare(const persistent_ptr<TreeType> tree) {
  auto &treeRef = *tree;
  for (auto j = 1; j < LEAFKEYS + 1; ++j) {
    auto tup = MyTuple(j, j * 100, j * 1.0);
    treeRef.insert(j, tup);
  }
}
