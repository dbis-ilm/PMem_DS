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
#include "utils/PersistEmulation.hpp"

constexpr auto L3 = 30 * 1024 * 1024;
constexpr auto ArraySize = L3 / 512;  // TARGET_LEAF_SIZE;

struct root {
  pptr<TreeType> tree;
  pptr<pmem::obj::array<pptr<TreeType::LeafNode>, ArraySize>> leafArray;
};

pptr<TreeType> tree;
pptr<pmem::obj::array<pptr<TreeType::LeafNode>, ArraySize>> leafArray;
pptr<TreeType::LeafNode> leaf;

void prepare(pmem::obj::pool<root> &pop, pobj_alloc_class_desc alloc_class);
void prepare(const persistent_ptr<TreeType> tree);

/* Get benchmarks on Tree */
static void BM_TreeSplit(benchmark::State &state) {
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
      leafArray = pop.root()->leafArray;
      leaf = tree->rootNode.leaf;
    } else {
      prepare(pop, alloc_class);
    }
  }
  pop.drain();

  const auto reqTup = MyTuple(ELEMENTS + 1, (ELEMENTS + 1) * 100, (ELEMENTS + 1) * 1.0);
  TreeType::SplitInfo splitInfo;
  bool split;
  auto &treeRef = *tree;

  /* BENCHMARKING */
  for (auto _ : state) {
    state.PauseTiming();
    std::cout.setstate(std::ios_base::failbit);
    const auto leafNodePos = std::rand() % ArraySize;
    auto leafNode = (*leafArray)[leafNodePos];
    auto &leafRef = *leafNode;
    dbis::PersistEmulation::getBytesWritten();
    state.ResumeTiming();

    benchmark::DoNotOptimize(*leafNode);
    // transaction::run(pop, [&] {
    treeRef.splitLeafNode(leafNode, &splitInfo);
    // leafRef.nextLeaf = treeRef.newLeafNode(leafNode);
    // });
    benchmark::DoNotOptimize(*leafNode);
    benchmark::DoNotOptimize(*leafRef.nextLeaf);

    pop.flush(leafRef.numKeys);
    // pop.flush(leafRef.bits);
    // pop.flush(&leafRef.slot.get_ro(),
    //          sizeof(leafRef.slot.get_ro()) + sizeof(leafRef.bits.get_ro()));
    // pop.flush(&leafRef.keys.get_ro(), sizeof(MyKey) * (LEAFKEYS)/2);
    // pop.flush(&leafRef.values.get_ro(), sizeof(MyTuple) * (LEAFKEYS)/2);

    pop.flush(leafRef.nextLeaf->numKeys);
    // pop.flush(leafRef.nextLeaf->bits);
    // pop.flush(&leafRef.nextLeaf->bits.get_ro(), + sizeof(leafRef.bits.get_ro())+
    //  sizeof(leafRef.fp.get_ro()));
    // pop.flush(&leafRef.nextLeaf->slot.get_ro(), sizeof(leafRef.slot.get_ro()) +
    // sizeof(leafRef.bits.get_ro()));
    pop.flush(&leafRef.nextLeaf->keys.get_ro(), sizeof(MyKey) * (LEAFKEYS + 1) / 2);
    pop.flush(&leafRef.nextLeaf->values.get_ro(), sizeof(MyTuple) * (LEAFKEYS + 1) / 2);
    // leafRef.nextLeaf.flush(pop);
    pop.drain();

    state.PauseTiming();
    treeRef.depth = 0;
    treeRef.rootNode = leaf;
    *leafNode = *leaf;  ///< reset the modified node
    treeRef.deleteLeafNode(leafRef.nextLeaf);
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

BENCHMARK(BM_TreeSplit);
BENCHMARK_MAIN();

/**
 * Prepare multiple nodes for splitting
 */
void prepare(pmem::obj::pool<root> &pop, pobj_alloc_class_desc alloc_class) {
  transaction::run(pop, [&] {
    pop.root()->tree = make_persistent<TreeType>(alloc_class);
    pop.root()->leafArray =
        make_persistent<pmem::obj::array<pptr<TreeType::LeafNode>, ArraySize>>();
    tree = pop.root()->tree;
    leafArray = pop.root()->leafArray;
    prepare(tree);
    leaf = tree->rootNode.leaf;

    for (int i = 0; i < ArraySize; ++i) {
      auto &a = *leafArray;
      a[i] = make_persistent<TreeType::LeafNode>(allocation_flag::class_id(alloc_class.class_id),
                                                 *leaf);
    }
  });
}

/* preparing inserts */
void prepare(const persistent_ptr<TreeType> tree) {
  auto &treeRef = *tree;
  for (auto j = 1; j < LEAFKEYS + 1; ++j) {
    auto tup = MyTuple(j, j * 100, j * 1.0);
    treeRef.insert(j, tup);
  }
}
