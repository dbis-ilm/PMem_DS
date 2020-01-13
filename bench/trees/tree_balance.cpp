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

#include "common.hpp"
#include "utils/PersistEmulation.hpp"

constexpr auto L3 = 30 * 1024 * 1024;
constexpr auto ArraySize = L3 / TARGET_LEAF_SIZE;
using ArrayType = pmem::obj::array<pptr<TreeType::LeafNode>, ArraySize>;

struct root {
  pptr<TreeType> tree;
  pptr<TreeType::LeafNode> receiver;
  pptr<ArrayType> donorArray;
  pptr<ArrayType> receiverArray;
};

pptr<TreeType> tree;
pptr<ArrayType> donorArray;
pptr<ArrayType> receiverArray;
pptr<TreeType::LeafNode> leaf;
pptr<TreeType::LeafNode> leaf2;

void prepare(pmem::obj::pool<root> &pop, pobj_alloc_class_desc alloc_class);
void prepare(const pptr<TreeType> &tree, const pptr<TreeType::LeafNode> &leaf2);

/* Get benchmarks on Tree */
static void BM_TreeBalance(benchmark::State &state) {
  std::cout << "BRANCHKEYS: " << BRANCHKEYS << "\nLEAFKEYS: " << LEAFKEYS << "\n";

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
    auto rootTree = pop.root()->tree;
    if (rootTree != nullptr) tree = rootTree;
    else prepare(pop, alloc_class);
  }

  auto root = pop.root();
  auto &treeRef = *tree;
  donorArray = root->donorArray;
  receiverArray = root->receiverArray;
  leaf = treeRef.rootNode.leaf;
  leaf2 = root->receiver;
  pop.drain();

  /* BENCHMARKING */
  for (auto _ : state) {
    state.PauseTiming();
    std::cout.setstate(std::ios_base::failbit);
    const auto leafNodePos = std::rand() % ArraySize;
    auto donorNode = (*donorArray)[leafNodePos];
    auto receiverNode = (*receiverArray)[leafNodePos];
    auto &donorRef = *donorNode;
    auto &receiverRef = *receiverNode;
    dbis::PersistEmulation::getBytesWritten();
    state.ResumeTiming();

    benchmark::DoNotOptimize(*donorNode);
    benchmark::DoNotOptimize(*receiverNode);
    // transaction::run(pop, [&] {
    treeRef.balanceLeafNodes(donorNode, receiverNode);
    // });
    benchmark::DoNotOptimize(*donorNode);
    benchmark::DoNotOptimize(*receiverNode);

    constexpr auto Balanced = ((LEAFKEYS - 1) / 2 + LEAFKEYS) / 2;
    constexpr auto SortedMoves = (LEAFKEYS - 1) / 2 + LEAFKEYS - Balanced;
    constexpr auto UnsortedMoves = LEAFKEYS - Balanced;
    pop.flush(donorRef.numKeys);
    // pop.flush(donorRef.bits);
    // pop.flush(&donorRef.slot.get_ro(),
    //           sizeof(donorRef.slot.get_ro()) + sizeof(donorRef.bits.get_ro()));
    // pop.flush(&donorRef.keys.get_ro(), sizeof(MyKey) * UnsortedMoves);
    // pop.flush(&donorRef.values.get_ro(), sizeof(MyTuple) * UnsortedMoves);
    // pop.flush(&donorRef.keys.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyKey) * UnsortedMoves);
    // pop.flush(&donorRef.values.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyTuple) * UnsortedMoves);

    pop.flush(receiverRef.numKeys);
    // pop.flush(receiverRef.bits);
    // pop.flush(&receiverRef.bits.get_ro(),
    //           sizeof(receiverRef.bits.get_ro()) + sizeof(receiverRef.fp.get_ro()));
    // pop.flush(&receiverRef.slot.get_ro(),
    //           sizeof(receiverRef.slot.get_ro()) + sizeof(receiverRef.bits.get_ro()));

    pop.flush(&receiverRef.keys.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyKey) * SortedMoves);
    pop.flush(&receiverRef.values.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyTuple) * SortedMoves);
    // pop.flush(&receiverRef.keys.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyKey) * UnsortedMoves);
    // pop.flush(&receiverRef.values.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyTuple) * UnsortedMoves);
    // receiverNode.flush(pop);
    pop.drain();

    state.PauseTiming();
    *donorNode = *leaf;      ///< reset the modified node
    *receiverNode = *leaf2;  ///< reset the modified node
    state.ResumeTiming();
  }

  // treeRef.printLeafNode(0, leaf);
  std::cout.clear();
  std::cout << "Writes:" << dbis::PersistEmulation::getBytesWritten() << '\n';
  std::cout << "Elements:" << ELEMENTS << '\n';
  transaction::run(pop, [&] {
    delete_persistent<TreeType>(tree);
    delete_persistent<ArrayType>(donorArray);
    delete_persistent<ArrayType>(receiverArray);
  });
  pop.close();
  pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_TreeBalance);
BENCHMARK_MAIN();

void prepare(pmem::obj::pool<root> &pop, pobj_alloc_class_desc alloc_class) {
  transaction::run(pop, [&] {
    auto root = pop.root();
    root->tree = make_persistent<TreeType>(alloc_class);
    root->donorArray = make_persistent<ArrayType>();
    root->receiverArray = make_persistent<ArrayType>();
    root->receiver = root->tree->newLeafNode();

    tree = root->tree;
    auto &treeRef = *tree;
    prepare(tree, root->receiver);

    for (int i = 0; i < ArraySize; ++i) {
      auto &a = *root->donorArray;
      auto &b = *root->receiverArray;
      a[i] = treeRef.newLeafNode(treeRef.rootNode.leaf);
      b[i] = treeRef.newLeafNode(root->receiver);
    }
  });
}

/* preparing inserts */
void prepare(const pptr<TreeType> &tree, const pptr<TreeType::LeafNode> &leaf2) {
  auto &treeRef = *tree;
  auto j = 1u;
  if constexpr (MOVE_TO_RIGHT) {
    for (; j < LEAFKEYS + 1; ++j) {
      auto tup = MyTuple(j, j * 100, j * 1.0);
      treeRef.insert(j, tup);
    }
    for (; j < (3 * LEAFKEYS + 1) / 2; ++j) {
      auto tup = MyTuple(j, j * 100, j * 1.0);
      TreeType::SplitInfo splitInfo;
      treeRef.insertInLeafNode(leaf2, j, tup, &splitInfo);
    }
  } else {
    for (; j < (LEAFKEYS + 1) / 2; ++j) {
      auto tup = MyTuple(j, j * 100, j * 1.0);
      TreeType::SplitInfo splitInfo;
      treeRef.insertInLeafNode(leaf2, j, tup, &splitInfo);
    }
    for (; j < (3 * LEAFKEYS + 1) / 2; ++j) {
      auto tup = MyTuple(j, j * 100, j * 1.0);
      treeRef.insert(j, tup);
    }
  }
}