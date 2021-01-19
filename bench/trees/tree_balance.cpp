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

#include "common.hpp"
#include "utils/PersistEmulation.hpp"
#ifdef ENABLE_PCM
#include <cpucounters.h>
#endif

uint64_t pmmwrites = 0;
uint64_t pmmreads = 0;
uint64_t modBytes = 0;  ///< modified Bytes
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
#ifdef ENABLE_PCM
  PCM *pcm_ = PCM::getInstance();
  auto s = pcm_->program();
  if (s != PCM::Success) {
    std::cerr << "Error creating PCM instance: " << s << std::endl;
    if (s == PCM::PMUBusy)
      pcm_->resetPMU();
    else
      exit(0);
  }
#endif

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

#ifdef ENABLE_PCM
  SocketCounterState before_sstate;
  SocketCounterState after_sstate;
#endif

  /// Lambda function for measured part (needed twice)
  auto benchmark = [&pop, &treeRef](
      pptr<TreeType::LeafNode> &donorNode,
      pptr<TreeType::LeafNode> &receiverNode) {
    auto &donorRef = *donorNode;
    auto &receiverRef = *receiverNode;

    benchmark::DoNotOptimize(*donorNode);
    benchmark::DoNotOptimize(*receiverNode);
    // transaction::run(pop, [&] {
    treeRef.balanceLeafNodes(donorNode, receiverNode);
    // });

    constexpr auto Balanced = ((LEAFKEYS - 1) / 2 + LEAFKEYS) / 2;
    constexpr auto SortedMoves = (LEAFKEYS - 1) / 2 + LEAFKEYS - Balanced;
    constexpr auto UnsortedMoves = LEAFKEYS - Balanced;
    pop.flush(donorRef.numKeys);
    // pop.flush(donorRef.bits);
    // pop.flush(&donorRef.slot.get_ro(),
    //           sizeof(donorRef.slot.get_ro()) + sizeof(donorRef.bits.get_ro()));
    // pop.flush(&donorRef.keys.get_ro(), sizeof(MyKey) * SortedMoves);
    // pop.flush(&donorRef.values.get_ro(), sizeof(MyTuple) * SortedMoves);
    // pop.flush(&donorRef.keys.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyKey) * UnsortedMoves);
    // pop.flush(&donorRef.values.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyTuple) * UnsortedMoves);
    // donorNode.flush(pop);

    pop.flush(receiverRef.numKeys);
    // pop.flush(receiverRef.bits);
    // pop.flush(&receiverRef.bits.get_ro(),
    //          sizeof(receiverRef.bits.get_ro()) + sizeof(receiverRef.fp.get_ro()));
    // pop.flush(&receiverRef.slot.get_ro(),
    //           sizeof(receiverRef.slot.get_ro()) + sizeof(receiverRef.bits.get_ro()));
    pop.flush(&receiverRef.keys.get_ro(), sizeof(MyKey) * SortedMoves);
    pop.flush(&receiverRef.values.get_ro(), sizeof(MyTuple) * SortedMoves);
    // pop.flush(&receiverRef.keys.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyKey) * UnsortedMoves);
    // pop.flush(&receiverRef.values.get_ro()[(LEAFKEYS - 1) / 2], sizeof(MyTuple) * UnsortedMoves);
    // receiverNode.flush(pop);
    pop.drain();

    benchmark::DoNotOptimize(*donorNode);
    benchmark::DoNotOptimize(*receiverNode);
    benchmark::ClobberMemory();
  };

  /// BENCHMARK Writes
  if (pmmwrites == 0) {
    auto donorNode = (*donorArray)[0];
    auto receiverNode = (*receiverArray)[0];

    dbis::PersistEmulation::getBytesWritten();
#ifdef ENABLE_PCM
    before_sstate = getSocketCounterState(1);
#endif
    benchmark(donorNode, receiverNode);
#ifdef ENABLE_PCM
    after_sstate = getSocketCounterState(1);
    pmmreads = getBytesReadFromPMM(before_sstate, after_sstate);
    pmmwrites = getBytesWrittenToPMM(before_sstate, after_sstate);
#endif
    modBytes = dbis::PersistEmulation::getBytesWritten();
    *donorNode = *leaf;      ///< reset the modified node
    *receiverNode = *leaf2;  ///< reset the modified node
    donorNode.flush(pop);
    receiverNode.flush(pop);
    pop.drain();
  }

  /// BENCHMARKING
  std::cout.setstate(std::ios_base::failbit);
  std::srand(std::time(nullptr));
  for (auto _ : state) {
    const auto leafNodePos = std::rand() % ArraySize;
    auto donorNode = (*donorArray)[leafNodePos];
    auto receiverNode = (*receiverArray)[leafNodePos];
    auto &donorRef = *donorNode;
    auto &receiverRef = *receiverNode;

    auto start = std::chrono::high_resolution_clock::now();
    benchmark(donorNode, receiverNode);
    auto end = std::chrono::high_resolution_clock::now();
    // treeRef.printLeafNode(0, donorNode);
    // treeRef.printLeafNode(0, receiverNode);

    donorRef = *leaf;      ///< reset the modified node
    receiverRef = *leaf2;  ///< reset the modified node
    donorNode.flush(pop);
    receiverNode.flush(pop);
    pop.drain();
    auto elapsed_seconds =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    state.SetIterationTime(elapsed_seconds.count());
  }

  std::cout.clear();
  // treeRef.printLeafNode(0, leaf);
  // treeRef.printLeafNode(0, leaf2);
  std::cout << "Iterations:" << state.iterations() << '\n';
  std::cout << "PMM Reads:" << pmmreads << '\n';
  std::cout << "Writes:" << modBytes << '\n';
  std::cout << "PMM Writes:" << pmmwrites << '\n';
  std::cout << "Elements:" << ELEMENTS << '\n';

  pop.close();
  pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_TreeBalance)->UseManualTime();
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
