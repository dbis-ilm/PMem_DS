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
  pptr<TreeType::LeafNode> target;
  pptr<ArrayType> sourceArray;
  pptr<ArrayType> targetArray;
};

pptr<TreeType> tree;
pptr<ArrayType> sourceArray;
pptr<ArrayType> targetArray;
pptr<TreeType::LeafNode> leaf;
pptr<TreeType::LeafNode> leaf2;

void prepare(pmem::obj::pool<root> &pop, pobj_alloc_class_desc alloc_class);
void prepare(const pptr<TreeType> &tree, const pptr<TreeType::LeafNode> &leaf);

/* Get benchmarks on Tree */
static void BM_TreeMerge(benchmark::State &state) {
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
      sourceArray = pop.root()->sourceArray;
      targetArray = pop.root()->targetArray;
      leaf = tree->rootNode.leaf;
      leaf2 = pop.root()->target;
    } else {
      prepare(pop, alloc_class);
    }
  }
  pop.drain();
  auto &treeRef = *tree;

#ifdef ENABLE_PCM
  SocketCounterState before_sstate;
  SocketCounterState after_sstate;
#endif

	/// Lambda function for measured part (needed twice)
  auto benchmark = [&pop, &treeRef](
      pptr<TreeType::LeafNode> &sourceNode,
      pptr<TreeType::LeafNode> &targetNode) {
    auto &sourceRef = *sourceNode;
    auto &targetRef = *targetNode;

    benchmark::DoNotOptimize(*sourceNode);
    benchmark::DoNotOptimize(*targetNode);
    // transaction::run(pop, [&] {
    treeRef.mergeLeafNodes(targetNode, sourceNode);
    // delete_persistent<TreeType::LeafNode>(sourceNode);
    // });

    pop.flush(targetRef.numKeys);
    // pop.flush(targetRef.bits);
    // pop.flush(&targetRef.bits.get_ro(),
    //           sizeof(targetRef.bits.get_ro()) + sizeof(targetRef.fp.get_ro()));
    // pop.flush(&targetRef.slot.get_ro(),
    //           sizeof(targetRef.slot.get_ro()) + sizeof(targetRef.bits.get_ro()));
    pop.flush(&targetRef.keys.get_ro()[(LEAFKEYS + 1) / 2], sizeof(MyKey) * (LEAFKEYS - 1) / 2);
    pop.flush(&targetRef.values.get_ro()[(LEAFKEYS + 1) / 2], sizeof(MyTuple) * (LEAFKEYS - 1) / 2);
    // targetNode.flush(pop);
    pop.drain();

    benchmark::DoNotOptimize(*sourceNode);
    benchmark::DoNotOptimize(*targetNode);
    benchmark::ClobberMemory();
	};

  /// BENCHMARK Writes
  if (pmmwrites == 0) {
    auto sourceNode = (*sourceArray)[0];
    auto targetNode = (*targetArray)[0];
    dbis::PersistEmulation::getBytesWritten();
#ifdef ENABLE_PCM
    before_sstate = getSocketCounterState(1);
#endif
    benchmark(sourceNode, targetNode);
#ifdef ENABLE_PCM
    after_sstate = getSocketCounterState(1);
    pmmreads = getBytesReadFromPMM(before_sstate, after_sstate);
    pmmwrites = getBytesWrittenToPMM(before_sstate, after_sstate);
#endif
    modBytes = dbis::PersistEmulation::getBytesWritten();
    *targetNode = *leaf2; ///< reset the modified node
    // (*sourceArray)[0] = treeRef.newLeafNode(leaf);
    targetNode.persist(pop);
  }

  /// BENCHMARKING
  std::cout.setstate(std::ios_base::failbit);
  std::srand(std::time(nullptr));
  for (auto _ : state) {
    const auto leafNodePos = std::rand() % ArraySize;
    auto sourceNode = (*sourceArray)[leafNodePos];
    auto targetNode = (*targetArray)[leafNodePos];
    auto &sourceRef = *sourceNode;
    auto &targetRef = *targetNode;

    auto start = std::chrono::high_resolution_clock::now();
    benchmark(sourceNode, targetNode);
    auto end = std::chrono::high_resolution_clock::now();

    *targetNode = *leaf2; ///< reset the modified node
    targetNode.persist(pop);
    // (*sourceArray)[leafNodePos] = treeRef.newLeafNode(leaf);
    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    state.SetIterationTime(elapsed_seconds.count());
  }

  // treeRef.printLeafNode(0, leaf);
  std::cout.clear();
  std::cout << "Iterations:" << state.iterations() << '\n';
  std::cout << "PMM Reads:" << pmmreads << '\n';
  std::cout << "Writes:" << modBytes << '\n';
  std::cout << "PMM Writes:" << pmmwrites << '\n';
  std::cout << "Elements:" << ELEMENTS << '\n';

  pop.close();
  pmempool_rm(path.c_str(), 0);
}
BENCHMARK(BM_TreeMerge)->UseManualTime();

BENCHMARK_MAIN();

/**
 * Prepare multiple pairs of nodes (source and target) for merging
 */
void prepare(pmem::obj::pool<root> &pop, pobj_alloc_class_desc alloc_class) {
  transaction::run(pop, [&] {
      auto root = pop.root();
      root->tree = make_persistent<TreeType>(alloc_class);
      root->sourceArray = make_persistent<ArrayType>();
      root->targetArray = make_persistent<ArrayType>();
      root->target = root->tree->newLeafNode();
      tree = pop.root()->tree;
      auto &treeRef = *tree;
      sourceArray = pop.root()->sourceArray;
      targetArray = pop.root()->targetArray;
      leaf2 = pop.root()->target;
      prepare(tree, leaf2);
      leaf = treeRef.rootNode.leaf;

      for (int i = 0; i < ArraySize; ++i) {
        auto &a = *sourceArray;
        auto &b = *targetArray;
        a[i] = treeRef.newLeafNode(leaf);
        b[i] = treeRef.newLeafNode(leaf2);
      }
    });
}

/* preparing inserts */
void prepare(const pptr<TreeType> &tree, const pptr<TreeType::LeafNode> &leaf) {
  auto &treeRef = *tree;
  auto j = 1u;
  for (; j < (LEAFKEYS + 3) / 2; ++j) {
    auto tup = MyTuple(j, j * 100, j * 1.0);
    treeRef.insert(j, tup);
  }
  for (; j < LEAFKEYS; ++j) {
    auto tup = MyTuple(j, j * 100, j * 1.0);
    TreeType::SplitInfo splitInfo;
    treeRef.insertInLeafNode(leaf, j, tup, &splitInfo);
  }
}
