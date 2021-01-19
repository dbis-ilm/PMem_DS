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

#include <libpmemobj++/container/array.hpp>
#include "common.hpp"
#include "utils/PersistEmulation.hpp"
#ifdef ENABLE_PCM
#include <cpucounters.h>
#endif

void prepare(const persistent_ptr<TreeType> tree);

uint64_t pmmwrites = 0;
uint64_t pmmreads = 0;
uint64_t modBytes = 0;  ///< modified Bytes
constexpr auto ArraySize = 4 * L3 / 256;

/* Get benchmarks on Tree */
static void BM_TreeInsert(benchmark::State &state) {
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

  std::cout << "BRANCHKEYS: " << BRANCHKEYS << " -> "
            << sizeof(TreeType::BranchNode) << "\nLEAFKEYS: " << LEAFKEYS
            << " -> " << sizeof(TreeType::LeafNode) << "\n";

  struct root {
    pptr<TreeType> tree;
  };

  pool<root> pop;

  pobj_alloc_class_desc alloc_class;
  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>(
        "heap.alloc_class.new.desc", TreeType::AllocClass);
    transaction::run(pop, [&] {
      pop.root()->tree = make_persistent<TreeType>(alloc_class);
    });
  } else {
    LOG("Warning: " << path << " already exists");
    pmempool_rm(path.c_str(), 0);
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>(
        "heap.alloc_class.new.desc", TreeType::AllocClass);
    transaction::run(pop, [&] {
      pop.root()->tree = make_persistent<TreeType>(alloc_class);
    });
  }
  auto tree = pop.root()->tree;
  prepare(tree);
  auto &treeRef = *tree;
  tree.flush(pop);
  auto leaf = treeRef.rootNode.leaf;
  leaf.flush(pop);

  pptr<pmem::obj::array<pptr<TreeType::LeafNode>, ArraySize>> leafArray;
  transaction::run(pop, [&] {
    leafArray = make_persistent<pmem::obj::array<pptr<TreeType::LeafNode>, ArraySize>>();
    for (int i = 0; i < ArraySize; ++i) {
      auto &a = *leafArray;
      a[i] = make_persistent<TreeType::LeafNode>(
          allocation_flag::class_id(alloc_class.class_id), *leaf);
    }
  });
  pop.drain();

  const auto reqTup = MyTuple(KEYPOS, KEYPOS * 100, KEYPOS * 1.0);
  const auto pos = treeRef.lookupPositionInLeafNode(leaf, KEYPOS);
  // const auto pos = leaf->bits.get_ro().getFreeZero();

#ifdef ENABLE_PCM
  SocketCounterState before_sstate;
  SocketCounterState after_sstate;
#endif

  /// Lambda function for measured part (needed twice)
  auto benchmark = [&pop, &treeRef, &reqTup, &pos](
                       const pptr<TreeType::LeafNode> &leafNode) {
    auto &leafRef = *leafNode;
    benchmark::DoNotOptimize(*leafNode);
    // transaction::run(pop, [&] {
    treeRef.insertInLeafNodeAtPosition(leafNode, pos, KEYPOS, reqTup);
    //});
    pop.flush(leafRef.numKeys);
    // pop.flush(leafRef.bits);
    // pop.flush(&leafRef.bits.get_ro(),
    //           sizeof(leafRef.bits.get_ro()) + sizeof(leafRef.fp.get_ro()));
    // pop.flush(&leafRef.slot.get_ro(),
    //           sizeof(leafRef.slot.get_ro()) + sizeof(leafRef.bits.get_ro()));
    // pop.flush(&leafRef.keys.get_ro()[LEAFKEYS-1], sizeof(MyKey));
    // pop.flush(&leafRef.values.get_ro()[LEAFKEYS-1], sizeof(MyTuple));
    pop.flush(&leafRef.keys.get_ro()[pos], sizeof(MyKey) * (ELEMENTS - pos));
    pop.flush(&leafRef.values.get_ro()[pos], sizeof(MyTuple) * (ELEMENTS - pos));
    pop.drain();
    // leafNode.persist(pop);

    benchmark::DoNotOptimize(*leafNode);
    benchmark::ClobberMemory();
  };

  /// BENCHMARK Writes
  if (pmmwrites == 0) {
    auto &leafNode = (*leafArray)[0];

    dbis::PersistEmulation::getBytesWritten();
#ifdef ENABLE_PCM
    before_sstate = getSocketCounterState(1);
#endif
    benchmark(leafNode);
#ifdef ENABLE_PCM
    after_sstate = getSocketCounterState(1);
    pmmreads = getBytesReadFromPMM(before_sstate, after_sstate);
    pmmwrites = getBytesWrittenToPMM(before_sstate, after_sstate);
#endif
    modBytes = dbis::PersistEmulation::getBytesWritten();
    *leafNode = *leaf;  ///< reset the modified node
    leafNode.flush(pop);
    // pop.flush((char *)((uintptr_t)&*leafNode - 64), 64); //< flush header
    pop.drain();
  }

  /// BENCHMARK Timing
  /// avoid output to stdout during benchmark
  std::cout.setstate(std::ios_base::failbit);
  std::srand(std::time(nullptr));
  for (auto _ : state) {
    const auto leafNodePos = std::rand() % ArraySize;
    auto leafNode = (*leafArray)[leafNodePos];
    auto &leafRef = *leafNode;

    auto start = std::chrono::high_resolution_clock::now();
    benchmark(leafNode);
    auto end = std::chrono::high_resolution_clock::now();

    leafRef = *leaf;  ///< reset the modified node
    leafNode.flush(pop);
    // pop.flush((char *)((uintptr_t)&*leafNode - 64), 64); //< flush header
    pop.drain();
    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    state.SetIterationTime(elapsed_seconds.count());
  }

  /// Evaluation & cleanup
  // treeRef.printLeafNode(0, leafNode);
  std::cout.clear();
  std::cout << "Iterations:" << state.iterations() << '\n';
  std::cout << "PMM Reads:" << pmmreads << '\n';
  std::cout << "Writes:" << modBytes << '\n';
  std::cout << "PMM Writes:" << pmmwrites << '\n';
  std::cout << "Elements:" << ELEMENTS << '\n';

  pop.close();
  pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_TreeInsert)->UseManualTime();
BENCHMARK_MAIN();

/* preparing inserts */
void prepare(const persistent_ptr<TreeType> tree) {
  auto &treeRef = *tree;
  auto insertLoop = [&treeRef](const auto start, const auto end) {
    for (auto j = start; j < end + 1; ++j) {
      auto tup = MyTuple(j, j * 100, j * 1.0);
      treeRef.insert(j, tup);
    }
  };
  switch (KEYPOS) {
    case 1 /*first*/:
      insertLoop(2, LEAFKEYS);
      break;
    case ELEMENTS /*last*/:
      insertLoop(1, LEAFKEYS - 1);
      break;
    case (ELEMENTS + 1) / 2 /*middle*/: {
      insertLoop(1, KEYPOS - 1);
      insertLoop(KEYPOS + 1, LEAFKEYS);
    }
  }
}
