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

#include <chrono>

#include <libpmempool.h>
#include <libpmemobj++/pool.hpp>
using pmem::obj::pool;

#include "common.hpp"
#include "benchmark/benchmark.h"
#ifdef ENABLE_PCM
#include <cpucounters.h>
#endif

void prepare(const pptr<ListType> &list);

uint64_t pmmwrites = 0;
uint64_t pmmreads = 0;
uint64_t modBytes = 0;  ///< modified Bytes

/*========== Insert benchmark on skip list =======================================================*/
static void BM_PSkiplistInsert(benchmark::State &state) {
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

  std::cout << "Keys per Node: " << NODEKEYS << " - " << sizeof(ListType::SkipNode) << "\n";

  using NodeArray = pmem::obj::array<pptr<ListType::SkipNode>, ArraySize>;
  struct root {
    pptr<ListType> list;
    pptr<NodeArray> nodeArray;
  };

  pool<root> pop;
  pobj_alloc_class_desc alloc_class;

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class =
        pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.new.desc", ListType::AllocClass);
    transaction::run(pop, [&] { pop.root()->list = make_persistent<ListType>(alloc_class); });
  } else {
    LOG("Warning: " << path << " already exists");
    pmempool_rm(path.c_str(), 0);
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class =
        pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.new.desc", ListType::AllocClass);
    transaction::run(pop, [&] { pop.root()->list = make_persistent<ListType>(alloc_class); });
  }

  auto &list = pop.root()->list;
  prepare(list);
  auto &listRef = *list;
  list.flush(pop);
  auto &nodeArray = pop.root()->nodeArray;
  auto &node = listRef.head->forward[0];
  node.flush(pop);

  transaction::run(pop, [&] {
    nodeArray = make_persistent<NodeArray>();
    for (int i = 0; i < ArraySize; ++i) {
      auto &a = *nodeArray;
      a[i] = make_persistent<ListType::SkipNode>(allocation_flag::class_id(alloc_class.class_id),
                                                 *node);
    }
  });
  pop.drain();

  const auto reqVal = ValueType(TARGET_KEY, TARGET_KEY * 100, TARGET_KEY * 1.0);

#ifdef ENABLE_PCM
  SocketCounterState before_sstate;
  SocketCounterState after_sstate;
#endif

  /// Lambda function for measured part (needed twice)
  auto benchmark = [&pop, &listRef, &reqVal] (ListType::SkipNode* node) {

    auto &nodeRef = *node;
    benchmark::DoNotOptimize(nodeRef);
    listRef.insertInNode(node, TARGET_KEY, reqVal);
    pop.flush(node, TARGET_NODE_SIZE);
    pop.drain();
    benchmark::DoNotOptimize(nodeRef);
    benchmark::ClobberMemory();
  };

  /// BENCHMARK Writes
  if (pmmwrites == 0) {
    auto randNode = (*nodeArray)[0].get();

    //dbis::PersistEmulation::getBytesWritten();
#ifdef ENABLE_PCM
    before_sstate = getSocketCounterState(0);
#endif
    benchmark(randNode);
#ifdef ENABLE_PCM
    after_sstate = getSocketCounterState(0);
    pmmreads = getBytesReadFromPMM(before_sstate, after_sstate);
    pmmwrites = getBytesWrittenToPMM(before_sstate, after_sstate);
#endif
    //modBytes = dbis::PersistEmulation::getBytesWritten();
    *randNode = *node;  ///< reset the modified node
    pop.persist(randNode, TARGET_NODE_SIZE);
  }

  /// BENCHMARK Timing
  /// avoid output to stdout during benchmark
  std::cout.setstate(std::ios_base::failbit);
  std::srand(std::time(nullptr));
  for (auto _ : state) {
    const auto nodePos = std::rand() % ArraySize;
    auto randNode = (*nodeArray)[nodePos].get();
    auto &nodeRef = *randNode;

    auto start = std::chrono::high_resolution_clock::now();
    benchmark(randNode);
    auto end = std::chrono::high_resolution_clock::now();

    nodeRef = *node;  ///< reset the modified node
    pop.persist(randNode, TARGET_NODE_SIZE);
    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    state.SetIterationTime(elapsed_seconds.count());
  }

  /// Evaluation & cleanup
  // listRef.printList(0);
  std::cout.clear();
  std::cout << "Iterations:" << state.iterations() << '\n';
  std::cout << "PMM Reads:" << pmmreads << '\n';
  std::cout << "Writes:" << modBytes << '\n';
  std::cout << "PMM Writes:" << pmmwrites << '\n';
  std::cout << "Elements:" << NODEKEYS << '\n';

  pop.close();
  pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_PSkiplistInsert)->UseManualTime();
BENCHMARK_MAIN();

/* preparing inserts */
void prepare(const pptr<ListType> &list) {
  auto &listRef = *list;
  auto insertLoop = [&listRef](const auto start, const auto end) {
    for (auto j = start; j < end + 1; ++j) {
      auto tup = ValueType(j, j * 100, j * 1.0);
      listRef.insert(j, tup);
    }
  };
  switch (TARGET_KEY) {
    case 1 /*first*/:
      insertLoop(2, NODEKEYS);
      break;
    case NODEKEYS /*last*/:
      insertLoop(1, NODEKEYS - 1);
      break;
    case (NODEKEYS + 1) / 2 /*middle*/: {
                                          insertLoop(1, TARGET_KEY - 1);
                                          insertLoop(TARGET_KEY + 1, NODEKEYS);
                                        }
  }
}
