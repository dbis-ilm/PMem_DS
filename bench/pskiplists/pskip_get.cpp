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

#include <chrono>

#include <libpmempool.h>
#include <libpmemobj++/pool.hpp>
using pmem::obj::pool;

#include "common.hpp"
#include "benchmark/benchmark.h"

/*========== Get benchmark on skip list ==========================================================*/
static void BM_PSkiplistGet(benchmark::State &state) {
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
  auto &listRef = *list;
  for (auto i = 0u; i < NODEKEYS; ++i) {
    auto tup = ValueType(i + 1, (i + 1) * 100, (i + 1) * 1.0);
    listRef.insert(i + 1, tup);
  }
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

  std::srand(std::time(nullptr));

  /// BENCHMARKING
  for (auto _ : state) {
    const auto nodePos = std::rand() % ArraySize;
    const auto randNode = (*nodeArray)[nodePos].get();
    const auto &nodeRef = *randNode;
    ValueType value;

    auto start = std::chrono::high_resolution_clock::now();
    benchmark::DoNotOptimize(nodeRef);
    listRef.searchInNode(randNode, TARGET_KEY, value);
    benchmark::DoNotOptimize(nodeRef);
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    state.SetIterationTime(elapsed_seconds.count());
  }

  /// Clean up
  // listRef.printList(0);
  pop.close();
  pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_PSkiplistGet)->UseManualTime();
BENCHMARK_MAIN();
