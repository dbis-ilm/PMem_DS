/*
 * Copyright (C) 2017-2019 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of our NVM-based Data Structure Repository.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <filesystem>
#include "common.hpp"

void prepare(const persistent_ptr<TreeType> tree);

/* Get benchmarks on Tree */
static void BM_TreeInsert(benchmark::State &state) {
  std::cout << "BRANCHKEYS: " << BRANCHKEYS
    << "\nLEAFKEYS: " << LEAFKEYS
    << "\n";
  struct root {
    persistent_ptr<TreeType> tree;
  };

  pool<root> pop;

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    transaction::run(pop, [&] {
      pop.root()->tree = make_persistent<TreeType>();
    });
  } else {
    LOG("Warning: " << path << " already exists");
    auto n = std::filesystem::remove_all(path);
    pop = pool<root>::create(path, LAYOUT);
    transaction::run(pop, [&] {
      delete_persistent<TreeType>(pop.root()->tree);
      pop.root()->tree = make_persistent<TreeType>();
    });
  }
  auto tree = pop.root()->tree;
 
  /* Getting the leaf node */
  auto leaf = tree->rootNode.leaf;
	//tree->printLeafNode(0, leaf);

	const auto reqTup = MyTuple(KEYPOS, KEYPOS * 100, KEYPOS * 1.0);
  TreeType::SplitInfo splitInfo;
  bool split;
	
  /* BENCHMARKING */
  for (auto _ : state) {
    state.PauseTiming();
    tree->rootNode = tree->newLeafNode();
    leaf = tree->rootNode.leaf;
    prepare(tree);
    //leaf->keys.getWrites();
    state.ResumeTiming();
    
    split = tree->insertInLeafNode(leaf, KEYPOS, reqTup, &splitInfo);
  
    state.PauseTiming();
    assert(split == false);
    //leaf->keys.getWrites();
    tree->deleteLeafNode(leaf);
    state.ResumeTiming();
  }

  //tree->printLeafNode(0, leaf);
  std::cout << "Elements:" << ELEMENTS << '\n';
  pop.close();
}
BENCHMARK(BM_TreeInsert);

BENCHMARK_MAIN();

/* preparing inserts */
void prepare(const persistent_ptr<TreeType> tree) {
  auto insertLoop = [&tree](int start, int end) {
    for (auto j = start; j < end + 1; ++j) {
      auto tup = MyTuple(j, j * 100, j * 1.0);
      tree->insert(j, tup);
    }
  };
  switch (KEYPOS) {
    case 1 /*first*/: insertLoop(2, ELEMENTS); break;
    case ELEMENTS /*last*/:  insertLoop(1, ELEMENTS-1);break;
    case ELEMENTS/2 /*middle*/: {insertLoop(1, KEYPOS-1); insertLoop(KEYPOS+1, ELEMENTS);}
  }
}
