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

#include <filesystem>
#include "common.hpp"
#include "utils/PersistEmulation.hpp"

void prepare(const persistent_ptr<TreeType> tree);

/* Get benchmarks on Tree */
static void BM_TreeSplit(benchmark::State &state) {
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
  auto &treeRef = *tree;

  /* Getting the leaf node */
  auto leaf = treeRef.rootNode.leaf;
  //treeRef.printLeafNode(0, leaf);

  const auto reqTup = MyTuple(ELEMENTS+1, (ELEMENTS+1) * 100, (ELEMENTS+1) * 1.0);
  TreeType::SplitInfo splitInfo;
  bool split;

  /* BENCHMARKING */
  for (auto _ : state) {
    state.PauseTiming();
    std::cout.setstate(std::ios_base::failbit);
    treeRef.rootNode = treeRef.newLeafNode();
    leaf = treeRef.rootNode.leaf;
    prepare(tree);
    dbis::PersistEmulation::getBytesWritten();
    //treeRef.printLeafNode(0, leaf);
    state.ResumeTiming();

    treeRef.splitLeafNode(leaf, &splitInfo);

    state.PauseTiming();
    //treeRef.printLeafNode(0, leaf);
    treeRef.deleteLeafNode(leaf->nextLeaf);
    treeRef.deleteLeafNode(leaf);
    state.ResumeTiming();
  }

  //treeRef.printLeafNode(0, leaf);
  std::cout.clear();
  std::cout << "Writes:" << dbis::PersistEmulation::getBytesWritten() << '\n';
  std::cout << "Elements:" << ELEMENTS << '\n';
  pop.close();
}
BENCHMARK(BM_TreeSplit);

BENCHMARK_MAIN();

/* preparing inserts */
void prepare(const persistent_ptr<TreeType> tree) {
  auto &treeRef = *tree;
  for (auto j = 1; j < ELEMENTS+1; ++j) {
    auto tup = MyTuple(j, j * 100, j * 1.0);
    treeRef.insert(j, tup);
  }
}
