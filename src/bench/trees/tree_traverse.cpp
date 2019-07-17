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

/* Traverse benchmarks on Tree */
static void BM_TreeTraverse(benchmark::State &state) {
  std::cout << "BRANCHKEYS: " << BRANCHKEYS //<< " --> " << BRANCHSIZE
    << "\nLEAFKEYS: " << LEAFKEYS //<< " --> " << LEAFSIZE
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
    insert(pop.root()->tree);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
    hybridWrapper.recover(*pop.root()->tree);
  }
  auto tree = pop.root()->tree;
  auto &treeRef = *tree;

  persistent_ptr<TreeType::LeafNode> leaf;
  /* BENCHMARKING */
  for (auto _ : state) {
    /* Getting a leaf node */
    auto d = hybridWrapper.getDepth(treeRef);
    auto node = treeRef.rootNode;
    while ( --d > 0) node =
      hybridWrapper.getChildAt(node, treeRef.lookupPositionInBranchNode(node.branch, KEYPOS));

    benchmark::DoNotOptimize(
      leaf = node.leaf
    );
    //auto p = treeRef.lookupPositionInLeafNode(leaf, 1);
  }
  std::cout << "Elements:" << ELEMENTS << "\n";
  transaction::run(pop, [&] { delete_persistent<TreeType>(tree); });
  pop.close();
  std::experimental::filesystem::remove_all(path);
}
BENCHMARK(BM_TreeTraverse);

BENCHMARK_MAIN();
