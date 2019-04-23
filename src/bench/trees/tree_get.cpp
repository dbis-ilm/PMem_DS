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

#include "common.hpp"

/* Get benchmarks on Tree */
static void BM_TreeGet(benchmark::State &state) {
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
    insert(pop.root()->tree);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
    pop.root()->tree->recover(); //< FPTree only
  }
  auto tree = pop.root()->tree;

  /* Getting a leaf node */
  auto node = tree->rootNode;

  /* FPTree version */
  auto d = tree->depth;
  while ( d > 1) {
    node = node.branch->children[0];
    --d;
  }
  if(d == 1) node = node.lowestbranch->children[0];

  /* other trees */
/*
  auto d = tree->depth.get_ro();
  while ( --d > 0) node = node.branch->children.get_ro()[0];
*/
  auto leaf = node.leaf;

  /* BENCHMARKING */
  for (auto _ : state) {
    //MyTuple tp;
    //auto c = 0u;
    //auto func = [&](MyKey const &key, MyTuple const &val) { c++; };
    //tree->scan(ELEMENTS/2, ELEMENTS/2 + 99, func);
    //tree->lookup(ELEMENTS/2, &tp);
    auto p = tree->lookupPositionInLeafNode(leaf, 1);
  }
  //tree->printBranchNode(0, tree->rootNode.branch);
  std::cout << "Elements:" << ELEMENTS << '\n';
  pop.close();
}
BENCHMARK(BM_TreeGet);//->Apply(KeyRangeArguments);

BENCHMARK_MAIN();
