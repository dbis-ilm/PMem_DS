/*
 * Copyright (C) 2017 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of PTable.
 *
 * PTable is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PTable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PTable.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <unistd.h>
#include "benchmark/benchmark.h"
#include "fmt/format.h"
#include "common.h"

using namespace dbis::ptable;

static void BM_PointQuery(benchmark::State& state) {

  pool<root> pop;

  const std::string path = dbis::gPmemPath + "benchdb" + std::to_string(state.range(0)) + ".db";

  //std::remove(path.c_str());
  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, state.range(0));
  } else {
    //std::cerr << "WARNING: Table already exists" << std::endl;
    pop = pool<root>::open(path, LAYOUT);
  }

  auto pTable = pop.root()->pTable;

  for (auto _ : state) {
    auto ptp = pTable->getByKey(state.range(1));
    if (ptp.getNode() != nullptr) ptp;// ptp.createTuple();
    else std::cerr << "key not found" << '\n';
  }
  pop.close();
  std::remove(path.c_str());
}
static void VectorArguments(benchmark::internal::Benchmark* b) {
  for (const auto &arg : POINT_ACCESS) b->Args(arg);
}
BENCHMARK(BM_PointQuery)->Apply(VectorArguments);

BENCHMARK_MAIN();
