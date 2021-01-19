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

#include <unistd.h>
#include <random>

#include "benchmark/benchmark.h"
#include "fmt/format.h"

#include "common.hpp"

using namespace dbis::ptable;

static void BM_PointQuery(benchmark::State& state) {
  pool<root> pop;

  const std::string path = dbis::gPmemPath + "benchdb" + std::to_string(state.range(0)) + ".db";
  std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dis(1, state.range(0));

  // std::remove(path.c_str());
  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, state.range(0));
  } else {
    // std::cerr << "WARNING: Table already exists" << std::endl;
    pop = pool<root>::open(path, LAYOUT);
  }

  auto& pTable = *pop.root()->pTable;

  for (auto _ : state) {
    //auto ptp = pTable.getByKey(state.range(1));
    auto ptp = pTable.getByKey(dis(gen));
    if (ptp.getNode() != nullptr) {
      ptp.createTuple();
      //benchmark::DoNotOptimize(ptp);
    } else std::cerr << "key not found" << '\n';
  }

  transaction::run(pop, [&] { delete_persistent<PTableType>(pop.root()->pTable); });
  pop.close();
  pmempool_rm(path.c_str(), 0);
}
static void VectorArguments(benchmark::internal::Benchmark* b) {
  for (const auto& arg : POINT_ACCESS) b->Args(arg);
}
BENCHMARK(BM_PointQuery)->Apply(VectorArguments);

BENCHMARK_MAIN();
