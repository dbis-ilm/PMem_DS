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

#include <unistd.h>
#include "benchmark/benchmark.h"
#include "common.hpp"

using namespace dbis::ptable;


static void BM_RangeScan(benchmark::State &state) {

  pool<root> pop;

  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, NUM_TUPLES);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
  }

  auto pTable = pop.root()->pTable;

  /* RangeScan using Block iterator */
  for (auto _ : state) {
    auto iter = pTable->rangeScan(ColumnRangeMap({{0, {(int)state.range(0), (int)state.range(1)}}}));
    for (const auto &tp: iter) {
      tp;
    }
  }
  pop.close();
}
static void KeyRangeArguments(benchmark::internal::Benchmark* b) {
  for (const auto &arg : KEY_RANGES) b->Args(arg);
}
BENCHMARK(BM_RangeScan)->Apply(KeyRangeArguments);

static void BM_NonKeyRangeScan(benchmark::State &state) {

  pool<root> pop;

  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, NUM_TUPLES);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
  }

  auto pTable = pop.root()->pTable;

  /* RangeScan using Block iterator */
  for (auto _ : state) {
    auto iter = pTable->rangeScan(ColumnRangeMap({{0, {(int)state.range(0), (int) state.range(1)}},
                                                  {3, {(double)state.range(2), (double)state.range(3)}}}));
    for (const auto &tp: iter) {
      tp;
    }
  }
  pop.close();
}
static void NonKeyRangeArguments(benchmark::internal::Benchmark* b) {
  for (const auto &arg : NON_KEY_RANGES) b->Args(arg);
}
BENCHMARK(BM_NonKeyRangeScan)->Apply(NonKeyRangeArguments);

static void BM_PBPTreeKeyScan(benchmark::State &state) {

  pool<root> pop;

  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, NUM_TUPLES);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
  }

  auto pTable = pop.root()->pTable;

  for (auto _ : state) {
    /* Scan via Index scan */
    pTable->rangeScan2(state.range(0), state.range(1), [](int k, const PTuple<int, MyTuple> tp){tp;});
  }

  pop.close();
}
BENCHMARK(BM_PBPTreeKeyScan)->Apply(KeyRangeArguments);

static void BM_PBPTreeScan(benchmark::State &state) {

  pool<root> pop;

  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, NUM_TUPLES);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
  }

  auto &pTable = *pop.root()->pTable;

  for (auto _ : state) {
    /* Scan via PTuple iterator */
    auto eIter = pTable.end();
    auto iter = pTable.select(
      [&](const PTuple<int, MyTuple> &tp) {
        return (tp.get<0>() >= state.range(0)) && (tp.get<0>() <= state.range(1)) &&
               (tp.get<3>() >= state.range(2)) && (tp.get<3>() <= state.range(3));
      });
    for (; iter != eIter; iter++) {
      iter;
      //(*iter).get<0>();
    }
  }

  transaction::run(pop, [&] { delete_persistent<PTableType>(pop.root()->pTable); });
  pop.close();
  std::experimental::filesystem::remove_all(path);
}
BENCHMARK(BM_PBPTreeScan)->Apply(NonKeyRangeArguments);

BENCHMARK_MAIN();

