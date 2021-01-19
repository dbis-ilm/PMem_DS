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

#include <random>
#include <unistd.h>
#include "benchmark/benchmark.h"
#include "common.hpp"

using namespace dbis::ptable;

static void BM_RangeScan(benchmark::State &state) {
  pool<root> pop;

  std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dis(1, state.range(1));

  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, NUM_TUPLES);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
  }

  auto pTable = pop.root()->pTable;

  /* RangeScan using Block iterator */
  for (auto _ : state) {
    //PTuple<int, MyTuple> mtp;
    MyTuple mtp;
    const int rangeStart = dis(gen);
    auto iter =
        pTable->rangeScan(ColumnRangeMap({{0, {rangeStart, (int) (rangeStart + state.range(0) - 1)}}}));
    for (const auto &tp : iter) {
      benchmark::DoNotOptimize(tp);
      benchmark::DoNotOptimize(mtp);
      mtp = *tp.createTuple();
      //mtp = tp;
      benchmark::ClobberMemory();
    }
  }
  pop.close();
}
static void KeyRangeArguments(benchmark::internal::Benchmark *b) {
  for (const auto &arg : KEY_RANGES) b->Args(arg);
}
BENCHMARK(BM_RangeScan)->Apply(KeyRangeArguments);

static void BM_NonKeyRangeScan(benchmark::State &state) {
  pool<root> pop;

  std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dis(1, state.range(1));

  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, NUM_TUPLES);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
  }

  auto pTable = pop.root()->pTable;

  /* RangeScan using Block iterator */
  for (auto _ : state) {
    //PTuple<int, MyTuple> mtp;
    MyTuple mtp;
    const auto rangeStart = dis(gen);
    auto iter =
        pTable->rangeScan(ColumnRangeMap({{0, {(int)(rangeStart - state.range(0)), (int)(rangeStart + state.range(0))}},
                                          {3, {(double)rangeStart, (double)(rangeStart + 2 * state.range(0) - 1)}}}));
    //auto c = 0u;
    for (const auto &tp : iter) {
      benchmark::DoNotOptimize(tp);
      benchmark::DoNotOptimize(mtp);
      mtp = *tp.createTuple();
      //mtp = tp;
      benchmark::ClobberMemory();
      //++c;
    }
    //std::cout << "Tuples scanned: " << c << std::endl;
  }
  pop.close();
}
static void NonKeyRangeArguments(benchmark::internal::Benchmark *b) {
  for (const auto &arg : NON_KEY_RANGES) b->Args(arg);
}
BENCHMARK(BM_NonKeyRangeScan)->Apply(NonKeyRangeArguments);

static void BM_PBPTreeKeyScan(benchmark::State &state) {
  pool<root> pop;

  std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dis(1, state.range(1));

  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, NUM_TUPLES);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
  }

  auto pTable = pop.root()->pTable;

  for (auto _ : state) {
    PTuple<int, MyTuple> mtp;
    //MyTuple mtp;
    const int rangeStart = dis(gen);
    /* Scan via Index scan */
    pTable->rangeScan2(rangeStart, rangeStart + state.range(0) - 1,
                       [&mtp](int k, const PTuple<int, MyTuple> tp) {
      benchmark::DoNotOptimize(tp);
      benchmark::DoNotOptimize(mtp);
      //mtp = *tp.createTuple();
      mtp = tp;
      benchmark::ClobberMemory();
    });
  }

  pop.close();
}
BENCHMARK(BM_PBPTreeKeyScan)->Apply(KeyRangeArguments);

static void BM_PBPTreeScan(benchmark::State &state) {
  pool<root> pop;

  std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<> dis(1, state.range(1));

  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, NUM_TUPLES);
  } else {
    LOG("Warning: " << path << " already exists");
    pop = pool<root>::open(path, LAYOUT);
  }

  auto &pTable = *pop.root()->pTable;

  for (auto _ : state) {
    /* Scan via PTuple iterator */
    PTuple<int, MyTuple> mtp;
    //MyTuple mtp;
    const auto rangeStart = dis(gen);
    auto eIter = pTable.end();
    auto iter = pTable.select([&](const PTuple<int, MyTuple> &tp) {
      return (tp.get<0>() >= rangeStart - state.range(0)/2) &&
             (tp.get<0>() <= rangeStart + state.range(0)/2) &&
             (tp.get<3>() >= rangeStart) &&
             (tp.get<3>() <= rangeStart + state.range(0) - 1);
    });
    for (; iter != eIter; iter++) {
      benchmark::DoNotOptimize(iter);
      benchmark::DoNotOptimize(mtp);
      //mtp = *(*iter).createTuple();
      mtp = *iter;
      //(*iter).get<0>();
      benchmark::ClobberMemory();
    }
  }

  //transaction::run(pop, [&] { delete_persistent<PTableType>(pop.root()->pTable); });
  pop.close();
  //pmempool_rm(path.c_str(), 0);
}
BENCHMARK(BM_PBPTreeScan)->Apply(NonKeyRangeArguments);

BENCHMARK_MAIN();
