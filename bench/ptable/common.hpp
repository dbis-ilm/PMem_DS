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

#ifndef PTABLE_COMMON_H
#define PTABLE_COMMON_H

#include <libpmempool.h>
#include <unistd.h>
#include <chrono>
#include "ptable/PTable.hpp"

using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;

using MyTuple = std::tuple<int, int, std::string, double>;
using MyKey = int;
using PTableType = dbis::ptable::PTable<MyKey, MyTuple>;
using Vector = std::vector<long int>;
using VectorVector = std::vector<Vector>;

int hibit_pos(int n) noexcept;

template <size_t SIZE>
static inline VectorVector *createPointVector(VectorVector *v);

struct root {
  persistent_ptr<PTableType> pTable;
};

const std::string path = dbis::gPmemPath + "benchdb" + std::to_string(dbis::ptable::gBlockSize) + "B.db";
const auto NUM_TUPLES = 1000 * 10;
const auto POOL_SIZE = 1024 * 1024 * 1024 * 4ull; // 4GB

const auto ALIGNMENT = hibit_pos(NUM_TUPLES) + 1;
VectorVector pv;
const auto POINT_ACCESS = *createPointVector<NUM_TUPLES>(&pv);

/*const VectorVector KEY_RANGES = {
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 2000, NUM_TUPLES / 2 + NUM_TUPLES / 2000}, //  0,1%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 400, NUM_TUPLES / 2 + NUM_TUPLES / 400},   //  0,5%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 200, NUM_TUPLES / 2 + NUM_TUPLES / 200},   //  1,0%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 40, NUM_TUPLES / 2 + NUM_TUPLES / 40},     //  5,0%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 20, NUM_TUPLES / 2 + NUM_TUPLES / 20},     // 10,0%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 10, NUM_TUPLES / 2 + NUM_TUPLES / 10},     // 20,0%
  Vector{0, NUM_TUPLES-1}                                                         //100,0%
};*/

/// Format: (window size, max value)
const VectorVector KEY_RANGES = {
  Vector{NUM_TUPLES / 1000, NUM_TUPLES - NUM_TUPLES / 1000}, //  0,1%
  Vector{NUM_TUPLES /  500, NUM_TUPLES - NUM_TUPLES /  500}, //  0,2%
  Vector{NUM_TUPLES /  200, NUM_TUPLES - NUM_TUPLES /  200}, //  0,5%
  Vector{NUM_TUPLES /  100, NUM_TUPLES - NUM_TUPLES /  100}, //  1,0%
  Vector{NUM_TUPLES /   50, NUM_TUPLES - NUM_TUPLES /   50}, //  2,0%
  Vector{NUM_TUPLES /   20, NUM_TUPLES - NUM_TUPLES /   20}, //  5,0%
  Vector{NUM_TUPLES /   10, NUM_TUPLES - NUM_TUPLES /   10}, // 10,0%
  Vector{NUM_TUPLES /    5, NUM_TUPLES - NUM_TUPLES /    5}, // 20,0%
  Vector{NUM_TUPLES       , 1}};                             //100,0%

/*const VectorVector NON_KEY_RANGES = {
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 1000, NUM_TUPLES / 2 + NUM_TUPLES / 1000,
         NUM_TUPLES / 2, NUM_TUPLES / 2 + NUM_TUPLES / 500},                      //  0,1%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 200, NUM_TUPLES / 2 + NUM_TUPLES / 200,
         NUM_TUPLES / 2, NUM_TUPLES / 2 + NUM_TUPLES / 100},                      //  0,5%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 100, NUM_TUPLES / 2 + NUM_TUPLES / 100,
         NUM_TUPLES / 2, NUM_TUPLES / 2 + NUM_TUPLES / 50},                       //  1,0%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 20, NUM_TUPLES / 2 + NUM_TUPLES / 20,
         NUM_TUPLES / 2, NUM_TUPLES / 2 + NUM_TUPLES / 10},                       //  5,0%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 10, NUM_TUPLES / 2 + NUM_TUPLES / 10,
         NUM_TUPLES / 2, NUM_TUPLES / 2 + NUM_TUPLES / 5},                        // 10,0%
  Vector{NUM_TUPLES / 2 - NUM_TUPLES / 5, NUM_TUPLES / 2 + NUM_TUPLES / 5,
         NUM_TUPLES / 2, NUM_TUPLES / 2 + NUM_TUPLES * 2 / 5 },                   // 20,0%
  Vector{0, NUM_TUPLES - 1, 0, NUM_TUPLES - 1}                                    //100,0%
};*/

/// Format: (window size, max of second attribute)
const VectorVector NON_KEY_RANGES = {
  Vector{NUM_TUPLES / 1000, NUM_TUPLES - NUM_TUPLES / 500},  //  0,1%
  Vector{NUM_TUPLES /  500, NUM_TUPLES - NUM_TUPLES / 250},  //  0,2%
  //Vector{NUM_TUPLES * 45/  10000, NUM_TUPLES - NUM_TUPLES*45 / 500},  //  0,45%
  //Vector{NUM_TUPLES /  250, NUM_TUPLES - NUM_TUPLES / 125},  //  0,4%
  //Vector{NUM_TUPLES*3 /  500, NUM_TUPLES - NUM_TUPLES*3 / 250},  //  0,6%
  //Vector{NUM_TUPLES /  125, NUM_TUPLES - NUM_TUPLES*2 / 125}  //  0,8%
  Vector{NUM_TUPLES /  200, NUM_TUPLES - NUM_TUPLES / 100},  //  0,5%
  Vector{NUM_TUPLES /  100, NUM_TUPLES - NUM_TUPLES /  50},  //  1,0%
  Vector{NUM_TUPLES /   50, NUM_TUPLES - NUM_TUPLES /  25},  //  2,0%
  Vector{NUM_TUPLES /   20, NUM_TUPLES - NUM_TUPLES /  10},  //  5,0%
  Vector{NUM_TUPLES /   10, NUM_TUPLES - NUM_TUPLES /   5},  // 10,0%
  Vector{NUM_TUPLES /    5, NUM_TUPLES - NUM_TUPLES * 2 / 5},// 20,0%
  Vector{NUM_TUPLES       , 1}                              //100,0%
};
int hibit_pos(int n) noexcept {
  int c = 0;
  while (n>>1 != 0) {
    c++;
    n>>=1;
  }
  return c;
}

template<size_t SIZE>
static inline VectorVector *createPointVector(VectorVector *v) {
  if constexpr (SIZE >= 100) {
    createPointVector<SIZE / 10>(v);
    v->emplace_back(Vector{SIZE, SIZE / 2});
  }
  return v;
}

void insert (pool<root> &pop, const std::string &path, size_t entries) {
  using namespace dbis::ptable;
  std::chrono::high_resolution_clock::time_point start, end;
  std::vector<typename std::chrono::duration<int64_t, std::micro>::rep> measures;

  pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
  /// Only if index is persistent version
  const auto alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>(
      "heap.alloc_class.128.desc", PTableType::IndexType::AllocClass);
  transaction::run(pop, [&] {
    const auto tInfo = VTableInfo<MyKey, MyTuple>("MyTable", {"a","b","c","d"});
    const auto dims = Dimensions({
                                   {0, 10, ALIGNMENT},
                                   {3, 10, ALIGNMENT}
                                 });
    pop.root()->pTable = make_persistent<PTableType>(alloc_class, tInfo, dims);
  });

  auto &pTable = pop.root()->pTable;

  for (auto i = 0u; i < entries; i++) {
    auto tup = MyTuple(i + 1,
                       (i + 1) * 100,
                       fmt::format("String #{0}", i % 1000),
                       (i + 1) * 1.0);
    start = std::chrono::high_resolution_clock::now();
    if (i % (entries/100) == 0) {
      std::cout << "Inserting tuple: " << (i+1)*100/entries << "%\r";
      std::cout.flush();
    }
    pTable->insert(i + 1, tup);
    end = std::chrono::high_resolution_clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    measures.push_back(diff);
  }
  std::cout << std::endl;

  const auto avg = std::accumulate(measures.begin(), measures.end(), 0) / measures.size();
  const auto minmax = std::minmax_element(std::begin(measures), std::end(measures));
  LOG("\nInsert Statistics in µs: "
         << "\n\tAvg: \t" << avg
         << "\n\tMin: \t" << *minmax.first
         << "\n\tMax: \t" << *minmax.second);

}

#endif /* PTABLE_COMMON_H */
