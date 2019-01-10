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

#include <unistd.h>
#include <libpmemobj++/make_persistent_atomic.hpp>

#include "catch.hpp"
#include "PTable.hpp"

using namespace dbis::ptable;

using pmem::obj::delete_persistent_atomic;
using pmem::obj::pool;

using MyTuple = std::tuple<int, int, std::string, double>;
using PTableType = PTable<int, std::tuple<int, int, std::string, double>>;

TEST_CASE("Storing tuples in PTable", "[PTable]") {
  struct root {
    persistent_ptr<PTableType> pTable;
  };

  pool<root> pop;

  const std::string path = dbis::gPmemPath + "testdb.db";

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, LAYOUT, 16 * 1024 * 1024);
    transaction::run(pop, [&] {
      auto tInfo = VTableInfo<int, MyTuple>("MyTable", {"a","b","c","d"});
      auto dims = Dimensions({{0, 4, 4}, {3, 6, 6}});
      pop.root()->pTable = make_persistent<PTableType>(tInfo, dims);
    });
  } else {
    std::cerr << "WARNING: Table already exists" << std::endl;
    pop = pool<root>::open(path, LAYOUT);
  }
  auto q = pop.root();
  auto pTable = q->pTable;

  //SECTION("insert and count")
  auto c = 0u;
  for (auto i = 0u; i < 10; i++) {
    auto tup = MyTuple(i + 1,
                       (i + 1) * 100,
                       fmt::format("String #{0}", i),
                       (i + 1) * 12.345);
    c += pTable->insert(i + 1, tup);
  }
  REQUIRE(c == 10);
  REQUIRE(pTable->count() == 10);

  SECTION("delete and get by key, update by using delete/insert") {
    for (auto i = 0u; i < 10; i++) {
      REQUIRE(get<0>(pTable->getByKey(i + 1)) == i + 1);
      REQUIRE(get<1>(pTable->getByKey(i + 1)) == (i + 1) * 100);
      REQUIRE(get<2>(pTable->getByKey(i + 1)) == fmt::format("String #{0}", i));
      REQUIRE(get<3>(pTable->getByKey(i + 1)) == (i + 1) * 12.345);
     }

    auto c = 0u;
    for (auto i = 0u; i < 5; i++) {
      c += pTable->deleteByKey(i + 1);
    }
    REQUIRE(c == 5);
    REQUIRE(pTable->count() == 5);

    for (auto i = 5u; i < 10; i++) {
      REQUIRE(get<0>(pTable->getByKey(i + 1)) == i + 1);
      REQUIRE(get<1>(pTable->getByKey(i + 1)) == (i + 1) * 100);
      REQUIRE(get<2>(pTable->getByKey(i + 1)) == fmt::format("String #{0}", i));
      REQUIRE(get<3>(pTable->getByKey(i + 1)) == (i + 1) * 12.345);
    }

    for (auto i = 0u; i < 5; i++) {
      try {
        pTable->getByKey(i + 1);
        REQUIRE(false);
      } catch (PTableException &exc) {
        // do nothing, correct behaviour
      }
    }

    for (auto i = 5u; i < 10; i++) {
      auto tup = MyTuple(i + 1,
                         (i + 1) * 200,
                         fmt::format("String #{0}", i),
                         (i + 1) * 1.0);
      pTable->updateComplete(i + 1, tup);
    }
    for (auto i = 5u; i < 10; i++) {
      REQUIRE(get<0>(pTable->getByKey(i + 1)) == i + 1);
      REQUIRE(get<1>(pTable->getByKey(i + 1)) == (i + 1) * 200);
      REQUIRE(get<2>(pTable->getByKey(i + 1)) == fmt::format("String #{0}", i));
      REQUIRE(get<3>(pTable->getByKey(i + 1)) == (i + 1) * 1.0);
    }

    for (auto i = 0u; i < 5; i++) {
      auto tup = MyTuple(i + 1,
                         (i + 1) * 100,
                         fmt::format("String #{0}", i),
                         (i + 1) * 12.345);
      try {
        pTable->updateComplete(i + 1, tup);
        REQUIRE(false);
      } catch (PTableException &exc) {
        // do nothing, correct behaviour
      }
    }
  }

  SECTION("iterate and select") {
    auto eIter = pTable->end();
    auto iter = pTable->select([](const PTuple<int, MyTuple> &tp) { return (tp.get<0>() % 2 == 0); });

    for (; iter != eIter; iter++) {
      REQUIRE((*iter).get<0>() % 2 == 0);
    }
  }

  SECTION("range scan") {
    auto iter = pTable->rangeScan(ColumnRangeMap({{0, {5, 10}}, {3, {25.0, 90.0}}}));
    auto c = 0u;
    auto d = 0.0;
    for (const auto &ptp: iter) {
      c++;
      d += get<3>(ptp);
    }
    REQUIRE(c == 3); // 5,6,7
    REQUIRE(d == 5*12.345 + 6*12.345 + 7*12.345);
  }

  pTable->print();

  /* Clean up */
  delete_persistent_atomic<PTableType>(q->pTable);
  delete_persistent_atomic<root>(q);
  pop.close();
  std::remove(path.c_str());
}
