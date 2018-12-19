/*
 * Copyright (C) 2017-2018 DBIS Group - TU Ilmenau, All Rights Reserved.
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
#include "config.h"
#include "core/PTableInfo.hpp"
#include "core/PTableException.hpp"

using namespace dbis::ptable;

using pmem::obj::delete_persistent_atomic;
using pmem::obj::make_persistent_atomic;
using pmem::obj::pool;
using pmem::obj::transaction;

TEST_CASE("creating a new PTableInfo (persistent TableInfo) instance", "[PTableInfo]") {
  using MyTuple = std::tuple<int, double, std::string>;
  using MyKey = int;
  using PTableInfoType = PTableInfo<MyKey, MyTuple>;

  struct root {
    persistent_ptr<PTableInfoType> tInfo;
    persistent_ptr<PTableInfoType> tInfo2;
  };

  pool<root> pop;

  const std::string path = dbis::gPmemPath + "pTableInfo";

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, "PTableInfo", 8 * 1024 * 1024);
    auto vTableInfo = VTableInfo<MyKey, MyTuple>("MyTable", {"a","b","c"});
    transaction::run(pop, [&] {
      pop.root()->tInfo = make_persistent<PTableInfoType>(vTableInfo);
      pop.root()->tInfo2 = make_persistent<PTableInfoType>("MyTable2", StringInitList{"a","b", "c"});
    });
  } else throw PTableException("pTableInfo pool should not exist");

  auto q = pop.root();
  auto tInfo = q->tInfo;
  auto tInfo2 = q->tInfo2;

  SECTION("retrieving name and type of columns") {
    REQUIRE(tInfo->columnInfo(0).getName() == "a");
    REQUIRE(tInfo->columnInfo(1).getName() == "b");
    REQUIRE(tInfo->columnInfo(2).getName() == "c");
    REQUIRE(tInfo->columnInfo(0).getType() == ColumnType::IntType);
    REQUIRE(tInfo->columnInfo(1).getType() == ColumnType::DoubleType);
    REQUIRE(tInfo->columnInfo(2).getType() == ColumnType::StringType);

    REQUIRE(tInfo2->columnInfo(0).getName() == "a");
    REQUIRE(tInfo2->columnInfo(1).getName() == "b");
    REQUIRE(tInfo2->columnInfo(2).getName() == "c");
    REQUIRE(tInfo2->columnInfo(0).getType() == ColumnType::IntType);
    REQUIRE(tInfo2->columnInfo(1).getType() == ColumnType::DoubleType);
    REQUIRE(tInfo2->columnInfo(2).getType() == ColumnType::StringType);
  }

  SECTION("retrieving table data") {
    REQUIRE(tInfo->tableName() == "MyTable");
    REQUIRE(tInfo->numColumns() == 3);
    REQUIRE(tInfo->typeOfKey() == ColumnType::IntType);

    REQUIRE(tInfo2->tableName() == "MyTable2");
    REQUIRE(tInfo2->numColumns() == 3);
    REQUIRE(tInfo2->typeOfKey() == ColumnType::IntType);
  }

  SECTION("iterate through columns") {
    auto c = 0u;
    for (const auto &col : *tInfo) {
      c++;
    }
    REQUIRE(c == 3);
    for (const auto &col : *tInfo2) {
      c++;
    }
    REQUIRE(c == 6);
  }

  SECTION("find column by name") {
    REQUIRE(tInfo->findColumnByName("a") == 0);
    REQUIRE(tInfo->findColumnByName("b") == 1);
    REQUIRE(tInfo->findColumnByName("c") == 2);
    REQUIRE(tInfo->findColumnByName("d") == -1);

    REQUIRE(tInfo2->findColumnByName("a") == 0);
    REQUIRE(tInfo2->findColumnByName("b") == 1);
    REQUIRE(tInfo2->findColumnByName("c") == 2);
    REQUIRE(tInfo2->findColumnByName("d") == -1);
  }

  SECTION("replace columns") {
    ColumnVector cv = {Column("d", ColumnType::DoubleType), Column("e", ColumnType::IntType)};
    tInfo->setColumns(cv);
    REQUIRE(tInfo->columnInfo(0).getName() == "d");
    REQUIRE(tInfo->columnInfo(1).getName() == "e");
    REQUIRE(tInfo->columnInfo(0).getType() == ColumnType::DoubleType);
    REQUIRE(tInfo->columnInfo(1).getType() == ColumnType::IntType);

    tInfo2->setColumns(cv);
    REQUIRE(tInfo2->columnInfo(0).getName() == "d");
    REQUIRE(tInfo2->columnInfo(1).getName() == "e");
    REQUIRE(tInfo2->columnInfo(0).getType() == ColumnType::DoubleType);
    REQUIRE(tInfo2->columnInfo(1).getType() == ColumnType::IntType);
  }

  /* Clean up */
  delete_persistent_atomic<PTableInfoType>(tInfo);
  delete_persistent_atomic<PTableInfoType>(tInfo2);
  delete_persistent_atomic<root>(q);
  pop.close();
  std::remove(path.c_str());
}
