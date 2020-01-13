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

#include "catch.hpp"
#include "config.h"
#include "core/VTableInfo.hpp"

using namespace dbis::ptable;

TEST_CASE("creating a new VTableInfo (volatile TableInfo) instance", "[VTableInfo]") {

  using MyTuple = std::tuple<int, double, std::string>;
  using MyKey = int;
  std::vector<std::string> cv = {"a", "b", "c"};
  auto tInfo = VTableInfo<MyKey, MyTuple>("MyTable", cv);
  auto tInfo2 = VTableInfo<MyKey, MyTuple>("MyTable2", {"a","b","c"});

  SECTION("retrieving name and type of columns") {
    REQUIRE(tInfo.columns[0].first == "a");
    REQUIRE(tInfo.columns[1].first == "b");
    REQUIRE(tInfo.columns[2].first == "c");
    REQUIRE(tInfo.columns[0].second == ColumnType::IntType);
    REQUIRE(tInfo.columns[1].second == ColumnType::DoubleType);
    REQUIRE(tInfo.columns[2].second == ColumnType::StringType);

    REQUIRE(tInfo2.columns[0].first == "a");
    REQUIRE(tInfo2.columns[1].first == "b");
    REQUIRE(tInfo2.columns[2].first == "c");
    REQUIRE(tInfo2.columns[0].second == ColumnType::IntType);
    REQUIRE(tInfo2.columns[1].second == ColumnType::DoubleType);
    REQUIRE(tInfo2.columns[2].second == ColumnType::StringType);
  }

  SECTION("retrieving other table data") {
    REQUIRE(tInfo.name == "MyTable");
    REQUIRE(tInfo.columns.size() == 3);
    REQUIRE(tInfo.typeOfKey() == ColumnType::IntType);

    REQUIRE(tInfo2.name == "MyTable2");
    REQUIRE(tInfo2.columns.size() == 3);
    REQUIRE(tInfo.typeOfKey() == ColumnType::IntType);
  }

  SECTION("iterate through columns") {
    auto c = 0u;
    for (const auto &col : tInfo) {
      c++;
    }
    REQUIRE(c == 3);
    for (const auto &col : tInfo2) {
      c++;
    }
    REQUIRE(c == 6);
  }
}
