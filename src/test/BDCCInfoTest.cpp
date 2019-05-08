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

#include "catch.hpp"
#include "config.h"
#include "core/BDCCInfo.hpp"
#include "core/PTableException.hpp"

using namespace dbis::ptable;

using pmem::obj::pool;

TEST_CASE("Testing to create a new BDCCInfo instance", "[BDCCInfo]") {
  struct root {
    persistent_ptr<BDCCInfo> bdccInfo;
  };

  pool<root> pop;

  const std::string path = dbis::gPmemPath + "bdccInfo";

  auto dims = Dimensions({{0, 4, 4}, {3, 6, 6}});
  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, "BDCCInfo", 8 * 1024 * 1024);
    transaction::run(pop, [&] {
      pop.root()->bdccInfo = make_persistent<BDCCInfo>(dims);
    });
  } else {
    throw PTableException("bdccInfo pool should not exist");
  }
  auto bdccInfo = pop.root()->bdccInfo;

  REQUIRE(bdccInfo->numBins() == 10);

  std::bitset<32> expected[2] = {
    0b0000000000000000001010101000,
    0b0000000000000000000101010111
  };
  auto i = 0u;
  for (const auto &dim : *bdccInfo) {
    REQUIRE(std::get<0>(dim) == std::get<0>(dims[i]));
    REQUIRE(std::get<1>(dim) == std::get<1>(dims[i]));
    REQUIRE(std::get<3>(dim) == std::get<2>(dims[i]));
    REQUIRE(std::get<2>(dim) == expected[i]);
    ++i;
  }

  /* Clean up */
  transaction::run(pop, [&] {
    delete_persistent<BDCCInfo>(bdccInfo);
    delete_persistent<root>(pop.root());
  });
  pop.close();
  std::remove(path.c_str());
}

TEST_CASE("Testing to apply a mask to various value types", "[BDCCInfo]") {
  auto mask = 0b10101010;     /* 170/AA (dec/hex) */

  int value1 = 240;           /* -> 240 = F0/11110000 (hex/bin) */
  REQUIRE(applyMask(value1, mask) == 0b10100000);

  double value2 = 240.5;      /* -> 240 = F0/11110000 (hex/bin) */
  REQUIRE(applyMask(value2, mask) == 0b10100000);

  std::string value3 = "240"; /* -> "2" = 32/00110010 (hex/bin) */
  REQUIRE(applyMask(value3, mask) == 0b00100010);
}
