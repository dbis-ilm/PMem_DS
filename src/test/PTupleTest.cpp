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
#include <sstream>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>
#include "catch.hpp"
#include "config.h"
#include "core/PTuple.hpp"
#include "core/PTableException.hpp"
#include "core/utils.hpp"

using namespace dbis::ptable;

using pmem::obj::delete_persistent_atomic;
using pmem::obj::make_persistent;
using pmem::obj::pool;
using pmem::obj::transaction;

TEST_CASE("creating a new PTuple instance", "[PTuple]") {
  using TupleType = std::tuple<int, double, std::string>;
  using KeyType = int;
  using PTupleType = PTuple<KeyType, TupleType>;

  // 1 | 1.25 | String1
  std::array<uint16_t, PTupleType::NUM_ATTRIBUTES> pTupleOffsets = {0, 4, 12};
  BDCC_Block data;
  copyToByteArray(data, 1, sizeof(int), pTupleOffsets[0]);
  copyToByteArray(data, 1.25, sizeof(double), pTupleOffsets[1]);
  copyToByteArray(data, "String1\0", 8, pTupleOffsets[2]);

  struct root {
    persistent_ptr<DataNode<KeyType>> node;
    persistent_ptr<PTupleType> ptp;
  };

  pool<root> pop;

  const std::string path = dbis::gPmemPath + "pTuple";

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, "PTuple", 12 * 1024 * 1024);
    transaction::run(pop, [&] {
      pop.root()->node = make_persistent<DataNode<KeyType>>(data);
      pop.root()->ptp = make_persistent<PTupleType>(pop.root()->node, pTupleOffsets);
    });
  } else throw PTableException("pTuple pool should not exist");

  auto q = pop.root();
  auto ptp = q->ptp;
  auto node = q->node;

  SECTION("check if correctly initialized") {
    REQUIRE(ptp->getNode() == node);
    REQUIRE(ptp->getOffsetAt(0) == pTupleOffsets[0]);
    REQUIRE(ptp->getOffsetAt(1) == pTupleOffsets[1]);
    REQUIRE(ptp->getOffsetAt(2) == pTupleOffsets[2]);
//    REQUIRE(ptp->NUM_ATTRIBUTES == 3);
  }

  SECTION("checking member functions") {
    REQUIRE(ptp->get<0>() == 1);
    REQUIRE(ptp->get<1>() == 1.25);
    REQUIRE(ptp->get<2>() == "String1");
    REQUIRE(ptp->getAttribute<0>() == 1);
    REQUIRE(ptp->getAttribute<1>() == 1.25);
    REQUIRE(ptp->getAttribute<2>() == "String1");

    std::ostringstream oss;
    ptp->print(oss);
    REQUIRE(oss);
    REQUIRE(oss.str() == "1,1.25,String1");
  }

  SECTION("create Tuple and check for correct fields") {
    auto ptp_ptr = ptp->createTuple();
    REQUIRE(std::get<0>(*ptp_ptr) == 1);
    REQUIRE(std::get<1>(*ptp_ptr) == 1.25);
    REQUIRE(std::get<2>(*ptp_ptr) == "String1");
  }

  SECTION("check global accessor functions") {
    REQUIRE(get<0>(*ptp) == 1);
    REQUIRE(get<1>(*ptp) == 1.25);
    REQUIRE(get<2>(*ptp) == "String1");

    std::ostringstream oss;
    print(oss, *ptp);
    REQUIRE(oss);
    REQUIRE(oss.str() == "1,1.25,String1");
  }

  /* Clean up */
  delete_persistent_atomic<PTupleType>(ptp);
  delete_persistent_atomic<DataNode<KeyType>>(q->node);
  delete_persistent_atomic<root>(q);
  pop.close();
  std::remove(path.c_str());
}
