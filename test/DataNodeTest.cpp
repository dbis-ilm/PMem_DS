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
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "catch.hpp"
#include "config.h"
#include "core/DataNode.hpp"
#include "core/PTableException.hpp"

using namespace dbis::ptable;

using pmem::obj::delete_persistent_atomic;
using pmem::obj::make_persistent_atomic;
using pmem::obj::pool;
using pmem::obj::transaction;

TEST_CASE("Testing to create a new BDCCInfo instance", "[BDCCInfo]") {
  using KeyType = int;
  struct root {
    persistent_ptr<DataNode<KeyType>> node;
  };

  pool<root> pop;

  const std::string path = dbis::gPmemPath + "dataNode";

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, "DataNode", 11 * 1024 * 1024);
    make_persistent_atomic<DataNode<KeyType>>(pop, pop.root()->node, BDCC_Block());
  } else throw PTableException("dataNode pool should not exist");

  auto q = pop.root();
  auto node = q->node;

  transaction::run(pop, [&] {
    for (auto i = 0u; i < 10; ++i) {
      node->bdccSum += (i % 2) + 5;
      node->block.get_rw()[i] = 0xFF;
    }
    reinterpret_cast<uint16_t &>(node->block.get_rw()[gCountPos]) = 10;
  });

  REQUIRE(node->calcAverageBDCC() == 5);  /* 5.5 */
  for (auto i = 0u; i < 10; ++i) {
    if (i != gCountPos && i != gCountPos + 1)
      REQUIRE(node->block.get_ro()[i] == 0xFF);
  }

  /* Clean up */
  pmem::obj::delete_persistent_atomic<DataNode<KeyType>>(node);
  pmem::obj::delete_persistent_atomic<root>(q);
  pop.close();
  std::remove(path.c_str());
}
