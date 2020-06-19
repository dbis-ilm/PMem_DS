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

#include "catch.hpp"
#include "config.h"
#define UNIT_TESTS 1
#include "simplePSkiplist.hpp"

using namespace dbis::pskiplists;

using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;

TEST_CASE("Insert and lookup key", "[simplePSkiplist]") {
  using simplePSkipType = simplePSkiplist<int, int, 5>;

  struct root {
    persistent_ptr<simplePSkipType> skiplist;
  };

  pool<root> pop;
  const std::string path = dbis::gPmemPath + "simplePSkiplistTest";

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, "simplePSkiplist", ((size_t)(1024 * 1024 * 16)));
  } else {
    pop = pool<root>::open(path, "simplePSkiplist");
    pop.root()->skiplist->recover();
  }

  auto q = pop.root();
  auto &rootRef = *q;

  if (!rootRef.skiplist)
    transaction::run(pop, [&] { rootRef.skiplist = make_persistent<simplePSkipType>(); });

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Looking up a key") {
    auto &sl = *rootRef.skiplist;
    auto nextNode = sl.head->forward[0];
    for (auto i = 5; i > 0; --i) {
      pptr<simplePSkipType::SkipNode> newNode;
      sl.newSkipNode(i, i, 0, newNode);
      sl.head->forward[0] = newNode;
      newNode->forward[0] = nextNode;
      nextNode = newNode;
      ++sl.nodeCount.get_rw();
    }

    int val;
    REQUIRE(sl.search(3, val));
    REQUIRE(sl.search(5, val));
    REQUIRE(!sl.search(0, val));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting keys") {
    auto &sl = *rootRef.skiplist;
    for (auto i = 0u; i < 8; ++i) {
      sl.printList();
      sl.insert(i, i*i);
    }

    int val;
    for (auto i = 0u; i < 8; ++i) {
      REQUIRE(sl.search(i, val));
      REQUIRE(val == i * i);
    }
    REQUIRE(sl.nodeCount == 8);

  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Recover the skip list") {
    auto &sl = *rootRef.skiplist;
    for (auto i = 0u; i < 10; ++i) {
      sl.insert(i, i*i);
    }
    sl.~simplePSkipType();
    sl.recover();
    sl.insert(10, 100);

    int value = 0;
    REQUIRE(!sl.search(11, value));
    REQUIRE(value == 0);
    REQUIRE(sl.search(10, value));
    REQUIRE(value == 100);
    REQUIRE(sl.search(5, value));
    REQUIRE(value == 25);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Print the skip list") {
    auto &sl = *rootRef.skiplist;
    for (auto i = 0u; i < 10; ++i) {
      sl.insert(i, i*i);
    }
    sl.printList();
  }

  /* Clean up ----------------------------------------------------------------------------------- */
  pop.close();
  std::remove(path.c_str());
}
