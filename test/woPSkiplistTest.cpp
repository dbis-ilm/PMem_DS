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
#include "woPSkiplist.hpp"

using namespace dbis::pskiplists;

using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;

TEST_CASE("Insert and lookup key", "[woPSkiplist]") {
  using woPSkipType5 = woPSkiplist<int, int, 5, 5>;
  using woPSkipType8 = woPSkiplist<int, int, 8, 5>;

  struct root {
    persistent_ptr<woPSkipType5> skiplist5;
    persistent_ptr<woPSkipType8> skiplist8;
  };

  pool<root> pop;
  const std::string path = dbis::gPmemPath + "woPSkiplistTest";

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, "woPSkiplist", ((size_t)(1024 * 1024 * 16)));
  } else {
    pop = pool<root>::open(path, "woPSkiplist");
    pop.root()->skiplist5->recover();
    pop.root()->skiplist8->recover();
  }

  auto q = pop.root();
  auto &rootRef = *q;
  const auto alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.new.desc",
                                                                     woPSkipType5::AllocClass);

  if (!rootRef.skiplist5)
    transaction::run(pop, [&] { rootRef.skiplist5 = make_persistent<woPSkipType5>(alloc_class); });
  if (!rootRef.skiplist8)
    transaction::run(pop, [&] { rootRef.skiplist8 = make_persistent<woPSkipType8>(alloc_class); });

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Looking up a key") {
    auto &sl = *rootRef.skiplist5;
    auto &node = *sl.head->forward[0];
    for (auto i = 0; i < 5; ++i) {
      node.keys[i] = i;
      node.bits.get_rw().set(i);
    }
    node.minKey.get_rw() = 0;
    node.maxKey.get_rw() = 4;

    int val;
    REQUIRE(sl.search(0, val));
    REQUIRE(sl.search(3, val));
    REQUIRE(!sl.search(5, val));
  }


  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting keys") {
    auto &sl = *rootRef.skiplist8;
    for (auto i = 0u; i < 8; ++i) {
      sl.insert(i, i*i);
    }

    const auto &node = *sl.searchNode(0);
    REQUIRE(node.isFull());
    REQUIRE(node.minKey == 0);
    REQUIRE(node.maxKey == 7);
    REQUIRE(node.forward[0] == nullptr);
    std::array<int, 8> expectedKeys{{0, 1, 2, 3, 4, 5, 6, 7}};
    std::array<int, 8> expectedValues{{0, 1, 4, 9, 16, 25, 36, 49}};
    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(node.keys)));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(node.values)));
  }
  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting keys with split") {
    auto &sl = *rootRef.skiplist8;
    for (auto i = 0u; i < 10; ++i) {
      sl.insert(i, i*i);
    }

    const auto &node1 = *sl.searchNode(0);
    REQUIRE(node1.forward[0] != nullptr);
    const auto &node2 = *node1.forward[0];
    REQUIRE(node2.forward[0] == nullptr);
    REQUIRE(!node1.isFull());
    REQUIRE(!node2.isFull());
    REQUIRE(node1.minKey == 0);
    REQUIRE(node1.maxKey == 3);
    REQUIRE(node2.minKey == 4);
    REQUIRE(node2.maxKey == 9);
    std::array<int, 4> expectedKeys1{{0, 1, 2, 3}};
    std::array<int, 4> expectedValues1{{0, 1, 4, 9}};
    std::array<int, 6> expectedKeys2{{4, 5, 6, 7, 8, 9}};
    std::array<int, 6> expectedValues2{{16, 25, 36, 49, 64, 81}};
    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1), node1.keys.cbegin()));
    REQUIRE(std::equal(std::begin(expectedValues1), std::end(expectedValues1),
                       node1.values.cbegin()));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2), std::begin(node2.keys)));
    REQUIRE(std::equal(std::begin(expectedValues2), std::end(expectedValues2),
                       std::begin(node2.values)));
    REQUIRE(sl.nodeCount.get_ro() == 2);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Recover the skip list") {
    auto &sl = *rootRef.skiplist8;
    for (auto i = 0u; i < 10; ++i) {
      sl.insert(i, i*i);
    }
    sl.~woPSkipType8();
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
    auto &sl = *rootRef.skiplist8;
    for (auto i = 0u; i < 10; ++i) {
      sl.insert(i, i*i);
    }
    sl.printList();
  }

  /* Clean up ----------------------------------------------------------------------------------- */
  pop.close();
  std::remove(path.c_str());
}
