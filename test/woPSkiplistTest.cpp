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
#include "woPSkiplist.hpp"

using namespace dbis::pskiplists;

using pmem::obj::make_persistent;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;

TEST_CASE("Insert and lookup key") {
  using woPSkip = woPSkiplist<int, int, 8, 8>;

  struct root {
    persistent_ptr<woPSkip> skiplist;
  };

  pool<root> pop;
  const std::string path = dbis::gPmemPath + "woPSkiplistTest";

  //std::remove(path.c_str());
  if (access(path.c_str(), F_OK) != 0)
    pop = pool<root>::create(path, "woPSkiplist", ((size_t)(1024 * 1024 * 16)));
  else
    pop = pool<root>::open(path, "woPSkiplist");

  auto q = pop.root();
  auto &rootRef = *q;

  if(!rootRef.skiplist)
    transaction::run(pop, [&] {rootRef.skiplist = make_persistent<woPSkip>(); });

  SECTION("Inserting keys") {
    auto &sl = *rootRef.skiplist;
    for(int i=0; i<10; ++i) {
      sl.insert(i, i*i);
    }
    REQUIRE(sl.search(5) != nullptr);
  }
}
