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

#include "config.h"
#include "catch.hpp"
#include "Patricia.hpp"
#include <libpmemobj++/utils.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

static std::vector<std::pair<std::string, int>> entries = { std::make_pair("java", 34)
        , std::make_pair("pearl", 12)
        , std::make_pair("c", 3)
        , std::make_pair("cpp", 300)
        , std::make_pair("Honorificabilitudinitatibus", 10)};

TEST_CASE("Testing PatriciaTrie", "PatriciaTrie")
{
    const std::string path = dbis::gPmemPath + "PatriciaTest";
    pmem::obj::pool<PatriciaTree<int>> pop;
    if (access(path.c_str(), F_OK) != 0)
    {
        pop = pmem::obj::pool<PatriciaTree<int>>::create(path, "BaseTrie", 1024 * 1024 * 16);
    }
    else
    {
        pop = pmem::obj::pool<PatriciaTree<int>>::open(path, "BaseTrie");
    }
    const auto& root = pop.root();

    SECTION("Insert") {
        for (const auto entry_pair : entries)
        {
            root->insert(entry_pair.first.c_str(), entry_pair.first.size(), entry_pair.second);
        }
    }

    SECTION("Lookup") {
        int* result = new int(-1);
        for (const auto entry_pair : entries)
        {
            REQUIRE((root->lookup(entry_pair.first.c_str(), entry_pair.first.size(), result) && *result == entry_pair.second));
        }
        std::string not_existing("not existing");
        REQUIRE(!root->lookup(not_existing.c_str(), not_existing.size(), result));
    }

    SECTION("Update") {
        int update_val = 4;
        int* result = new int(-1);
        for (const auto entry_pair : entries)
        {
            root->insert(entry_pair.first.c_str(), entry_pair.first.size(), ++update_val);
            REQUIRE((root->lookup(entry_pair.first.c_str(), entry_pair.first.size(), result) && *result == update_val));
        }
    }

    SECTION("Delete") {
        int* result = new int(-1);
        assert(entries.size() > 0);
        auto& entry_pair = entries[0];
        assert(root->lookup(entry_pair.first.c_str(), entry_pair.first.size()));
        root->remove(entry_pair.first.c_str(), entry_pair.first.size());
        REQUIRE(!root->lookup(entry_pair.first.c_str(), entry_pair.first.size()));
        entries.erase(entries.begin());
    }

    pop.close();
}
