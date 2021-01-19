/*
 * Copyright (C) 2017-2021 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of our PMem-based Data Structures repository.
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

#ifndef DBIS_SKIP_COMMON_HPP
#define DBIS_SKIP_COMMON_HPP

#include <unistd.h>

#include "config.h"
#define UNIT_TESTS
#include "woPSkiplist.hpp"

using namespace dbis::pskiplists;

/*========== Types and constants =================================================================*/
constexpr auto CACHE_LINE = 64;
constexpr auto DCPMM_LINE = 4 * CACHE_LINE;
constexpr auto KiB = 1024;
constexpr auto MiB = KiB * KiB;
constexpr auto GiB = MiB * KiB;
constexpr auto L3 = 14080 * KiB;
constexpr auto TARGET_NODE_SIZE = 16 * DCPMM_LINE;
constexpr auto ArraySize = 4 * L3 / DCPMM_LINE; /// or ... / TARGET_NODE_SIZE
constexpr auto NODE_PTR_SIZE = sizeof(pptr<int>);
const std::string path = dbis::gPmemPath + "pskiplist_bench.data";
constexpr auto LAYOUT = "PSkipList";
constexpr auto POOL_SIZE = 2ull * GiB;
constexpr auto LEVEL = 5;

using KeyType = unsigned long long;
using ValueType = std::tuple<int, int, double>;
template <size_t KEYS>
constexpr size_t getNodeKeys() {
  constexpr auto wordBits = sizeof(unsigned long) * 8;
  constexpr auto BM = (KEYS + wordBits - 1) / wordBits * 8;  ///< bitmap size round to full words
  constexpr auto FORW = LEVEL * NODE_PTR_SIZE;
  constexpr auto CL = ((BM + FORW + 2 * sizeof(KeyType) + sizeof(size_t) + 63) / 64) * 64;
  constexpr auto SIZE = CL + KEYS * (sizeof(KeyType) + sizeof(ValueType));
  if constexpr (SIZE <= TARGET_NODE_SIZE)
    return KEYS;
  else
    return getNodeKeys<KEYS - 1>();
}

constexpr size_t getNodeKeys() {
  constexpr auto KEYS = ((TARGET_NODE_SIZE - 64) / (sizeof(KeyType) + sizeof(ValueType)));
  return getNodeKeys<KEYS>();
}

constexpr auto NODEKEYS = getNodeKeys();
constexpr auto TARGET_KEY = NODEKEYS;
using ListType = woPSkiplist<KeyType, ValueType, NODEKEYS, LEVEL>;

#endif /// DBIS_SKIP_COMMON_HPP
