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

#include "PTableInfo.hpp"

using namespace dbis::ptable;

PColumnInfo::PColumnInfo(pool_base pop) : PColumnInfo(pop, "", ColumnType::VoidType) {};

PColumnInfo::PColumnInfo(pool_base pop, Column col) : PColumnInfo(pop, col.first, col.second) {};

PColumnInfo::PColumnInfo(pool_base pop, const std::string &n, ColumnType ct) : type(ct) {
  if (pmemobj_tx_stage() == TX_STAGE_NONE) {
    transaction::run(pop, [&] {
        name = make_persistent<char[]>(n.length() + 1);
        strcpy(name.get(), n.c_str());
        });
  } else {
    name = make_persistent<char[]>(n.length() + 1);
    strcpy(name.get(), n.c_str());
  }
}
