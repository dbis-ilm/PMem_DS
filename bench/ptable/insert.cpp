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

#include "common.hpp"

using namespace dbis::ptable;

int main() {
  pool<root> pop;

  pmempool_rm(path.c_str(), 0);
  if (access(path.c_str(), F_OK) != 0) {
    insert(pop, path, NUM_TUPLES);
  } else {
    std::cerr << "Table already exists" << std::endl;
  }

  auto pTable = pop.root()->pTable;

  pop.close();
}
