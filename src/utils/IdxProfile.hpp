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

#ifndef DBIS_IDXPROFILE_HPP
#define DBIS_IDXPROFILE_HPP

#include <iostream>

namespace dbis {
  class IdxProfile {
    int read_count;
    int write_count;
    int split_count;
    int underflow_count;
    public:
    IdxProfile() {
      read_count = 0;
      write_count = 0;
      split_count = 0;
      underflow_count = 0;
    }

    void read() { read_count++; }
    void read(int x) { read_count+=x; }
    void write() { write_count++; }
    void write(int x) { write_count+=x; }
    void split() { split_count++; }

    void underflow() { underflow_count++; }

    void diff(IdxProfile prof) {
      this->read_count -= prof.read_count;
      this->write_count -= prof.write_count;
      this->split_count -= prof.split_count;
      this->underflow_count -= prof.underflow_count;
    }

    void print() {
      std::cout << "\nIndex Profile" << std::endl;
      std::cout << "\tReads:\t" << read_count << std::endl;
      std::cout << "\tWrites:\t" << write_count << std::endl;
      std::cout << "\tSplits:\t" << split_count << std::endl;
      std::cout << "\tUnderflows:\t" << underflow_count << std::endl;
    }

  };//end class
} /* end namespace dbis */

#endif /* DBIS_IDXPROFILE_HPP */
