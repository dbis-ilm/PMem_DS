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

#ifndef DBIS_RANDOM_HPP
#define DBIS_RANDOM_HPP

#include <cstdint> ///< uint32_t/uint64_t
#include <cstdlib> ///< size_t

namespace dbis {

class Random {
  uint32_t seed;

  public:
    explicit Random(uint32_t s) : seed(s & 0x7fffffffu) {
      if (seed == 0 || seed == 2147483647L) {
        seed = 1;
      }
    }

    inline uint32_t Next() {
      static constexpr uint32_t M = 2147483647L;
      static constexpr uint64_t A = 16807;
      const uint64_t product = seed * A;

      seed = static_cast<uint32_t>((product >> 31) + (product & M));
      if (seed > M) seed -= M;
      return seed;
    }

    inline uint32_t Uniform(size_t n) {
      return (Next() % n);
    }

    inline bool OneIn(size_t n) {
      return (Next() % n) == 0;
    }

    inline uint32_t Skewed(size_t max_log) {
      return Uniform(1 << Uniform(max_log + 1));
    }

}; /// class Random

} /// namespace dbis

#endif /// DBIS_RANDOM_HPP
