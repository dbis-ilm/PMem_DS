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

#ifndef DBIS_BITOPERATIONS_HPP
#define DBIS_BITOPERATIONS_HPP

#include <bitset>

namespace dbis {

class BitOperations {
 public:
   static constexpr auto deBruijnSeq = 0x07EDD5E59A4E28C2;
   static constexpr uint8_t tab64[64] = {
     63, 0, 58, 1, 59, 47, 53, 2, 60, 39, 48, 27, 54, 33, 42, 3,
     61, 51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4,
     62, 57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21,
     56, 45, 25, 31, 35, 16, 9, 12, 44, 24, 15, 8, 23, 7, 6, 5};

  template<std::size_t E>
  static inline auto getFreeZero(const std::bitset<E> &b) {
    if constexpr (E > 64) {
      auto idx = 0u;
      while (idx < E && b.test(idx)) ++idx;
      return idx;
    } else {
      const auto w = ~b.to_ullong(); ///< we need the complement here!
      if (w != 0) {
        /// count consecutive one bits using multiply and lookup
        /// Applying deBruijn hash function + lookup
        return tab64[((uint64_t) ((w & -w) * deBruijnSeq)) >> 58];
      }
      /// Valid result is between 0 and 63; 64 means no free position
      return static_cast<uint8_t>(64);
    }
  }

  template<std::size_t E>
  static inline auto getFirstSet(const std::bitset<E> &b) {
    if constexpr (E > 64) {
      auto idx = 0u;
      while (idx < E && !b.test(idx)) ++idx;
      return idx;
    } else {
      const auto w = b.to_ullong();
      assert(w != 0); ///< this should never happen in our use cases
      /// count consecutive zero bits using multiply and lookup
      /// Applying deBruijn hash function + lookup
      return tab64[((uint64_t) ((w & -w) * deBruijnSeq)) >> 58];
    }
  }
}; /* end class BitOperations */

} /* end namespace dbis */

#endif /* DBIS_BITOPERATIONS_HPP */
