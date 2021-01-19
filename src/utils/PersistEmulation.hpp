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

#ifndef DBIS_PERSISTEMULATION_HPP
#define DBIS_PERSISTEMULATION_HPP

#include <cstddef>

namespace dbis {

static unsigned int bytesWritten = 0u;

class PersistEmulation {
  public:
    static unsigned int getBytesWritten() {
      auto bytes = bytesWritten;
      bytesWritten = 0;
      return bytes;
    }

    template<std::size_t B>
    static void writeBytes() {
      bytesWritten += B;
    }

    static void writeBytes(std::size_t B) {
      bytesWritten += B;
    }

    static inline void persistStall() {
      constexpr auto freq = 3470; //< in MHz
      constexpr auto nsecs = 750-75;
      constexpr auto overhead = 32;
      constexpr auto cycles =  freq * (nsecs - overhead) / 1000;
      __asm__ __volatile__(
          "RDTSCP;"
          "movl %%eax, %%ebx;"
          "1:"
          "RDTSC;"
          "subl %%ebx, %%eax;"
          "cmpl %[cyc], %%eax;"
          "jl 1b"
          : : [cyc]"r"(cycles)
          : "%eax", "%ebx", "%ecx", "%edx"
          );

    }
}; /* end class PersistEmulation */

} /* end namespace dbis */

#endif /* DBIS_PERSISTEMULATION_HPP */
