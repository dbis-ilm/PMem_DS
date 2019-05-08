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

#ifndef DBIS_ELEMENTOFRANKK_HPP
#define DBIS_ELEMENTOFRANKK_HPP

#include <cstdlib> //< rand()
#include <bitset>

namespace dbis {

/* Based on: https://www.geeksforgeeks.org/kth-smallestlargest-element-unsorted-array-set-3-worst-case-linear-time/
 * · Extension of QuickSelect
 */
class ElementOfRankK {
 public:
  template <typename KeyType>
  static size_t elementOfRankK(const size_t k, KeyType data[], const size_t start,
                             const size_t length) {
    const size_t end = start + length - 1;
    size_t i;
    const auto parts = (length  + 4) / 5;
    KeyType median[parts];
    for (i = 0; i < length / 5; i++) median[i] = findMedian(data + start + i * 5, 5);
    if (i * 5 < length) {
      median[i] = findMedian(data + start + i * 5, length % 5);
      i++;
    }
    const KeyType medOfMed = (i == 1)? median[i - 1]: elementOfRankK(i / 2, median, 0, i);
    const size_t pos = partition(data, start, end, medOfMed);
    if (pos - start == k - 1) return data[pos];
    if (pos - start  > k - 1) return elementOfRankK(k, data, start, pos - start);
    return elementOfRankK(k - pos + start - 1, data, pos + 1, end - pos);
  }


  private:
    template <typename KeyType>
    static inline KeyType findMedian(KeyType data[], const size_t length) {
      std::sort(data, data + length);
      return data[length / 2];
    }

    template <typename KeyType>
    static auto partition(KeyType data[], const size_t start, const size_t end, const KeyType &x) {
      size_t i;
      for (i = start; i < end; i++) if (data[i] == x) break;
      std::swap(data[i], data[end]);

      /// quick partition algorithm
      i = start;
      for (int j = start; j < end; j++)
        if (data[j] <= x) std::swap(data[i++], data[j]);
      std::swap(data[i], data[end]);
      return i;
    }

}; /* end class */

  template<typename KeyType, size_t M>
  std::pair<std::bitset<M>, size_t>
  findSplitKey(const std::array<KeyType, M> &data) {
    auto repData = data;
    std::bitset<M> b{};
    const auto splitKey = ElementOfRankK::elementOfRankK((M+1)/2, repData.data(), 0, M);
    size_t splitPos;
    for(auto i = 0u; i < M; i++) {
      if(data[i] <= splitKey) {
        b.set(i);
        if(data[i] == splitKey) splitPos = i;
      }
    }
    return std::make_pair(b, splitPos);
  }

} /* end namespace dbis */

#endif /* DBIS_ELEMENTOFRANKK_HPP */
