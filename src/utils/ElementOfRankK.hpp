/*
 * Copyright (C) 2017-2019 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of our NVM-based Data Structure Repository.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef DBIS_ELEMENTOFRANKK_HPP
#define DBIS_ELEMENTOFRANKK_HPP

#include <cstdlib> //< rand()

namespace dbis {

/* Based on: https://www.geeksforgeeks.org/kth-smallestlargest-element-unsorted-array-set-2-expected-linear-time/
 * · Extension of QuickSelect
 */
class ElementOfRankK {
  public:
    template <typename KeyType, size_t L>
    static KeyType elementOfRankK(int const k, std::array<KeyType, L> &data, int const start, int const length) {
      assert(k > 0 && k <= length);

      auto const end = start + length - 1;
      int pos = setPivot(data, start, start + length - 1);
      if (pos - start == k-1) return data[pos];
      if (pos - start > k-1) return elementOfRankK(k, data, start, pos-start+1);
      return elementOfRankK(k-pos+start-1, data, pos+1, length-(pos+1-start));
    }
  private:
			template <typename KeyType, size_t L>
			static int quickPartition(std::array<KeyType, L> &data, int const start, int const end) {
				int x = data[end], i = start;
				for (int j = start; j < end; j++)
					if (data[j] <= x)
          std::swap(data[i++], data[j]);
				std::swap(data[i], data[end]);
				return i;
			}

			template <typename KeyType, size_t L>
			static int setPivot(std::array<KeyType, L> &data, int const start, int const end) {
				auto const pivot = rand()/((RAND_MAX + 1u)/(end-start+1));
				std::swap(data[start + pivot], data[end]);
				return quickPartition(data, start, end);
			}


}; /* end class */
} /* end namespace dbis */

#endif /* DBIS_ELEMENTOFRANKK_HPP */
