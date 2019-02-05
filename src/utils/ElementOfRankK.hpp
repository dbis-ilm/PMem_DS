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

namespace dbis {

  class ElementOfRankK {
    private:

			template<typename KeyType, std::size_t L>
			static void insertionSort(std::array<KeyType, L> &data, int left, int right) {
				for (auto i = left + 1; i <= right; i++) {
					auto temp = data[i];
					auto j = i - 1;
					while (data[j] > temp && j >= left) {
						data[j+1] = data[j];
						j--;
					}
					data[j+1] = temp;
				}
			}

			template<typename KeyType, std::size_t L>
			static void bubbleSort(std::array<KeyType, L> &data, int left, int right) {
        bool swapped = true;
        while (swapped) {
          swapped = false;
          for (int i = left; i < right; i++) {
            if (data[i] > data[i + 1]) {
              std::swap(data[i], data[i + 1]);
              swapped = true;
            }
          }
				}
			}

      template <typename KeyType, std::size_t L>
      static KeyType elementOfRankKBySorting(int k, std::array<KeyType, L> &data, int start, int length) {
				insertionSort(data, start, start + length - 1);
        return data[k-1];
      }

      template <typename KeyType, std::size_t L>
      static KeyType findPivot(std::array<KeyType, L> &data, int start) {
        constexpr auto parts = L / 5;
        if (parts <= 1) { return elementOfRankKBySorting(start + (L + 1) / 2 - 1, data, L, start); }

        std::array<KeyType, parts> medians;
        for (int i = 0; i < parts; i++) {
          medians[i] = elementOfRankKBySorting(2, data, 5, start + i * 5);
        }
        return findPivot(medians, 0);
      }

    public:
      template <typename KeyType, std::size_t L>
      static KeyType elementOfRankK(int k, std::array<KeyType, L> &data, int start, int length) {
        if (length <= 15) {
          return elementOfRankKBySorting(k, data, start, length);
        }
        auto p = start, q = start;
        KeyType pivot = findPivot(data, start);
        for (auto i = start; i < start + length; i++) {
          if (data[i] < pivot) {
            std::swap(data[i], data[q]);
            std::swap(data[p], data[q]);
            q++; p++;
          }
          if (data[i] == pivot) {
            std::swap(data[i], data[q]);
            q++;
          }
        }
        if (k < p) { return elementOfRankK(k, data, start, p - start); }
        if (k >= q) { return elementOfRankK(k, data, q, length + start - q - 1); }

        return pivot;
      }

  }; /* end class */
} /* end namespace dbis */

#endif /* DBIS_ELEMENTOFRANKK_HPP */
