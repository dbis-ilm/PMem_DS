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

namespace dbis::pbptree {

class ElementOfRank {
  private:
    template <typename KeyType>
      static void swap(KeyType *data, int p, int q) {
        if (p == q) return;
        KeyType merke = data[p];
        data[p] = data[q];
        data[q] = merke;
      }
    template <typename KeyType>
      static KeyType elementOfRankBySorting(int m, KeyType *data, int length, int start) {
        int k = m - 1;
        bool swapped = true;
        while (swapped == true) {
          swapped = false;
          for (int i = start; i < start + length-1; i++) {
            if (data[i] > data[i + 1]) {
              swap(data, i, i + 1);
              swapped = true;
            }
          }
        }
        return data[k];
      }
    template <typename KeyType>
      static KeyType findPivot(KeyType *data, int length, int start) {
        int parts = length / 5;
        KeyType medians[parts];

        if (parts <= 1) { return elementOfRankBySorting(start + (length + 1) / 2, data, length, start); }

        for (int i = 0; i < parts; i++) {
          medians[i] = elementOfRankBySorting(3, data, 5, start + i * 5);
        }
        return findPivot(medians, parts, 0);

      }

  public:
    template <typename KeyType>
      static KeyType elementOfRank(int m, KeyType *data, int length, int start) {
        int k = m - 1;
        if (length <= 15) {
          return elementOfRankBySorting(m, data, length, start);
        }
        int p = start, q = start;
        KeyType pivot = findPivot(data, length, start);
        for (int i = start; i < start + length; i++) {
          if (data[i] < pivot) {
            swap(data, i, q);
            swap(data, p, q);
            q++;
            p++;
          }
          if (data[i] == pivot) {
            swap(data, i, q);
            q++;
          }
        }
        if (k < p) {return elementOfRank(m, data, p - start, start); }
        if (k >= q) {return elementOfRank(m, data, length + start - q - 1, q); }

        return pivot;
      }
}; /* end class */
} /* end namespace dbis::pbptree */

#endif /* DBIS_ELEMENTOFRANKK_HPP */
