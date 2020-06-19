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

#ifndef DBIS_ELEMENTOFRANKK_HPP
#define DBIS_ELEMENTOFRANKK_HPP

#include <libpmemobj++/persistent_ptr.hpp>

#include <algorithm>
#include <array>
#include <bitset>
#include <cstdlib> //< rand()

namespace dbis {

template<typename Object>
using pptr = pmem::obj::persistent_ptr<Object>;

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
    for (i = 0; i < length / 5; ++i) median[i] = findMedian(data + start + i * 5, 5);
    if (i * 5 < length) {
      median[i] = findMedian(data + start + i * 5, length % 5);
      ++i;
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
      for (i = start; i < end; ++i) if (data[i] == x) break;
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
findSplitKey(const KeyType * const data) {
  ///TODO: can this be accelerated?
  std::array<KeyType, M> repData;
  memcpy(repData.begin(), data, M * sizeof(KeyType));
  std::bitset<M> b{};
  const auto splitKey = ElementOfRankK::elementOfRankK((M+1)/2 + 1, repData.data(), 0, M);
  size_t splitPos;
  for(auto i = 0u; i < M; ++i) {
    if(data[i] < splitKey) {
      b.set(i);
    } else if(data[i] == splitKey) splitPos = i;
  }
  return std::make_pair(b, splitPos);
}

/**
 * Find the minimum key in a persistent node without bitmap or other supporting structures.
 *
 * @param node the node to find the minimum key in
 * @return position of the minimum key
 */
template<typename Node>
static inline auto findMinKey(const pptr<Node> &node) {
  const auto &nodeRef = *node;
  const auto &keysRef = nodeRef.keys.get_ro();
  auto pos = 0u;
  auto currMinKey = keysRef[0];
  /// with partitioning this could be done in O(log n) - but necessary?
  for (auto i = 1u; i < nodeRef.numKeys.get_ro(); ++i) {
    if (keysRef[i] < currMinKey) { currMinKey = keysRef[i]; pos = i; }
  }
  return pos;
}

/**
 * Find the minimum key in a key array with bitmap.
 *
 * @param node the node to find the minimum key in
 * @return position of the minimum key
 */
template<typename KeyType, size_t N>
static inline auto findMinKey(const std::array<KeyType, N> &keysRef,
                              const std::bitset<N> &bitsRef) {
  auto pos = 0u;
  auto currMinKey = std::numeric_limits<KeyType>::max();
  for (auto i = 0u; i < N; ++i) {
    if (bitsRef.test(i) && keysRef[i] < currMinKey) {
      currMinKey = keysRef[i]; pos = i;
    }
  }
  return pos;
}

/**
 * Find the maximum key in a persistent node without bitmap or other supporting structures.
 *
 * @param node the node to find the maximum key in
 * @return position of the maximum key
 */
template<typename Node>
static inline auto findMaxKey(const pptr<Node> &node) {
  const auto &nodeRef = *node;
  const auto &keysRef = nodeRef.keys.get_ro();
  auto pos = 0u;
  auto currMaxKey = keysRef[0];
  /// with partitioning this could be done in O(log n) - but necessary?
  for (auto i = 1u; i < nodeRef.numKeys.get_ro(); ++i) {
    if (keysRef[i] > currMaxKey) { currMaxKey = keysRef[i]; pos = i; }
  }
  return pos;
}

/**
 * Find the maximum key in a key array with bitmap.
 *
 * @param keysRef a reference to the node's keys to find the maximum key in
 * @param bitsRef a reference to the bitset of the node
 * @return position of the maximum key
 */
template<typename KeyType, size_t N>
static inline auto findMaxKey(const std::array<KeyType, N> &keysRef,
                              const std::bitset<N> &bitsRef) {
  auto pos = 0u;
  auto currMaxKey = std::numeric_limits<KeyType>::min();
  for (auto i = 0u; i < N; ++i) {
    if (bitsRef.test(i) && keysRef[i] > currMaxKey) {
      currMaxKey = keysRef[i]; pos = i;
    }
  }
  return pos;
}

/**
 * Searches for the next greater key than @key in a key array with bitmap.
 *
 * @param keysRef a reference to the node's keys to find the key in
 * @param bitsRef a reference to the bitset of the node
 * @param key the current minimum key
 * @return position of the next minimum key
 */
template<size_t N, typename KeyType>
static inline auto findMinKeyGreaterThan(const std::array<KeyType, N> &keysRef,
                                         const std::bitset<N> &bitsRef,
                                         const KeyType &key) {
  auto pos = 0ul;
  constexpr auto maxLmt = std::numeric_limits<KeyType>::max();
  auto currMinKey = maxLmt;
  for (auto i = 0u; i < N; ++i) {
    if (bitsRef.test(i) && keysRef[i] < currMinKey && keysRef[i] > key) {
      currMinKey = keysRef[i]; pos = i;
    }
  }
  if (currMinKey == maxLmt) return N;
  return pos;
}

/**
 * Searches for the next smaller key than @key in a key array with bitmap.
 *
 * @param keysRef a reference to the node's keys to find the key in
 * @param bitsRef a reference to the bitset of the node
 * @param key the current maximum key
 * @return position of the next maximum key
 */
template<size_t N, typename KeyType>
static inline auto findMaxKeySmallerThan(const std::array<KeyType, N> &keysRef,
                                         const std::bitset<N> &bitsRef,
                                         const KeyType &key) {
  auto pos = 0ul;
  constexpr auto minLmt = std::numeric_limits<KeyType>::min();
  auto currMaxKey = minLmt;
  for (auto i = 0u; i < N; ++i) {
    if (bitsRef.test(i) && keysRef[i] > currMaxKey && keysRef[i] < key) {
      currMaxKey = keysRef[i]; pos = i;
    }
  }
  if (currMaxKey == minLmt) return N;
  return pos;
}

/**
 * Does a binary searches for key @key in a sorted array of keys.
 *
 * @param keys array of sorted keys
 * @param l left fence of considered range
 * @param r right fence of considered range
 * @param key the searched key
 * @return position of the key
 */
template<bool isBranch, typename KeyType, size_t N>
static inline unsigned int binarySearch(const std::array<KeyType, N> &keys, int l, int r,
                                        const KeyType &key) {
  auto pos = 0u;
  while (l <= r) {
    pos = (l + r) / 2;
    if (keys[pos] == key) {
      if constexpr (isBranch) return ++pos;
      else return pos;
    }
    if (keys[pos] < key) l = ++pos;
    else r = pos - 1;
  }
  return pos;
}

/**
 * Does a binary searches for key @key in an array of keys with sorted indirection slots.
 *
 * @param keys array of keys
 * @param slots array of sorted slots pointing to key
 * @param l left fence of considered range
 * @param r right fence of considered range
 * @param key the searched key
 * @return position of the key
 */
template<bool isBranch, typename KeyType, size_t N>
static inline unsigned int binarySearch(const std::array<KeyType, N> &keys,
                                        const std::array<uint8_t, N+1> &slots, int l, int r,
                                        const KeyType &key) {
  auto pos = 1u;
  while (l <= r) {
    pos = (l + r) / 2;
    if (keys[slots[pos]] == key) {
      if constexpr (isBranch) return ++pos;
      else return pos;
    }
    if (keys[slots[pos]] < key) l = ++pos;
    else r = pos - 1;
  }
  return pos;
}

} /* end namespace dbis */

#endif /* DBIS_ELEMENTOFRANKK_HPP */
