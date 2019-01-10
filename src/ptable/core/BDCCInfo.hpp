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

#ifndef BDCCInfo_hpp_
#define BDCCInfo_hpp_

#include <bitset>
#include <map>
#include <numeric>

#include <libpmemobj++/allocator.hpp>
#include <libpmemobj++/p.hpp>

#include "PTableInfo.hpp"

namespace dbis::ptable {

using pmem::obj::allocator;
using pmem::obj::p;

/**************************************************************************//**
 * \brief This type is used for defining the dimensions for the table.
 *
 *  entry: <ColumnID, number of BDCC bits , bit alignment when applying mask on value>
 *****************************************************************************/
using Dimensions = std::vector<std::tuple<uint16_t, uint16_t, uint16_t>>;

/**************************************************************************//**
 * \brief Info structure about the BDCC meta data.
 *
 * It is used in persistent tables to store the BDCC meta data and statistics.
 *****************************************************************************/
class BDCCInfo {
  /** (Column ID, # BDCC bits, positions in BDCC key value, bit alignment when applying mask on value) */
  using DimensionUse = std::tuple<uint16_t,
                                  uint16_t,
                                  std::bitset<32>,
                                  uint16_t>;
  using DimensionUses = std::vector<DimensionUse, allocator<DimensionUse>>;

  p<size_t> numberOfBins;
  p<DimensionUses> dimensions;

 public:
  BDCCInfo() : numberOfBins(0), dimensions() {}

  explicit BDCCInfo(const Dimensions &_bitMap) :
    numberOfBins(std::accumulate(_bitMap.begin(), _bitMap.end(), 0,
                                 [](const size_t sum, decltype(*_bitMap.begin()) p) {
                                   return sum + std::get<1>(p);
                                 })),
    dimensions() { deriveMasks(_bitMap); }

  const auto numBins() const { return numberOfBins.get_ro(); }

  const auto begin() const { return dimensions.get_ro().cbegin(); }

  const auto end() const { return dimensions.get_ro().cend(); }

 private:
  void deriveMasks(Dimensions dims) {
    /* Initialize */
    for (const auto &dim: dims) {
      dimensions.get_rw().emplace_back(std::get<0>(dim), std::get<1>(dim), std::bitset<32>(), std::get<2>(dim));
    }

    /* Round robin the bins for mapping */
    auto bdccSize = numBins();
    while (bdccSize > 0) {
      auto i = 0ul;
      for (auto &dim: dims) {
        if (std::get<1>(dim) > 0) {
          --std::get<1>(dim);
          std::get<2>(dimensions.get_rw()[i])[--bdccSize] = 1;
        }
        ++i;
      }
    }
  }
};/* struct BDCCInfo */

/**************************************************************************//**
 * \brief applyMask is a helper function to apply a mask on an arbitrary value.
 *
 * \tparam T
 *   the type of the value
 * \param value
 *   the value the mask should be applied on
 * \param mask
 *   the mask applied on the value
 * \return
 *   the index masked value
 *****************************************************************************/
template<typename T>
inline int applyMask(const T &value, int mask);

template<typename T>
inline int applyMask(const T &value, int mask) {
  return *reinterpret_cast<const int *>(&value) & mask;
}

template<>
inline int applyMask(const int &value, int mask) { return value & mask; }

template<>
inline int applyMask(const double &value, int mask) { return (int) value & mask; }

template<>
inline int applyMask(const std::string &value, int mask) {
  return *reinterpret_cast<const int *>(value.data()) & mask;
}

} /* namespace dbis::ptable */

#endif /* BDCCInfo_hpp_ */
