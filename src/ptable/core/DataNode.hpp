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

#ifndef DataNode_hpp_
#define DataNode_hpp_

#include <array>
#include <vector>

#include "config.h"
#include <libpmemobj++/allocator.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

namespace dbis::ptable {

using pmem::obj::allocator;
using pmem::obj::p;
using pmem::obj::persistent_ptr;

/** Positions in NVM_Block */
constexpr int gBDCCRangePos1 = 0;
constexpr int gBDCCRangePos2 = 4;
constexpr int gCountPos = 8;
constexpr int gFreeSpacePos = 12;
constexpr int gSmaOffsetPos = 14;
constexpr int gDataOffsetPos = 16;

/** Sizes/Lengths in NVM_Block */
constexpr int gFixedHeaderSize = 14;
constexpr int gBDCCValueSize = 4;
constexpr int gAttrOffsetSize = 4;
constexpr int gOffsetSize = 2;

/** The size of a single block in persistent memory */
static constexpr std::size_t gBlockSize = 1 << 15; // 12->4KB, 15->32KB, max 16 due to data types

/**
 * \brief This type represents a byte array used for persistent structures.
 *
 * A BDCC_Block is a PAX oriented data block with the following structure for 32KB:
 * <bdcc_range><bdcc_cnt><sma_offset_0><data_offset_0> ...<sma_offset_n><data_offset_n>
 * <sma_min_0><sma_max_0><data_vector_0> ... <sma_min_n><sma_max_n><data_vector_n>
 *  0 bdcc_range          -> long (x2) - 8 Byte
 *  8 bdcc_cnt            -> long - 4 Byte
 * 12 free_space         -> unsigned short
 * for each attribute:
 * 14 sma_offset_x       -> unsigned short - 2 Byte (depends on block size)
 * 16 data_offset_x      -> unsigned short
 * ...
 *
 * for each attribute (int, double):
 *  . sma_min_x          -> size of attributes data type
 *  . sma_max_x          -> size of attributes data type
 *  . data_vector        -> size of attributes data type * bdcc_cnt
 *  ...
 *
 * for each attribute (string - data starts at the end of the minipage):
 *  . sma_min_offset_x   -> unsigned short
 *  . sma_max_offset_x   -> unsigned short
 *  . data_offset_vector -> unsigned short * bdcc_cnt
 *  . ...
 *  . data               -> size of all strings + bdcc_cnt (Nul termination)
 */
using BDCC_Block = typename std::array<uint8_t, gBlockSize>;

template<typename KeyType>
struct DataNode {
  using DeletedVector = std::vector<uint16_t, allocator<uint16_t>>;
  using KeyVector = std::array<KeyType, 8192>;
  /*
  using HistogramType = std::unordered_map<uint32_t,
                                          std::size_t,
                                          std::hash<uint32_t>,
                                          std::equal_to<uint32_t>,
                                          allocator<std::pair<const uint32_t, std::size_t>>>;*/

  DataNode() : next(nullptr) {}
  DataNode(BDCC_Block _block) : next(nullptr), block(_block) {}

  persistent_ptr<DataNode> next;
  p<BDCC_Block> block;
  //p<KeyVector> keys;
  p<DeletedVector> deleted;
  //p<HistogramType> histogram;
  p<size_t> bdccSum{0};

  inline uint32_t calcAverageBDCC() const {
    //auto sum = 0u;
    //for(const auto &bdccValue : histogram.get_ro()) {
    //  sum += bdccValue.first * bdccValue.second;
    //}
    return bdccSum / reinterpret_cast<const uint32_t &>(block.get_ro()[gCountPos]);
  }
};

} /* end namespace dbis::ptable */

#endif /* DataNode_hpp_ */
