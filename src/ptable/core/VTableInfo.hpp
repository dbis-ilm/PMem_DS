/*
 * Copyright (C) 2017 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of PTable.
 *
 * PTable is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PTable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PTable.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef VTableInfo_hpp_
#define VTableInfo_hpp_

#include <memory>
#include <string>
#include <vector>
#include "DataNode.hpp"
#include "PTableException.hpp"
#include "serialize.hpp"
#include "utils.hpp"

namespace dbis::ptable {

enum class ColumnType { VoidType, IntType, DoubleType, StringType };

template<typename T>
constexpr ColumnType toColumnType() { return ColumnType::VoidType; }
template<>
constexpr ColumnType toColumnType<int>() { return ColumnType::IntType; }
template<>
constexpr ColumnType toColumnType<double>() { return ColumnType::DoubleType; }
template<>
constexpr ColumnType toColumnType<std::string>() { return ColumnType::StringType; }

template<ColumnType CT>
struct ColumnAttributes;

template<>
struct ColumnAttributes<ColumnType::VoidType> {
  using type = void;
  using typeInBlock = void;
  static constexpr auto typeName = "Void";
  static constexpr int dataSize = gOffsetSize;
  static constexpr int miniPagePortion = 0;
  static inline const int dataOffsetPos(const BDCC_Block &b, int colDataOffset, int pos) {
    return reinterpret_cast<const uint16_t &>(b[colDataOffset]) + pos * dataSize;
  }
  static inline const auto smaPos(const BDCC_Block &b, int colSmaOffset) {
    const auto &smaPos = reinterpret_cast<const uint16_t &>(b[colSmaOffset]);
    const auto &smaMin = reinterpret_cast<const int &>(b[smaPos]);
    const auto &smaMax = reinterpret_cast<const int &>(b[smaPos + dataSize]);
    return std::pair<int, int>(smaMin, smaMax);
  }
  static inline const unsigned int freeSpace(const BDCC_Block &b, int dataPos, int cnt, int nCols, int idx) {
    const auto nextMiniPageStart = (nCols == idx + 1) ? gBlockSize :
                                   reinterpret_cast<const uint16_t &>(b[gSmaOffsetPos + (idx + 1) * gAttrOffsetSize]);
    return nextMiniPageStart - dataPos - (cnt * dataSize);
  }
  static inline const bool enoughSpace(unsigned int freeSpace, unsigned int *recordOffset, const StreamType &buf) {
    *recordOffset += dataSize;
    if (freeSpace < dataSize) return false;
    return true;
  }
};

template<>
struct ColumnAttributes<ColumnType::IntType> {
  using type = int;
  using typeInBlock = const int&;
  static constexpr auto typeName = "Int";
  static constexpr int dataSize = sizeof(int);
  static constexpr int miniPagePortion = 1;
  static inline const int dataOffsetPos(const BDCC_Block &b, int colDataOffset, int pos) {
    return reinterpret_cast<const uint16_t &>(b[colDataOffset]) + pos * dataSize;
  }
  static inline const auto smaPos(const BDCC_Block &b, int colSmaOffset) {
    const auto &smaPos = reinterpret_cast<const uint16_t &>(b[colSmaOffset]);
    const auto &smaMin = reinterpret_cast<const int &>(b[smaPos]);
    const auto &smaMax = reinterpret_cast<const int &>(b[smaPos + dataSize]);
    return std::pair<int,int>(smaMin, smaMax);
  }
  static inline const unsigned int freeSpace(const BDCC_Block &b, int dataPos, int cnt, int nCols, int idx) {
    const auto nextMiniPageStart = (nCols == idx + 1) ? gBlockSize :
                                   reinterpret_cast<const uint16_t &>(b[gSmaOffsetPos + (idx + 1) * gAttrOffsetSize]);
    return nextMiniPageStart - dataPos - (cnt * dataSize);
  }
  static inline const bool enoughSpace(unsigned int freeSpace, unsigned int *recordOffset, const StreamType &buf) {
    *recordOffset += dataSize;
    if (freeSpace < dataSize) return false;
    return true;
  }
};

template<>
struct ColumnAttributes<ColumnType::DoubleType> {
  using type = double;
  using typeInBlock = const double&;
  static constexpr auto typeName = "Double";
  static constexpr int dataSize = sizeof(double);
  static constexpr int miniPagePortion = 2;
  static inline const int dataOffsetPos(const BDCC_Block &b, int colDataOffset, int pos) {
    return reinterpret_cast<const uint16_t &>(b[colDataOffset]) + pos * dataSize;
  }
  static inline const auto smaPos(const BDCC_Block &b, int colSmaOffset) {
    const auto &smaPos = reinterpret_cast<const uint16_t &>(b[colSmaOffset]);
    const auto &smaMin = reinterpret_cast<const double &>(b[smaPos]);
    const auto &smaMax = reinterpret_cast<const double &>(b[smaPos + dataSize]);
    return std::pair<double, double>(smaMin, smaMax);
  }
  static inline const unsigned int freeSpace(const BDCC_Block &b, int dataPos, int cnt, int nCols, int idx) {
    const auto nextMiniPageStart = (nCols == idx + 1) ? gBlockSize :
                                   reinterpret_cast<const uint16_t &>(b[gSmaOffsetPos + (idx + 1) * gAttrOffsetSize]);
    return nextMiniPageStart - dataPos - (cnt * dataSize);
  }
  static inline const bool enoughSpace(unsigned int freeSpace, unsigned int *recordOffset, const StreamType &buf) {
    *recordOffset += dataSize;
    if (freeSpace < dataSize) return false;
    return true;
  }
};

template<>
struct ColumnAttributes<ColumnType::StringType> {
  using type = std::string;
  using typeInBlock = const char(&)[];
  static constexpr auto typeName = "String";
  static constexpr int dataSize = gOffsetSize; //indirection via offsets
  static constexpr int miniPagePortion = 4;
  static inline const int dataOffsetPos(const BDCC_Block &b, int colDataOffset, int pos) {
    return reinterpret_cast<const uint16_t &>(b[reinterpret_cast<const uint16_t &>(b[colDataOffset]) + pos * dataSize]);
  }
  static inline const auto smaPos(const BDCC_Block &b, int colSmaOffset) {
    const auto &smaPos = reinterpret_cast<const uint16_t &>(b[colSmaOffset]);
    const auto &smaMinPos = reinterpret_cast<const uint16_t &>(b[smaPos]);
    const auto &smaMaxPos = reinterpret_cast<const uint16_t &>(b[smaPos + dataSize]);
    const auto smaMin(reinterpret_cast<const char (&)[]>(b[smaMinPos]));
    const auto smaMax(reinterpret_cast<const char (&)[]>(b[smaMaxPos]));
    return std::pair<std::string, std::string>(smaMin, smaMax);
  }
  static inline const unsigned int freeSpace(const BDCC_Block &b, int dataPos, int cnt, int nCols, int idx) {
    int freeSpaceMiniPage;
    if (cnt != 0) {
      const auto currentOffsetPos = dataPos + cnt * gOffsetSize;
      const auto &currentOffset = reinterpret_cast<const uint16_t &>(b[currentOffsetPos - gOffsetSize]);
      freeSpaceMiniPage = currentOffset - currentOffsetPos;
    } else {
      const auto nextMiniPageStart = (nCols == idx + 1) ? gBlockSize :
                                     reinterpret_cast<const uint16_t &>(b[gSmaOffsetPos + (idx + 1) * gAttrOffsetSize]);
      freeSpaceMiniPage = nextMiniPageStart - dataPos;
    }
    return freeSpaceMiniPage;
  }
  static inline const bool enoughSpace(unsigned int freeSpace, unsigned int *recordOffset, const StreamType &buf) {
    auto iterBegin = buf.cbegin() + *recordOffset;
    const auto value = deserialize<std::string>(iterBegin, buf.end());
    const auto stringSize = value.size() + 1;
    *recordOffset += stringSize - 1 + sizeof(uint64_t);
    if (freeSpace < stringSize + gOffsetSize) return false;
    return true;
  }
};

using Column = std::pair<std::string, ColumnType>;
using ColumnInitList = std::initializer_list<Column>;
using ColumnVector = std::vector<Column>;
using ColumnIterator = ColumnVector::const_iterator;
using StringVector = std::vector<std::string>;
using StringInitList = std::initializer_list<std::string>;

template<class Tuple, std::size_t N>
struct ColumnsConstructor {
  static void apply(ColumnVector &cv, const std::vector<std::string> &sv) {
    ColumnsConstructor<Tuple, N - 1>::apply(cv, sv);
    cv.emplace_back(sv[N - 1], toColumnType<typename std::tuple_element<N - 1, Tuple>::type>());
  }
};

template<class Tuple>
struct ColumnsConstructor<Tuple, 1> {
  static void apply(ColumnVector &cv, const std::vector<std::string> &sv) {
    cv.emplace_back(sv[0], toColumnType<typename std::tuple_element<0, Tuple>::type>());
  }
};

/* VTableInfo ============================================================== */
template<typename KeyType, typename TupleType>
struct VTableInfo {
  VTableInfo() {}

  VTableInfo(const std::string &_name, const StringVector &_columns) : name(_name) {
    if (_columns.size() != std::tuple_size<TupleType>::value) {
      throw PTableException("Number of column names has to match with tuple elements");
    }

    ColumnsConstructor<TupleType, std::tuple_size<TupleType>::value>::apply(columns, _columns);
  }

  VTableInfo(const std::string &_name, StringInitList _columns) :
    VTableInfo(_name, StringVector{_columns}) {}

  static ColumnType typeOfKey() { return toColumnType<KeyType>(); }

  ColumnIterator begin() const { return columns.begin(); }
  ColumnIterator end() const { return columns.end(); }

  std::string name;
  ColumnVector columns;
};

} /* namespace dbis::ptable */

#endif /* VTableInfo_hpp_ */
