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

#ifndef PTable_hpp_
#define PTable_hpp_

#include <algorithm>
#include <bitset>
#include <variant>

#include "core/BDCCInfo.hpp"
#include "core/PTableException.hpp"
#include "core/PTableInfo.hpp"
#include "core/PTuple.hpp"
#include "core/serialize.hpp"
#include "core/utils.hpp"
#include "core/VTableInfo.hpp"
#include "PBPTree.hpp"

namespace dbis::ptable {

const std::string LAYOUT = "PTable";

auto const TARGET_INDEX_NODE_SIZE =  1 * 1024; // 1KB

/** Abort reason codes: */
const auto NOT_ENOUGH_SPACE = 1;

/** A variant to support multiple types in ColumnRangeMap*/
using IntDoubleString = std::variant<int, double, std::string>;

/** A mapping from Column ID to desired Range; used for range scans */
using ColumnRangeMap = std::unordered_map<uint16_t, std::pair<IntDoubleString, IntDoubleString>>;

/**************************************************************************//**
 * \brief A persistent table used for PMEM technologies or emulations.
 *
 * \author Philipp Goetze <philipp.goetze@tu-ilmenau.de>
 *****************************************************************************/
template<typename KeyType, class TupleType>
class PTable;

template<typename KeyType, class... Types>
class PTable<KeyType, std::tuple<Types...>> {
  using Tuple = std::tuple<Types...>;
  static const auto BRANCHKEYS = ((TARGET_INDEX_NODE_SIZE - 28) / (sizeof(KeyType) + 24)) & ~1;
  static const auto LEAFKEYS = ((TARGET_INDEX_NODE_SIZE - 36) /
    (sizeof(KeyType) + sizeof(PTuple<KeyType, Tuple>))) & ~1;

  using VTableInfoType = VTableInfo<KeyType, Tuple>;
  using PTableInfoType = PTableInfo<KeyType, Tuple>;
  using ColumnIntMap = std::map<uint16_t, uint16_t>;
  using IndexType = pbptree::PBPTree<KeyType, PTuple<KeyType, Tuple>, BRANCHKEYS, LEAFKEYS>;
  using DataNodePtr = persistent_ptr<DataNode<KeyType>>;

  /************************************************************************//**
   * \brief Iterator to iterate over all tuples using the blocks.
   ***************************************************************************/
  class BlockIterator {
    using DataNodeVector = std::vector<persistent_ptr<DataNode<KeyType>>>;
    const PTable<KeyType, Tuple>& parent;
    const ColumnRangeMap predicates;
    const DataNodeVector candidates;
    typename DataNodeVector::const_iterator currentNode;
    uint16_t currentCnt = 0u;
    uint16_t currentPos = 0u;

   public:
    BlockIterator(const PTable<KeyType, Tuple> &_parent) :
      parent(_parent), currentNode(nullptr), currentPos(1) {}

    BlockIterator(const PTable<KeyType, Tuple> &_parent,
                  const ColumnRangeMap &_predicates) :
      BlockIterator(_parent, _predicates, _parent.getCandidateBlocks(_predicates)) {}

    BlockIterator(const PTable<KeyType, Tuple> &_parent,
                  const ColumnRangeMap &_predicates,
                  const DataNodeVector &_candidates) :
      parent(_parent),
      predicates(_predicates),
      candidates(_candidates),
      currentNode(candidates.cbegin()),
      currentCnt(candidates.size() > 0 ?
                 reinterpret_cast<const uint32_t &>((*currentNode)->block.get_ro()[gCountPos]) : 0) {
      if (candidates.size() > 0) ++(*this);
      else currentPos = 1; // --> setting to end()
    }


    BlockIterator &operator++() {
      if (++currentPos > currentCnt) {
        ++currentNode;
        currentPos = 1u;
        if (*currentNode != nullptr) {
          currentCnt = reinterpret_cast<const uint32_t &>((*currentNode)->block.get_ro()[gCountPos]);
        } else {
          currentCnt = 0u;
          return *this;
        }
      }

      // Check Tuple for inRange
      if (parent.isPTupleinRange(**this, predicates)) {
        return *this;
      } else {
        return ++(*this);
      }
    }

    BlockIterator &next() {
      return ++(*this);
    }

    BlockIterator operator++(int) {
      BlockIterator retval = *this;
      ++(*this);
      return retval;
    }

    BlockIterator begin() {
      return BlockIterator(parent, predicates, candidates);
    }

    BlockIterator end() {
      return BlockIterator(parent);
    }

    bool operator==(BlockIterator other) const {
      return (
//        currentBlock == other.currentBlock &&  // This prevents the comparison with end()
        currentPos == other.currentPos && currentCnt == other.currentCnt);
    }
    bool operator!=(BlockIterator other) const { return !(*this == other); }

    PTuple<KeyType, Tuple> operator*() {
      std::array<uint16_t, PTuple<KeyType, Tuple>::NUM_ATTRIBUTES> pTupleOffsets;
      /*
      auto setPTupleOffsets = [&](auto element, std::size_t idx) {
        pTupleOffsets[idx] = ColumnAttributes<toColumnType<decltype(element)>()>::dataOffsetPos(
          currentNode->get()->block.get_ro(),
          gDataOffsetPos + idx * gAttrOffsetSize,
          currentPos
        );
      };
      forEachTypeInTuple<decltype(setPTupleOffsets), Types...>(setPTupleOffsets);

      return PTuple<KeyType, Tuple>(*currentNode, pTupleOffsets);
       */

      auto idx = 0u;
      for (const auto &c : *parent.root->tInfo) {
        const auto
          &dataPos =
          reinterpret_cast<const uint16_t &>((*currentNode)->block.get_ro()[gDataOffsetPos + idx * gAttrOffsetSize]);
        uint16_t dataOffset;
        switch (c.getType()) {
          case ColumnType::IntType: {
            dataOffset = dataPos + (currentPos - 1) * sizeof(int);
          }
            break;
          case ColumnType::DoubleType: {
            dataOffset = dataPos + (currentPos - 1) * sizeof(double);
          }
            break;
          case ColumnType::StringType: {
            dataOffset = reinterpret_cast<const uint16_t &>((*currentNode)->block.get_ro()[dataPos
              + (currentPos - 1) * gOffsetSize]);
          }
            break;
          default: throw PTableException("unsupported column type\n");
        }
        pTupleOffsets[idx] = dataOffset;
        ++idx;
      }
      return std::move(PTuple<KeyType, Tuple>(*currentNode, pTupleOffsets));
    }

    // iterator traits
    using difference_type = long;
    using value_type = PTuple<KeyType, Tuple>;
    using pointer = const PTuple<KeyType, Tuple> *;
    using reference = const PTuple<KeyType, Tuple> &;
    using iterator_category = std::forward_iterator_tag;
  };

 public:
  /************************************************************************//**
   * \brief Public Iterator to iterate over all inserted tuples using the index.
   ***************************************************************************/
  class iterator {
   public:
    using Predicate = std::function<bool(const PTuple<KeyType, Tuple> &)>;
    using TreeIter = typename IndexType::iterator;

    iterator() : treeIter() {}

    iterator(TreeIter _iter, TreeIter _end, Predicate _pred = [](const PTuple<KeyType, Tuple> &) { return true; })
      : treeIter(_iter), end(_end), pred(_pred) {
      while (isValid() && !pred((*treeIter).second))
        treeIter++;
    }

    iterator &operator++() {
      treeIter++;
      while (isValid() && !pred((*treeIter).second))
        treeIter++;
      return *this;
    }

    iterator &next() {
      treeIter++;
      while (isValid() && !pred((*treeIter).second))
        treeIter++;
      return *this;
    }

    iterator operator++(int) {
      iterator retval = *this;
      ++(*this);
      return retval;
    }

    inline const bool isValid() const {
      return treeIter != end;
    }

    bool operator==(iterator other) const { return (treeIter == other.treeIter); }

    bool operator!=(iterator other) const { return !(treeIter == other.treeIter); }

    PTuple<KeyType, Tuple> operator*() {
      return (*treeIter).second;
    }

    // iterator traits
    using difference_type = long;
    using value_type = PTuple<KeyType, Tuple>;
    using pointer = const PTuple<KeyType, Tuple> *;
    using reference = const PTuple<KeyType, Tuple> &;
    using iterator_category = std::forward_iterator_tag;
   protected:
    TreeIter treeIter, end;
    Predicate pred;
  };

  iterator begin() { return iterator(root->index->begin(), root->index->end()); }

  iterator end() { return iterator(); }

  /************************************************************************//**
   * \brief Default Constructor.
   ***************************************************************************/
  PTable() {
    auto pop = pool_by_vptr(this);
    transaction::run(pop, [&] { init("", {}, Dimensions()); });
  }

  /************************************************************************//**
   * \brief Constructor for a given schema and dimension clustering.
   ***************************************************************************/
  PTable(const std::string &tName, ColumnInitList columns,
         const Dimensions &_bdccInfo = Dimensions()) {
    auto pop = pool_by_vptr(this);
    transaction::run(pop, [&] { init(tName, columns, _bdccInfo); });
  }

  /************************************************************************//**
   * \brief Constructor for a given schema (using TableInfo) and dimension clustering.
   ***************************************************************************/
  PTable(const VTableInfoType &tInfo, const Dimensions &_bdccInfo = Dimensions()) {
    auto pop = pool_by_vptr(this);
    transaction::run(pop, [&] { init(tInfo, _bdccInfo); });
  }

  /************************************************************************//**
   * \brief Default Destructor.
   ***************************************************************************/
  ~PTable() {
//    LOG("closing pool");
//    auto pop = pool_by_vptr(this);
//    pop.close();
//    LOG("closed");
  }

  iterator select(typename iterator::Predicate func) {
    return iterator(root->index->begin(), root->index->end(), func);
  }

  /************************************************************************//**
   * \brief Inserts a new record into the persistent table to the fitting
   *        DataNode.
   *
   * \param[in] rec
   *   the new record/tuple to insert.
   * \return
   *   number of inserted tuples
   ***************************************************************************/
  int insert(KeyType key, Tuple rec) {

    /* Block across implementation
     *
     * X Calc bdcc value
     * X Search correct block
     * X Check for enough space
     *   - yes, but not in minipage -> rearrange (average) <-- TODO:
     *   X no -> split block
     * X Insert + adapt SMAs and count
     */

    //TODO: Check if already inserted in Index (Update or Exception?)
    auto targetNode = root->dataNodes;
    StreamType buf;
    serialize(rec, buf);

    /* Calculate BDCC value for input tuple*/
    auto xtr = static_cast<uint32_t>(getBDCCFromTuple(rec).to_ulong());
    /* Search for correct Block */
    do {
      /* Retrieve BDCC Range */
      const auto &bdcc_min = reinterpret_cast<const uint32_t &>(targetNode->block.get_ro()[gBDCCRangePos1]);
      const auto &bdcc_max = reinterpret_cast<const uint32_t &>(targetNode->block.get_ro()[gBDCCRangePos2]);

      if (xtr >= bdcc_min && xtr <= bdcc_max) break; //Found correct block

      targetNode = targetNode->next;
    } while (targetNode != nullptr); /* Should not reach end! */

    auto needsSplit = findInsertNodeOrSplit(targetNode, rec);
    if (needsSplit) {
      LOG("Need to split for tuple: " << rec);
      try {
        auto pop = pool_by_pptr(root);
        std::pair < DataNodePtr, DataNodePtr > newNodes;
        if (pmemobj_tx_stage() == TX_STAGE_NONE)
          transaction::run(pop, [&] { newNodes = splitBlock(targetNode); });
        else
          newNodes = splitBlock(targetNode);
        return insert(key, rec);
      } catch (std::exception &te) {
        std::cerr << te.what() << '\n'
                  << "Splitting table block failed. Tuple not inserted: "
                  << rec << '\n';
      }
    }

    return insertTuple(key, rec, targetNode);
  }

  /************************************************************************//**
   * \brief Update a specific attribute of a tuple specified by the given key.
   *
   * Update the complete tuple in the table associated with the given key. For
   * that the tuple is deleted first and then newly inserted.
   *
   * \param[in] key
   *   the key value
   * \param[in] pos
   *   the index of the tuple's attribute
   * \param[in] rec
   *   the new tuple values for this key
   * \return
   *   the number of modified tuples
   ***************************************************************************/
  int updateAttribute(KeyType key, size_t pos, Tuple rec) {
    //TODO: Implement. But is this really necessary?
    throw PTableException("Updating a single attribute is not implemented, yet.");
  }

  /************************************************************************//**
   * \brief Update the complete tuple specified by the given key.
   *
   * Update the complete tuple in the table associated with the given key. For
   * that the tuple is deleted first and then newly inserted.
   *
   * \param[in] key
   *   the key value
   * \param[in] rec
   *   the new tuple values for this key
   * \return
   *   the number of modified tuples
   ***************************************************************************/
  int updateComplete(KeyType key, Tuple rec) {
    auto nres = deleteByKey(key);
    if (!nres)
      throw PTableException("key not found");
    nres = insert(key, rec);
    return nres;
  }

  /************************************************************************//**
   * \brief Delete the tuple from the table associated with the given key.
   *
   * Delete the tuple from the persistent table and index structure that is
   * associated with the given key.
   *
   * \param[in] key
   *   the key value
   * \return
   *   the number of tuples deleted
   ***************************************************************************/
  int deleteByKey(KeyType key) {
    std::size_t nres;
    auto pop = pool_by_vptr(this);
    transaction::run(pop, [&] {
      auto ptp = getByKey(key);

      // indicate the deleted tuple
      const auto &dataPos = reinterpret_cast<const uint16_t &>(ptp.getNode()->block.get_ro()[gDataOffsetPos]);
      const auto colSize = ColumnAttributes<toColumnType<typename std::tuple_element<0, Tuple>::type>()>::dataSize;
      const auto pos = (ptp.getOffsetAt(0) - dataPos) / colSize;
      ptp.getNode()->deleted.get_rw().emplace_back(pos);

      //TODO: Delete from histogram?
//      const auto &ptp = getByKey(key);
//      const auto xtr = static_cast<uint32_t>(getBDCCFromTuple(*ptp.createTuple()).to_ulong());
//      ptp.getNode()->histogram[xtr]--;

      // delete from index
      nres = static_cast<std::size_t>(root->index->erase(key));

      // free space of PTuple
//      delete_persistent<PTuple<KeyType, Tuple>>(ptp);
    });
    return nres;
  }

  /************************************************************************//**
   * \brief Return the PTuple associated with the given key.
   *
   * Return the tuple from the persistent table that is associated with the
   * given key. If the key doesn't exist, an exception is thrown.
   *
   * \param[in] key
   *   the key value
   * \return
   *   the PTuple associated with the given key
   ***************************************************************************/
  PTuple<KeyType, Tuple> getByKey(KeyType key) const {
    PTuple<KeyType, Tuple> val;
    if (root->index->lookup(key, &val)) {
      return val;
    } else {
      throw PTableException("key not found");
    }
  }

  /************************************************************************//**
   * \brief Return an iterator to navigate over all tuples being valid
   *        regarding the \p rangePredicates.
   *
   *
   * \param[in] rangePredicates
   *   the range predicates used for this scan represented as map using a
   *   column id to a min and max value.
   * \return
   *   a \c BlockIterator for navigating through the valid tuples.
   ***************************************************************************/
  BlockIterator rangeScan(const ColumnRangeMap &rangePredicates) const {
    return BlockIterator(*this, rangePredicates);
  }

  void rangeScan2(const KeyType &min, const KeyType &max, typename IndexType::ScanFunc func) const {
    root->index->scan(min, max, func);
  }

  /************************************************************************//**
   * @brief Return the number of tuples stored in the table.
   *
   * @return the number of tuples
   ***************************************************************************/
  unsigned long count() const {
    auto cnt = 0ul;
    auto targetNode = root->dataNodes;
    do {
      cnt += reinterpret_cast<const uint32_t &>(targetNode->block.get_ro()[gCountPos])
        - targetNode->deleted.get_ro().size();
      targetNode = targetNode->next;
    } while (targetNode != nullptr);
    return cnt;
  }

  /************************************************************************//**
   * \brief Prints the table content column-wise.
   *
   * \param[in] raw
   *   set to true to additionaly print out the complete raw byte arrays.
   ***************************************************************************/
  void print(bool raw = false) {

    auto currentNode = root->dataNodes;
    const auto &tInfo = *root->tInfo;
    const auto colCnt = tInfo.numColumns();

    do {
      auto b = currentNode->block.get_ro();

      const auto &key1 = reinterpret_cast<const uint32_t &>(b[gBDCCRangePos1]);
      const auto &key2 = reinterpret_cast<const uint32_t &>(b[gBDCCRangePos2]);
      const auto &cnt = reinterpret_cast<const uint32_t &>(b[gCountPos]);
      const auto &space = reinterpret_cast<const uint16_t &>(b[gFreeSpacePos]);
      const auto headerSize = gFixedHeaderSize + gAttrOffsetSize * colCnt;
      const auto bodySize = gBlockSize - headerSize;

      /* Plain byte-by-byte output */
      if (raw) {
        size_t i = 0;
        printf("[ ");
        for (auto &byte : b) {
          printf("%02x ", byte);
          if (++i % 32 == 0) {
            printf("]");
            if (i < b.size())
              printf("\n[ ");
          }
        }
      }

      /* Header/General information */
      std::cout << "\nBDCC Range min: " << key1 << '\n'
                << "BDCC Range max: " << key2 << '\n'
                << "Tuple count: " << cnt - currentNode->deleted.get_ro().size() << '\n'
                << "Header size: " << headerSize << " Bytes" << '\n'
                << "Body size: " << bodySize << " Bytes" << '\n'
                << "Free Space: " << space << " Bytes" << std::endl;

      /* Body/Column/Minipage data */
      if (cnt > 0) {
        size_t idx = 0;
        const auto nCols = std::tuple_size<Tuple>::value;
        auto printColumns = [&] (auto element, std::size_t idx) {
          const auto colName = tInfo.columnInfo(idx).getName();
          using cAttributes = ColumnAttributes<toColumnType<decltype(element)>()>;
          std::cout << "Column Info: " << colName << ": " << cAttributes::typeName << std::endl;

          const auto smas = cAttributes::smaPos(b, gSmaOffsetPos + idx * gAttrOffsetSize);

          const auto &dataPos = reinterpret_cast<const uint16_t &>(b[gDataOffsetPos + idx * gAttrOffsetSize]);
          const auto &data = reinterpret_cast<const int (&)[cnt]>(b[dataPos]);

          /* Remaining Space */
          const auto freeSpace = cAttributes::freeSpace(b, dataPos, cnt, nCols, idx);

          std::cout << "Column[" << idx << "]: " << colName
                    << "\n\tSpace left: " << freeSpace << " Bytes"
                    << "\n\tsmaMin: " << smas.first
                    << "\n\tsmaMax: " << smas.second
                    << "\n\tData: {";
//              const char *padding = "";
//              for (auto i = 0u; i < cnt; i++) {
//                std::cout << padding << data[i];
//                padding = ", ";
//              }
          std::cout << "}\n";
        };
        forEachTypeInTuple<decltype(printColumns), Types...>(printColumns);
      } /* if cnt > 0 */
      std::cout << "\n";
      currentNode = currentNode->next;
    } while (currentNode != nullptr);
  }

// Private ////////////////////////////////////////////////////////////////////
 private:

  struct root {
    DataNodePtr dataNodes;
    persistent_ptr<IndexType> index;
    persistent_ptr<PTableInfoType> tInfo;
    persistent_ptr<BDCCInfo> bdccInfo;
  };
  persistent_ptr<struct root> root;

  /************************************************************************//**
   * \brief Helper function to calculate the minipage sizes for a given schema.
   *
   * \param[in] totalSize
   *   total available space/size
   * \param[in] customizations
   *   optional map of weightings for specific columns.
   * \return
   *   a mapping from ColumnInfo to the calculated minipage size
   ***************************************************************************/
  ColumnIntMap calcMinipageSizes(uint16_t totalSize, ColumnIntMap customizations = ColumnIntMap()) {
    auto portions = 0u;
    ColumnIntMap miniPageSizes = ColumnIntMap();

    /* Get the sum of all column portitions */

    /*
      auto countPortitions = [&] (auto element, std::size_t idx) {
      if (customizations.find(idx) == customizations.end()) {
        portions += ColumnAttributes<toColumnType<decltype(element)>()>::miniPagePortion;
      } else {
        portions += customizations[idx];
      }
    };
    forEachTypeInTuple<decltype(countPortitions), Types...>(countPortitions);
    */

    for (auto i = 0u; i < root->tInfo->numColumns(); i++) {
      const auto &c = root->tInfo->columnInfo(i);
      if (customizations.find(i) == customizations.end()) {
        switch (c.getType()) {
          case ColumnType::IntType:portions += 1;
            break;
          case ColumnType::DoubleType:portions += 2;
            break;
          case ColumnType::StringType:portions += 4;
            break;
          default:throw PTableException("unsupported column type\n");
        }
      } else
        portions += customizations[i];
    }


    /* Calculate and save the minipage sizes for all columns */
    /*
    auto calcMiniPageSizes = [&] (auto element, std::size_t idx) {
      if (customizations.find(idx) == customizations.end()) {
        miniPageSizes[idx] = ColumnAttributes<toColumnType<decltype(element)>()>::miniPagePortion * totalSize / portions;
      } else {
        miniPageSizes[idx] = customizations[idx] * totalSize / portions;
      }
    };
    forEachTypeInTuple<decltype(calcMiniPageSizes), Types...>(calcMiniPageSizes);
    */

    for (auto i = 0u; i < root->tInfo->numColumns(); i++) {
      const auto &c = root->tInfo->columnInfo(i);
      if (customizations.find(i) == customizations.end()) {
        switch (c.getType()) {
          case ColumnType::IntType:miniPageSizes[i] = 1 * totalSize / portions;
            break;
          case ColumnType::DoubleType:miniPageSizes[i] = 2 * totalSize / portions;
            break;
          case ColumnType::StringType:miniPageSizes[i] = 4 * totalSize / portions;
            break;
          default:throw PTableException("unsupported column type\n");
        }
      } else
        miniPageSizes[i] = customizations[i] * totalSize / portions;
    }

    return miniPageSizes;
  }

  /************************************************************************//**
   * \brief Initialization function for creating the necessary structures.
   *
   * \param[in] _tName
   *   the name of the table
   * \param[in] _columns
   *   a list of column name and type pairs
   * \param[in] _bdccInfo
   *   a mapping of column ids to number of BDCC bits to use
   ***************************************************************************/
  void init(const std::string &_tName, const StringInitList &_columns, const Dimensions &_bdccInfo) {
    this->root = make_persistent<struct root>();
    this->root->tInfo.get_rw() = make_persistent<PTableInfoType>(_tName, _columns);
    this->root->bdccInfo = make_persistent<BDCCInfo>(_bdccInfo);
    this->root->index = make_persistent<IndexType>();
    this->root->dataNodes = make_persistent<DataNode<KeyType>>();
    this->root->dataNodes->block.get_rw() = initBlock(0, ((1L << this->root->bdccInfo->numBins()) - 1));
  }

  /************************************************************************//**
   * \brief Initialization function for creating the necessary structures.
   *
   * \param[in] _tInfo
   *   the underlying schema to use
   * \param[in] _bdccInfo
   *   a mapping of column ids to number of BDCC bits to use
   ***************************************************************************/
  void init(const VTableInfoType &_tInfo, const Dimensions &_bdccInfo) {
    this->root = make_persistent<struct root>();
    this->root->tInfo = make_persistent<PTableInfoType>(_tInfo);
    this->root->bdccInfo = make_persistent<BDCCInfo>(_bdccInfo);
    this->root->index = make_persistent<IndexType>();
    this->root->dataNodes = make_persistent<struct DataNode<KeyType>>();
    this->root->dataNodes->block.get_rw() = initBlock(0, ((1L << this->root->bdccInfo->numBins()) - 1));
  }

  /************************************************************************//**
   * \brief Initialize a new BDCC_Block.
   *
   * \return a new initialized BDCC_Block.
   ***************************************************************************/
  BDCC_Block initBlock(const uint32_t &bdcc0, const uint32_t &bdcc1) {
    const auto &tInfo = *root->tInfo;
    auto b = BDCC_Block{};

    /* Set BDCC Range */
    copyToByteArray(b, bdcc0, gBDCCValueSize, gBDCCRangePos1);
    copyToByteArray(b, bdcc1, gBDCCValueSize, gBDCCRangePos2);

    const auto colCnt = tInfo.numColumns();
    const auto headerSize = gFixedHeaderSize + colCnt * gAttrOffsetSize;
    const auto bodySize = gBlockSize - headerSize;
    const auto miniPageSizes = calcMinipageSizes(bodySize);

    /* Set Offsets */
    auto smaSize = 0u;
    auto currentOffset = std::move(headerSize);

    /* Set/Save SMA and data offset for each attribute */
    /*
    auto setOffsets = [&] (auto element, size_t idx) {
      const auto dataSize = ColumnAttributes<toColumnType<decltype(element)>()>::dataSize;
      uint16_t smaOffset = currentOffset;
      uint16_t dataOffset = smaOffset + 2 * dataSize;
      smaSize += 2 * dataSize;
      currentOffset += miniPageSizes.at(idx);

      copyToByteArray(b, smaOffset, gOffsetSize, gSmaOffsetPos + idx * gAttrOffsetSize);
      copyToByteArray(b, dataOffset, gOffsetSize, gDataOffsetPos + idx * gAttrOffsetSize);
    };
    forEachTypeInTuple<decltype(setOffsets), Types...>(setOffsets);
    */

    auto idx = 0u;
    for (auto i = 0u; i < colCnt; i++) {
      const auto &c = tInfo.columnInfo(i);
      uint16_t smaOffset = currentOffset;
      uint16_t dataOffset;
      switch (c.getType()) {
        case ColumnType::IntType: {
          dataOffset = smaOffset + 2 * sizeof(int);
          smaSize += 2 * sizeof(int);
        }
          break;
        case ColumnType::DoubleType: {
          dataOffset = smaOffset + 2 * sizeof(double);
          smaSize += 2 * sizeof(double);
        }
          break;
        case ColumnType::StringType: {
          dataOffset = smaOffset + gAttrOffsetSize;
          smaSize += gAttrOffsetSize;
        }
          break;
        default:throw PTableException("unsupported column type\n");
      }
      currentOffset += miniPageSizes.at(i);

      /* Set/Save SMA and data offset for this attribute */
      copyToByteArray(b, smaOffset, gOffsetSize, gSmaOffsetPos + idx * gAttrOffsetSize);
      copyToByteArray(b, dataOffset, gOffsetSize, gDataOffsetPos + idx * gAttrOffsetSize);

      ++idx;
    }


    /* Set Free Space field */
    const uint16_t freeSpace = bodySize - smaSize;
    copyToByteArray(b, freeSpace, gOffsetSize, gFreeSpacePos);

    return b;
  }

  /************************************************************************//**
   * \brief Insert a new Tuple into the given block.
   *
   * \return number of inserted elements.
   ***************************************************************************/
  int insertTuple(KeyType key, Tuple tp, DataNodePtr &targetNode) {
    auto pop = pool_by_vptr(this);
    auto &b = targetNode->block.get_rw();
    const auto &tInfo = *this->root->tInfo;
    auto recordSize = 0u;
    auto recordOffset = 0u;
    auto idx = 0u;
    auto &cnt = reinterpret_cast<uint32_t &>(b[gCountPos]);
    auto &freeSpace = reinterpret_cast<uint16_t &>(b[gFreeSpacePos]);
    std::array<uint16_t, PTuple<KeyType, Tuple>::NUM_ATTRIBUTES> pTupleOffsets;
    StreamType buf;
    serialize(tp, buf);

    try {
      transaction::run(pop, [&] {
        // Each attribute (SMA + Data)
        for (auto &c : tInfo) {
          const auto &smaPos = reinterpret_cast<const uint16_t &>(b[gSmaOffsetPos + idx * gAttrOffsetSize]);
          const auto &dataPos = reinterpret_cast<const uint16_t &>(b[gDataOffsetPos + idx * gAttrOffsetSize]);

          switch (c.getType()) {

            case ColumnType::IntType: {
              /* Get Record Value */
              auto iterBegin = buf.cbegin() + recordOffset;
              auto iterEnd = iterBegin + sizeof(int);
              const auto value = deserialize<int>(iterBegin, iterEnd);

              /* Insert Data */
              const auto dataOffset = dataPos + cnt * sizeof(int);
              copyToByteArray(b, value, sizeof(int), dataOffset);

              /* Update SMA */
              auto &smaMin = reinterpret_cast<int &>(b[smaPos]);
              auto &smaMax = reinterpret_cast<int &>(b[smaPos + sizeof(int)]);
              if (smaMin > value || cnt == 0) smaMin = value;
              if (smaMax < value || cnt == 0) smaMax = value;

              /* Set new positions and sizes */
              recordOffset += sizeof(int);
              recordSize += sizeof(int);
              pTupleOffsets[idx] = dataOffset;

            }
              break;

            case ColumnType::DoubleType: {
              /* Get Record Value */
              auto iterBegin = buf.cbegin() + recordOffset;
              auto iterEnd = iterBegin + sizeof(double);
              const auto value = deserialize<double>(iterBegin, iterEnd);

              /* Insert Data */
              const auto dataOffset = dataPos + cnt * sizeof(double);
              copyToByteArray(b, value, sizeof(double), dataOffset);

              /* Update SMA */
              auto &smaMin = reinterpret_cast<double &>(b[smaPos]);
              auto &smaMax = reinterpret_cast<double &>(b[smaPos + sizeof(double)]);
              if (smaMin > value || cnt == 0) smaMin = value;
              if (smaMax < value || cnt == 0) smaMax = value;

              /* Set new positions and sizes */
              recordOffset += sizeof(double);
              recordSize += sizeof(double);
              pTupleOffsets[idx] = dataOffset;

            }
              break;

            case ColumnType::StringType: {
              /* Get Record Value */
              auto iterBegin = buf.cbegin() + recordOffset;
              const auto value = deserialize<std::string>(iterBegin, buf.end());
              const auto cValue = value.c_str();
              const auto stringSize = value.size() + 1;

              /* Insert Data - Get target position */
              const auto targetOffsetPos = dataPos + cnt * gOffsetSize;
              uint16_t targetDataPos = 0;
              if (cnt == 0) {
                const auto end_minipage =
                  (PTuple<KeyType, Tuple>::NUM_ATTRIBUTES <= idx + 1) ? gBlockSize
                                                                      : reinterpret_cast<const uint16_t &>(b[
                    gSmaOffsetPos
                      + (idx + 1) * gAttrOffsetSize]);
                targetDataPos = end_minipage - stringSize;
              } else /* cnt != 0 */{
                const auto &last_offset = reinterpret_cast<const uint16_t &>(b[targetOffsetPos - gOffsetSize]);
                targetDataPos = last_offset - stringSize;
              }

              /* Insert Data - Set offset and string data */
              copyToByteArray(b, targetDataPos, gOffsetSize, targetOffsetPos);
              copyToByteArray(b, *cValue, stringSize, targetDataPos);

              /* Update SMA */
              auto &smaMinPos = reinterpret_cast<uint16_t &>(b[smaPos]);
              auto &smaMaxPos = reinterpret_cast<uint16_t &>(b[smaPos + gOffsetSize]);
              if (cnt != 0) {
                std::string smaMin(reinterpret_cast<const char (&)[]>(b[smaMinPos]));
                std::string smaMax(reinterpret_cast<const char (&)[]>(b[smaMaxPos]));
                if (smaMin > value) smaMinPos = targetDataPos;
                else if (smaMax < value) smaMaxPos = targetDataPos;
              } else /* cnt == 0 */{
                smaMinPos = targetDataPos;
                smaMaxPos = targetDataPos;
              }

              /* Set new positions and sizes */
              recordOffset += stringSize - 1 + sizeof(uint64_t);
              recordSize += stringSize + gOffsetSize;
              pTupleOffsets[idx] = targetDataPos;

            }
              break;

            default:throw PTableException("unsupported column type\n");
          } /* end of switch */
          ++idx;
        } /* end of for */
        ++cnt;
        freeSpace -= recordSize;
        /* Insert into index structure */
        root->index->insert(key, PTuple<KeyType, Tuple>(targetNode, pTupleOffsets));
        /* Add key to KeyVector */
        targetNode->keys.get_rw()[cnt - 1] = key;
        /* Update histogram */
        const auto xtr = static_cast<uint32_t>(getBDCCFromTuple(tp).to_ulong());
        targetNode->histogram.get_rw()[xtr]++;
      }); /* end of transaction */

    } catch (std::exception &te) {
      std::cerr << te.what() << '\n' << "Inserting Tuple failed: " << tp << '\n';
    }

    return 1;
  }

  std::pair<DataNodePtr, DataNodePtr> splitBlock(DataNodePtr &oldNode) {
    auto pop = pool_by_vptr(this);
    const auto &tInfo = *this->root->tInfo;
    const auto &block0 = oldNode->block.get_ro();

    /* Calculate new ranges from histogram (at half for the beginning) */
    const auto &bdccMin = reinterpret_cast<const uint32_t &>(block0[gBDCCRangePos1]);
    const auto &bdccMax = reinterpret_cast<const uint32_t &>(block0[gBDCCRangePos2]);
    const auto splitValue = oldNode->calcAverageBDCC();
//    const auto splitValue = bdccMin + (bdccMax - bdccMin) / 2;
    LOG("Splitting at: " << splitValue << " (" << bdccMin << ", " << bdccMax << ")");

    DataNodePtr newNode1;
    DataNodePtr newNode2;

    /* Create two new blocks */
    newNode1 = make_persistent<struct DataNode<KeyType>>();
    newNode2 = make_persistent<struct DataNode<KeyType>>();
    newNode1->block.get_rw() = initBlock(bdccMin, splitValue);
    if (splitValue == bdccMax) {
      newNode2->block.get_rw() = initBlock(bdccMax, bdccMax);
    } else {
      newNode2->block.get_rw() = initBlock(splitValue + 1, bdccMax);
    }
    const auto &block1 = newNode1->block.get_ro();
    const auto &block2 = newNode2->block.get_ro();

    /* Get, Calculate BDCC, Insert and delete all current values to corresponding block */
    const auto &cnt = reinterpret_cast<const uint16_t &>(block0[gCountPos]);
    for (auto tuplePos = 0u; tuplePos < cnt; tuplePos++) {
      // Skip if marked as deleted
      auto &deleted = oldNode->deleted.get_ro();
      if (std::find(deleted.begin(), deleted.end(), tuplePos) != deleted.end())
        continue;

      std::array<uint16_t, PTuple<KeyType, Tuple>::NUM_ATTRIBUTES> pTupleOffsets;
      const auto &key = oldNode->keys.get_ro()[tuplePos];

      /*
      auto setPTupleOffsets = [&](auto element, std::size_t idx) {
        pTupleOffsets[idx] = ColumnAttributes<toColumnType<decltype(element)>()>::dataOffsetPos(
          block0,
          gDataOffsetPos + idx * gAttrOffsetSize,
          tuplePos
        );
      };
      forEachTypeInTuple<decltype(setPTupleOffsets), Types...>(setPTupleOffsets);
      */

      auto attributeIdx = 0u;
      for (auto &c : tInfo) {
        const auto &dataPos =
          reinterpret_cast<const uint16_t &>(block0[gDataOffsetPos + attributeIdx * gAttrOffsetSize]);
        switch (c.getType()) {
          case ColumnType::IntType: {
            pTupleOffsets[attributeIdx] = dataPos + tuplePos * sizeof(int);
          }
            break;
          case ColumnType::DoubleType: {
            pTupleOffsets[attributeIdx] = dataPos + tuplePos * sizeof(double);
          }
            break;
          case ColumnType::StringType: {
            pTupleOffsets[attributeIdx] =
              reinterpret_cast<const uint16_t &>(block0[dataPos + tuplePos * gOffsetSize]);
          }
            break;
          default:throw PTableException("unsupported column type\n");
        } /* end of column type switch */
        ++attributeIdx;
      } /* end of attribute loop */

      const auto oldPTuple = PTuple<KeyType, Tuple>(oldNode, pTupleOffsets);

      /* Insert into correct new Block depending on BDCC value */
      root->index->erase(key);
      const auto tp = oldPTuple.createTuple();
      if (splitValue == bdccMax) {
        (tuplePos < cnt / 2) ? insertTuple(key, *tp, newNode1) : insertTuple(key, *tp, newNode2);
      } else {
        const auto xtr = static_cast<uint32_t>(getBDCCFromTuple(*tp).to_ulong());
        (xtr <= splitValue) ? insertTuple(key, *tp, newNode1) : insertTuple(key, *tp, newNode2);
      }
    } /* end of tuple loop */

    // adapt pointers
    if (root->dataNodes == oldNode) {
      root->dataNodes = newNode1;
    } else {
      auto prevBlock = root->dataNodes;
      while (prevBlock->next != oldNode) {
        prevBlock = prevBlock->next;
      }
      prevBlock->next = newNode1;
    }
    newNode1->next = newNode2;
    newNode2->next = oldNode->next;
    delete_persistent<DataNode<KeyType>>(oldNode);

    return std::make_pair(newNode1, newNode2);
  }

  const std::bitset<32> getBDCCFromTuple(const Tuple &tp) const {
    const auto &bdccInfo = *this->root->bdccInfo;
    const auto &tInfo = *this->root->tInfo;
    auto dimCnt = 0u;        //< dimension column counter
    std::bitset<32> xtr = 0; //< extracted bdcc value


    for (const auto &dim: bdccInfo) {
      callForIndex(std::get<0>(dim), tp, [&dim, &xtr](auto &value) {
        const auto &nBits = std::get<1>(dim);
        const auto &mapping = std::get<2>(dim);
        const auto alignment = std::get<3>(dim) - nBits;
        const auto &mask = ((1L << nBits) - 1) << alignment; // based on maximum inserted value

        /* Calculate tuples bin for current dimension and realign bits */
        auto x = applyMask(value, mask) >> alignment;

        /* Map the tuples bin to the dimensions BDCC positions */
        auto j = nBits - 1;
        auto dimXtr = mapping;
        for (auto i = 31; i >= 0; --i) {
          dimXtr[i] = ((x >> j) & 1) & mapping[i];
          j -= mapping[i];
        }
        xtr |= dimXtr;
      });
    }
//    LOG("Tuple:" << tp << ", BDCC Value: " << xtr);
    return xtr;
  }

  const std::vector<persistent_ptr<DataNode<KeyType>>> getCandidateBlocks(const ColumnRangeMap &predicates) const {
    std::vector<persistent_ptr<DataNode<KeyType>>> candidates;
    auto currentNode = root->dataNodes;
    const auto &tInfo = *root->tInfo;
    auto bCnt = 0u;
    while (currentNode != nullptr) {
      ++bCnt;
      const auto &b = currentNode->block;
      auto inRange = true;

      /*
      auto checkPredicates = [&](auto element, std::size_t idx) {
        if(predicates.find(idx) == predicates.end()) return; // Skip not specified attributes
        auto smas = ColumnAttributes<toColumnType<decltype(element)>()>::smaPos(
          b.get_ro(), gSmaOffsetPos + idx * gAttrOffsetSize);
        LOG("predicate range: " << std::get<decltype(element)>(predicates.at(idx).first) << '-'
                                 << std::get<decltype(element)>(predicates.at(idx).second)
                                 << ", block range: " << smas.first << '-' << smas.second)
        if (std::get<decltype(element)>(predicates.at(idx).second) < smas.first ||
            std::get<decltype(element)>(predicates.at(idx).first) > smas.second) {
          inRange = false;
        }
      };
      forEachTypeInTuple<decltype(checkPredicates), Types...>(checkPredicates);
      */

      for (const auto &p: predicates) {
        const auto &smaPos = reinterpret_cast<const uint16_t &>(b.get_ro()[gSmaOffsetPos + p.first * gAttrOffsetSize]);
        switch (tInfo.columnInfo(p.first).getType()) {
          case ColumnType::IntType: {
            const auto &smaMin = reinterpret_cast<const int &>(b.get_ro()[smaPos]);
            const auto &smaMax = reinterpret_cast<const int &>(b.get_ro()[smaPos + sizeof(int)]);
            LOG("predicate range: " << std::get<int>(p.second.first) << '-'
                                     << std::get<int>(p.second.second)
                                     << ", block range: " << smaMin << '-' << smaMax)
            if (std::get<int>(p.second.second) < smaMin || std::get<int>(p.second.first) > smaMax) {
              inRange = false;
              goto marker;
            }
          }
            break;
          case ColumnType::DoubleType: {
            const auto &smaMin = reinterpret_cast<const double &>(b.get_ro()[smaPos]);
            const auto &smaMax = reinterpret_cast<const double &>(b.get_ro()[smaPos + sizeof(double)]);
            LOG("predicate range: " << std::get<double>(p.second.first) << '-'
                                     << std::get<double>(p.second.second)
                                     << ", block range: " << smaMin << '-' << smaMax)
            if (std::get<double>(p.second.second) < smaMin || std::get<double>(p.second.first) > smaMax) {
              inRange = false;
              goto marker;
            }
          }
            break;
          case ColumnType::StringType: {
            const auto &smaMinPos = (uint16_t &) b.get_ro()[smaPos];
            const auto &smaMaxPos = (uint16_t &) b.get_ro()[smaPos + gOffsetSize];
            const auto smaMin(reinterpret_cast<const char (&)[]>(b.get_ro()[smaMinPos]));
            const auto smaMax(reinterpret_cast<const char (&)[]>(b.get_ro()[smaMaxPos]));
            LOG("predicate range: " << std::get<std::string>(p.second.first) << '-'
                                     << std::get<std::string>(p.second.second)
                                     << ", block range: " << smaMin << '-' << smaMax)
            if (std::get<std::string>(p.second.second) < smaMin || std::get<std::string>(p.second.first) > smaMax) {
              inRange = false;
              goto marker;
            }
          }
            break;
          default:throw PTableException("unsupported column type\n");
        }
      }
      marker:;

      if (inRange) candidates.push_back(currentNode);
      currentNode = currentNode->next;
    }
    LOG("global blocks: " << bCnt);
    LOG("candidate blocks: " << candidates.size());
    return candidates;
  }

  const bool isPTupleinRange(const PTuple<KeyType, Tuple> &ptp, const ColumnRangeMap &predicates) const {
    const auto &tInfo = *root->tInfo;
    const auto &b = ptp.getNode()->block.get_ro();
    auto inRange = true;

    /*
    auto isInRange = [&](auto element, std::size_t idx) {
      if (!inRange) return; // previous element already not in range
      if (predicates.find(idx) == predicates.end()) return; // check if column is used in predicates
      using typeInBlock = typename ColumnAttributes<toColumnType<decltype(element)>()>::typeInBlock;
      using type = typename ColumnAttributes<toColumnType<decltype(element)>()>::type;
      const auto &value = reinterpret_cast<typeInBlock>(b[ptp.getOffsetAt(idx)]);
      if (std::get<type>(predicates.at(idx).second) < value ||
          std::get<type>(predicates.at(idx).first) > value) {
        inRange = false;
      }
    };
    forEachTypeInTuple<decltype(isInRange), Types...>(isInRange);
    return inRange;
    */

    for (const auto &p: predicates) {
      const auto offset = ptp.getOffsetAt(p.first);

      switch (tInfo.columnInfo(p.first).getType()) {
        case ColumnType::IntType: {
          const auto &value = reinterpret_cast<const int &>(b[offset]);
          if (std::get<int>(p.second.second) < value || std::get<int>(p.second.first) > value) {
            return false;
          }
        }
          break;
        case ColumnType::DoubleType: {
          const auto &value = reinterpret_cast<const double &>(b[offset]);
          if (std::get<double>(p.second.second) < value || std::get<double>(p.second.first) > value) {
            return false;
          }
        }
          break;
        case ColumnType::StringType: {
          const std::string value(reinterpret_cast<const char (&)[]>(b[offset]));
          if (std::get<std::string>(p.second.second) < value || std::get<std::string>(p.second.first) > value) {
            return false;
          }
        }
          break;
        default:throw PTableException("unsupported column type\n");
      }
    }
    return true;
  }

  const bool findInsertNodeOrSplit(DataNodePtr &node, const Tuple &tp) const {

    auto bdcc_min = reinterpret_cast<const uint32_t &>(node->block.get_ro()[gBDCCRangePos1]);
    auto bdcc_max = reinterpret_cast<const uint32_t &>(node->block.get_ro()[gBDCCRangePos2]);
    auto enoughSpace = hasEnoughSpace(node, tp);
    auto splitNode = node;

    /* Loop for special case where multiple nodes share the same bdcc value */
    while (bdcc_min == bdcc_max && !enoughSpace && splitNode->next != nullptr) {
      splitNode = splitNode->next;
      bdcc_min = reinterpret_cast<const uint32_t &>(splitNode->block.get_ro()[gBDCCRangePos1]);
      bdcc_max = reinterpret_cast<const uint32_t &>(splitNode->block.get_ro()[gBDCCRangePos2]);
      enoughSpace = hasEnoughSpace(splitNode, tp);
    }
    /* reset if all nodes with same bdcc value are full */
    auto xtr = static_cast<uint32_t>(getBDCCFromTuple(tp).to_ulong());
    if (xtr < bdcc_min) {
      splitNode = node;
      enoughSpace = false;
    }

    /* set results */
    node = splitNode;
    return enoughSpace ? false : true; // split or not
  }

  const bool hasEnoughSpace(const DataNodePtr &node, const Tuple &tp) const {
    StreamType buf;
    serialize(tp, buf);

    const auto b = node->block.get_ro();

    /* first check total space of node */
    const auto globalSpace = reinterpret_cast<const uint16_t &>(b[gFreeSpacePos]);
    if (globalSpace < buf.size())
      return false;

    /* then check space of minipages of the node */
    auto recordOffset = 0u;
    auto idx = 0u;
    const auto &cnt = reinterpret_cast<const uint32_t &>(b[gCountPos]);
    /*const auto &nCols = std::tuple_size<Tuple>::value;

    auto enoughSpace = true;
    auto checkColumnSpace = [&](auto element, std::size_t idx) {
      if (!enoughSpace) return; // there was not enough space for a previous column
      const auto &dataPos = reinterpret_cast<const uint16_t &>(b[gDataOffsetPos + idx * gAttrOffsetSize]);
      const auto freeSpace = ColumnAttributes<toColumnType<decltype(element)>()>::freeSpace(b, dataPos, cnt, nCols, idx);
      enoughSpace = ColumnAttributes<toColumnType<decltype(element)>()>::enoughSpace(freeSpace, &recordOffset, buf);
    };
    forEachTypeInTuple<decltype(checkColumnSpace), Types...>(checkColumnSpace);

    return enoughSpace;
     */

    for (auto &c : *root->tInfo) {
      const auto &dataPos = reinterpret_cast<const uint16_t &>(b[gDataOffsetPos + idx * gAttrOffsetSize]);
      switch (c.getType()) {

        case ColumnType::IntType: {
          const auto nextMiniPageStart =
            (std::tuple_size<Tuple>::value == idx + 1) ? gBlockSize : reinterpret_cast<const uint16_t &>(b[gSmaOffsetPos
              + (idx + 1) * gAttrOffsetSize]);
          const auto freeSpaceMiniPage = nextMiniPageStart - dataPos - (cnt * sizeof(int));
          if (freeSpaceMiniPage < sizeof(int)) return false;
          recordOffset += sizeof(int);
        }
          break;

        case ColumnType::DoubleType: {
          const auto nextMiniPageStart =
            (std::tuple_size<Tuple>::value == idx + 1) ? gBlockSize : reinterpret_cast<const uint16_t &>(b[gSmaOffsetPos
              + (idx + 1) * gAttrOffsetSize]);
          const auto freeSpaceMiniPage = nextMiniPageStart - dataPos - (cnt * sizeof(double));
          if (freeSpaceMiniPage < sizeof(double)) return false;
          recordOffset += sizeof(double);
        }
          break;

        case ColumnType::StringType: {
          uint16_t freeSpaceMiniPage;
          auto iterBegin = buf.cbegin() + recordOffset;
          const auto value = deserialize<std::string>(iterBegin, buf.end());
          const auto cValue = value.c_str();
          const auto stringSize = value.size() + 1;

          if (cnt != 0) {
            const auto currentOffsetPos = dataPos + cnt * gOffsetSize;
            const auto &currentOffset = reinterpret_cast<const uint16_t &>(b[currentOffsetPos - gOffsetSize]);
            freeSpaceMiniPage = currentOffset - currentOffsetPos;
          } else {
            const auto nextMiniPageStart =
              (std::tuple_size<Tuple>::value == idx + 1) ? gBlockSize : reinterpret_cast<const uint16_t &>(b[
                gSmaOffsetPos
                  + (idx + 1) * gAttrOffsetSize]);
            freeSpaceMiniPage = nextMiniPageStart - dataPos;
          }
          if (freeSpaceMiniPage < stringSize + gOffsetSize) return false;
          recordOffset += stringSize - 1 + sizeof(uint64_t);
        }
          break;
        default: break;
      }
      ++idx;
    }
    return true;
  }

}; /* class persistent_table */

} /* namespace dbis::ptable */

#endif /* PTable_hpp_ */
