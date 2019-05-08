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

#ifndef PTableInfo_hpp_
#define PTableInfo_hpp_

#include "DataNode.hpp"
#include "PString.hpp"
#include "VTableInfo.hpp"

#include <libpmemobj++/allocator.hpp>
#include <libpmemobj++/detail/persistent_ptr_base.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/make_persistent_array.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::pool_base;
using pmem::obj::pool_by_vptr;
using pmem::obj::transaction;

namespace dbis::ptable {

class PColumnInfo {
  persistent_ptr<char[]> name;
  p<ColumnType> type;

 public:
  PColumnInfo(pool_base pop);
  PColumnInfo(pool_base pop, Column col);
  PColumnInfo(pool_base pop, const std::string &n, ColumnType ct);

  const std::string getName() const { return name.get(); }
  const ColumnType getType() const { return type.get_ro(); }
};

using PColumnVector = std::vector<PColumnInfo, pmem::obj::allocator<PColumnInfo>>;
using PColumnIterator = PColumnVector::const_iterator;

template<typename KeyType, typename TupleType>
class PTableInfo {
  PString name;
  persistent_ptr<PColumnVector> columns;
//  p<ColumnType> keyType;
 public:

  PTableInfo() {}
  PTableInfo(const VTableInfo<KeyType,TupleType> &_tInfo) {
    auto pop = pool_by_vptr(this);
    transaction::run(pop, [&] {
      name.set(const_cast<std::string *>(&_tInfo.name));
      columns = make_persistent<PColumnVector>();
      for (const auto &c : _tInfo)
        columns->push_back(PColumnInfo(pop, c.first, c.second));
    });
  }

  PTableInfo(const std::string &_name, std::initializer_list<std::string> _columns) {
    auto pop = pool_by_vptr(this);
    transaction::run(pop, [&] {
      name.set(const_cast<std::string *>(&_name));
      columns = make_persistent<PColumnVector>();
      ColumnVector cv;
      ColumnsConstructor<TupleType, std::tuple_size<TupleType>::value>::apply(cv, std::vector<std::string>{_columns});
      for (const auto &c : cv){
        columns->push_back(PColumnInfo(pop, c.first, c.second)); // emplace_back?
      }
    });
  }

  std::string tableName() const { return std::string(name.data()); }
  ColumnType typeOfKey() const { return toColumnType<KeyType>(); }
  const PColumnInfo &columnInfo(int pos) const { return columns->at(pos); }
  std::size_t numColumns() const { return columns->size(); }
  PColumnIterator begin() const { return columns->begin(); }
  PColumnIterator end() const { return columns->end(); }

  void setColumns(const ColumnVector vec) {
    auto pop = pool_by_vptr(this);
    transaction::run(pop, [&] {
      delete_persistent<PColumnVector>(columns);
      columns = make_persistent<PColumnVector>();
      for (const auto &c : vec)
        columns->push_back(PColumnInfo(pop, c.first, c.second));
    });
  }

  int findColumnByName(const std::string &colName) const {
    for (std::size_t i = 0; i < (*columns).size(); i++) {
      if (columns->at(i).getName() == colName) return (int) i;
    }
    return -1;
  }

};

} /* namespace dbis::ptable */

#endif /* PTableInfo_hpp_ */
