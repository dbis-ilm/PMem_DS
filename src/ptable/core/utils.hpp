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

#ifndef PTABLE_UTILS_HPP_
#define PTABLE_UTILS_HPP_

#include "DataNode.hpp"

#include <algorithm>
#include <cstdint>
#include <ostream>

namespace dbis::ptable {

template<typename T>
inline void copyToByteArray(BDCC_Block &b, const T &data, const std::size_t size, const std::size_t targetPos) {
  std::copy(reinterpret_cast<const uint8_t *>(&data),
            reinterpret_cast<const uint8_t *>(&data) + size,
            b.begin() + targetPos);
}

/* ========================================================================= */
/* from https://stackoverflow.com/a/28440573
 * calling a function on a tuple at a specified index */
template<std::size_t I = 0, typename FuncT, typename... Tp>
inline typename std::enable_if<I == sizeof...(Tp), void>::type
callForIndex(int, const std::tuple<Tp...> &, FuncT) {}

template<std::size_t I = 0, typename FuncT, typename... Tp>
inline typename std::enable_if<I < sizeof...(Tp), void>::type
callForIndex(int index, const std::tuple<Tp...> &t, FuncT f) {
  if (index == 0) f(std::get<I>(t));
  callForIndex<I + 1, FuncT, Tp...>(index - 1, t, f);
}

/* ========================================================================= */
/* based on https://stackoverflow.com/a/26902803
 * calling a function on each element of a tuple */
template<typename FuncT, typename... Ts, std::size_t... Is>
void forEachInTuple(const std::tuple<Ts...> &t, FuncT f, std::index_sequence<Is...>){
  using expander = int[];
  (void) expander {0, ((void) f(std::get<Is>(t)), 0)...};
};
template<typename FuncT, typename... Ts>
void forEachInTuple(const std::tuple<Ts...> &t, FuncT f){
  forEachInTuple(t, f, std::make_index_sequence<sizeof...(Ts)>());
};

/* ========================================================================= */
/* calling a function for each tuple element type (no tuple instance) */
template<typename FuncT, typename... Ts, std::size_t... Is>
void forEachTypeInTuple(FuncT f, std::index_sequence<Is...>) {
  using expander = int[];
  using Tuple = std::tuple<Ts...>;
  (void) expander {0, ((void) f(typename std::tuple_element<Is, Tuple>::type(),Is), 0)...};
};

template<typename FuncT, typename... Ts>
void forEachTypeInTuple(FuncT f) {
  forEachTypeInTuple<FuncT, Ts..., decltype(std::make_index_sequence<sizeof...(Ts)>())>
    (f, std::make_index_sequence<sizeof...(Ts)>());
};

/* ========================================================================= */
/* from http://en.cppreference.com/w/cpp/utility/tuple/tuple*/
// helper function to print a tuple of any size
template<class Tuple, std::size_t N>
struct TuplePrinter {
  static void print(std::ostream &os, const Tuple &t) {
    TuplePrinter<Tuple, N - 1>::print(os, t);
    os << ", " << std::get<N - 1>(t);
  }
};

template<class Tuple>
struct TuplePrinter<Tuple, 1> {
  static void print(std::ostream &os, const Tuple &t) {
    os << std::get<0>(t);
  }
};

template<class... Args>
void print(std::ostream &os, const std::tuple<Args...> &t) {
  os << "(";
  TuplePrinter<decltype(t), sizeof...(Args)>::print(os, t);
  os << ")\n";
}

template<typename... Types>
std::ostream &operator<<(std::ostream &os, std::tuple<Types...> tp) {
  ptable::print(os, tp);
  return os;
}

} /* namespace dbis::ptable */

#endif /* PTABLE_UTILS_HPP_ */
