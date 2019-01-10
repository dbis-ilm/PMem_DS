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

#ifndef PTuple_hpp_
#define PTuple_hpp_

#include "DataNode.hpp"

using pmem::obj::persistent_ptr;
using pmem::obj::p;

namespace dbis::ptable {

namespace detail {

/**************************************************************************//**
 * \brief get_helper is a helper function to receive an attribute of a PTuple.
 *
 * \tparam T
 *   the type of the requested attribute
 * \tparam ID
 *   the index of the requested attribute
 *****************************************************************************/
template<typename T, std::size_t ID, typename KeyType>
struct get_helper;

/**************************************************************************//**
 * \brief General overload for any type of attribute.
 *
 * \tparam T
 *   the type of the requested attribute
 * \tparam ID
 *   the index of the requested attribute
 *****************************************************************************/
template<typename T, std::size_t ID, typename KeyType>
struct get_helper {
  static T apply(persistent_ptr<DataNode<KeyType>> node, const uint16_t *offsets) {
    T val;
    uint8_t *ptr = reinterpret_cast<uint8_t *>(&val);
    std::copy(node->block.get_ro().begin() + offsets[ID], node->block.get_ro().begin() + offsets[ID] + sizeof(T), ptr);
    return val;
  }
};

/**************************************************************************//**
 * \brief Specialization for retrieving an attribute of type string.
 *
 * \tparam T
 *   the type of the requested attribute
 * \tparam ID
 *   the index of the requested attribute
 *****************************************************************************/
template<std::size_t ID, typename KeyType>
struct get_helper<std::string, ID, KeyType> {
  static std::string apply(persistent_ptr<DataNode<KeyType>> node, const uint16_t *offsets) {
    return reinterpret_cast<const char (&)[]>(node->block.get_ro()[offsets[ID]]);
  }
};

/**************************************************************************//**
 * \brief Specialization for retrieving an attribute of type 32 byte integer.
 *
 * \tparam T
 *   the type of the requested attribute
 * \tparam ID
 *   the index of the requested attribute
 *****************************************************************************/
template<std::size_t ID, typename KeyType>
struct get_helper<int, ID, KeyType> {
  static int apply(persistent_ptr<DataNode<KeyType>> node, const uint16_t *offsets) {
    return reinterpret_cast<const int &>(node->block.get_ro()[offsets[ID]]);
  }
};

/**************************************************************************//**
 * \brief Specialization for retrieving an attribute of type double.
 *
 * \tparam T
 *   the type of the requested attribute
 * \tparam ID
 *   the index of the requested attribute
 *****************************************************************************/
template<std::size_t ID, typename KeyType>
struct get_helper<double, ID, KeyType> {
  static double apply(persistent_ptr<DataNode<KeyType>> node, const uint16_t *offsets) {
    return reinterpret_cast<const double &>(node->block.get_ro()[offsets[ID]]);
  }
};

/**************************************************************************//**
 * \brief getAll_helper is a helper function to add all attributes of a PTuple
 *        to a passed TuplePtr instance.
 *
 * \tparam Tuple
 *   the underlying and desired Tuple type of the PTuple
 * \tparam CurrentIndex
 *   the index of the attribute to set next
 *****************************************************************************/
template<class Tuple, std::size_t CurrentIndex, typename KeyType>
struct getAll_helper;

/**************************************************************************//**
 * \brief General overload for setting Tuples with more than 1 element to add.
 *
 * This specialization will add the remaining elements first and then set the
 * current attribute value.
 *
 * \tparam Tuple
 *   the underlying and desired Tuple type of the PTuple
 * \tparam CurrentIndex
 *   the index of the attribute to add next
 *****************************************************************************/
template<class Tuple, std::size_t CurrentIndex, typename KeyType>
struct getAll_helper {
  static void apply(std::shared_ptr<Tuple> tptr,
                    persistent_ptr<DataNode<KeyType>> node,
                    const uint16_t *const offsets) {
    getAll_helper<Tuple, CurrentIndex - 1, KeyType>::apply(tptr, node, offsets);
    auto val = get_helper<typename std::tuple_element<CurrentIndex - 1, Tuple>::type, CurrentIndex - 1, KeyType>::apply(
      node,
      offsets);
    std::get<CurrentIndex - 1>(*tptr) = val;
  }
};

/**************************************************************************//**
 * \brief Specialization for setting the first attribute.
 *
 * This specialization will just set the first attribute value.
 *
 * \tparam Tuple
 *    the underlying tuple type having one element
 *****************************************************************************/
template<class Tuple, typename KeyType>
struct getAll_helper<Tuple, 1, KeyType> {
  static void apply(std::shared_ptr<Tuple> tptr,
                    persistent_ptr<DataNode<KeyType>> node,
                    const uint16_t *const offsets) {
    auto val = get_helper<typename std::tuple_element<0, Tuple>::type, 0, KeyType>::apply(node, offsets);
    std::get<0>(*tptr) = val;
  }
};

/**************************************************************************//**
 * \brief PTuplePrinter is a helper function to print a persistent tuple of any
 *        size.
 *
 * PTuplePrinter is a helper function to print a PTuple instance of any size
 * and member types to std::ostream. This template should not be directly used,
 * but only via the Tuple members.
 *
 * \tparam Tuple
 *    the tuple type
 * \tparam CurrentIndex
 *    the index of the attribute value to be printed
 *****************************************************************************/
template<class Tuple, std::size_t CurrentIndex, typename KeyType>
struct PTuplePrinter;

/**************************************************************************//**
 * \brief General overload for printing more than 1 element.
 *
 * This specialization will print the remaining elements first and appends the
 * current one after a comma.
 *
 * \tparam Tuple
 *    the underlying tuple type
 * \tparam CurrentIndex
 *    the index of the attribute value to be printed
 *****************************************************************************/
template<class Tuple, std::size_t CurrentIndex, typename KeyType>
struct PTuplePrinter {
  static void print(std::ostream &os, persistent_ptr<DataNode<KeyType>> node, const uint16_t *offsets) {
    PTuplePrinter<Tuple, CurrentIndex - 1, KeyType>::print(os, node, offsets);
    auto val =
      get_helper<typename std::tuple_element<CurrentIndex - 1, Tuple>::type, CurrentIndex - 1, KeyType>::apply(
        node,
        offsets);
    os << "," << val;
  }
};

/**************************************************************************//**
 * \brief Specialization for printing a persistent tuple with 1 element.
 *
 * This specialization will just print the element.
 *
 * \tparam Tuple
 *    the underlying tuple type having one element
 *****************************************************************************/
template<class Tuple, typename KeyType>
struct PTuplePrinter<Tuple, 1, KeyType> {
  static void print(std::ostream &os, persistent_ptr<DataNode<KeyType>> node, const uint16_t *offsets) {
    os << get_helper<typename std::tuple_element<0, Tuple>::type, 0, KeyType>::apply(node, offsets);
  }
};

/**************************************************************************//**
 * \brief Specialization for printing a persistent tuple with no elements.
 *
 * This specialization will do nothing.
 *
 * \tparam Tuple
 *    the underlying tuple type having no elements
 *****************************************************************************/
template<class Tuple, typename KeyType>
struct PTuplePrinter<Tuple, 0, KeyType> {
  static void print(std::ostream &os, persistent_ptr<DataNode<KeyType>> node, const uint16_t *offsets) {
  }
};

} /* end namespace detail */

/**************************************************************************//**
 * \brief A persistent Tuple used for referencing tuples in a persistent table.
 *
 * A PTuple consist of a persistent pointer to the \c node where the
 * underlying tuple is stored. The \c offsets are used to locate the individual
 * attributes of the tuple within the \c node.
 *
 * \code
 * persistent_ptr<DataNode<KeyType>> node;
 * std::vector<uint16_t> tupleOffsets;
 *
 * // Insert into node and save the offsets ...
 *
 * PTuple<int, std::tuple<int, double, std::string>> ptp(node, tupleOffsets);
 * \endcode
 *
 * Get reference to single attribute:
 *
 * \code
 * auto attr1 = ptp.template get<0>;
 * // or:
 * auto attr1 = get<0>(ptp);
 * \endcode
 *
 * \note string attributes are returned as reference to a char array
 * \author Philipp Goetze <philipp.goetze@tu-ilmenau.de>
 *****************************************************************************/
template<typename KeyType, typename Tuple>
class PTuple {
 public:

  /************************************************************************//**
   * \brief the number of attributes for this tuple type.
   ***************************************************************************/
  static const auto NUM_ATTRIBUTES = std::tuple_size<Tuple>::value;

  /************************************************************************//**
   * \brief Meta function returning the type of a specific tuple attribute.
   *
   * \tparam ID
   *   the index of the requested attribute.
   ***************************************************************************/
  template<std::size_t ID>
  struct getAttributeType {
    using type = typename std::tuple_element<ID, Tuple>::type;
  };

  /************************************************************************//**
   * \brief Constructs a new persistent tuple using a persistent node and
   *        offsets for the tuple elements.
   *
   * \tparam Tuple
   *   the underlying tuple type used as base
   * \param[in] _node
   *   the persistent node containing the tuple data (bytes)
   * \param[in] _offsets
   *   the offsets for each tuple element
   ***************************************************************************/
  PTuple(persistent_ptr<DataNode<KeyType>> _node, std::array<uint16_t, NUM_ATTRIBUTES> _offsets) :
    node(_node), offsets(_offsets) {}

  PTuple() : node(nullptr), offsets(std::array<uint16_t, std::tuple_size<Tuple>::value>()) {}

  /************************************************************************//**
   * \brief Get a specific attribute value from the persistent tuple.
   *
   * \tparam ID
   *   the index of the requested attribute.
   * \return
   *   a reference to the persistent tuple's attribute with the requested \c ID
   ***************************************************************************/
  template<std::size_t ID>
  auto getAttribute() {
    return this->get<ID>();
  }

  /************************************************************************//**
   * \brief Get a specific attribute value from the persistent tuple.
   *
   * \tparam ID
   *   the index of the requested attribute.
   * \return
   *   a reference to the persistent tuple's attribute with the requested \c ID
   ***************************************************************************/
  template<std::size_t ID>
  inline auto get() {
    return detail::get_helper<typename getAttributeType<ID>::type, ID, KeyType>::apply(node, offsets.get_ro().data());
  }

  /************************************************************************//**
   * \brief Get a specific attribute value from the persistent tuple.
   *
   * \tparam ID
   *   the index of the requested attribute.
   * \return
   *   a reference to the persistent tuple's attribute with the requested \c ID
   ***************************************************************************/
  template<std::size_t ID>
  const auto getAttribute() const {
    return this->get<ID>();
  }

  /************************************************************************//**
   * \brief Get a specific attribute value from the persistent tuple.
   *
   * \tparam ID
   *   the index of the requested attribute.
   * \return
   *   a reference to the persistent tuple's attribute with the requested \c ID
   ***************************************************************************/
  template<std::size_t ID>
  inline auto get() const {
    return detail::get_helper<typename getAttributeType<ID>::type, ID, KeyType>::apply(node, offsets.get_ro().data());
  }

  persistent_ptr<DataNode<KeyType>> getNode() const {
    return node;
  }

  uint16_t getOffsetAt(std::size_t pos) const {
    return offsets.get_ro()[pos];
  }

  /************************************************************************//**
   * \brief Print this persistent tuple to an ostream.
   *
   * \param[in] os
   *   the output stream to print the tuple
   ***************************************************************************/
  void print(std::ostream &os) const {
    detail::PTuplePrinter<Tuple, NUM_ATTRIBUTES, KeyType>::print(os, node, offsets.get_ro().data());
  }

  /************************************************************************//**
   * \brief Create a new Tuple from this PTuple and return a pointer to it.
   *
   * \return
   *   a smart pointer to the newly created Tuple
   ***************************************************************************/
  std::shared_ptr<Tuple> createTuple() const {
    //typename Tuple::Base tp{};
    Tuple tp{};
    std::shared_ptr<Tuple> tptr(new Tuple(tp));
    detail::getAll_helper<Tuple, NUM_ATTRIBUTES, KeyType>::apply(tptr, node, offsets.get_ro().data());
    return tptr;
  }

 private:

  persistent_ptr<DataNode<KeyType>> node;
  p<std::array<uint16_t, NUM_ATTRIBUTES>> offsets;

}; /* class PTuple */

/**************************************************************************//**
 * \brief Get a specific attribute reference from the PTuple.
 *
 * A global accessor function to reduce boilerplate code that needs to be
 * written to access a specific attribute of a PTuple.

 * \tparam ID
 *   the index of the requested attribute.
 * \tparam Tuple
 *   the underlying tuple type used as base
 * \param[in] ptp
 *   the persistent tuple (PTuple) instance
 * \return
 *   a reference to the persistent tuple's attribute with the requested \c ID
 *****************************************************************************/
template<std::size_t ID, typename KeyType, typename Tuple>
auto get(const PTuple<KeyType, Tuple> &ptp) -> decltype((ptp.template get<ID>())) {
  return ptp.template get<ID>();
}

/**************************************************************************//**
   * \brief Print a persistent tuple to an ostream.
   *
   * \param[in] os
   *   the output stream to print the tuple
 *****************************************************************************/
template<typename KeyType, typename Tuple>
void print(std::ostream &os, const PTuple<KeyType, Tuple> &ptp) {
  ptp.print(os);
}

} /* end namespace dbis::ptable */

/**************************************************************************//**
 * \brief Helper template for printing persistent tuples to an ostream
 *
 * \tparam Tuple
 *   the underlying Tuple of the PTuple
 * \param[in] os
 *   the output stream to print the tuple
 * \param[in] ptp
 *   PTuple instance to print
 * \return
 *   the output stream
 *****************************************************************************/
template<typename KeyType, typename Tuple>
std::ostream &operator<<(std::ostream &os, const dbis::ptable::PTuple<KeyType, Tuple> &ptp) {
  ptp.print(os);
  return os;
}

#endif /* PTuple_hpp_ */
