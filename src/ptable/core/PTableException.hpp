/*
 * Copyright (C) 2017-2018 DBIS Group - TU Ilmenau, All Rights Reserved.
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

#ifndef PTableException_hpp_
#define PTableException_hpp_

#include <exception>
#include <iostream>

#include "fmt/format.h"

namespace dbis::ptable {

/**
 * @brief An exception for signaling errors in table processing.
 *
 * PTableExecption is an exception class for signaling errors while
 * processing a persistent table.
 */
class PTableException : public std::exception {
  std::string msg;  //< a message string

 public:
  /**
   * Construct a new PTableException instance.
   *
   * @param s the message string
   */
  PTableException(const char* s = "") : msg(s) {}

  /**
   * Returns th message string describing the exception.
   *
   * @return the message string
   */
  virtual const char* what() const throw() {
    return fmt::format("PTableException: {}", msg).c_str();
  }
};

} /* namespace dbis::ptable*/

#endif /* PTableException_hpp_ */

