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

#ifndef TRIES_HELPERS_H
#define TRIES_HELPERS_H

#include <assert.h>

#include <chrono>
#include <cstddef>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <string>

/// mirror the received bytes of the input structure
template <typename T>
T reverse(T n, size_t b = std::numeric_limits<T>::digits) {
  assert(b <= std::numeric_limits<T>::digits);

  T rv = 0;

  for (size_t i = 0; i < b; ++i, n >>= 1) {
    rv = (rv << 1) | (n & 0x01);
  }

  return rv;
}

/** @brief
 *
 *
 */
static std::string toBinaryString(void *in, const int &size) {
  auto ptr = reinterpret_cast<uint8_t *>(in);
  std::stringstream ss;
  for (size_t i = 0; i <= size / 8; ++i) {
    for (int j = 0; j < 8; ++j) {
      if (i * 8 + j == size) {
        return ss.str();
      }
      char c = ptr[i] & (1 << 7 - j) ? '1' : '0';
      ss << c;
    }
  }
  return ss.str();
}

template <class T>
static std::string toHexString(const T &in) {
  /*char chars[17] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
  'F'}; uint8_t bitmask[] = {0xF0, 0x0F};

  auto ptr = reinterpret_cast<uint8_t *>(in);
  std::stringstream ss;
  for (size_t i = 0; i < size; ++i)
  {
      ss << chars[(bitmask[0] & ptr[i]) >> 4];
      ss << chars[bitmask[1] & ptr[i]];
  }
  return ss.str();*/
  // TODO
  return "";
}

static std::string toString(void *in, const size_t &size) {
  // uint8_t bitmask[] = {0xF0, 0x0F};

  auto ptr = reinterpret_cast<uint8_t *>(in);
  std::stringstream ss;
  for (size_t i = 0; i < size; ++i) {
    ss << ptr[i];
  }
  return ss.str();
}

class RNGBin {
 public:
  RNGBin(const std::string &seed) : distribution(0x00, 0xFF) {
    std::seed_seq seq(seed.begin(), seed.end());
    engine = std::default_random_engine(seq);
  }

  uint8_t *next(size_t &maxsize) {
    assert(maxsize > 0);

    maxsize = std::rand() % maxsize + 1;

    auto ptr = new uint8_t;
    for (size_t i = 0; i < maxsize; ++i) {
      ptr[i] = distribution(engine);
    }
    return ptr;
  }

 private:
  std::uniform_int_distribution<std::size_t> distribution;
  std::default_random_engine engine;
};

static int KEY_SIZE = 4;

class RNG {
 public:
  enum Type {
    DIGITS,     // 0 - 255
    CHARACTERS  // just lower letter, ASCII 97 - 122 // NOT AVAILABLE
  };

  enum Distribution { INCREMENTAL, UNIFORM };

  RNG(Type t = DIGITS, Distribution d = INCREMENTAL, std::string in_seed = "bench")
      : seed(in_seed), distri_type(d), type(t), inc_string("a") {
    switch (t) {
      case DIGITS:
        distribution = std::uniform_int_distribution<uint8_t>(0);
        inc_val = new uint64_t(0);
        break;
      case CHARACTERS:
        distribution = std::uniform_int_distribution<uint8_t>(97, 122);
        break;
      default:
        break;
    }
    reset();
  }

  void next(uint8_t *key, size_t size = 1) {
    // FIX remove complex switches
    assert(size > 0);

    size = KEY_SIZE;
    switch (distri_type) {
      case INCREMENTAL: {
        if (type == DIGITS) {
          for (size_t i = 0; i < size; ++i) {
            // ptr[i] = reinterpret_cast<uint8_t *>(inc_val)[i];             // big    endian
            key[size - 1 - i] = reinterpret_cast<uint8_t *>(inc_val)[i];  // little endian
          }
          ++(*inc_val);
        } else {
          for (size_t i = 0; i < size; ++i) {
            if (i < inc_string.size()) {
              key[i] = inc_string[i];
            } else {
              key[i] = 0;
            }
          }
          if (inc_string.size() == 1 && inc_string.back() == 'z') {
            inc_string = "ba";
          }
          if (inc_string.back() == 'z') {
            auto index = inc_string.size() - 2;
            while (index >= 0) {
              if (inc_string[index] != 'z') {
                ++inc_string[index];
                ++index;
                break;
              }
              if (index == 0) {
                inc_string = std::string(inc_string.size() + 1, 'a');
                index = inc_string.size();
                break;
              }
              --index;
            }
            while (index < inc_string.size()) {
              inc_string[index] = 'a';
              ++index;
            }
          } else {
            ++inc_string.back();  // returns reference
          }
        }
        break;
      }
      case UNIFORM:
        for (size_t i = 0; i < size; ++i) {
          key[i] = distribution(engine);
        }
        break;
    }
  }

  void reset() {
    std::seed_seq seq(seed.begin(), seed.end());
    engine = std::default_random_engine(seq);
    *inc_val = 0;
  }

 private:
  Distribution distri_type;
  std::uniform_int_distribution<uint8_t> distribution;
  std::default_random_engine engine;
  const std::string seed;

  Type type;
  uint64_t *inc_val;
  std::string inc_string;
};

static std::string fun(std::size_t length) {
  static const std::string alphabet = "abcdefghijklmnopqrstuvwxyz";
  std::seed_seq seed1(alphabet.begin(), alphabet.end());
  static std::default_random_engine rng(seed1);
  static std::uniform_int_distribution<std::size_t> distribution(0, alphabet.size() - 1);

  std::string str;
  while (str.size() < length) {
    str += alphabet[distribution(rng)];
  }
  return str;
};

class Timer {
 public:
  Timer() : mStart(std::chrono::high_resolution_clock::now()), mLastSplit(mStart){};

  // return elapsed time since creation in ms
  int getDuration() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::high_resolution_clock::now() - mStart)
        .count();
  }

  int split() {
    auto now = std::chrono::high_resolution_clock::now();
    auto split_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - mLastSplit).count();
    mLastSplit = now;
    return split_time;
  }

 private:
  std::chrono::high_resolution_clock::time_point mStart;
  std::chrono::high_resolution_clock::time_point mLastSplit;
};

#endif  // TRIES_HELPERS_H