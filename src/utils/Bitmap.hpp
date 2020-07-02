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

#ifndef DBIS_BITMAP_HPP
#define DBIS_BITMAP_HPP

#include <locale>  ///< ctype

namespace dbis {

#define BITMAP_WORD size_t

static constexpr auto deBruijnSeq = 0x07EDD5E59A4E28C2;
static constexpr uint8_t tab64[64] = {
    63, 0,  58, 1,  59, 47, 53, 2,  60, 39, 48, 27, 54, 33, 42, 3,  61, 51, 37, 40, 49, 18,
    28, 20, 55, 30, 34, 11, 43, 14, 22, 4,  62, 57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19,
    29, 10, 13, 21, 56, 45, 25, 31, 35, 16, 9,  12, 44, 24, 15, 8,  23, 7,  6,  5};

/**
 * Own bitmap implementation to get access to underlying words.
 * (inspired by GCC bitset)
 *
 * @tparam NUM_BITS the number of bits to store.
 */
template <size_t NUM_BITS>
class Bitmap {
  using Word = BITMAP_WORD;
  static constexpr auto BITS_PER_WORD = sizeof(Word) * 8;
  static constexpr auto NUM_WORDS = (NUM_BITS + BITS_PER_WORD - 1) / BITS_PER_WORD;
  using WordArray = Word[NUM_WORDS];
  WordArray words;

  Word& getWord(size_t bitPos) noexcept { return words[bitPos / BITS_PER_WORD]; }

  constexpr Word getWord(size_t bitPos) const noexcept { return words[bitPos / BITS_PER_WORD]; }

  constexpr Word maskBit(size_t bitPos) const noexcept {
    return (static_cast<Word>(1)) << (bitPos % BITS_PER_WORD);
  }

  void check(size_t bitpos) const {
    if (bitpos >= NUM_BITS)
      throw std::out_of_range("pos (which is " + std::to_string(bitpos) +
                              ") >= NUM_BITS (which is " + std::to_string(NUM_BITS) + ')');
  }

  /**
   *  Helper method to zero out the unused high-order bits in the highest word.
   */
  void sanitize() noexcept {
    words[NUM_WORDS - 1] &= ~((~static_cast<BITMAP_WORD>(0)) << (NUM_BITS % BITS_PER_WORD));
  }

 public:
  /**
   *  "This encapsulates the concept of a single bit.  An instance of this class is a proxy for
   * an actual bit."
   */
  class Bitref {
    friend class Bitmap;
    Word* wordPtr;
    size_t bitPos;

    Bitref();

   public:
    Bitref(Bitmap& bm, size_t pos) noexcept {
      wordPtr = &bm.getword(pos);
      bitPos = pos % BITS_PER_WORD;
    }

    ~Bitref() noexcept {}

    Bitref& operator=(bool x) noexcept {
      if (x)
        *wordPtr |= maskBit(bitPos);
      else
        *wordPtr &= ~maskBit(bitPos);
      return *this;
    }

    Bitref& operator=(const Bitref& other) noexcept {
      if ((*(other.wordPtr) & Bitmap::maskbit(other.bitPos)))
        *wordPtr |= Bitmap::maskbit(bitPos);
      else
        *wordPtr &= ~Bitmap::maskbit(bitPos);
      return *this;
    }

    bool operator~() const noexcept { return (*(wordPtr)&Bitmap::maskbit(bitPos)) == 0; }

    operator bool() const noexcept { return (*(wordPtr)&Bitmap::maskbit(bitPos)) != 0; }

    Bitref& flip() noexcept {
      *wordPtr ^= Bitmap::maskbit(bitPos);
      return *this;
    }
  };  /// end class Bitref

  friend class Bitref;

  /**
   * Constructor for creating new array of words.
   */
  constexpr Bitmap() noexcept : words() {}

  /**
   * Test if a bit at the given position is set.
   *
   * @param pos the bit position (index) to test.
   * @return the true if set, false otherwise.
   */
  constexpr bool test(size_t pos) const {
    check(pos);
    return getWord(pos) & maskBit(pos);
  }

  /**
   * Array-indexing support.
   *
   * @param pos the bit position (index) to test.
   * @return A bool for a const Bitmap and an instance of the proxy class Bitref for non-const
   *         Bitmap.
   */
  constexpr bool operator[](size_t pos) const {
    check(pos);
    return getWord(pos) & maskBit(pos);
  }
  Bitref operator[](size_t pos) { return Bitref(*this, pos); }

  template <class CharType, class Traits, class Alloc>
  void copyToString(std::basic_string<CharType, Traits, Alloc>&, CharType, CharType) const;
  template <class CharType, class Traits, class Alloc>
  void copyToString(std::basic_string<CharType, Traits, Alloc>& s) const {
    copyToString(s, CharType('0'), CharType('1'));
  }

  /**
   * Determine the number of set bits.
   *
   * @return the counted true bits.
   */
  size_t count() const noexcept {
    size_t c = 0;
    for (size_t i = 0; i < NUM_WORDS; ++i) {
      /// http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel
      auto w = words[i];
      w = w - ((w >> 1) & (Word) ~(Word)0 / 3);
      w = (w & (Word) ~(Word)0 / 15 * 3) + ((w >> 2) & (Word) ~(Word)0 / 15 * 3);
      w = (w + (w >> 4)) & (Word) ~(Word)0 / 255 * 15;
      c += (Word)(w * ((Word) ~(Word)0 / 255)) >> (sizeof(Word) - 1) * 8;
    }
    return c;
  }

  /**
   * Tests whether all the bits are set.
   *
   * @return true if all are set.
   */
  bool all() const noexcept {
    for (size_t i = 0; i < NUM_WORDS - 1; ++i)
      if (words[i] != ~static_cast<Word>(0)) return false;
    return words[NUM_WORDS - 1] ==
           (~static_cast<Word>(0) >> (NUM_WORDS * BITS_PER_WORD - NUM_BITS));
  }
  /**
   * Set all bits to its opposite value.
   *
   * @return a reference to the updated bitmap.
   */
  Bitmap<NUM_BITS>& flip() noexcept {
    for (size_t i = 0; i < NUM_WORDS; ++i) words[i] = ~words[i];
    sanitize();
    return *this;
  }

  /**
   * Set all bits to true/1.
   *
   * @return a reference to the updated bitmap.
   */
  Bitmap<NUM_BITS>& set() noexcept {
    for (size_t i = 0; i < NUM_WORDS; ++i) words[i] = ~static_cast<Word>(0);
    sanitize();
    return *this;
  }

  /**
   * Set bit at given position to value (default true/1).
   *
   * @param pos the bit position (index) to update.
   * @param value either true or false, defaults to true.
   * @return a reference to the updated bitmap.
   */
  Bitmap<NUM_BITS>& set(size_t pos, bool value = true) {
    check(pos);
    if (value)
      getWord(pos) |= maskBit(pos);
    else
      getWord(pos) &= ~maskBit(pos);
    return *this;
  }

  /**
   * Reset bit at given position.
   *
   * @param pos the bit position (index) to reset.
   * @return a reference to the updated bitmap
   */
  Bitmap<NUM_BITS>& reset(size_t pos) {
    check(pos);
    getWord(pos) &= ~maskBit(pos);
    return *this;
  }

  /**
   * Retrieve the underlying words of this bitmap.
   *
   * @return a const reference to the word array.
   */
  const WordArray& getWords() const noexcept { return words; }

  /**
   * Find a free slot in the bitmap.
   *
   * @return the index of the free bit.
   */
  auto getFreeZero() const noexcept {
    for (size_t i = 0; i < NUM_WORDS; ++i) {
      /// http://graphics.stanford.edu/~seander/bithacks.html#ZerosOnRightMultLookup
      const auto w = ~words[i];  ///< we need the complement here!
      if (w != 0) {
        /// count consecutive one bits using multiply and lookup
        /// Applying deBruijn hash function + lookup
        return 64 * i + tab64[((uint64_t)((w & -w) * deBruijnSeq)) >> 58];
      }
    }
    /// Valid result is between 0 and 63; 64 means no free position
    return 64 * NUM_WORDS;
  }

  /**
   * Find first set position in the bitmap.
   *
   * @return the index of the set bit.
   */
  auto getFirstSet() const noexcept {
    for (size_t i = 0; i < NUM_WORDS; ++i) {
      const auto w = words[i];
      if (w != 0) {
        /// count consecutive zero bits using multiply and lookup
        /// Applying deBruijn hash function + lookup
        return 64 * i + tab64[((uint64_t)((w & -w) * deBruijnSeq)) >> 58];
      }
    }
    /// Valid result is between 0 and 63; 64 means no free position
    return 64 * NUM_WORDS;
  }

};  /// end class Bitmap

template <size_t NB>
template <class CharType, class Traits, class Alloc>
void Bitmap<NB>::copyToString(std::basic_string<CharType, Traits, Alloc>& s, CharType zero,
                              CharType one) const {
  s.assign(NB, zero);
  for (size_t i = NB; i > 0; --i)
    if (getWord(i - 1) & maskBit(i - 1)) Traits::assign(s[NB - i], one);
}

}  // namespace dbis

/**
 * For pretty binary printing.
 */
template <class CharType, class Traits, size_t NB>
std::basic_ostream<CharType, Traits>& operator<<(std::basic_ostream<CharType, Traits>& os,
                                                 const dbis::Bitmap<NB>& bm) {
  std::basic_string<CharType, Traits> tmp;
  const std::ctype<CharType>& ct = std::use_facet<std::ctype<CharType> >(os.getloc());
  bm.copyToString(tmp, ct.widen('0'), ct.widen('1'));
  return os << tmp;
}

#endif  /// DBIS_BITMAP_HPP