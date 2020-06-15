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

#include <cstdlib>
#include <ctime>
#include <libpmemobj++/container/array.hpp>
#include <thread>
#include "common.hpp"

constexpr auto ArraySize = 4 * L3 / 256; //TARGET_LEAF_SIZE;

/* Get benchmarks on Tree */
static void BM_TreeGet(benchmark::State &state) {
  std::cout << "BRANCHKEYS: " << BRANCHKEYS << " - " << sizeof(TreeType::BranchNode)
            << "\nLEAFKEYS: " << LEAFKEYS << " - " << sizeof(TreeType::LeafNode) << "\n";

  using LeafArray = pmem::obj::array<pptr<TreeType::LeafNode>, ArraySize>;
  struct root {
    pptr<TreeType> tree;
    pptr<LeafArray> leafArray;
  };

  pool<root> pop;
  pobj_alloc_class_desc alloc_class;

  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class =
        pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.new.desc", TreeType::AllocClass);
    transaction::run(pop, [&] { pop.root()->tree = make_persistent<TreeType>(alloc_class); });
  } else {
    LOG("Warning: " << path << " already exists");
    pmempool_rm(path.c_str(), 0);
    pop = pool<root>::create(path, LAYOUT, POOL_SIZE);
    alloc_class =
        pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.new.desc", TreeType::AllocClass);
    transaction::run(pop, [&] { pop.root()->tree = make_persistent<TreeType>(alloc_class); });
  }

  /// DRAM considerations
  /*
  struct alignas(64) DRAMLeafNode {
    DRAMLeafNode() : numKeys(0), nextLeaf(nullptr), prevLeaf(nullptr) {}
    DRAMLeafNode(const TreeType::LeafNode &other)
        : numKeys(other.numKeys.get_ro()),
          nextLeaf(nullptr),
          prevLeaf(nullptr),
          keys(other.keys.get_ro())
    // values(other.values.get_ro()),
    {}

    size_t numKeys;                        ///< the number of currently stored keys - 0x00
    DRAMLeafNode *nextLeaf;                ///< pointer to the subsequent sibling   - 0x08
    DRAMLeafNode *prevLeaf;                ///< pointer to the preceding sibling    - 0x10
    char padding[40];                      ///< padding to align keys to 64 byte    - 0x18
    std::array<MyKey, LEAFKEYS> keys;      ///< the actual keys                     - 0x40
    std::array<MyTuple, LEAFKEYS> values;  ///< the actual values
  }; */

  /*
  static constexpr auto SlotSize = ((LEAFKEYS+1 + 7) / 8) * 8;
  static constexpr auto HashSize = ((LEAFKEYS + 7) / 8) * 8;
  static constexpr auto BitsetSize = ((LEAFKEYS + 63) / 64) * 8;
  static constexpr auto SearchSize = BitsetSize + HashSize + 16;
  static constexpr auto PaddingSize = (64 - SearchSize % 64) % 64;

  struct alignas(64) DRAMLeafNode {
    DRAMLeafNode() : nextLeaf(nullptr), prevLeaf(nullptr) {
      //slot[0] = 0;
    }
    DRAMLeafNode(const TreeType::LeafNode &other)
      : slot(other.slot.get_ro()),
        bits(other.bits.get_ro()),
        // fp(other.fp.get_ro()),
        nextLeaf(nullptr),
        prevLeaf(nullptr),
        keys(other.keys.get_ro())
        // values(other.values.get_ro()),
    {}

    std::array<uint8_t, LEAFKEYS+1> slot;
    std::bitset<LEAFKEYS>           bits;
    // std::array<uint8_t, LEAFKEYS>     fp;
    DRAMLeafNode               *nextLeaf;
    DRAMLeafNode               *prevLeaf;
    char            padding[PaddingSize];
    std::array<MyKey, LEAFKEYS>     keys;
    std::array<MyTuple, LEAFKEYS> values;
  };
  */

  /// Both PMem and DRAM
  auto &tree = pop.root()->tree;
  insert(tree, 0);
  auto &treeRef = *tree;
  tree.flush(pop);
  auto &leafArray = pop.root()->leafArray;  ///< PMem
  auto &leaf = treeRef.rootNode.leaf;
  leaf.flush(pop);

  /// PMem considerations
  ///*
  transaction::run(pop, [&] {
    leafArray = make_persistent<LeafArray>();
    for (int i = 0; i < ArraySize; ++i) {
      auto &a = *leafArray;
      a[i] = make_persistent<TreeType::LeafNode>(allocation_flag::class_id(alloc_class.class_id),
                                                 *leaf);
    }
  });
  pop.drain();
  //*/

  /// DRAM considerations
  /*
  std::unique_ptr<std::array<DRAMLeafNode *, ArraySize>> leafArray(
      new std::array<DRAMLeafNode *, ArraySize>);
  for (int i = 0; i < ArraySize; ++i) {
    auto &a = *leafArray;
    a[i] =  new DRAMLeafNode(*leaf);
  }
  */

  std::srand(std::time(nullptr));
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(2s);

/* BENCHMARKING */
for (auto _ : state) {
  const auto leafNodePos = std::rand() % ArraySize;
  const auto leafNode = (*leafArray)[leafNodePos];
  const auto key = (long long unsigned int)KEYPOS;

  auto start = std::chrono::high_resolution_clock::now();
  benchmark::DoNotOptimize(*leafNode);
  treeRef.lookupPositionInLeafNode(leafNode, KEYPOS); ///< PMem variant
  /// DRAM considerations
  /*
  // auto pos = 0u;
  const auto &nodeRef = *leafNode;
  const auto &keys = nodeRef.keys;
  // const auto num = nodeRef.numKeys;
  const auto &slots = nodeRef.slot;
  // const auto &bits = nodeRef.bits;
  // const auto hash = (uint8_t)(key & 0xFF);
  // const auto &hashs = nodeRef.fp;
  // for (; pos < LEAFKEYS; ++pos)
    // if (//hashs[pos] == hash &&
    // bits.test(pos) && keys[pos] == key) break;
  // for (; pos < num && keys[pos] != key; ++pos);
  // auto pos = dbis::binarySearch<false>(keys, 0, num-1, key);
  auto pos = dbis::binarySearch<false>(keys, slots, 1, slots[0], key);
  benchmark::DoNotOptimize(pos);
  */
  benchmark::DoNotOptimize(*leafNode);
  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed_seconds =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
  state.SetIterationTime(elapsed_seconds.count());
}

// treeRef.printBranchNode(0, treeRef.rootNode.branch);
std::cout << "Elements:" << ELEMENTS << '\n';

transaction::run(pop, [&] { delete_persistent<TreeType>(tree); });
pop.close();
pmempool_rm(path.c_str(), 0);
}

BENCHMARK(BM_TreeGet)->UseManualTime();
BENCHMARK_MAIN();
