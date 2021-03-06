/*
 * Copyright (C) 2017-2021 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of our PMem-based Data Structures repository.
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

#include <algorithm>
#include <unistd.h>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "catch.hpp"
#include "config.h"
#define UNIT_TESTS 1
#include "FPTree.hpp"

using namespace dbis::pbptrees;

using pmem::obj::delete_persistent_atomic;
using pmem::obj::pool;

TEST_CASE("Finding the leaf node containing a key", "[FPTree]") {
  using FPTreeType4 = FPTree<int, int, 4, 4>;
  using FPTreeType6 = FPTree<int, int, 6, 6>;
  using FPTreeType10  = FPTree<int, int, 10, 10> ;
  using FPTreeType12 = FPTree<int, int, 12, 12>;
  using FPTreeType20 = FPTree<int, int, 20, 20>;

  struct root {
    pptr<FPTreeType4> btree4;
    pptr<FPTreeType6> btree6;
    pptr<FPTreeType10> btree10;
    pptr<FPTreeType12> btree12;
    pptr<FPTreeType20> btree20;
  };

  pool<root> pop;
  const std::string path = dbis::gPmemPath + "FPTreeTest";

  //std::remove(path.c_str());
  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, "FPTree", ((std::size_t)(1024*1024*16)));
  } else {
    pop = pool<root>::open(path, "FPTree");
  }

  auto q = pop.root();
  auto &rootRef = *q;
  const auto alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.128.desc",
                                                                     FPTreeType4::AllocClass);

  if (!rootRef.btree4)
    transaction::run(pop, [&] { rootRef.btree4 = make_persistent<FPTreeType4>(alloc_class); });

  if (!rootRef.btree6)
    transaction::run(pop, [&] { rootRef.btree6 = make_persistent<FPTreeType6>(alloc_class); });

  if (!rootRef.btree10)
    transaction::run(pop, [&] { rootRef.btree10 = make_persistent<FPTreeType10>(alloc_class); });

  if (!rootRef.btree12)
    transaction::run(pop, [&] { rootRef.btree12 = make_persistent<FPTreeType12>(alloc_class); });

  if (!rootRef.btree20)
    transaction::run(pop, [&] { rootRef.btree20 = make_persistent<FPTreeType20>(alloc_class); });

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Looking up a key in an inner node") {
    auto &btree = *rootRef.btree10;
    auto node = btree.newBranchNode();
    auto &nodeRef = *node;
    for (auto i = 0; i < 10; i++) nodeRef.keys[i] = i + 1;
    nodeRef.numKeys = 10;

    REQUIRE(btree.lookupPositionInBranchNode(node, 0) == 0);
    REQUIRE(btree.lookupPositionInBranchNode(node, 1) == 1);
    REQUIRE(btree.lookupPositionInBranchNode(node, 10) == 10);
    REQUIRE(btree.lookupPositionInBranchNode(node, 5) == 5);
    REQUIRE(btree.lookupPositionInBranchNode(node, 20) == 10);

    btree.deleteBranchNode(node);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Looking up a key in a leaf node") {
    auto &btree = *rootRef.btree10;
    auto node = btree.newLeafNode();
    auto &nodeRef = *node;
    for (auto i = 0; i < 10; i++) {
      nodeRef.keys.get_rw()[i] = i + 1;
      nodeRef.bits.get_rw().set(i);
      nodeRef.fp.get_rw()[i] = btree.fpHash(i + 1);
    }

    REQUIRE(btree.lookupPositionInLeafNode(node, 1) == 0);
    REQUIRE(btree.lookupPositionInLeafNode(node, 10) == 9);
    REQUIRE(btree.lookupPositionInLeafNode(node, 5) == 4);
    REQUIRE(btree.lookupPositionInLeafNode(node, 20) == 10);

    btree.deleteLeafNode(node);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Balancing two inner nodes from left to right") {
    auto &btree = *rootRef.btree4;
    std::array<pptr<FPTreeType4::LeafNode>, 7> leafNodes = {
      { btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(),
        btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode() }
    };

    auto node1 = btree.newBranchNode();
    auto &node1Ref = *node1;
    auto node2 = btree.newBranchNode();
    auto &node2Ref = *node2;
    auto node3 = btree.newBranchNode();
    auto &node3Ref = *node3;

    node1Ref.keys[0] = 8;
    node1Ref.keys[1] = 20;
    node1Ref.children[0] = node2;
    node1Ref.children[1] = node3;
    node1Ref.numKeys = 2;

    node2Ref.keys[0] = 3;
    node2Ref.keys[1] = 4;
    node2Ref.keys[2] = 5;
    node2Ref.keys[3] = 6;
    node2Ref.children[0] = leafNodes[0];
    node2Ref.children[1] = leafNodes[1];
    node2Ref.children[2] = leafNodes[2];
    node2Ref.children[3] = leafNodes[3];
    node2Ref.children[4] = leafNodes[4];
    node2Ref.numKeys = 4;

    node3Ref.keys[0] = 10;
    node3Ref.children[0] = leafNodes[5];
    node3Ref.children[1] = leafNodes[6];
    node3Ref.numKeys = 1;

    btree.rootNode = node1;
    btree.depth = 2;

    btree.balanceBranchNodes(node2, node3, node1, 0);

    REQUIRE(node1->numKeys == 2);
    REQUIRE(node2->numKeys == 2);
    REQUIRE(node3->numKeys == 3);

    std::array<int, 2> expectedKeys1{{5, 20}};
    std::array<int, 2> expectedKeys2{{3, 4}};
    std::array<int, 3> expectedKeys3{{6, 8, 10}};

    std::array<pptr<FPTreeType4::LeafNode>, 3> expectedChildren2 = {
      { leafNodes[0], leafNodes[1], leafNodes[2] }
    };
    std::array<pptr<FPTreeType4::LeafNode>, 4> expectedChildren3 = {
      { leafNodes[3], leafNodes[4], leafNodes[5], leafNodes[6] }
    };

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(node1Ref.keys)));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(node2Ref.keys)));
    REQUIRE(std::equal(std::begin(expectedKeys3), std::end(expectedKeys3),
                       std::begin(node3Ref.keys)));

    std::array<pptr<FPTreeType4::LeafNode>, 3> node2Children;
    std::transform(std::begin(node2Ref.children), std::end(node2Ref.children),
                   std::begin(node2Children), [](FPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren2), std::end(expectedChildren2),
                       std::begin(node2Children)));

    std::array<pptr<FPTreeType4::LeafNode>, 4> node3Children;
    std::transform(std::begin(node3Ref.children), std::end(node3Ref.children),
                   std::begin(node3Children), [](FPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren3), std::end(expectedChildren3),
                       std::begin(node3Children)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Balancing two inner nodes from right to left") {
    auto &btree = *rootRef.btree4;
    std::array<pptr<FPTreeType4::LeafNode>, 7> leafNodes = {
      { btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(),
        btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode() }
    };

    auto node1 = btree.newBranchNode();
    auto &node1Ref = *node1;
    auto node2 = btree.newBranchNode();
    auto &node2Ref = *node2;
    auto node3 = btree.newBranchNode();
    auto &node3Ref = *node3;

    node1Ref.keys[0] = 4;
    node1Ref.keys[1] = 20;
    node1Ref.children[0] = node2;
    node1Ref.children[1] = node3;
    node1Ref.numKeys = 2;

    node2Ref.keys[0] = 3;
    node2Ref.children[0] = leafNodes[0];
    node2Ref.children[1] = leafNodes[1];

    node2Ref.numKeys = 1;

    node3Ref.keys[0] = 5;
    node3Ref.keys[1] = 6;
    node3Ref.keys[2] = 7;
    node3Ref.keys[3] = 8;
    node3Ref.children[0] = leafNodes[2];
    node3Ref.children[1] = leafNodes[3];
    node3Ref.children[2] = leafNodes[4];
    node3Ref.children[3] = leafNodes[5];
    node3Ref.children[4] = leafNodes[6];

    node3Ref.numKeys = 4;

    btree.rootNode = node1;
    btree.depth = 2;

    btree.balanceBranchNodes(node3, node2, node1, 0);

    REQUIRE(node1->numKeys == 2);
    REQUIRE(node2->numKeys == 3);
    REQUIRE(node3->numKeys == 2);

    std::array<int, 2> expectedKeys1{{6, 20}};
    std::array<int, 3> expectedKeys2{{3, 4, 5}};
    std::array<int, 2> expectedKeys3{{7, 8}};

    std::array<pptr<FPTreeType4::LeafNode>, 4> expectedChildren2 = {
      { leafNodes[0], leafNodes[1], leafNodes[2], leafNodes[3] }
    };
    std::array<pptr<FPTreeType4::LeafNode>, 3> expectedChildren3 = {
      { leafNodes[4], leafNodes[5], leafNodes[6] }
    };

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(node1Ref.keys)));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(node2Ref.keys)));
    REQUIRE(std::equal(std::begin(expectedKeys3), std::end(expectedKeys3),
                       std::begin(node3Ref.keys)));
    std::array<pptr<FPTreeType4::LeafNode>, 4> node2Children;
    std::transform(std::begin(node2Ref.children), std::end(node2Ref.children),
                   std::begin(node2Children), [](FPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren2), std::end(expectedChildren2),
                       std::begin(node2Children)));
    std::array<pptr<FPTreeType4::LeafNode>, 3> node3Children;
    std::transform(std::begin(node3Ref.children), std::end(node3Ref.children),
                   std::begin(node3Children), [](FPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren3), std::end(expectedChildren3),
                       std::begin(node3Children)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Handling of underflow at a inner node by rebalance") {
    auto &btree = *rootRef.btree4;
    std::array<pptr<FPTreeType4::LeafNode>, 6> leafNodes = {
      { btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(),
        btree.newLeafNode(), btree.newLeafNode() }
    };

    std::array<FPTreeType4::BranchNode*, 3> innerNodes = {
      { btree.newBranchNode(), btree.newBranchNode(), btree.newBranchNode() }
    };

    FPTreeType4::SplitInfo splitInfo;
    btree.insertInLeafNode(leafNodes[0], 1, 10, &splitInfo);
    btree.insertInLeafNode(leafNodes[0], 2, 20, &splitInfo);
    btree.insertInLeafNode(leafNodes[0], 3, 30, &splitInfo);

    btree.insertInLeafNode(leafNodes[1], 5, 50, &splitInfo);
    btree.insertInLeafNode(leafNodes[1], 6, 60, &splitInfo);

    btree.insertInLeafNode(leafNodes[2], 7, 70, &splitInfo);
    btree.insertInLeafNode(leafNodes[2], 8, 80, &splitInfo);

    btree.insertInLeafNode(leafNodes[3], 9, 90, &splitInfo);
    btree.insertInLeafNode(leafNodes[3], 10, 100, &splitInfo);

    btree.insertInLeafNode(leafNodes[4], 11, 110, &splitInfo);
    btree.insertInLeafNode(leafNodes[4], 12, 120, &splitInfo);

    btree.insertInLeafNode(leafNodes[5], 13, 130, &splitInfo);
    btree.insertInLeafNode(leafNodes[5], 14, 140, &splitInfo);

    btree.rootNode = innerNodes[0];
    btree.depth = 2;

    auto &inner1Ref = *innerNodes[0];
    inner1Ref.keys[0] = 7;
    inner1Ref.children[0] = innerNodes[1];
    inner1Ref.children[1] = innerNodes[2];
    inner1Ref.numKeys = 1;

    auto &inner2Ref = *innerNodes[1];
    inner2Ref.keys[0] = 7;
    inner2Ref.keys[0] = 5;
    inner2Ref.children[0] = leafNodes[0];
    inner2Ref.children[1] = leafNodes[1];
    inner2Ref.numKeys = 1;

    auto &inner3Ref = *innerNodes[2];
    inner3Ref.keys[0] = 9;
    inner3Ref.keys[1] = 11;
    inner3Ref.keys[2] = 13;
    inner3Ref.children[0] = leafNodes[2];
    inner3Ref.children[1] = leafNodes[3];
    inner3Ref.children[2] = leafNodes[4];
    inner3Ref.children[3] = leafNodes[5];
    inner3Ref.numKeys = 3;

    btree.underflowAtBranchLevel(innerNodes[0], 0, innerNodes[1]);

    REQUIRE(innerNodes[0]->numKeys == 1);
    REQUIRE(innerNodes[0]->keys[0] == 9);

    REQUIRE(innerNodes[1]->numKeys == 2);
    REQUIRE(innerNodes[2]->numKeys == 2);

    std::array<int, 2> expectedKeys1{{5, 7}};
    std::array<int, 2> expectedKeys2{{11, 13}};

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(inner2Ref.keys)));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(inner3Ref.keys)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Merging two inner nodes") {
    auto &btree = *rootRef.btree4;
    std::array<pptr<FPTreeType4::LeafNode>, 5> leafNodes = {
      { btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(),
        btree.newLeafNode() }
    };

    auto node1 = btree.newBranchNode();
    auto &node1Ref = *node1;
    auto node2 = btree.newBranchNode();
    auto &node2Ref = *node2;

    node1Ref.keys[0] = 5;
    node1Ref.children[0] = leafNodes[0];
    node1Ref.children[1] = leafNodes[1];
    node1Ref.numKeys = 1;

    node2Ref.keys[0] = 20;
    node2Ref.keys[1] = 30;
    node2Ref.children[0] = leafNodes[2];
    node2Ref.children[1] = leafNodes[3];
    node2Ref.children[2] = leafNodes[4];
    node2Ref.numKeys = 2;

    auto root0 = btree.newBranchNode();
    auto &root0Ref = *root0;
    root0Ref.keys[0] = 15;
    root0Ref.children[0] = node1;
    root0Ref.children[1] = node2;
    root0Ref.numKeys = 1;

    btree.rootNode = root0;
    btree.depth = 2;

    btree.mergeBranchNodes(node1, root0Ref.keys[0], node2);

    REQUIRE(node1->numKeys == 4);

    std::array<int, 4> expectedKeys{{5, 15, 20, 30}};
    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(node1Ref.keys)));
    std::array<pptr<FPTreeType4::LeafNode>, 5> node1Children;
    std::transform(std::begin(node1Ref.children), std::end(node1Ref.children),
                   std::begin(node1Children), [](FPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(leafNodes), std::end(leafNodes), std::begin(node1Children)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Merging two leaf nodes") {
    auto &btree = *rootRef.btree10;

    auto node1 = btree.newLeafNode();
    auto &node1Ref = *node1;
    auto node2 = btree.newLeafNode();
    auto &node2Ref = *node2;

    for (auto i = 0; i < 4; i++) {
      node1Ref.keys.get_rw()[i] = i;
      node1Ref.values.get_rw()[i] = i + 100;
      node1Ref.bits.get_rw().set(i);
      node1Ref.fp.get_rw()[i] = btree.fpHash(i);
    }

    for (auto i = 0; i < 4; i++) {
      node2Ref.keys.get_rw()[i] = i + 10;
      node2Ref.values.get_rw()[i] = i + 200;
      node2Ref.bits.get_rw().set(i);
      node2Ref.fp.get_rw()[i] = btree.fpHash(i + 10);
    }
    node1Ref.nextLeaf = node2;

    btree.mergeLeafNodes(node1, node2);

    REQUIRE(node1Ref.nextLeaf == nullptr);
    REQUIRE(node1Ref.bits.get_ro().count() == 8);

    std::array<int, 8> expectedKeys{{0, 1, 2, 3, 10, 11, 12, 13}};
    std::array<int, 8> expectedValues{{100, 101, 102, 103, 200, 201, 202, 203}};
    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(node1Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(node1Ref.values.get_ro())));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Balancing two leaf nodes") {
    auto &btree = *rootRef.btree10;

    auto node1 = btree.newLeafNode();
    auto &node1Ref = *node1;
    auto node2 = btree.newLeafNode();
    auto &node2Ref = *node2;

    for (auto i = 0; i < 8; i++) {
      node1Ref.keys.get_rw()[i] = i + 1;
      node1Ref.values.get_rw()[i] = i * 100;
      node1Ref.bits.get_rw().set(i);
      node1Ref.fp.get_rw()[i] = btree.fpHash(i+1);
    }

    for (auto i = 0; i < 4; i++) {
      node2Ref.keys.get_rw()[i] = i + 11;
      node2Ref.values.get_rw()[i] = i * 200;
      node2Ref.bits.get_rw().set(i);
      node2Ref.fp.get_rw()[i] = btree.fpHash(i+11);
    }

    btree.balanceLeafNodes(node1, node2);
    REQUIRE(node2Ref.bits.get_ro().count() == 6);
    REQUIRE(node1Ref.bits.get_ro().count() == 6);

    std::array<int, 6> expectedKeys1{{1, 2, 3, 4, 5, 6}};
    std::array<int, 6> expectedValues1{{0, 100, 200, 300, 400, 500}};
    REQUIRE(std::equal(std::begin(expectedKeys1), std::begin(expectedKeys1) + 6,
                       std::begin(node1Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues1), std::end(expectedValues1),
                       std::begin(node1Ref.values.get_ro())));

    std::array<int, 6> expectedKeys2{{7, 8, 11, 12, 13, 14}};
    std::array<int, 6> expectedValues2{{0, 200, 400, 600, 600, 700}};
    std::vector<int> node2vecK {std::begin(node2Ref.keys.get_ro()),
                                std::begin(node2Ref.keys.get_ro()) + 6};
    std::vector<int> node2vecV {std::begin(node2Ref.values.get_ro()),
                                std::begin(node2Ref.values.get_ro()) + 6};
    std::sort(std::begin(node2vecK), std::end(node2vecK));
    std::sort(std::begin(node2vecV), std::end(node2vecV));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::begin(expectedKeys2) + 6,
                       std::begin(node2vecK)));
    REQUIRE(std::equal(std::begin(expectedValues2), std::begin(expectedValues2) + 6,
                       std::begin(node2vecV)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Handling of underflow at a leaf node by merge and replace the root node") {
    auto &btree = *rootRef.btree6;

    auto leaf1 = btree.newLeafNode();
    auto &leaf1Ref = *leaf1;
    auto leaf2 = btree.newLeafNode();
    auto &leaf2Ref = *leaf2;
    leaf1Ref.nextLeaf = leaf2;
    leaf2Ref.prevLeaf = leaf1;
    const auto parent = btree.newBranchNode();
    auto &parentRef = *parent;

    FPTreeType6::SplitInfo splitInfo;
    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);
    btree.insertInLeafNode(leaf2, 4, 40, &splitInfo);
    btree.insertInLeafNode(leaf2, 5, 50, &splitInfo);

    parentRef.keys[0] = 4;
    parentRef.numKeys = 1;
    parentRef.children[0] = leaf1;
    parentRef.children[1] = leaf2;
    btree.rootNode = parent;
    btree.depth = 1;

    btree.underflowAtLeafLevel(parent, 1, leaf2);
    REQUIRE(leaf1Ref.bits.get_ro().count() == 5);
    REQUIRE(btree.rootNode.leaf == leaf1);

    std::array<int, 5> expectedKeys{{1, 2, 3, 4, 5}};
    std::array<int, 5> expectedValues{{10, 20, 30, 40, 50}};

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(leaf1Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(leaf1Ref.values.get_ro())));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Handling of underflow at a leaf node by merge") {
    auto &btree = *rootRef.btree6;

    auto leaf1 = btree.newLeafNode();
    auto &leaf1Ref = *leaf1;
    auto leaf2 = btree.newLeafNode();
    auto &leaf2Ref = *leaf2;
    auto leaf3 = btree.newLeafNode();
    auto &leaf3Ref = *leaf3;
    leaf1Ref.nextLeaf = leaf2;
    leaf2Ref.prevLeaf = leaf1;
    leaf2Ref.nextLeaf = leaf3;
    leaf3Ref.prevLeaf = leaf2;
    const auto parent = btree.newBranchNode();
    auto &parentRef = *parent;

    FPTreeType6::SplitInfo splitInfo;
    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);
    btree.insertInLeafNode(leaf2, 4, 40, &splitInfo);
    btree.insertInLeafNode(leaf2, 5, 50, &splitInfo);
    btree.insertInLeafNode(leaf3, 7, 70, &splitInfo);
    btree.insertInLeafNode(leaf3, 8, 80, &splitInfo);
    btree.insertInLeafNode(leaf3, 9, 90, &splitInfo);

    parentRef.keys[0] = 4;
    parentRef.keys[1] = 7;
    parentRef.numKeys = 2;
    parentRef.children[0] = leaf1;
    parentRef.children[1] = leaf2;
    parentRef.children[1] = leaf3;
    btree.rootNode = parent;

    btree.underflowAtLeafLevel(parent, 1, leaf2);
    REQUIRE(leaf1Ref.bits.get_ro().count() == 5);
    REQUIRE(btree.rootNode.branch == parent);
    REQUIRE(parent->numKeys == 1);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Handling of underflow at a leaf node by rebalance") {
    auto &btree = *rootRef.btree6;

    auto leaf1 = btree.newLeafNode();
    auto &leaf1Ref = *leaf1;
    auto leaf2 = btree.newLeafNode();
    auto &leaf2Ref = *leaf2;
    leaf1Ref.nextLeaf = leaf2;
    leaf2Ref.prevLeaf = leaf1;
    const auto parent = btree.newBranchNode();
    auto &parentRef = *parent;

    FPTreeType6::SplitInfo splitInfo;
    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);
    btree.insertInLeafNode(leaf1, 4, 40, &splitInfo);
    btree.insertInLeafNode(leaf2, 5, 50, &splitInfo);
    btree.insertInLeafNode(leaf2, 6, 60, &splitInfo);

    parentRef.keys[0] = 5;
    parentRef.numKeys = 1;
    parentRef.children[0] = leaf1;
    parentRef.children[1] = leaf2;

    btree.underflowAtLeafLevel(parent, 1, leaf2);
    REQUIRE(leaf1Ref.bits.get_ro().count() == 3);
    REQUIRE(leaf2Ref.bits.get_ro().count() == 3);

    std::array<int, 3> expectedKeys1{{1, 2, 3}};
    std::array<int, 3> expectedValues1{{10, 20, 30}};

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(leaf1Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues1), std::end(expectedValues1),
                       std::begin(leaf1Ref.values.get_ro())));

    std::array<int, 3> expectedKeys2{{4, 5, 6}};
    std::array<int, 3> expectedValues2{{40, 50, 60}};

    std::vector<int> leaf2vecK {std::begin(leaf2Ref.keys.get_ro()),
                                std::begin(leaf2Ref.keys.get_ro()) + 3};
    std::vector<int> leaf2vecV {std::begin(leaf2Ref.values.get_ro()),
                                std::begin(leaf2Ref.values.get_ro()) + 3};
    std::sort(std::begin(leaf2vecK), std::end(leaf2vecK));
    std::sort(std::begin(leaf2vecV), std::end(leaf2vecV));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2), std::begin(leaf2vecK)));
    REQUIRE(std::equal(std::begin(expectedValues2), std::end(expectedValues2),
                       std::begin(leaf2vecV)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Handling of underflow at a inner node") {
    auto &btree = *rootRef.btree4;
    FPTreeType4::SplitInfo splitInfo;

    auto leaf1 = btree.newLeafNode();
    auto &leaf1Ref = *leaf1;
    btree.insertInLeafNode(leaf1, 1, 1, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 2, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 3, &splitInfo);

    auto leaf2 = btree.newLeafNode();
    auto &leaf2Ref = *leaf2;
    btree.insertInLeafNode(leaf2, 5, 5, &splitInfo);
    btree.insertInLeafNode(leaf2, 6, 6, &splitInfo);
    leaf1Ref.nextLeaf = leaf2;
    leaf2Ref.prevLeaf = leaf1;

    auto leaf3 = btree.newLeafNode();
    auto &leaf3Ref = *leaf3;
    btree.insertInLeafNode(leaf3, 10, 10, &splitInfo);
    btree.insertInLeafNode(leaf3, 11, 11, &splitInfo);
    leaf2Ref.nextLeaf = leaf3;
    leaf3Ref.prevLeaf = leaf2;

    auto leaf4 = btree.newLeafNode();
    auto &leaf4Ref = *leaf4;
    btree.insertInLeafNode(leaf4, 15, 15, &splitInfo);
    btree.insertInLeafNode(leaf4, 16, 16, &splitInfo);
    leaf3Ref.nextLeaf = leaf4;
    leaf4Ref.prevLeaf = leaf3;

    auto leaf5 = btree.newLeafNode();
    auto &leaf5Ref = *leaf5;
    btree.insertInLeafNode(leaf5, 20, 20, &splitInfo);
    btree.insertInLeafNode(leaf5, 21, 21, &splitInfo);
    btree.insertInLeafNode(leaf5, 22, 22, &splitInfo);
    leaf4Ref.nextLeaf = leaf5;
    leaf5Ref.prevLeaf = leaf4;

    auto leaf6 = btree.newLeafNode();
    auto &leaf6Ref = *leaf6;
    btree.insertInLeafNode(leaf6, 31, 31, &splitInfo);
    btree.insertInLeafNode(leaf6, 32, 32, &splitInfo);
    btree.insertInLeafNode(leaf6, 33, 33, &splitInfo);
    leaf5Ref.nextLeaf = leaf6;
    leaf6Ref.prevLeaf = leaf5;

    const auto inner1 = btree.newBranchNode();
    auto &inner1Ref = *inner1;
    inner1Ref.keys[0] = 5;
    inner1Ref.keys[1] = 10;
    inner1Ref.children[0] = leaf1;
    inner1Ref.children[1] = leaf2;
    inner1Ref.children[2] = leaf3;
    inner1Ref.numKeys = 2;

    const auto inner2 = btree.newBranchNode();
    auto &inner2Ref = *inner2;
    inner2Ref.keys[0] = 20;
    inner2Ref.keys[1] = 30;
    inner2Ref.children[0] = leaf4;
    inner2Ref.children[1] = leaf5;
    inner2Ref.children[2] = leaf6;
    inner2Ref.numKeys = 2;

    auto root0 = btree.newBranchNode();
    auto &root0Ref = *root0;
    root0Ref.keys[0] = 15;
    root0Ref.children[0] = inner1;
    root0Ref.children[1] = inner2;
    root0Ref.numKeys = 1;

    btree.rootNode = root0;
    btree.depth = 2;
    btree.eraseFromBranchNode(root0, btree.depth, 10);
    REQUIRE(btree.rootNode.branch != root0);
    REQUIRE(btree.depth < 2);

    REQUIRE(inner1->numKeys == 4);
    std::array<int, 4> expectedKeys{{5, 15, 20, 30}};
    std::array<pptr<FPTreeType4::LeafNode>, 5> expectedChildren {
      {leaf1, leaf2, leaf4, leaf5, leaf6}
    };

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(inner1Ref.keys)));
    std::array<pptr<FPTreeType4::LeafNode>, 5> inner1Children;
    std::transform(std::begin(inner1Ref.children), std::end(inner1Ref.children),
                   std::begin(inner1Children), [](FPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren), std::end(expectedChildren),
                       std::begin(inner1Children)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting an entry into a leaf node") {
    auto &btree = *rootRef.btree12;
    FPTreeType12::SplitInfo splitInfo;
    auto res = false;
    auto node = btree.newLeafNode();
    auto &nodeRef = *node;

    for (auto i = 0; i < 9; i++) {
      nodeRef.keys.get_rw()[i] = (i + 1) * 2;
      nodeRef.values.get_rw()[i] = (i * 2) + 100;
      nodeRef.bits.get_rw().set(i);
      nodeRef.fp.get_rw()[i] = btree.fpHash((i + 1) * 2);
    }

    res = btree.insertInLeafNode(node, 5, 5000, &splitInfo);
    REQUIRE(res == false);
    REQUIRE(nodeRef.bits.get_ro().count() == 10);

    res = btree.insertInLeafNode(node, 1, 1, &splitInfo);
    REQUIRE(res == false);
    REQUIRE(nodeRef.bits.get_ro().count() == 11);

    res = btree.insertInLeafNode(node, 2, 1000, &splitInfo);
    REQUIRE(res == false);
    REQUIRE(nodeRef.bits.get_ro().count() == 11);

    std::array<int, 11> expectedKeys{{1, 2, 4, 5, 6, 8, 10, 12, 14, 16, 18}};
    std::array<int, 11> expectedValues{{1, 102, 104, 106, 108, 110, 112, 114, 116, 1000, 5000}};

    std::vector<int> nodevecK {std::begin(nodeRef.keys.get_ro()),
                               std::begin(nodeRef.keys.get_ro()) + 11};
    std::vector<int> nodevecV {std::begin(nodeRef.values.get_ro()),
                               std::begin(nodeRef.values.get_ro()) + 11};
    std::sort(std::begin(nodevecK), std::end(nodevecK));
    std::sort(std::begin(nodevecV), std::end(nodevecV));

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(nodevecK)));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(nodevecV)));

    res = btree.insertInLeafNode(node, 20, 21, &splitInfo);
    REQUIRE(res == false);

    res = btree.insertInLeafNode(node, 25, 25, &splitInfo);
    REQUIRE(res == true);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting an entry into a leaf node at a given position") {
    auto &btree = *rootRef.btree20;
    auto node = btree.newLeafNode();
    auto &nodeRef = *node;

    for (auto i = 0; i < 9; i++) {
      nodeRef.keys.get_rw()[i] = (i + 1) * 2;
      nodeRef.values.get_rw()[i] = (i * 2) + 100;
      nodeRef.bits.get_rw().set(i);
      nodeRef.fp.get_rw()[i] = btree.fpHash((i + 1) * 2);
    }

    btree.insertInLeafNodeAtPosition(node, 9, 5, 5000);
    REQUIRE(nodeRef.bits.get_ro().count() == 10);
    btree.insertInLeafNodeAtPosition(node, 10, 1, 1);
    REQUIRE(nodeRef.bits.get_ro().count() == 11);

    std::array<int, 11> expectedKeys{{1, 2, 4, 5, 6, 8, 10, 12, 14, 16, 18}};
    std::array<int, 11> expectedValues{{1, 100, 102,104, 106, 108, 110, 112, 114, 116, 5000}};

    std::vector<int> nodevecK {std::begin(nodeRef.keys.get_ro()),
                               std::begin(nodeRef.keys.get_ro()) + 11};
    std::vector<int> nodevecV {std::begin(nodeRef.values.get_ro()),
                               std::begin(nodeRef.values.get_ro()) + 11};
    std::sort(std::begin(nodevecK), std::end(nodevecK));
    std::sort(std::begin(nodevecV), std::end(nodevecV));

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys), std::begin(nodevecK)));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(nodevecV)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting an entry into an inner node without a split") {
    auto &btree = *rootRef.btree4;
    FPTreeType4::SplitInfo splitInfo;

    auto leaf1 = btree.newLeafNode();
    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);

    auto leaf2 = btree.newLeafNode();
    btree.insertInLeafNode(leaf2, 10, 100, &splitInfo);
    btree.insertInLeafNode(leaf2, 12, 120, &splitInfo);

    auto node = btree.newBranchNode();
    auto &nodeRef = *node;
    nodeRef.keys[0] = 10;
    nodeRef.children[0] = leaf1;
    nodeRef.children[1] = leaf2;
    nodeRef.numKeys = 1;

    btree.rootNode = node;
    btree.depth = 2;

    btree.insertInBranchNode(node, 1, 11, 112, &splitInfo);
    REQUIRE(leaf2->bits.get_ro().count() == 3);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting an entry into an inner node with split") {
    auto &btree = *rootRef.btree4;
    FPTreeType4::SplitInfo splitInfo;

    auto leaf1 = btree.newLeafNode();
    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);

    auto leaf2 = btree.newLeafNode();
    btree.insertInLeafNode(leaf2, 10, 100, &splitInfo);
    btree.insertInLeafNode(leaf2, 11, 110, &splitInfo);
    btree.insertInLeafNode(leaf2, 13, 130, &splitInfo);
    btree.insertInLeafNode(leaf2, 14, 140, &splitInfo);

    auto node = btree.newBranchNode();
    auto &nodeRef = *node;
    nodeRef.keys[0] = 10;
    nodeRef.children[0] = leaf1;
    nodeRef.children[1] = leaf2;
    nodeRef.numKeys = 1;

    btree.rootNode = node;
    btree.depth = 2;

    btree.insertInBranchNode(node, 1, 12, 112, &splitInfo);
    REQUIRE(leaf2->bits.get_ro().count() == 2);
    REQUIRE(node->numKeys == 2);

    std::array<int, 2> expectedKeys{{10, 12}};
    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys), std::begin(nodeRef.keys)));

    std::array<int, 3> expectedKeys2{{12, 13, 14}};
    auto &leaf3Ref = *nodeRef.children[2].leaf;
    std::vector<int> actualKeys{};
    for(auto i = 0u; i < 4; i++)
      if(leaf3Ref.bits.get_ro().test(i))
        actualKeys.push_back(leaf3Ref.keys.get_ro()[i]);
    std::sort(std::begin(actualKeys), std::end(actualKeys));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2), std::begin(actualKeys)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Deleting an entry from a leaf node") {
    auto &btree = *rootRef.btree20;
    auto node = btree.newLeafNode();
    auto &nodeRef = *node;

    for (auto i = 0; i < 9; i++) {
      nodeRef.keys.get_rw()[i] = i + 1;
      nodeRef.values.get_rw()[i] = i + 100;
      nodeRef.bits.get_rw().set(i);
      nodeRef.fp.get_rw()[i] = btree.fpHash(i + 1);
    }

    REQUIRE(btree.eraseFromLeafNode(node, 5) == true);
    REQUIRE(nodeRef.bits.get_ro().count() == 8);
    REQUIRE(btree.eraseFromLeafNode(node, 15) == false);
    REQUIRE(nodeRef.bits.get_ro().count() == 8);

    std::array<int, 8> expectedKeys{{1, 2, 3, 4, 6, 7, 8, 9 }};
    std::array<int, 8> expectedValues{
        {100, 101, 102, 103, 105, 106, 107, 108 }};

    std::vector<int> nodevecK {std::begin(nodeRef.keys.get_ro()),
                               std::begin(nodeRef.keys.get_ro()) + 9};
    nodevecK.erase(nodevecK.begin() + 4);
    std::vector<int> nodevecV {std::begin(nodeRef.values.get_ro()),
                               std::begin(nodeRef.values.get_ro()) + 9};
    nodevecV.erase(nodevecV.begin() + 4);
    std::sort(std::begin(nodevecK), std::end(nodevecK));
    std::sort(std::begin(nodevecV), std::end(nodevecV));

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys), std::begin(nodevecK)));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(nodevecV)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Testing delete from a leaf node in a B+ tree") {
    auto &btree = *rootRef.btree20;
    for (int i = 0; i < 20; i += 2) btree.insert(i, i);
    REQUIRE (btree.erase(10) == true);
    int res;
    REQUIRE (btree.lookup(10, &res) == false);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Constructing a B+ tree and iterating over it") {
    auto btree = rootRef.btree10;
    transaction::run(pop, [&] {
      delete_persistent<FPTreeType10>(btree);
      btree = make_persistent<FPTreeType10>(alloc_class);
      auto &btreeRef = *btree;
      for (int i = 0; i < 50; ++i)
        btreeRef.insert(i, i * 2);
    });

    int num = 0;
    auto iter = btree->begin();
    auto end = btree->end();
    while (iter != end) {
      REQUIRE((*iter).second == num * 2);
      iter++;
      num++;
    }
    REQUIRE(num == 50);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Constructing a LeafList and recover the branches") {
    auto btree = rootRef.btree4;

    transaction::run(pop, [&] {
      if (btree) delete_persistent<FPTreeType4>(btree);
      btree = make_persistent<FPTreeType4>(alloc_class);

      auto &btreeRef = *btree;

      auto prevLeaf = btreeRef.newLeafNode();
      auto &pLeafRef = *prevLeaf;
      for (auto k = 0u; k < 4; ++k) {
        pLeafRef.keys.get_rw()[k] = k + 1;
        pLeafRef.values.get_rw()[k] = k + 1;
        pLeafRef.fp.get_rw()[k] = btreeRef.fpHash(k+1);
        pLeafRef.bits.get_rw().set(k);
      }
      btreeRef.leafList = prevLeaf;

      for (auto l = 1u; l < 25; ++l) {
        auto newLeaf = btreeRef.newLeafNode();
        prevLeaf->nextLeaf = newLeaf;
        auto &leafRef = *newLeaf;
        for (auto k = 0u; k < 4; ++k) {
          leafRef.keys.get_rw()[k] = k + 1 + l * 4;
          leafRef.values.get_rw()[k] = k + 1 + l * 4;
          leafRef.fp.get_rw()[k] = btreeRef.fpHash(k+1+l*4);
          leafRef.bits.get_rw().set(k);
        }
        prevLeaf = newLeaf;
      }
    });

    auto &btreeRef = *btree;
    btreeRef.recover();
    REQUIRE(btreeRef.depth == 2);
    const auto &rootNodeRef = *btreeRef.rootNode.branch;
    REQUIRE(rootNodeRef.keys == std::array<int, 4>{{21, 41, 61, 81}});
    REQUIRE(rootNodeRef.numKeys == 4);

    for (auto b1 = 0; b1 < 4; ++b1) {
      const auto &branchRef = *rootNodeRef.children[b1].branch;
      std::array<int, 4> expectedKeys {{5, 9, 13, 17}};
      std::transform(expectedKeys.begin(), expectedKeys.end(), expectedKeys.begin(),
          [b1](int k) { return (k + b1 * 20); });
      REQUIRE(branchRef.keys == expectedKeys);
      REQUIRE(branchRef.numKeys == 4);
      for (auto b2 = 0; b2 < 4; ++b2) {
        const auto &leafRef = *branchRef.children[b2].leaf;
        std::array<int, 4> expectedKeys2 {{1, 2, 3, 4}};
        std::transform(expectedKeys2.begin(), expectedKeys2.end(), expectedKeys2.begin(),
            [b1, b2](int k) { return (k + b2 * 4 + b1 * 20); });
        REQUIRE(leafRef.keys.get_ro() == expectedKeys2);
        std::array<uint8_t, 4> expectedHashes {{1, 2, 3, 4}};
        std::transform(expectedHashes.begin(), expectedHashes.end(), expectedHashes.begin(),
            [b1, b2, &btreeRef](int k) { return btreeRef.fpHash(k + b2 * 4 + b1 * 20); });
        REQUIRE(leafRef.fp.get_ro() ==  expectedHashes);
        REQUIRE(leafRef.bits.get_ro().all());
      }
    }
  }

  /* Clean up */
  delete_persistent_atomic<FPTreeType4>(rootRef.btree4);
  delete_persistent_atomic<FPTreeType6>(rootRef.btree6);
  delete_persistent_atomic<FPTreeType10>(rootRef.btree10);
  delete_persistent_atomic<FPTreeType12>(rootRef.btree12);
  delete_persistent_atomic<FPTreeType20>(rootRef.btree20);
  delete_persistent_atomic<root>(q);
  pop.close();
  std::remove(path.c_str());
}
