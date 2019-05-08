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

#include <algorithm>
#include <unistd.h>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include "catch.hpp"
#include "config.h"
#define UNIT_TESTS 1
#include "PBPTree.hpp"


using namespace dbis::pbptree;

using pmem::obj::delete_persistent;
using pmem::obj::delete_persistent_atomic;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;

TEST_CASE("Finding the leaf node containing a key", "[PBPTree]") {
  using PBPTreeType4 = PBPTree<int, int, 4, 4>;
  using PBPTreeType6 = PBPTree<int, int, 6, 6>;
  using PBPTreeType10  = PBPTree<int, int, 10, 10> ;
  using PBPTreeType12 = PBPTree<int, int, 12, 12>;
  using PBPTreeType20 = PBPTree<int, int, 20, 20>;

  struct root {
    pptr<PBPTreeType4> btree4;
    pptr<PBPTreeType6> btree6;
    pptr<PBPTreeType10> btree10;
    pptr<PBPTreeType12> btree12;
    pptr<PBPTreeType20> btree20;
  };

  pool<root> pop;
  const std::string path = dbis::gPmemPath + "PBPTreeTest";

  //std::remove(path.c_str());
  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, "PBPTree", ((std::size_t)(1024*1024*16)));
  } else {
    pop = pool<root>::open(path, "PBPTree");
  }

  auto q = pop.root();
  auto &rootRef = *q;

  if (!rootRef.btree4)
    transaction::run(pop, [&] { rootRef.btree4 = make_persistent<PBPTreeType4>(); });

  if (!rootRef.btree6)
    transaction::run(pop, [&] { rootRef.btree6 = make_persistent<PBPTreeType6>(); });

  if (!rootRef.btree10)
    transaction::run(pop, [&] { rootRef.btree10 = make_persistent<PBPTreeType10>(); });

  if (!rootRef.btree12)
    transaction::run(pop, [&] { rootRef.btree12 = make_persistent<PBPTreeType12>(); });

  if (!rootRef.btree20)
    transaction::run(pop, [&] { rootRef.btree20 = make_persistent<PBPTreeType20>(); });

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Looking up a key in an inner node") {
    auto &btree = *rootRef.btree10;
    auto node = btree.newBranchNode();
    auto &nodeRef = *node;
    for (auto i = 0; i < 10; i++) nodeRef.keys.get_rw()[i] = i + 1;
    nodeRef.numKeys.get_rw() = 10;

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
    for (auto i = 0; i < 10; i++) nodeRef.keys.get_rw()[i] = i + 1;
    nodeRef.numKeys.get_rw() = 10;

    REQUIRE(btree.lookupPositionInLeafNode(node, 1) == 0);
    REQUIRE(btree.lookupPositionInLeafNode(node, 10) == 9);
    REQUIRE(btree.lookupPositionInLeafNode(node, 5) == 4);
    REQUIRE(btree.lookupPositionInLeafNode(node, 20) == 10);

    btree.deleteLeafNode(node);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Balancing two inner nodes from left to right") {
    auto &btree = *rootRef.btree4;
    std::array<pptr<PBPTreeType4::LeafNode>, 7> leafNodes = {
      { btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(),
        btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode()}
    };

    const auto node1 = btree.newBranchNode();
    const auto node2 = btree.newBranchNode();
    const auto node3 = btree.newBranchNode();
    auto &node1Ref = *node1;
    auto &node2Ref = *node2;
    auto &node3Ref = *node3;

    node1Ref.keys.get_rw()[0] = 8;
    node1Ref.keys.get_rw()[1] = 20;
    node1Ref.children.get_rw()[0] = node2;
    node1Ref.children.get_rw()[1] = node3;
    node1Ref.numKeys.get_rw() = 2;

    node2Ref.keys.get_rw()[0] = 3;
    node2Ref.keys.get_rw()[1] = 4;
    node2Ref.keys.get_rw()[2] = 5;
    node2Ref.keys.get_rw()[3] = 6;
    node2Ref.children.get_rw()[0] = leafNodes[0];
    node2Ref.children.get_rw()[1] = leafNodes[1];
    node2Ref.children.get_rw()[2] = leafNodes[2];
    node2Ref.children.get_rw()[3] = leafNodes[3];
    node2Ref.children.get_rw()[4] = leafNodes[4];
    node2Ref.numKeys.get_rw() = 4;

    node3Ref.keys.get_rw()[0] = 10;
    node3Ref.children.get_rw()[0] = leafNodes[5];
    node3Ref.children.get_rw()[1] = leafNodes[6];
    node3Ref.numKeys.get_rw() = 1;

    btree.rootNode = node1;
    btree.depth = 2;

    btree.balanceBranchNodes(node2, node3, node1, 0);

    REQUIRE(node1Ref.numKeys == 2);
    REQUIRE(node2Ref.numKeys == 2);
    REQUIRE(node3Ref.numKeys == 3);

    std::array<int, 2> expectedKeys1{{5, 20}};
    std::array<int, 2> expectedKeys2{{3, 4}};
    std::array<int, 3> expectedKeys3{{6, 8, 10}};

    std::array<pptr<PBPTreeType4::LeafNode>, 3> expectedChildren2 = {
      { leafNodes[0], leafNodes[1], leafNodes[2] }
    };
    std::array<pptr<PBPTreeType4::LeafNode>, 4> expectedChildren3 = {
      { leafNodes[3], leafNodes[4], leafNodes[5], leafNodes[6] }
    };

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(node1Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(node2Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedKeys3), std::end(expectedKeys3),
                       std::begin(node3Ref.keys.get_ro())));

    std::array<pptr<PBPTreeType4::LeafNode>, 3> node2Children;
    std::transform(std::begin(node2Ref.children.get_rw()), std::end(node2Ref.children.get_rw()),
                   std::begin(node2Children), [](PBPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren2), std::end(expectedChildren2),
                       std::begin(node2Children)));

    std::array<pptr<PBPTreeType4::LeafNode>, 4> node3Children;
    std::transform(std::begin(node3Ref.children.get_rw()), std::end(node3Ref.children.get_rw()),
                   std::begin(node3Children), [](PBPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren3), std::end(expectedChildren3),
                       std::begin(node3Children)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Balancing two inner nodes from right to left") {
    auto &btree = *rootRef.btree4;
    std::array<pptr<PBPTreeType4::LeafNode>, 7> leafNodes = {
      { btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(),
        btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode()}
    }; 

    const auto node1 = btree.newBranchNode();
    const auto node2 = btree.newBranchNode();
    const auto node3 = btree.newBranchNode();
    auto &node1Ref = *node1;
    auto &node2Ref = *node2;
    auto &node3Ref = *node3;

    node1Ref.keys.get_rw()[0] = 4;
    node1Ref.keys.get_rw()[1] = 20;
    node1Ref.children.get_rw()[0] = node2;
    node1Ref.children.get_rw()[1] = node3;
    node1Ref.numKeys.get_rw() = 2;

    node2Ref.keys.get_rw()[0] = 3;
    node2Ref.children.get_rw()[0] = leafNodes[0];
    node2Ref.children.get_rw()[1] = leafNodes[1];

    node2Ref.numKeys.get_rw() = 1;

    node3Ref.keys.get_rw()[0] = 5;
    node3Ref.keys.get_rw()[1] = 6;
    node3Ref.keys.get_rw()[2] = 7;
    node3Ref.keys.get_rw()[3] = 8;
    node3Ref.children.get_rw()[0] = leafNodes[2];
    node3Ref.children.get_rw()[1] = leafNodes[3];
    node3Ref.children.get_rw()[2] = leafNodes[4];
    node3Ref.children.get_rw()[3] = leafNodes[5];
    node3Ref.children.get_rw()[4] = leafNodes[6];

    node3Ref.numKeys.get_rw() = 4;

    btree.rootNode = node1;
    btree.depth = 2;

    btree.balanceBranchNodes(node3, node2, node1, 0);

    REQUIRE(node1Ref.numKeys == 2);
    REQUIRE(node2Ref.numKeys == 3);
    REQUIRE(node3Ref.numKeys == 2);

    std::array<int, 2> expectedKeys1{{6, 20}};
    std::array<int, 3> expectedKeys2{{3, 4, 5}};
    std::array<int, 2> expectedKeys3{{7, 8}};

    std::array<pptr<PBPTreeType4::LeafNode>, 4> expectedChildren2 = {
      { leafNodes[0], leafNodes[1], leafNodes[2], leafNodes[3] }
    };
    std::array<pptr<PBPTreeType4::LeafNode>, 3> expectedChildren3 = {
      { leafNodes[4], leafNodes[5], leafNodes[6] }
    };

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(node1Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(node2Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedKeys3), std::end(expectedKeys3),
                       std::begin(node3Ref.keys.get_ro())));
    std::array<pptr<PBPTreeType4::LeafNode>, 4> node2Children;
    std::transform(std::begin(node2Ref.children.get_rw()), std::end(node2Ref.children.get_rw()),
                   std::begin(node2Children), [](PBPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren2), std::end(expectedChildren2),
                       std::begin(node2Children)));
    std::array<pptr<PBPTreeType4::LeafNode>, 3> node3Children;
    std::transform(std::begin(node3Ref.children.get_rw()), std::end(node3Ref.children.get_rw()),
                   std::begin(node3Children), [](PBPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren3), std::end(expectedChildren3),
                       std::begin(node3Children)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Handling of underflow at a inner node by rebalance") {
    auto &btree = *rootRef.btree4;
    std::array<pptr<PBPTreeType4::LeafNode>, 6> leafNodes = {
      { btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(),
        btree.newLeafNode(), btree.newLeafNode()}
    };

    std::array<pptr<PBPTreeType4::BranchNode>, 3> innerNodes = {
      { btree.newBranchNode(), btree.newBranchNode(), btree.newBranchNode()}
    };

    PBPTreeType4::SplitInfo splitInfo;
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

    {
      auto &n = *innerNodes[0];
      n.keys.get_rw()[0] = 7;
      n.children.get_rw()[0] = innerNodes[1];
      n.children.get_rw()[1] = innerNodes[2];
      n.numKeys.get_rw() = 1;
    }
    {
      auto &n = *innerNodes[1];
      n.keys.get_rw()[0] = 5;
      n.children.get_rw()[0] = leafNodes[0];
      n.children.get_rw()[1] = leafNodes[1];
      n.numKeys.get_rw() = 1;
    }
    {
      auto &n = *innerNodes[2];
      n.keys.get_rw()[0] = 9;
      n.keys.get_rw()[1] = 11;
      n.keys.get_rw()[2] = 13;
      n.children.get_rw()[0] = leafNodes[2];
      n.children.get_rw()[1] = leafNodes[3];
      n.children.get_rw()[2] = leafNodes[4];
      n.children.get_rw()[3] = leafNodes[5];
      n.numKeys.get_rw() = 3;
    }

    btree.underflowAtBranchLevel(innerNodes[0], 0, innerNodes[1]);

    REQUIRE(innerNodes[0]->numKeys == 1);
    REQUIRE(innerNodes[0]->keys.get_ro()[0] == 9);

    REQUIRE(innerNodes[1]->numKeys == 2);
    REQUIRE(innerNodes[2]->numKeys == 2);

    std::array<int, 2> expectedKeys1{{5, 7}};
    std::array<int, 2> expectedKeys2{{11, 13}};

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(innerNodes[1]->keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(innerNodes[2]->keys.get_ro())));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Merging two inner nodes") {
    auto &btree = *rootRef.btree4;
    std::array<pptr<PBPTreeType4::LeafNode>, 5> leafNodes = {
      { btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(), btree.newLeafNode(),
        btree.newLeafNode() }
    };

    const auto node1 = btree.newBranchNode();
    const auto node2 = btree.newBranchNode();
    const auto root = btree.newBranchNode();
    auto &node1Ref = *node1;
    auto &node2Ref = *node2;
    auto &root1Ref = *root;

    node1Ref.keys.get_rw()[0] = 5;
    node1Ref.children.get_rw()[0] = leafNodes[0];
    node1Ref.children.get_rw()[1] = leafNodes[1];
    node1Ref.numKeys.get_rw() = 1;

    node2Ref.keys.get_rw()[0] = 20;
    node2Ref.keys.get_rw()[1] = 30;
    node2Ref.children.get_rw()[0] = leafNodes[2];
    node2Ref.children.get_rw()[1] = leafNodes[3];
    node2Ref.children.get_rw()[2] = leafNodes[4];
    node2Ref.numKeys.get_rw() = 2;

    root1Ref.keys.get_rw()[0] = 15;
    root1Ref.children.get_rw()[0] = node1;
    root1Ref.children.get_rw()[1] = node2;
    root1Ref.numKeys.get_rw() = 1;

    btree.rootNode = root;
    btree.depth = 2;

    btree.mergeBranchNodes(node1, root1Ref.keys.get_ro()[0], node2);

    REQUIRE(node1Ref.numKeys == 4);

    std::array<int, 4> expectedKeys{{5, 15, 20, 30}};
    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(node1Ref.keys.get_ro())));
    std::array<pptr<PBPTreeType4::LeafNode>, 5> node1Children;
    std::transform(std::begin(node1Ref.children.get_rw()), std::end(node1Ref.children.get_rw()),
                   std::begin(node1Children), [](PBPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(leafNodes), std::end(leafNodes),
                       std::begin(node1Children)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Merging two leaf nodes") {
    auto &btree = *rootRef.btree10;

    const auto node1 = btree.newLeafNode();
    const auto node2 = btree.newLeafNode();
    auto &node1Ref = *node1;
    auto &node2Ref = *node2;

    for (auto i = 0; i < 4; i++) {
      node1Ref.keys.get_rw()[i] = i;
      node1Ref.values.get_rw()[i] = i + 100;
    }
    node1Ref.numKeys.get_rw() = 4;

    for (auto i = 0; i < 4; i++) {
      node2Ref.keys.get_rw()[i] = i + 10;
      node2Ref.values.get_rw()[i] = i + 200;
    }
    node2Ref.numKeys.get_rw() = 4;
    node1Ref.nextLeaf = node2;

    btree.mergeLeafNodes(node1, node2);

    REQUIRE(node1Ref.nextLeaf == nullptr);
    REQUIRE(node1Ref.numKeys == 8);

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

    const auto node1 = btree.newLeafNode();
    const auto node2 = btree.newLeafNode();
    auto &node1Ref = *node1;
    auto &node2Ref = *node2;

    for (auto i = 0; i < 8; i++) {
      node1Ref.keys.get_rw()[i] = i + 1;
      node1Ref.values.get_rw()[i] = i * 100;
    }
    node1Ref.numKeys.get_rw() = 8;

    for (auto i = 0; i < 4; i++) {
      node2Ref.keys.get_rw()[i] = i + 11;
      node2Ref.values.get_rw()[i] = i * 200;
    }
    node2Ref.numKeys.get_rw() = 4;

    btree.balanceLeafNodes(node1, node2);
    REQUIRE(node1Ref.numKeys == 6);
    REQUIRE(node2Ref.numKeys == 6);

    std::array<int, 6> expectedKeys1{{1, 2, 3, 4, 5, 6}};
    std::array<int, 6> expectedValues1{{0, 100, 200, 300, 400, 500}};
    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(node1Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues1), std::end(expectedValues1),
                       std::begin(node1Ref.values.get_ro())));

    std::array<int, 6> expectedKeys2{{7, 8, 11, 12, 13, 14}};
    std::array<int, 6> expectedValues2{{600, 700, 0, 200, 400, 600}};
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(node2Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues2), std::end(expectedValues2),
                       std::begin(node2Ref.values.get_ro())));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Handling of underflow at a leaf node by merge and replace the root node") {
    auto &btree = *rootRef.btree6;

    const auto leaf1 = btree.newLeafNode();
    const auto leaf2 = btree.newLeafNode();
    const auto parent = btree.newBranchNode();
    auto &leaf1Ref = *leaf1;
    auto &leaf2Ref = *leaf2;
    auto &parentRef = *parent;

    leaf1Ref.nextLeaf = leaf2;
    leaf2Ref.prevLeaf = leaf1;

    PBPTreeType6::SplitInfo splitInfo;
    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);
    btree.insertInLeafNode(leaf2, 4, 40, &splitInfo);
    btree.insertInLeafNode(leaf2, 5, 50, &splitInfo);

    parent->keys.get_rw()[0] = 4;
    parent->numKeys.get_rw() = 1;
    parent->children.get_rw()[0] = leaf1;
    parent->children.get_rw()[1] = leaf2;
    btree.rootNode = parent;
    btree.depth = 1;

    btree.underflowAtLeafLevel(parent, 1, leaf2);
    REQUIRE(leaf1Ref.numKeys == 5);
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

    const auto leaf1 = btree.newLeafNode();
    const auto leaf2 = btree.newLeafNode();
    const auto leaf3 = btree.newLeafNode();
    const auto parent = btree.newBranchNode();
    auto &leaf1Ref = *leaf1;
    auto &leaf2Ref = *leaf2;
    auto &leaf3Ref = *leaf3;
    auto &parentRef = *parent;

    leaf1Ref.nextLeaf = leaf2;
    leaf2Ref.prevLeaf = leaf1;
    leaf2Ref.nextLeaf = leaf3;
    leaf3Ref.prevLeaf = leaf2;

    PBPTreeType6::SplitInfo splitInfo;
    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);
    btree.insertInLeafNode(leaf2, 4, 40, &splitInfo);
    btree.insertInLeafNode(leaf2, 5, 50, &splitInfo);
    btree.insertInLeafNode(leaf3, 7, 70, &splitInfo);
    btree.insertInLeafNode(leaf3, 8, 80, &splitInfo);
    btree.insertInLeafNode(leaf3, 9, 90, &splitInfo);

    parent->keys.get_rw()[0] = 4;
    parent->keys.get_rw()[1] = 7;
    parent->numKeys.get_rw() = 2;
    parent->children.get_rw()[0] = leaf1;
    parent->children.get_rw()[1] = leaf2;
    parent->children.get_rw()[1] = leaf3;
    btree.rootNode = parent;

    btree.underflowAtLeafLevel(parent, 1, leaf2);
    REQUIRE(leaf1Ref.numKeys == 5);
    REQUIRE(btree.rootNode.branch == parent);
    REQUIRE(parent->numKeys == 1);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Handling of underflow at a leaf node by rebalance") {
    auto &btree = *rootRef.btree6;

    const auto leaf1 = btree.newLeafNode();
    const auto leaf2 = btree.newLeafNode();
    const auto parent = btree.newBranchNode();
    auto &leaf1Ref = *leaf1;
    auto &leaf2Ref = *leaf2;
    auto &parentRef = *parent;

    leaf1Ref.nextLeaf = leaf2;
    leaf2Ref.prevLeaf = leaf1;

    PBPTreeType6::SplitInfo splitInfo;
    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);
    btree.insertInLeafNode(leaf1, 4, 40, &splitInfo);
    btree.insertInLeafNode(leaf2, 5, 50, &splitInfo);
    btree.insertInLeafNode(leaf2, 6, 60, &splitInfo);

    parent->keys.get_rw()[0] = 5;
    parent->numKeys.get_rw() = 1;
    parent->children.get_rw()[0] = leaf1;
    parent->children.get_rw()[1] = leaf2;

    btree.underflowAtLeafLevel(parent, 1, leaf2);
    REQUIRE(leaf1Ref.numKeys == 3);
    REQUIRE(leaf2Ref.numKeys == 3);

    std::array<int, 3> expectedKeys1{{1, 2, 3}};
    std::array<int, 3> expectedValues1{{10, 20, 30}};

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(leaf1Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues1), std::end(expectedValues1),
                       std::begin(leaf1Ref.values.get_ro())));

    std::array<int, 3> expectedKeys2{{4, 5, 6}};
    std::array<int, 3> expectedValues2{{40, 50, 60}};

    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(leaf2Ref.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues2), std::end(expectedValues2),
                       std::begin(leaf2Ref.values.get_ro())));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Handling of underflow at a inner node") {
    auto &btree = *rootRef.btree4;
    PBPTreeType4::SplitInfo splitInfo;

    const auto leaf1 = btree.newLeafNode();
    const auto leaf2 = btree.newLeafNode();
    const auto leaf3 = btree.newLeafNode();
    const auto leaf4 = btree.newLeafNode();
    const auto leaf5 = btree.newLeafNode();
    const auto leaf6 = btree.newLeafNode();
    const auto inner1 = btree.newBranchNode();
    const auto inner2 = btree.newBranchNode();
    const auto root = btree.newBranchNode();
    auto &leaf1Ref = *leaf1;
    auto &leaf2Ref = *leaf2;
    auto &leaf3Ref = *leaf3;
    auto &leaf4Ref = *leaf4;
    auto &leaf5Ref = *leaf5;
    auto &leaf6Ref = *leaf6;
    auto &inner1Ref = *inner1;
    auto &inner2Ref = *inner2;
    auto &root1Ref = *root;

    btree.insertInLeafNode(leaf1, 1, 1, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 2, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 3, &splitInfo);

    btree.insertInLeafNode(leaf2, 5, 5, &splitInfo);
    btree.insertInLeafNode(leaf2, 6, 6, &splitInfo);
    leaf1Ref.nextLeaf = leaf2;
    leaf2Ref.prevLeaf = leaf1;

    btree.insertInLeafNode(leaf3, 10, 10, &splitInfo);
    btree.insertInLeafNode(leaf3, 11, 11, &splitInfo);
    leaf2Ref.nextLeaf = leaf3;
    leaf3Ref.prevLeaf = leaf2;

    btree.insertInLeafNode(leaf4, 15, 15, &splitInfo);
    btree.insertInLeafNode(leaf4, 16, 16, &splitInfo);
    leaf3Ref.nextLeaf = leaf4;
    leaf4Ref.prevLeaf = leaf3;

    btree.insertInLeafNode(leaf5, 20, 20, &splitInfo);
    btree.insertInLeafNode(leaf5, 21, 21, &splitInfo);
    btree.insertInLeafNode(leaf5, 22, 22, &splitInfo);
    leaf4Ref.nextLeaf = leaf5;
    leaf5Ref.prevLeaf = leaf4;

    btree.insertInLeafNode(leaf6, 31, 31, &splitInfo);
    btree.insertInLeafNode(leaf6, 32, 32, &splitInfo);
    btree.insertInLeafNode(leaf6, 33, 33, &splitInfo);
    leaf5Ref.nextLeaf = leaf6;
    leaf6Ref.prevLeaf = leaf5;

    inner1Ref.keys.get_rw()[0] = 5;
    inner1Ref.keys.get_rw()[1] = 10;
    inner1Ref.children.get_rw()[0] = leaf1;
    inner1Ref.children.get_rw()[1] = leaf2;
    inner1Ref.children.get_rw()[2] = leaf3;
    inner1Ref.numKeys.get_rw() = 2;

    inner2Ref.keys.get_rw()[0] = 20;
    inner2Ref.keys.get_rw()[1] = 30;
    inner2Ref.children.get_rw()[0] = leaf4;
    inner2Ref.children.get_rw()[1] = leaf5;
    inner2Ref.children.get_rw()[2] = leaf6;
    inner2Ref.numKeys.get_rw() = 2;

    root1Ref.keys.get_rw()[0] = 15;
    root1Ref.children.get_rw()[0] = inner1;
    root1Ref.children.get_rw()[1] = inner2;
    root1Ref.numKeys.get_rw() = 1;

    btree.rootNode = root;
    btree.depth = 2;
    btree.eraseFromBranchNode(root, btree.depth, 10);
    REQUIRE(btree.rootNode.branch != root);
    REQUIRE(btree.depth < 2);

    REQUIRE(inner1Ref.numKeys == 4);
    std::array<int, 4> expectedKeys{{5, 15, 20, 30}};
    std::array<pptr<PBPTreeType4::LeafNode>, 5> expectedChildren {
      {leaf1, leaf2, leaf4, leaf5, leaf6}
    };

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(inner1Ref.keys.get_ro())));
    std::array<pptr<PBPTreeType4::LeafNode>, 5> inner1Children;
    std::transform(std::begin(inner1Ref.children.get_ro()), std::end(inner1Ref.children.get_ro()),
                   std::begin(inner1Children), [](PBPTreeType4::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren), std::end(expectedChildren),
                       std::begin(inner1Children)));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting an entry into a leaf node") {
    auto &btree = *rootRef.btree12;
    PBPTreeType12::SplitInfo splitInfo;
    auto res = false;
    const auto node = btree.newLeafNode();
    auto &nodeRef = *node;

    for (auto i = 0; i < 9; i++) {
      nodeRef.keys.get_rw()[i] = (i + 1) * 2;
      nodeRef.values.get_rw()[i] = (i * 2) + 100;
    }
    nodeRef.numKeys.get_rw() = 9;


    res = btree.insertInLeafNode(node, 5, 5000, &splitInfo);
    REQUIRE(res == false);
    REQUIRE(nodeRef.numKeys == 10);

    res = btree.insertInLeafNode(node, 1, 1, &splitInfo);
    REQUIRE(res == false);
    REQUIRE(nodeRef.numKeys == 11);

    res = btree.insertInLeafNode(node, 2, 1000, &splitInfo);
    REQUIRE(res == false);
    REQUIRE(nodeRef.numKeys == 11);

    std::array<int, 11> expectedKeys{{1, 2, 4, 5, 6, 8, 10, 12, 14, 16, 18}};
    std::array<int, 11> expectedValues{
        {1, 1000, 102, 5000, 104, 106, 108, 110, 112, 114, 116}};

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(nodeRef.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(nodeRef.values.get_ro())));

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
    }
    nodeRef.numKeys.get_rw() = 9;

    btree.insertInLeafNodeAtPosition(node, 2, 5, 5000);
    REQUIRE(nodeRef.numKeys == 10);
    btree.insertInLeafNodeAtPosition(node, 0, 1, 1);
    REQUIRE(nodeRef.numKeys == 11);

    std::array<int, 11> expectedKeys{{1, 2, 4, 5, 6, 8, 10, 12, 14, 16, 18}};
    std::array<int, 11> expectedValues{
        {1, 100, 102, 5000, 104, 106, 108, 110, 112, 114, 116}};

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(nodeRef.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(nodeRef.values.get_ro())));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting an entry into an inner node without a split") {
    auto &btree = *rootRef.btree4;
    PBPTreeType4::SplitInfo splitInfo;

    const auto leaf1 = btree.newLeafNode();
    const auto leaf2 = btree.newLeafNode();
    const auto node = btree.newBranchNode();
    auto &leaf1Ref = *leaf1;
    auto &leaf2Ref = *leaf2;
    auto &nodeRef = *node;

    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);

    btree.insertInLeafNode(leaf2, 10, 100, &splitInfo);
    btree.insertInLeafNode(leaf2, 12, 120, &splitInfo);

    nodeRef.keys.get_rw()[0] = 10;
    nodeRef.children.get_rw()[0] = leaf1;
    nodeRef.children.get_rw()[1] = leaf2;
    nodeRef.numKeys.get_rw() = 1;

    btree.rootNode = node;
    btree.depth = 2;

    btree.insertInBranchNode(node, 1, 11, 112, &splitInfo);
    REQUIRE(leaf2Ref.numKeys == 3);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Inserting an entry into an inner node with split") {
    auto &btree = *rootRef.btree4;
    PBPTreeType4::SplitInfo splitInfo;

    auto leaf1 = btree.newLeafNode();
    auto leaf2 = btree.newLeafNode();
    auto node = btree.newBranchNode();
    auto &leaf1Ref = *leaf1;
    auto &leaf2Ref = *leaf2;
    auto &nodeRef = *node;

    btree.insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree.insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree.insertInLeafNode(leaf1, 3, 30, &splitInfo);

    btree.insertInLeafNode(leaf2, 10, 100, &splitInfo);
    btree.insertInLeafNode(leaf2, 11, 110, &splitInfo);
    btree.insertInLeafNode(leaf2, 13, 130, &splitInfo);
    btree.insertInLeafNode(leaf2, 14, 140, &splitInfo);

    nodeRef.keys.get_rw()[0] = 10;
    nodeRef.children.get_rw()[0] = leaf1;
    nodeRef.children.get_rw()[1] = leaf2;
    nodeRef.numKeys.get_rw() = 1;

    btree.rootNode = node;
    btree.depth = 2;

    btree.insertInBranchNode(node, 1, 12, 112, &splitInfo);
    REQUIRE(leaf2Ref.numKeys == 2);
    REQUIRE(nodeRef.numKeys == 2);

    std::array<int, 2> expectedKeys{{10, 12}};
    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(nodeRef.keys.get_ro())));

    std::array<int, 3> expectedKeys2{{12, 13, 14}};
    const auto leaf3 = nodeRef.children.get_ro()[2].leaf;
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(leaf3->keys.get_ro())));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Deleting an entry from a leaf node") {
    auto &btree = *rootRef.btree20;

    const auto node = btree.newLeafNode();
    auto &nodeRef = *node;

    for (auto i = 0; i < 9; i++) {
      nodeRef.keys.get_rw()[i] = i + 1;
      nodeRef.values.get_rw()[i] = i + 100;
    }
    nodeRef.numKeys.get_rw() = 9;

    REQUIRE(btree.eraseFromLeafNode(node, 5) == true);
    REQUIRE(nodeRef.numKeys == 8);
    REQUIRE(btree.eraseFromLeafNode(node, 15) == false);
    REQUIRE(nodeRef.numKeys == 8);

    std::array<int, 8> expectedKeys{{1, 2, 3, 4, 6, 7, 8, 9 }};
    std::array<int, 8> expectedValues{
        {100, 101, 102, 103, 105, 106, 107, 108 }};

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(nodeRef.keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(nodeRef.values.get_ro())));
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Testing delete from a leaf node in a B+ tree") {
    auto &btree = *rootRef.btree20;
    for (int i = 0; i < 20; i += 2) {
      btree.insert(i, i);
    }
    REQUIRE (btree.erase(10) == true);
    int res;
    REQUIRE (btree.lookup(10, &res) == false);
  }

  /* -------------------------------------------------------------------------------------------- */
  SECTION("Constructing a B+ tree and iterating over it") {
    auto btree = rootRef.btree10;
    transaction::run(pop, [&] {
      if(btree) delete_persistent<PBPTreeType10>(btree);
      btree = make_persistent<PBPTreeType10>();
      auto &btreeRef = *btree;
      for (int i = 0; i < 50; i++) {
        btreeRef.insert(i, i * 2);
      }
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

  /* Clean up ----------------------------------------------------------------------------------- */
  delete_persistent_atomic<PBPTreeType4>(rootRef.btree4);
  delete_persistent_atomic<PBPTreeType6>(rootRef.btree6);
  delete_persistent_atomic<PBPTreeType10>(rootRef.btree10);
  delete_persistent_atomic<PBPTreeType12>(rootRef.btree12);
  delete_persistent_atomic<PBPTreeType20>(rootRef.btree20);
  delete_persistent_atomic<root>(q);
  pop.close();
  std::remove(path.c_str());
}
