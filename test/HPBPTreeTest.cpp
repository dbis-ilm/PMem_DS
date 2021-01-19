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
#include "HPBPTree.hpp"


using namespace dbis::pbptrees;

using pmem::obj::delete_persistent;
using pmem::obj::delete_persistent_atomic;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::pool;
using pmem::obj::transaction;

TEST_CASE("Finding the leaf node containing a key", "[HPBPTree]") {
  using HPBPTreeType  = HPBPTree<int, int, 10, 10> ;
  using HPBPTreeType2 = HPBPTree<int, int, 4, 4>;
  using HPBPTreeType3 = HPBPTree<int, int, 6, 6>;
  using HPBPTreeType4 = HPBPTree<int, int, 12, 12>;
  using HPBPTreeType5 = HPBPTree<int, int, 20, 20>;

  struct root {
    persistent_ptr<HPBPTreeType> btree1;
    persistent_ptr<HPBPTreeType2> btree2;
    persistent_ptr<HPBPTreeType3> btree3;
    persistent_ptr<HPBPTreeType4> btree4;
    persistent_ptr<HPBPTreeType5> btree5;
  };

  pool<root> pop;
  const std::string path = dbis::gPmemPath + "HPBPTreeTest";

  //std::remove(path.c_str());
  if (access(path.c_str(), F_OK) != 0) {
    pop = pool<root>::create(path, "HPBPTree", ((std::size_t)(1024*1024*16)));
  } else {
    pop = pool<root>::open(path, "HPBPTree");
  }

  auto q = pop.root();
  const auto alloc_class = pop.ctl_set<struct pobj_alloc_class_desc>("heap.alloc_class.128.desc",
                                                                     HPBPTreeType4::AllocClass);

  if (!q->btree1)
    transaction::run(pop, [&] { q->btree1 = make_persistent<HPBPTreeType>(alloc_class); });

  if (!q->btree2)
    transaction::run(pop, [&] { q->btree2 = make_persistent<HPBPTreeType2>(alloc_class); });

  if (!q->btree3)
    transaction::run(pop, [&] { q->btree3 = make_persistent<HPBPTreeType3>(alloc_class); });

  if (!q->btree4)
    transaction::run(pop, [&] { q->btree4 = make_persistent<HPBPTreeType4>(alloc_class); });

  if (!q->btree5)
    transaction::run(pop, [&] { q->btree5 = make_persistent<HPBPTreeType5>(alloc_class); });


  /* ------------------------------------------------------------------ */
  SECTION("Looking up a key in an inner node") {
    auto node = q->btree1->newBranchNode();
    for (auto i = 0; i < 10; i++) node->keys[i] = i + 1;
    node->numKeys = 10;

    REQUIRE(q->btree1->lookupPositionInBranchNode(node, 0) == 0);
    REQUIRE(q->btree1->lookupPositionInBranchNode(node, 1) == 1);
    REQUIRE(q->btree1->lookupPositionInBranchNode(node, 10) == 10);
    REQUIRE(q->btree1->lookupPositionInBranchNode(node, 5) == 5);
    REQUIRE(q->btree1->lookupPositionInBranchNode(node, 20) == 10);

    q->btree1->deleteBranchNode(node);
  }

  /* ------------------------------------------------------------------ */
  SECTION("Looking up a key in a leaf node") {
    auto node = q->btree1->newLeafNode();
    for (auto i = 0; i < 10; i++) node->keys.get_rw()[i] = i + 1;
    node->numKeys = 10;

    REQUIRE(q->btree1->lookupPositionInLeafNode(node, 1) == 0);
    REQUIRE(q->btree1->lookupPositionInLeafNode(node, 10) == 9);
    REQUIRE(q->btree1->lookupPositionInLeafNode(node, 5) == 4);
    REQUIRE(q->btree1->lookupPositionInLeafNode(node, 20) == 10);

    q->btree1->deleteLeafNode(node);
  }

  /* ------------------------------------------------------------------ */
  SECTION("Balancing two inner nodes from left to right") {
    auto btree = q->btree2;
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 7> leafNodes = {{btree->newLeafNode(), btree->newLeafNode(), btree->newLeafNode(),
        btree->newLeafNode(), btree->newLeafNode(), btree->newLeafNode(), btree->newLeafNode()}};

    auto node1 = btree->newBranchNode();
    auto node2 = btree->newBranchNode();
    auto node3 = btree->newBranchNode();

    node1->keys[0] = 8;
    node1->keys[1] = 20;
    node1->children[0] = node2;
    node1->children[1] = node3;
    node1->numKeys = 2;

    node2->keys[0] = 3;
    node2->keys[1] = 4;
    node2->keys[2] = 5;
    node2->keys[3] = 6;
    node2->children[0] = leafNodes[0];
    node2->children[1] = leafNodes[1];
    node2->children[2] = leafNodes[2];
    node2->children[3] = leafNodes[3];
    node2->children[4] = leafNodes[4];
    node2->numKeys = 4;

    node3->keys[0] = 10;
    node3->children[0] = leafNodes[5];
    node3->children[1] = leafNodes[6];
    node3->numKeys = 1;

    btree->rootNode = node1;
    btree->depth = 2;

    btree->balanceBranchNodes(node2, node3, node1, 0);

    REQUIRE(node1->numKeys == 2);
    REQUIRE(node2->numKeys == 2);
    REQUIRE(node3->numKeys == 3);

    std::array<int, 2> expectedKeys1{{5, 20}};
    std::array<int, 2> expectedKeys2{{3, 4}};
    std::array<int, 3> expectedKeys3{{6, 8, 10}};

    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 3> expectedChildren2 = {{ leafNodes[0], leafNodes[1], leafNodes[2] }};
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 4> expectedChildren3 = {{ leafNodes[3], leafNodes[4], leafNodes[5], leafNodes[6] }};

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(node1->keys)));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(node2->keys)));
    REQUIRE(std::equal(std::begin(expectedKeys3), std::end(expectedKeys3),
                       std::begin(node3->keys)));

    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 3> node2Children;
    std::transform(std::begin(node2->children), std::end(node2->children), std::begin(node2Children), [](HPBPTreeType2::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren2), std::end(expectedChildren2),
                       std::begin(node2Children)));

    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 4> node3Children;
    std::transform(std::begin(node3->children), std::end(node3->children), std::begin(node3Children), [](HPBPTreeType2::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren3), std::end(expectedChildren3),
                       std::begin(node3Children)));
  }

  /* ------------------------------------------------------------------ */
  SECTION("Balancing two inner nodes from right to left") {
    auto btree = q->btree2;
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 7> leafNodes = {{btree->newLeafNode(), btree->newLeafNode(), btree->newLeafNode(),
        btree->newLeafNode(), btree->newLeafNode(), btree->newLeafNode(), btree->newLeafNode()}};

    auto node1 = btree->newBranchNode();
    auto node2 = btree->newBranchNode();
    auto node3 = btree->newBranchNode();

    node1->keys[0] = 4;
    node1->keys[1] = 20;
    node1->children[0] = node2;
    node1->children[1] = node3;
    node1->numKeys = 2;

    node2->keys[0] = 3;
    node2->children[0] = leafNodes[0];
    node2->children[1] = leafNodes[1];

    node2->numKeys = 1;

    node3->keys[0] = 5;
    node3->keys[1] = 6;
    node3->keys[2] = 7;
    node3->keys[3] = 8;
    node3->children[0] = leafNodes[2];
    node3->children[1] = leafNodes[3];
    node3->children[2] = leafNodes[4];
    node3->children[3] = leafNodes[5];
    node3->children[4] = leafNodes[6];

    node3->numKeys = 4;

    btree->rootNode = node1;
    btree->depth = 2;

    btree->balanceBranchNodes(node3, node2, node1, 0);

    REQUIRE(node1->numKeys == 2);
    REQUIRE(node2->numKeys == 3);
    REQUIRE(node3->numKeys == 2);

    std::array<int, 2> expectedKeys1{{6, 20}};
    std::array<int, 3> expectedKeys2{{3, 4, 5}};
    std::array<int, 2> expectedKeys3{{7, 8}};

    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 4> expectedChildren2 = {{ leafNodes[0], leafNodes[1], leafNodes[2], leafNodes[3] }};
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 3> expectedChildren3 = {{ leafNodes[4], leafNodes[5], leafNodes[6] }};

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(node1->keys)));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(node2->keys)));
    REQUIRE(std::equal(std::begin(expectedKeys3), std::end(expectedKeys3),
                       std::begin(node3->keys)));
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 4> node2Children;
    std::transform(std::begin(node2->children), std::end(node2->children), std::begin(node2Children), [](HPBPTreeType2::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren2), std::end(expectedChildren2),
                       std::begin(node2Children)));
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 3> node3Children;
    std::transform(std::begin(node3->children), std::end(node3->children), std::begin(node3Children), [](HPBPTreeType2::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren3), std::end(expectedChildren3),
                       std::begin(node3Children)));
  }

  /* ------------------------------------------------------------------ */
  SECTION("Handling of underflow at a inner node by rebalance") {
    auto btree = q->btree2;
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 6> leafNodes = {{ btree->newLeafNode(), btree->newLeafNode(), btree->newLeafNode(),
        btree->newLeafNode(), btree->newLeafNode(), btree->newLeafNode()}};

    std::array<HPBPTreeType2::BranchNode*, 3> innerNodes = {{ btree->newBranchNode(), btree->newBranchNode(), btree->newBranchNode()}};

    HPBPTreeType2::SplitInfo splitInfo;
    btree->insertInLeafNode(leafNodes[0], 1, 10, &splitInfo);
    btree->insertInLeafNode(leafNodes[0], 2, 20, &splitInfo);
    btree->insertInLeafNode(leafNodes[0], 3, 30, &splitInfo);

    btree->insertInLeafNode(leafNodes[1], 5, 50, &splitInfo);
    btree->insertInLeafNode(leafNodes[1], 6, 60, &splitInfo);

    btree->insertInLeafNode(leafNodes[2], 7, 70, &splitInfo);
    btree->insertInLeafNode(leafNodes[2], 8, 80, &splitInfo);

    btree->insertInLeafNode(leafNodes[3], 9, 90, &splitInfo);
    btree->insertInLeafNode(leafNodes[3], 10, 100, &splitInfo);

    btree->insertInLeafNode(leafNodes[4], 11, 110, &splitInfo);
    btree->insertInLeafNode(leafNodes[4], 12, 120, &splitInfo);

    btree->insertInLeafNode(leafNodes[5], 13, 130, &splitInfo);
    btree->insertInLeafNode(leafNodes[5], 14, 140, &splitInfo);

    btree->rootNode = innerNodes[0];
    btree->depth = 2;

    {
      auto n = innerNodes[0];
      n->keys[0] = 7;
      n->children[0] = innerNodes[1];
      n->children[1] = innerNodes[2];
      n->numKeys = 1;
    }
    {
      auto n = innerNodes[1];
      n->keys[0] = 5;
      n->children[0] = leafNodes[0];
      n->children[1] = leafNodes[1];
      n->numKeys = 1;
    }
    {
      auto n = innerNodes[2];
      n->keys[0] = 9;
      n->keys[1] = 11;
      n->keys[2] = 13;
      n->children[0] = leafNodes[2];
      n->children[1] = leafNodes[3];
      n->children[2] = leafNodes[4];
      n->children[3] = leafNodes[5];
      n->numKeys = 3;
    }

    btree->underflowAtBranchLevel(innerNodes[0], 0, innerNodes[1]);

    REQUIRE(innerNodes[0]->numKeys == 1);
    REQUIRE(innerNodes[0]->keys[0] == 9);

    REQUIRE(innerNodes[1]->numKeys == 2);
    REQUIRE(innerNodes[2]->numKeys == 2);

    std::array<int, 2> expectedKeys1{{5, 7}};
    std::array<int, 2> expectedKeys2{{11, 13}};

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(innerNodes[1]->keys)));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(innerNodes[2]->keys)));

  }

  /* ------------------------------------------------------------------ */
  SECTION("Merging two inner nodes") {
    auto btree = q->btree2;
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 5> leafNodes = { {btree->newLeafNode(), btree->newLeafNode(), btree->newLeafNode(),
        btree->newLeafNode(), btree->newLeafNode()}};

    auto node1 = btree->newBranchNode();
    auto node2 = btree->newBranchNode();

    node1->keys[0] = 5;
    node1->children[0] = leafNodes[0];
    node1->children[1] = leafNodes[1];
    node1->numKeys = 1;

    node2->keys[0] = 20;
    node2->keys[1] = 30;
    node2->children[0] = leafNodes[2];
    node2->children[1] = leafNodes[3];
    node2->children[2] = leafNodes[4];
    node2->numKeys = 2;

    auto root = btree->newBranchNode();
    root->keys[0] = 15;
    root->children[0] = node1;
    root->children[1] = node2;
    root->numKeys = 1;

    btree->rootNode = root;
    btree->depth = 2;

    btree->mergeBranchNodes(node1, root->keys[0], node2);

    REQUIRE(node1->numKeys == 4);

    std::array<int, 4> expectedKeys{{5, 15, 20, 30}};
    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(node1->keys)));
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 5> node1Children;
    std::transform(std::begin(node1->children), std::end(node1->children), std::begin(node1Children), [](HPBPTreeType2::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(leafNodes), std::end(leafNodes),
                       std::begin(node1Children)));

  }

  /* ------------------------------------------------------------------ */
  SECTION("Merging two leaf nodes") {
    auto btree = q->btree1;

    auto node1 = btree->newLeafNode();
    auto node2 = btree->newLeafNode();

    for (auto i = 0; i < 4; i++) {
      (node1->keys.get_rw())[i] = i;
      (node1->values.get_rw())[i] = i + 100;
    }
    node1->numKeys = 4;

    for (auto i = 0; i < 4; i++) {
      (node2->keys.get_rw())[i] = i + 10;
      (node2->values.get_rw())[i] = i + 200;
    }
    node2->numKeys = 4;
    node1->nextLeaf = node2;

    btree->mergeLeafNodes(node1, node2);

    REQUIRE(node1->nextLeaf == nullptr);
    REQUIRE(node1->numKeys == 8);

    std::array<int, 8> expectedKeys{{0, 1, 2, 3, 10, 11, 12, 13}};
    std::array<int, 8> expectedValues{{100, 101, 102, 103, 200, 201, 202, 203}};
    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(node1->keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(node1->values.get_ro())));

  }

  /* ------------------------------------------------------------------ */
  SECTION("Balancing two leaf nodes") {
    auto btree = q->btree1;

    auto node1 = btree->newLeafNode();
    auto node2 = btree->newLeafNode();

    for (auto i = 0; i < 8; i++) {
      (node1->keys.get_rw())[i] = i + 1;
      (node1->values.get_rw())[i] = i * 100;
    }
    node1->numKeys = 8;

    for (auto i = 0; i < 4; i++) {
      (node2->keys.get_rw())[i] = i + 11;
      (node2->values.get_rw())[i] = i * 200;
    }
    node2->numKeys = 4;

    btree->balanceLeafNodes(node1, node2);
    REQUIRE(node1->numKeys == 6);
    REQUIRE(node2->numKeys == 6);

    std::array<int, 6> expectedKeys1{{1, 2, 3, 4, 5, 6}};
    std::array<int, 6> expectedValues1{{0, 100, 200, 300, 400, 500}};
    REQUIRE(std::equal(std::begin(expectedKeys1), std::begin(expectedKeys1)+6,
                       std::begin(node1->keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues1), std::end(expectedValues1),
                       std::begin(node1->values.get_ro())));

    std::array<int, 6> expectedKeys2{{7, 8, 11, 12, 13, 14}};
    std::array<int, 6> expectedValues2{{0, 200, 400, 600, 600, 700}};
    std::vector<int> node2vecK {std::begin(node2->keys.get_ro()), std::begin(node2->keys.get_ro())+6};
    std::vector<int> node2vecV {std::begin(node2->values.get_ro()), std::begin(node2->values.get_ro())+6};
    std::sort(std::begin(node2vecK), std::end(node2vecK));
    std::sort(std::begin(node2vecV), std::end(node2vecV));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::begin(expectedKeys2)+6,
                       std::begin(node2vecK)));
    REQUIRE(std::equal(std::begin(expectedValues2), std::begin(expectedValues2)+6,
                       std::begin(node2vecV)));

  }

  /* ------------------------------------------------------------------ */
  SECTION("Handling of underflow at a leaf node by merge and replace the root node") {
    auto btree = q->btree3;

    auto leaf1 = btree->newLeafNode();
    auto leaf2 = btree->newLeafNode();
    leaf1->nextLeaf = leaf2;
    leaf2->prevLeaf = leaf1;
    auto parent = btree->newBranchNode();

    HPBPTreeType3::SplitInfo splitInfo;
    btree->insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree->insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree->insertInLeafNode(leaf1, 3, 30, &splitInfo);
    btree->insertInLeafNode(leaf2, 4, 40, &splitInfo);
    btree->insertInLeafNode(leaf2, 5, 50, &splitInfo);

    parent->keys[0] = 4;
    parent->numKeys = 1;
    parent->children[0] = leaf1;
    parent->children[1] = leaf2;
    btree->rootNode = parent;
    btree->depth = 1;

    btree->underflowAtLeafLevel(parent, 1, leaf2);
    REQUIRE(leaf1->numKeys == 5);
    REQUIRE(btree->rootNode.leaf == leaf1);

    std::array<int, 5> expectedKeys{{1, 2, 3, 4, 5}};
    std::array<int, 5> expectedValues{{10, 20, 30, 40, 50}};

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(leaf1->keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(leaf1->values.get_ro())));

  }

  /* ------------------------------------------------------------------ */
  SECTION("Handling of underflow at a leaf node by merge") {
    auto btree = q->btree3;

    auto leaf1 = btree->newLeafNode();
    auto leaf2 = btree->newLeafNode();
    auto leaf3 = btree->newLeafNode();
    leaf1->nextLeaf = leaf2;
    leaf2->prevLeaf = leaf1;
    leaf2->nextLeaf = leaf3;
    leaf3->prevLeaf = leaf2;
    auto parent = btree->newBranchNode();

    HPBPTreeType3::SplitInfo splitInfo;
    btree->insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree->insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree->insertInLeafNode(leaf1, 3, 30, &splitInfo);
    btree->insertInLeafNode(leaf2, 4, 40, &splitInfo);
    btree->insertInLeafNode(leaf2, 5, 50, &splitInfo);
    btree->insertInLeafNode(leaf3, 7, 70, &splitInfo);
    btree->insertInLeafNode(leaf3, 8, 80, &splitInfo);
    btree->insertInLeafNode(leaf3, 9, 90, &splitInfo);

    parent->keys[0] = 4;
    parent->keys[1] = 7;
    parent->numKeys = 2;
    parent->children[0] = leaf1;
    parent->children[1] = leaf2;
    parent->children[1] = leaf3;
    btree->rootNode = parent;

    btree->underflowAtLeafLevel(parent, 1, leaf2);
    REQUIRE(leaf1->numKeys == 5);
    REQUIRE(btree->rootNode.branch == parent);
    REQUIRE(parent->numKeys == 1);

  }

  /* ------------------------------------------------------------------ */
  SECTION("Handling of underflow at a leaf node by rebalance") {
    auto btree = q->btree3;

    auto leaf1 = btree->newLeafNode();
    auto leaf2 = btree->newLeafNode();
    leaf1->nextLeaf = leaf2;
    leaf2->prevLeaf = leaf1;
    auto parent = btree->newBranchNode();

    HPBPTreeType3::SplitInfo splitInfo;
    btree->insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree->insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree->insertInLeafNode(leaf1, 3, 30, &splitInfo);
    btree->insertInLeafNode(leaf1, 4, 40, &splitInfo);
    btree->insertInLeafNode(leaf2, 5, 50, &splitInfo);
    btree->insertInLeafNode(leaf2, 6, 60, &splitInfo);

    parent->keys[0] = 5;
    parent->numKeys = 1;
    parent->children[0] = leaf1;
    parent->children[1] = leaf2;

    btree->underflowAtLeafLevel(parent, 1, leaf2);
    REQUIRE(leaf1->numKeys == 3);
    REQUIRE(leaf2->numKeys == 3);

    std::array<int, 3> expectedKeys1{{1, 2, 3}};
    std::array<int, 3> expectedValues1{{10, 20, 30}};

    REQUIRE(std::equal(std::begin(expectedKeys1), std::end(expectedKeys1),
                       std::begin(leaf1->keys.get_ro())));
    REQUIRE(std::equal(std::begin(expectedValues1), std::end(expectedValues1),
                       std::begin(leaf1->values.get_ro())));

    std::array<int, 3> expectedKeys2{{4, 5, 6}};
    std::array<int, 3> expectedValues2{{40, 50, 60}};

    std::vector<int> leaf2vecK {std::begin(leaf2->keys.get_ro()), std::begin(leaf2->keys.get_ro())+3};
    std::vector<int> leaf2vecV {std::begin(leaf2->values.get_ro()), std::begin(leaf2->values.get_ro())+3};
    std::sort(std::begin(leaf2vecK), std::end(leaf2vecK));
    std::sort(std::begin(leaf2vecV), std::end(leaf2vecV));
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(leaf2vecK)));
    REQUIRE(std::equal(std::begin(expectedValues2), std::end(expectedValues2),
                       std::begin(leaf2vecV)));

  }

  /* ------------------------------------------------------------------ */
  SECTION("Handling of underflow at a inner node") {
    auto btree = q->btree2;
    HPBPTreeType2::SplitInfo splitInfo;

    auto leaf1 = btree->newLeafNode();
    btree->insertInLeafNode(leaf1, 1, 1, &splitInfo);
    btree->insertInLeafNode(leaf1, 2, 2, &splitInfo);
    btree->insertInLeafNode(leaf1, 3, 3, &splitInfo);

    auto leaf2 = btree->newLeafNode();
    btree->insertInLeafNode(leaf2, 5, 5, &splitInfo);
    btree->insertInLeafNode(leaf2, 6, 6, &splitInfo);
    leaf1->nextLeaf = leaf2;
    leaf2->prevLeaf = leaf1;

    auto leaf3 = btree->newLeafNode();
    btree->insertInLeafNode(leaf3, 10, 10, &splitInfo);
    btree->insertInLeafNode(leaf3, 11, 11, &splitInfo);
    leaf2->nextLeaf = leaf3;
    leaf3->prevLeaf = leaf2;

    auto leaf4 = btree->newLeafNode();
    btree->insertInLeafNode(leaf4, 15, 15, &splitInfo);
    btree->insertInLeafNode(leaf4, 16, 16, &splitInfo);
    leaf3->nextLeaf = leaf4;
    leaf4->prevLeaf = leaf3;

    auto leaf5 = btree->newLeafNode();
    btree->insertInLeafNode(leaf5, 20, 20, &splitInfo);
    btree->insertInLeafNode(leaf5, 21, 21, &splitInfo);
    btree->insertInLeafNode(leaf5, 22, 22, &splitInfo);
    leaf4->nextLeaf = leaf5;
    leaf5->prevLeaf = leaf4;

    auto leaf6 = btree->newLeafNode();
    btree->insertInLeafNode(leaf6, 31, 31, &splitInfo);
    btree->insertInLeafNode(leaf6, 32, 32, &splitInfo);
    btree->insertInLeafNode(leaf6, 33, 33, &splitInfo);
    leaf5->nextLeaf = leaf6;
    leaf6->prevLeaf = leaf5;

    auto inner1 = btree->newBranchNode();
    inner1->keys[0] = 5;
    inner1->keys[1] = 10;
    inner1->children[0] = leaf1;
    inner1->children[1] = leaf2;
    inner1->children[2] = leaf3;
    inner1->numKeys = 2;

    auto inner2 = btree->newBranchNode();
    inner2->keys[0] = 20;
    inner2->keys[1] = 30;
    inner2->children[0] = leaf4;
    inner2->children[1] = leaf5;
    inner2->children[2] = leaf6;
    inner2->numKeys = 2;

    auto root = btree->newBranchNode();
    root->keys[0] = 15;
    root->children[0] = inner1;
    root->children[1] = inner2;
    root->numKeys = 1;

    btree->rootNode = root;
    btree->depth = 2;
    btree->eraseFromBranchNode(root, btree->depth, 10);
    REQUIRE(btree->rootNode.branch != root);
    REQUIRE(btree->depth < 2);

    REQUIRE(inner1->numKeys == 4);
    std::array<int, 4> expectedKeys{{5, 15, 20, 30}};
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 5> expectedChildren{{leaf1, leaf2, leaf4, leaf5, leaf6}};

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(inner1->keys)));
    std::array<persistent_ptr<HPBPTreeType2::LeafNode>, 5> inner1Children;
    std::transform(std::begin(inner1->children), std::end(inner1->children), std::begin(inner1Children), [](HPBPTreeType2::Node n) { return n.leaf; });
    REQUIRE(std::equal(std::begin(expectedChildren), std::end(expectedChildren),
                       std::begin(inner1Children)));

  }

  /* ------------------------------------------------------------------ */
  SECTION("Inserting an entry into a leaf node") {
    auto btree = q->btree4;
    HPBPTreeType4::SplitInfo splitInfo;
    auto res = false;
    auto node = btree->newLeafNode();

    for (auto i = 0; i < 9; i++) {
      (node->keys.get_rw())[i] = (i + 1) * 2;
      (node->values.get_rw())[i] = (i * 2) + 100;
    }
    node->numKeys = 9;


    res = btree->insertInLeafNode(node, 5, 5000, &splitInfo);
    REQUIRE(res == false);
    REQUIRE(node->numKeys == 10);

    res = btree->insertInLeafNode(node, 1, 1, &splitInfo);
    REQUIRE(res == false);
    REQUIRE(node->numKeys == 11);

    res = btree->insertInLeafNode(node, 2, 1000, &splitInfo);
    REQUIRE(res == false);
    REQUIRE(node->numKeys == 11);

    std::array<int, 11> expectedKeys{{1, 2, 4, 5, 6, 8, 10, 12, 14, 16, 18}};
    std::array<int, 11> expectedValues{
        {1, 102, 104, 106, 108, 110, 112, 114, 116, 1000, 5000}};

    std::vector<int> nodevecK {std::begin(node->keys.get_ro()), std::begin(node->keys.get_ro())+11};
    std::vector<int> nodevecV {std::begin(node->values.get_ro()), std::begin(node->values.get_ro())+11};
    std::sort(std::begin(nodevecK), std::end(nodevecK));
    std::sort(std::begin(nodevecV), std::end(nodevecV));

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(nodevecK)));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(nodevecV)));

    res = btree->insertInLeafNode(node, 20, 21, &splitInfo);
    REQUIRE(res == false);

    res = btree->insertInLeafNode(node, 25, 25, &splitInfo);
    REQUIRE(res == true);

  }

  /* ------------------------------------------------------------------ */
  SECTION("Inserting an entry into a leaf node at a given position") {
    auto btree = q->btree5;

    auto node = btree->newLeafNode();

    for (auto i = 0; i < 9; i++) {
      (node->keys.get_rw())[i] = (i + 1) * 2;
      (node->values.get_rw())[i] = (i * 2) + 100;
    }
    node->numKeys = 9;

    btree->insertInLeafNodeAtPosition(node, 2, 5, 5000);
    REQUIRE(node->numKeys == 10);
    btree->insertInLeafNodeAtPosition(node, 0, 1, 1);
    REQUIRE(node->numKeys == 11);

    std::array<int, 11> expectedKeys{{1, 2, 4, 5, 6, 8, 10, 12, 14, 16, 18}};
    std::array<int, 11> expectedValues{
        {1, 100, 102,104, 106, 108, 110, 112, 114, 116, 5000}};

    std::vector<int> nodevecK {std::begin(node->keys.get_ro()), std::begin(node->keys.get_ro())+11};
    std::vector<int> nodevecV {std::begin(node->values.get_ro()), std::begin(node->values.get_ro())+11};
    std::sort(std::begin(nodevecK), std::end(nodevecK));
    std::sort(std::begin(nodevecV), std::end(nodevecV));

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(nodevecK)));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(nodevecV)));

  }

  /* ------------------------------------------------------------------ */
  SECTION("Inserting an entry into an inner node without a split") {
    auto btree = q->btree2;
    HPBPTreeType2::SplitInfo splitInfo;

    auto leaf1 = btree->newLeafNode();
    btree->insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree->insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree->insertInLeafNode(leaf1, 3, 30, &splitInfo);

    auto leaf2 = btree->newLeafNode();
    btree->insertInLeafNode(leaf2, 10, 100, &splitInfo);
    btree->insertInLeafNode(leaf2, 12, 120, &splitInfo);

    auto node = btree->newBranchNode();
    node->keys[0] = 10;
    node->children[0] = leaf1;
    node->children[1] = leaf2;
    node->numKeys = 1;

    btree->rootNode = node;
    btree->depth = 2;

    btree->insertInBranchNode(node, 1, 11, 112, &splitInfo);
    REQUIRE(leaf2->numKeys == 3);

  }

  /* ------------------------------------------------------------------ */
  SECTION("Inserting an entry into an inner node with split") {
    auto btree = q->btree2;
    HPBPTreeType2::SplitInfo splitInfo;

    auto leaf1 = btree->newLeafNode();
    btree->insertInLeafNode(leaf1, 1, 10, &splitInfo);
    btree->insertInLeafNode(leaf1, 2, 20, &splitInfo);
    btree->insertInLeafNode(leaf1, 3, 30, &splitInfo);

    auto leaf2 = btree->newLeafNode();
    btree->insertInLeafNode(leaf2, 10, 100, &splitInfo);
    btree->insertInLeafNode(leaf2, 11, 110, &splitInfo);
    btree->insertInLeafNode(leaf2, 13, 130, &splitInfo);
    btree->insertInLeafNode(leaf2, 14, 140, &splitInfo);

    auto node = btree->newBranchNode();
    node->keys[0] = 10;
    node->children[0] = leaf1;
    node->children[1] = leaf2;
    node->numKeys = 1;

    btree->rootNode = node;
    btree->depth = 2;

    btree->insertInBranchNode(node, 1, 12, 112, &splitInfo);
    REQUIRE(leaf2->numKeys == 2);
    REQUIRE(node->numKeys == 2);

    std::array<int, 2> expectedKeys{{10, 12}};
    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(node->keys)));

    std::array<int, 3> expectedKeys2{{12, 13, 14}};
    auto leaf3 = node->children[2].leaf;
    REQUIRE(std::equal(std::begin(expectedKeys2), std::end(expectedKeys2),
                       std::begin(leaf3->keys.get_ro())));

  }

  /* ------------------------------------------------------------------ */
  SECTION("Deleting an entry from a leaf node") {
    auto btree = q->btree5;

    auto node = btree->newLeafNode();

    for (auto i = 0; i < 9; i++) {
      (node->keys.get_rw())[i] = i + 1;
      (node->values.get_rw())[i] = i + 100;
    }
    node->numKeys = 9;

    REQUIRE(btree->eraseFromLeafNode(node, 5) == true);
    REQUIRE(node->numKeys == 8);
    REQUIRE(btree->eraseFromLeafNode(node, 15) == false);
    REQUIRE(node->numKeys == 8);

    std::array<int, 8> expectedKeys{{1, 2, 3, 4, 6, 7, 8, 9 }};
    std::array<int, 8> expectedValues{
        {100, 101, 102, 103, 105, 106, 107, 108 }};

    std::vector<int> nodevecK {std::begin(node->keys.get_ro()), std::begin(node->keys.get_ro())+8};
    std::vector<int> nodevecV {std::begin(node->values.get_ro()), std::begin(node->values.get_ro())+8};
    std::sort(std::begin(nodevecK), std::end(nodevecK));
    std::sort(std::begin(nodevecV), std::end(nodevecV));

    REQUIRE(std::equal(std::begin(expectedKeys), std::end(expectedKeys),
                       std::begin(nodevecK)));
    REQUIRE(std::equal(std::begin(expectedValues), std::end(expectedValues),
                       std::begin(nodevecV)));

  }

  /* ----------------------------------------------------------- */
  SECTION("Testing delete from a leaf node in a B+ tree") {
    auto btree = q->btree5;
    for (int i = 0; i < 20; i += 2) {
      btree->insert(i, i);
    }
    REQUIRE (btree->erase(10) == true);
    int res;
    REQUIRE (btree->lookup(10, &res) == false);
  }

  /* ----------------------------------------------------------- */
  SECTION("Constructing a B+ tree and iterating over it") {
    auto btree = q->btree1;
    transaction::run(pop, [&] {
      if(btree) delete_persistent<HPBPTreeType>(btree);
      btree = make_persistent<HPBPTreeType>(alloc_class);
      for (int i = 0; i < 50; i++) {
        btree->insert(i, i * 2);
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

  /* Clean up */
  delete_persistent_atomic<HPBPTreeType>(q->btree1);
  delete_persistent_atomic<HPBPTreeType2>(q->btree2);
  delete_persistent_atomic<HPBPTreeType3>(q->btree3);
  delete_persistent_atomic<HPBPTreeType4>(q->btree4);
  delete_persistent_atomic<HPBPTreeType5>(q->btree5);
  delete_persistent_atomic<root>(q);
  pop.close();
  std::remove(path.c_str());
}
