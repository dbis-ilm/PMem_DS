#ifndef WOP_SKIPLIST_HPP
#define WOP_SKIPLIST_HPP

#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <array>
#include <bitset>

#include <libpmemobj++/utils.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

using pmem::obj::delete_persistent;
using pmem::obj::make_persistent;
using pmem::obj::p;
using pmem::obj::persistent_ptr;
using pmem::obj::transaction;



class Random {

public:
    explicit Random(uint32_t s) : seed(s & 0x7fffffffu) {
        if (seed == 0 || seed == 2147483647L) {
            seed = 1;
        }
    }

    uint32_t Next() {
        static const uint32_t M = 2147483647L;  
        static const uint64_t A = 16807;  
        uint64_t product = seed * A;

        seed = static_cast<uint32_t>((product >> 31) + (product & M));
        if (seed > M) {
            seed -= M;
        }
        return seed;
    }

    uint32_t Uniform(int n) { return (Next() % n); }
    bool OneIn(int n) { return (Next() % n) == 0; }
    uint32_t Skewed(int max_log) {
        return Uniform(1 << Uniform(max_log + 1));
    }


private:
    uint32_t seed;
};

//N = Bucket size
//M = Max Level
template<typename KeyType, typename ValueType, int N, int M>
class woPSkiplist {

    struct SkipNode {
        p<std::array<KeyType, N>> key;
        p<std::array<ValueType, N>> value;
        p<std::bitset<N>> bitset;

        p<KeyType> minKey;
        p<KeyType> maxKey;
        p<int> nodeLevel;

        persistent_ptr<std::array<persistent_ptr<SkipNode>, M>> forward;


        SkipNode() {
            this->maxKey = -100000000;
            this->minKey = 100000000;
            nodeLevel = 0;
        }

        SkipNode(const KeyType key, const ValueType value) {
            this->key = key;
            this->value = value;
        }

        KeyType getAvg() const {
            KeyType sum = 0;
            auto btCount = 0;
            for(int i=0; i < N; i++) {
                if(bitset.get_ro().test(i)) {
                    auto k = key.get_ro()[i];
                    if(k != minKey && k != maxKey) {
                        sum += k;
                        btCount++;
                    }
                }
            }
            return sum / btCount;
        }

        bool isFull() {
            return bitset.get_ro().all();
        }

        bool keyBetween(KeyType newKey) {
            if(minKey == - 1 ||
                maxKey == -1)
                return true;

            return newKey >= minKey && newKey <= maxKey;
        }

        ValueType* searchKey(const KeyType searchKey) {
            for(int i=0; i<N; i++) {
                if(bitset.get_ro().test(i)) {
                    if(key.get_ro()[i] == searchKey) {
                        return &value.get_rw().at(i);
                    }
                }
            }
            return nullptr;
        }

        void splitNode(persistent_ptr<SkipNode> prevNode, persistent_ptr<SkipNode> nextNode, int listLevel) {
            auto pop = pmem::obj::pool_by_vptr(this);
            transaction::run(pop, [&] {
                auto newNode = make_persistent<SkipNode>();

                newNode->forward = make_persistent<std::array<persistent_ptr<SkipNode>,M>>();
                newNode->forward.get()->at(0) = nextNode;
                newNode->maxKey = this->minKey;
                newNode->minKey = this->maxKey;
                this->forward.get()->at(0) = newNode;

                int leftPos = 0;

                auto avg = getAvg();

                for(int i=0; i<N; i++) {
                    if(bitset.get_ro().test(i)) {
                        if(key.get_ro()[i] >= avg) {
                            newNode->key.get_rw()[leftPos] = key.get_ro()[i];
                            newNode->value.get_rw()[leftPos] = value.get_ro()[i];
                            newNode->bitset.get_rw().set(leftPos);
                            bitset.get_rw().reset(i);
                            leftPos++;
                            if(key.get_ro()[i] < newNode->minKey)
                                newNode->minKey = key.get_ro()[i];
                            if(key.get_ro()[i] > newNode->maxKey)
                                newNode->maxKey = key.get_ro()[i];
                        }
                    }
                }
                refreshBounds();
            });
        }

        void refreshBounds() {
            KeyType tmpMax = -1;
            KeyType tmpMin = -1;

            for(int i=0; i<N; i++) {
                if(bitset.get_ro().test(i)) {
                    if(tmpMax == -1 || key.get_ro()[i] > tmpMax)
                        tmpMax = key.get_ro()[i];
                    if(tmpMin == -1 || key.get_ro()[i] < tmpMin)
                        tmpMin = key.get_ro()[i];
                }
            }


            maxKey.get_rw() = tmpMax;
            minKey.get_rw() = tmpMin;
        }

        void insertInNode(persistent_ptr<SkipNode> prevNode, KeyType k, ValueType v, bool& splitInfo, int level) {
            if(isFull()) {
                splitNode(prevNode, forward.get()->at(0), level);
                //prevNode->forward[0]->key[3] = k;
                //prevNode->forward[0]->bitset.set(3);
                splitInfo = true;
                return;
            }
            int freeSlot = 0;
            while(bitset.get_ro().test(freeSlot)) {
                freeSlot++;
            }

            key.get_rw().at(freeSlot) = k;
            value.get_rw().at(freeSlot)= v;
            bitset.get_rw().set(freeSlot);

            if(this->minKey == -1 || k < this->minKey) {
                this->minKey = k;
            } else if(this->maxKey == -1 || k > this->maxKey) {
                this->maxKey = k;
            }
        }

        void printNode() {
            std::cout << "{"<<minKey << "|" << maxKey <<"}[";
            for(int i=0; i<N; i++) {
                if(bitset.get_ro().test(i))
                    std::cout << key.get_ro()[i] << ",";
            }
            std::cout << "]->";
        }
    };

    persistent_ptr<SkipNode> head;
    persistent_ptr<SkipNode> tail;
    p<int> level;
    bool nl;
    p<size_t> nodeCount;
    static const int MAX_LEVEL  = M;
    Random rnd;

    void createNode(int level, persistent_ptr<SkipNode> &node) {
        auto pop = pmem::obj::pool_by_vptr(this);
        transaction::run(pop, [&] {
            node = make_persistent<SkipNode>();
            node->forward = make_persistent<std::array<persistent_ptr<SkipNode>,M>>();
            node->nodeLevel = level;
            node->maxKey = -1;
            node->minKey = -1;
        });
    }

    void createList(KeyType tailKey) {
        auto pop = pmem::obj::pool_by_vptr(this);
        transaction::run(pop, [&] {

            createNode(0, tail);
            tail->maxKey = -tailKey;
            tail->minKey = tailKey;
            this->level = 0;

            createNode(MAX_LEVEL, head);
            for (int i = 0; i < MAX_LEVEL; ++i) {
                head->forward.get()->at(i) = tail.get();
            }
            nodeCount = 0;
        });
    }

    int getRandomLevel() {
        int level = static_cast<int>(rnd.Uniform(MAX_LEVEL));
        if (level == 0) {
            level = 1;
        }
        return level;
    }

public:

    woPSkiplist(KeyType tailKey) : rnd(0x12378) {
        createList(tailKey);
        nl = false;
    }

    bool insert(KeyType key, ValueType value) {
        reInsert:
        auto update = new SkipNode[MAX_LEVEL]();

        auto node = head;
        auto prev = head;
        auto last = head;

        for(int i = level; i>= 0; --i) {
            while((node->maxKey <= key)) {
                if(node->forward.get()->at(i) == nullptr)
                    break;
                last = prev;
                prev = node;
                node = node->forward.get()->at(i);
            }
            if(node->maxKey > key) {
                node = prev;
                prev = last;
            }
            update[i] = *prev;
        }

        auto nodeLevel = getRandomLevel();

        bool splitInfo = false;
        if(node->forward.get()->at(0)) {
            prev = node;
            node = node->forward.get()->at(0);
        }

        node->insertInNode(prev, key, value, *&splitInfo, level);
        if(splitInfo) {
            nl = true;
            nodeCount.get_rw()++;
            goto reInsert;
        }

        if(nl) {
            if(nodeLevel > level.get_ro()) {
                nodeLevel = ++level.get_rw();
                update[nodeLevel] = *head;
            }
            auto n = &update[nodeLevel];
            for(int i = nodeLevel; i >= 1; --i) {
                update[i].forward.get()->at(i) = node;
                prev->forward[i] = update[i].forward[i];
            }
            nl = false;
        }
        return true;
    }

    persistent_ptr<SkipNode> searchNode(const KeyType key) {
        auto pred = head;
        for(int i= level; i>=0; i--) {
            auto item = pred->forward.get()->at(i);
            while(item != nullptr) {
                if( key > item->maxKey) {
                    pred = item;
                    item = item->forward.get()->at(i);
                } else if(key < item->minKey) {
                    break;
                } else {
                    return item;
                }
            }
        }
    }

    ValueType* search(const KeyType key) {
        auto node = searchNode(key);
        if(node) {
            return node->searchKey(key);
        } else {
            return nullptr;
        }
    }

    void printElementNode() {
        std::cout << "\nLevel: " << level << std::endl;
        auto tmp = head;

        for(int i = level; i>=0; i--) {
            while(tmp != nullptr && tmp->forward) {
                tmp->printNode();
                tmp = tmp->forward.get()->at(i);
            }
            tmp = head;
            std::cout << std::endl;
        }
    }

    void printLastLevel() {
        auto tmp = head;

        while(tmp != nullptr && tmp->forward) {
            tmp->printNode();
            tmp = tmp->forward[0];
        }
    }
};

#endif
