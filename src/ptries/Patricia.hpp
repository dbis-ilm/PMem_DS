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

#ifndef PATRICIA_HPP
#define PATRICIA_HPP

#include "PTreeNode.hpp"

#include <cstdlib>
#include <cstring>
#include <cassert>

#include <sys/types.h>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

const static u_int8_t bitmask[] =
        {0x00, 0x80, 0xc0, 0xe0, 0xf0, 0xf8, 0xfc, 0xfe, 0xff};

static const int LINK_NULL = 0x00;
static const int LINK_0_OK = 0x01;
static const int LINK_1_OK = 0x02;
static const int LINK_BOTH_OK = 0x03;

template<class T>
class PTreeNode;

template<class T>
class PatriciaTree
{
    template<class O>
    using pptr = pobj::persistent_ptr<O>;
public:
    PatriciaTree();
    ~PatriciaTree();

    bool insert(const void* key, size_t len, const T& data);
    bool lookup(const void* key, size_t len, T* val = nullptr) const;
    bool remove(const void* key, size_t len);

    unsigned int getCount();
    int getSubnodes() const;
    void print();

private:
    void branchNode(PTreeNode<T>* a, PTreeNode<T>* b);
    void deleteNode(PTreeNode<T>* t);

    static inline int MIN(int N1, int N2)
    {
        return (N1 < N2) ? N1 : N2;
    }

    static inline u_int8_t CHECK_BIT(const u_int8_t* K, int L)
    {
        return (K[L / 8] >> (7 - L % 8)) & 1;
    }

    static inline bool MATCH_KEY(u_int8_t* K1, const u_int8_t* K2, int L)
    {
        bool first = (L >= 8 ? (memcmp(K1, K2, L / 8) == 0) : 1);
        bool sec = (L % 8 > 0) ?
                   (K1[L / 8] & bitmask[L % 8]) == (K2[L / 8] & bitmask[L % 8])
                               : 1;
        bool ret = first && sec;

        return ret;
    }

private:
    pobj::p<unsigned int> dataCount{0};
    pptr<PTreeNode<T>> root;
};

template<class T>
PatriciaTree<T>::PatriciaTree()
{
    auto pop = pobj::pool_by_vptr(this);
    pobj::transaction::run(pop, [&] {
        root = pobj::make_persistent<PTreeNode<T>>();
    });
}

template<class T>
PatriciaTree<T>::~PatriciaTree()
{
    root->deleteAllNode();
}

template<class T>
bool PatriciaTree<T>::insert(const void* _key, size_t _len, const T& data)
{
    auto pop = pobj::pool_by_vptr(this);
    if (!root)
    {
        pobj::transaction::run(pop, [&] {
            root = pobj::make_persistent<PTreeNode<T>>();
        });
    }

    auto argKey = static_cast<const u_int8_t* >(_key);
    int argKeylen = static_cast<int>(_len * 8);

    pptr<PTreeNode<T>> bp, mp;                 // branch point, matched point
    pptr<PTreeNode<T>> t = nullptr;            // target node
    assert (argKey && argKeylen > 0);

    for (bp = root, mp = nullptr;
         bp != nullptr && MATCH_KEY(bp->key.get(), argKey, bp->keylen);
         bp = bp->link[CHECK_BIT(argKey, bp->keylen)])
    {
        mp = bp;

        if (bp->keylen >= argKeylen)
        {
            break;
        }
    }

    if (bp == nullptr)
    {
        bp = mp;
    }
    bool new_k = true;
    pobj::transaction::run(pop, [&]() {
        if (bp->keylen == argKeylen && MATCH_KEY(bp->key.get(), argKey, bp->keylen))
        {
            t = bp;
            if (t->data)
            {
                new_k = false;
                dataCount = dataCount - 1;
            }
        }
        else
        {
            t = pobj::make_persistent<PTreeNode<T>>(argKey, argKeylen);
            branchNode(bp.get(), t.get());
        }
        t->data = data;
        dataCount = dataCount + 1;
    });
    return new_k;
}

template<class T>
unsigned int PatriciaTree<T>::getCount()
{
    return dataCount;
}

template<class T>
void PatriciaTree<T>::print()
{
    root->print();
}

template<class T>
void PatriciaTree<T>::branchNode(PTreeNode<T>* a, PTreeNode<T>* b)
{
    int len, min, i;
    pptr<PTreeNode<T>> parent, branch, child;

    assert (b != nullptr);

    // search different point between keys
    min = MIN(a->keylen, b->keylen);
    for (i = 0, len = 0; i < (min / 8) && a->key[i] == b->key[i]; i++)
    {
        len += 8;
    }
    for (; len < min; len++)
    {
        if (CHECK_BIT(a->key.get(), len) != CHECK_BIT(b->key.get(), len))
        {
            break;
        }
    }

    assert (len != a->keylen || len != b->keylen);

    // select branch node
    if (a->parent != nullptr || b->parent != nullptr)
    {
        parent = (a->parent != nullptr) ? a->parent : b->parent;
    }
    else
    {
        parent = nullptr;
    }

    // ptree = (a->tree != nullptr) ? a->tree : b->tree;

    if (len == a->keylen || len == b->keylen)
    {
        // one node is prefix of the other
        branch = (a->keylen < b->keylen) ? a : b;
        child = (a->keylen < b->keylen) ? b : a;
        branch->setChild(child.get());
    }
    else
    {
        branch = pobj::make_persistent<PTreeNode<T>>(a->key.get(), len);
        branch->setChild(a);
        branch->setChild(b);
    }

    if (parent)
    {
        parent->setChild(branch.get());
    }
}

template<class T>
bool PatriciaTree<T>::lookup(const void* in_key, size_t in_len, T* val) const
{
    // to lookup with complete match
    auto argKey = static_cast<const u_int8_t*>(in_key);
    int argKeylen = static_cast<int>(in_len * 8);

    pptr<PTreeNode<T>> n;
    assert (argKey && argKeylen > 0);

    if (argKeylen == 0)
    {
        return false;
    }

    for (n = root;
         n && argKeylen >= n->keylen;
         n = n->link[CHECK_BIT(argKey, n->keylen)])
    {
        if (n->keylen == argKeylen && MATCH_KEY(n->key.get(), argKey, n->keylen))
        {
            if (val)
            {
                *val = n->data;
            }
            return true;
        }

        if (n->keylen >= argKeylen)
        {
            break;
        }
    }

    return false;
}

template<class T>
bool PatriciaTree<T>::remove(const void* in_key, size_t in_len)
{
    auto argKey = static_cast<const u_int8_t*>(in_key);
    int argKeylen = static_cast<int>(in_len * 8);

    pptr<PTreeNode<T>> n, t = nullptr;
    assert (argKey && argKeylen > 0);

    for (n = root; n && argKeylen >= n->keylen;
         n = n->link[CHECK_BIT(argKey, n->keylen)])
    {
        if (n->keylen == argKeylen && MATCH_KEY(n->key.get(), argKey, n->keylen))
        {
            t = n;
            break;
        }

        if (n->keylen >= argKeylen)
        {
            break;
        }
    }

    if (!t || !t->parent)
    {
        return false;
    } // nothig remove or this node is root

    deleteNode(t.get());

    // data counter that root node have
    dataCount = dataCount - 1;
    assert (dataCount >= 0);

    return true;
}

template<class T>
void PatriciaTree<T>::deleteNode(PTreeNode<T>* t)
{
    auto pop = pobj::pool_by_vptr(this);
    int st = LINK_NULL;
    bool delNode = true;
    pptr<PTreeNode<T>> parent = t->parent;

    if (!t->parent)
    {
        return;
    } // this is root node

    st |= (t->link[0] ? LINK_0_OK : LINK_NULL);
    st |= (t->link[1] ? LINK_1_OK : LINK_NULL);

    pobj::transaction::run(pop, [&] {
        switch (st)
        {
            case LINK_0_OK:
                t->parent->setChild(t->link[0].get());
                break;
            case LINK_1_OK:
                t->parent->setChild(t->link[1].get());
                break;
            case LINK_NULL:
                t->parent->unsetChild(t);
                break;
            case LINK_BOTH_OK:
                t->alloc = true;
                delNode = false;
                break;
        }

        if (delNode)
        {
            pobj::delete_persistent<PTreeNode<T>>(t);
            if ((parent->link[0] == nullptr || parent->link[1] == nullptr))
            {
                deleteNode(parent.get());
            }
        }
    });
}

template<class T>
int PatriciaTree<T>::getSubnodes() const
{
    return root->subNodes();
}

#endif
