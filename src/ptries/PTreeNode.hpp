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

#ifndef PTREENODE_HPP
#define PTREENODE_HPP

#include "utils/PatriciaUtils.hpp"

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <unistd.h>

#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/utils.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/make_persistent_array.hpp>

namespace pobj = pmem::obj;

template<class T>
class alignas(64) PTreeNode
{
    template<class O>
    friend class PatriciaTree;
    template<class O>
    using pptr = pobj::persistent_ptr<O>;

public:
    PTreeNode(const u_int8_t* _key = nullptr, const int& _keylen = 0);
    ~PTreeNode();

private:
    void setKey(const u_int8_t *key, const int &len);
    void setChild(PTreeNode *pnode);
    void unsetChild(PTreeNode *pnode);

    void deleteAllNode();

    int getDataCount() const;
    void print(int done = 0) const;
    int subNodes() const;


    // returns the number of bytes neccessary to store the bits
    static inline int OCTET_SPACE(int bits)
    {
        return ((bits - 1) / 8) + 1;
    }

    static inline u_int8_t CHECK_BIT(u_int8_t *K, int L)
    {
        return (K[L / 8] >> (7 - L % 8)) & 1;
    }

private:
    pptr<uint8_t[]> key{nullptr};
    pobj::p<int> keylen{0};

    pobj::p<T> data;

    pobj::p<bool> alloc{false};

    pptr<PTreeNode> parent{nullptr};
    pptr<PTreeNode> link[2]{nullptr, nullptr};
};

template<class T>
PTreeNode<T>::PTreeNode(const u_int8_t* _key, const int& _keylen)
{
    auto pop = pobj::pool_by_vptr(this);

    pobj::transaction::run(pop, [&] {
        if (_key)
        {
            setKey(_key, _keylen);
        }
    });
}

template<class T>
PTreeNode<T>::~PTreeNode()
{
    auto pop = pobj::pool_by_vptr(this);
    pobj::transaction::run(pop, [&] {
        if (data && alloc)
        {
            if (key)
            {
                pobj::delete_persistent<uint8_t[]>(key, OCTET_SPACE(keylen));
            }
        }
    });
}

template<class T>
void PTreeNode<T>::print(int done) const
{
    int c = 0;
    c += link[0] ? 1 : 0;
    c += link[1] ? 1 : 0;
    std::stringstream ss;
    ss << std::string(done, '-');


    if (key)
    {
        ss << toBinaryString(key.get(), keylen).substr(done);
    }
    else
    {
        ss << "<empty>";
    }
    if (data)
    {
        ss << "\t(" << toString(key.get(), OCTET_SPACE(keylen)) << ")";
        ss << "\t\tData:\t";
        ss << toHexString(data);
    }

    ss << "\t\t\t\t" << c << " children";


    std::cout << ss.str() << std::endl;
    if (link[0])
    {
        link[0]->print(keylen);
    }
    if (link[1])
    {
        link[1]->print(keylen);
    }
}

template<class T>
void PTreeNode<T>::setKey(const u_int8_t* _key, const int &_keylen)
{
    auto pop = pobj::pool_by_vptr(this);
    pobj::transaction::run(pop, [&] {
        if (key)
        {
            pobj::delete_persistent<uint8_t[]>(key, OCTET_SPACE(_keylen));
        }

        if (0 < (keylen = _keylen))
        {
            //key = (u_int8_t *) calloc(OCTET_SPACE(_keylen) + 1, sizeof(u_int8_t));
            //pop.memcpy_persist(&key, _key, OCTET_SPACE(_keylen));
            key = pobj::make_persistent<uint8_t[]>(OCTET_SPACE(_keylen));        // #TODO: This just copies the first byte! consider keylen as well
            for (int i = 0; i < keylen; ++i)
            {
                key[i] = _key[i];
            }
        }
        else
        {
            key = nullptr;
        }
    });
}

template<class T>
void PTreeNode<T>::setChild(PTreeNode *child)
{
    assert (child);

    auto pop = pobj::pool_by_vptr(this);
    pobj::transaction::run(pop, [&] {

        link[CHECK_BIT(child->key.get(), keylen)] = child;
        child->parent = this;
    });
}

template<class T>
void PTreeNode<T>::unsetChild(PTreeNode *child)
{
    assert (child);
    link[CHECK_BIT(child->key.get(), keylen)] = nullptr;
    child->parent = nullptr;
}

template<class T>
void PTreeNode<T>::deleteAllNode()
{
    auto pop = pobj::pool_by_vptr(this);

    pobj::transaction::run(pop, [&] {
        //pobj::delete_persistent<uint8_t>(key);

        if (link[0])
        {
            link[0]->deleteAllNode();
            assert (link[0]->link[0] == nullptr &&
                    link[0]->link[1] == nullptr);
            pobj::delete_persistent<PTreeNode>(link[0]);
            link[0] = nullptr;
        }
        if (link[1])
        {
            link[1]->deleteAllNode();
            assert (link[1]->link[0] == nullptr &&
                    link[1]->link[1] == nullptr);
            pobj::delete_persistent<PTreeNode>(link[1]);
            link[1] = nullptr;
        }
    });
}

template<class T>
int PTreeNode<T>::getDataCount() const
{
    // return dataCount; #TODO
    return 0;
}

template<class T>
int PTreeNode<T>::subNodes() const
{
    int count = 0;

    if (link[0])
    {
        count += link[0]->subNodes();
    }
    if (link[1])
    {
        count += link[1]->subNodes();
    }
    return count + 1;
}

#endif
