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

#ifndef BASETRIE_HPP
#define BASETRIE_HPP

// std
#include <array>
#include <cmath>
#include <iostream>
#include <memory>
#include <unistd.h>

// pmdk
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

namespace pobj = pmem::obj;
template <class T>
class alignas(64) BaseTrieNode
{
    typedef pobj::persistent_ptr<BaseTrieNode<T>> pptr;

public:
    BaseTrieNode() = default;
    ~BaseTrieNode()
    {
        auto pop = pobj::pool_by_vptr(this);
        pmem::obj::transaction::run(pop, [&]()
        {
            for (const pptr& ptr : mChildren.get_rw())
            {
                if (ptr)
                {
                    pmem::obj::delete_persistent<BaseTrieNode<T>>(ptr);
                }
            }
        });
    }

    // true if new, false if overwrite
    bool insert(const void* key, const size_t& key_size, const T& val);
    bool lookup(const void* key, const size_t& key_size, T* val = nullptr);
    bool remove(const void* key, const size_t& key_size);
    int subNodes();
    void print(int depth = 0) const;
    void graph() const;

private:
    pobj::p<std::array<pptr, 256>> mChildren {};
    pobj::p<T> mValue;
    pobj::p<uint16_t> mSize { 0 };

    pobj::p<bool> mIsValue { false };
};


template<class T>
bool BaseTrieNode<T>::insert(const void* key, const size_t& key_size, const T &val)
{
    // initialization
    auto key_bytes = static_cast<const u_int8_t*>(key);
    auto pop = pobj::pool_by_vptr(this);

    // find target node
    pptr cur_node = this;
    for (size_t index = 0; index < key_size; ++index)
    {
        auto &child_ptr = cur_node->mChildren.get_rw()[key_bytes[index]];
        if (!child_ptr)
        {
            pobj::transaction::run(pop, [&] () {
                child_ptr = pobj::make_persistent<BaseTrieNode>();
                cur_node->mSize = cur_node->mSize + 1;
            });
        }
        cur_node = child_ptr;
    }

    // insert value
    bool old_key = cur_node->mIsValue;
    pobj::transaction::run(pop, [&] () {
        cur_node->mValue = val;
        cur_node->mIsValue = true;
    });
    return !old_key;
}

template<class T>
bool BaseTrieNode<T>::lookup(const void* key, const size_t& key_size, T* val)
{
    auto key_bytes = static_cast<const u_int8_t*>(key);

    pptr cur_node = this;
    for (uint8_t index = 0; index < key_size; ++index)
    {
        auto child_ptr = cur_node->mChildren.get_ro()[key_bytes[index]];
        if (!child_ptr)
        {
            return false;
        }
        cur_node = child_ptr;
    }

    if (cur_node->mIsValue && val)
    {
        *val = cur_node->mValue;
    }
    return cur_node->mIsValue;
}

template<class T>
bool BaseTrieNode<T>::remove(const void* key, const size_t& key_size)
{
    auto key_bytes = static_cast<const u_int8_t*>(key);
    auto pop = pobj::pool_by_vptr(this);

    bool removed;
    if (0 == key_size)      // recursion base case
    {
        removed = mIsValue;
        pobj::transaction::run(pop, [&]
        {
            mIsValue = false;
        });
        return removed;
    }
    auto& child_ptr = mChildren.get_rw()[*key_bytes];
    if (!child_ptr)
    {
        return false;
    }
    removed = child_ptr->remove(key_bytes + 1, key_size - 1);
    if (removed && child_ptr->mSize == 0)
    {
        pobj::transaction::run(pop, [&] {
            pobj::delete_persistent<BaseTrieNode<T>>(child_ptr);
            child_ptr = nullptr;
            mSize = mSize - 1;
        });
    }
    return removed;
}

template<class T>
int BaseTrieNode<T>::subNodes()
{
    int count = 0, c = 0;
    for (const auto& subnode : mChildren.get_ro())
    {
        if (c == mSize)
        {
            // we found all nodes
            break;
        }
        if (subnode)
        {
            count += subnode->subNodes();
            ++c;
        }
    }
    return count + 1;
}

template<class T>
void BaseTrieNode<T>::print(int depth) const
{
    int count = 0;
    for (int i = 0; i < mChildren.get_ro().size(); ++i)
    {
        if (auto c = mChildren.get_ro()[i])
        {
            std::cout << std::string(depth, '-') << static_cast<char>(i);
            if (c->mIsValue)
            {
                std::cout << "\t\t\tVALUE";
            }
            std::cout << std::endl;
            c->print(depth + 1);
        }
    }
}

template<class T>
void BaseTrieNode<T>::graph() const
{
    std::cout << pptr(this).raw().off << " [shape=box, label = \"" << pptr(this).raw().off << "\"];"     << std::endl;
    for (int i = 0; i < mChildren.get_ro().size(); ++i)
    {
        if (mChildren.get_ro()[i])
        {
            std::cout << pptr(this).raw().off << " -> " << mChildren.get_ro()[i].raw().off << " [label=\"  " << static_cast<char>(i) << "\"];" << std::endl;
            mChildren.get_ro()[i]->graph();
        }
    }
}
#endif
