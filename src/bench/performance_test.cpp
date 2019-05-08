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

//
// Created by Steffen on 27.11.17.
//

#include "pfabric.hpp"
#include <stdlib.h>
#include <time.h>
#include <math.h>

using namespace pfabric::nvm;
using nvml::obj::make_persistent;
using nvml::obj::p;
using nvml::obj::persistent_ptr;
using nvml::obj::pool;
using nvml::obj::transaction;

using MyTuple = pfabric::Tuple<int, int, string, double>;
using PTableType = PTable<MyTuple, int>;

int main(){

        const int TEST_SIZE=100;
        srand(time(NULL));

        std::chrono::high_resolution_clock::time_point start, end;
        std::vector<typename std::chrono::duration<int64_t, micro>::rep> insert_measures, lookup_measures, delete_measures;

        struct root {
            persistent_ptr<PTableType> pTable;
        };

        pool<root> pop;

        const std::string path = "/mnt/mem/tests/testdb.db";
        bool recover=false;
        std::remove(path.c_str());

        if (access(path.c_str(), F_OK) != 0) {
            pop = pool<root>::create(path, LAYOUT, 16 * 1024 * 1024);
            transaction::exec_tx(pop, [&] {
                using namespace pfabric;
                auto tInfo = TableInfo("MyTable", {
                        ColumnInfo("a", ColumnInfo::Int_Type),
                        ColumnInfo("b", ColumnInfo::Int_Type),
                        ColumnInfo("c", ColumnInfo::String_Type),
                        ColumnInfo("d", ColumnInfo::Double_Type)
                });
                pop.get_root()->pTable = make_persistent<PTableType>(tInfo, BDCCInfo::ColumnBitsMap({{0, 4},
                                                                                                     {3, 6}}));
            });
        } else {
            std::cerr << "WARNING: Table already exists" << std::endl;
            pop = pool<root>::open(path, LAYOUT);
            recover=true;
        }

        auto pTable = pop.get_root()->pTable;
        if(recover){pTable->recoverIndex();};
        //pTable->print(false);

        std::cout << "\nTESTSIZE=" << TEST_SIZE << std::endl;
        std::cout << "\nINDEXKEYS=" << INDEXKEYS << std::endl;

        //Generate array of keys to stay unique while choose random keys to insert
        std::array<unsigned int, TEST_SIZE> keys;
        for (auto i = 0u; i < TEST_SIZE; i++) { keys[i] = i + 1; }

        //Randomly choose a key out of the array, generate the tuple and insert it into the table
        auto curr_test_size = TEST_SIZE;

        for (unsigned int i = 0; i < TEST_SIZE; i++) {
            auto choosen_key = rand() % curr_test_size;
            auto key = keys[choosen_key];
            auto tup = MyTuple(key,
                               key * 100,
                               fmt::format("String #{0}", key),
                               key * 12.345);
            start = std::chrono::high_resolution_clock::now();
            pTable->insert(key, tup);
            end = std::chrono::high_resolution_clock::now();
            if(i==TEST_SIZE/4){std::cout<<"Fortschritt: 25%"<<std::endl;}
            if(i==TEST_SIZE/2){std::cout<<"Fortschritt: 50%"<<std::endl;}
            if(i==3*TEST_SIZE/4){std::cout<<"Fortschritt: 75%"<<std::endl;}
            auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            insert_measures.push_back(diff);

            curr_test_size--;
            keys[choosen_key] = keys[curr_test_size];

        }
        auto avg = std::accumulate(insert_measures.begin(), insert_measures.end(), 0) / insert_measures.size();
        auto minmax = std::minmax_element(std::begin(insert_measures), std::end(insert_measures));
        std::cout << "\nInsert Statistics in µs: "
                  << "\n\tAverage: \t" << avg
                  << "\n\tMin: \t" << *minmax.first
                  << "\n\tMax: \t" << *minmax.second << '\n';
#ifdef IDXPROFILE
        pTable->printIdxProfile();
#endif
        std::cout << "\n####################################\n";

        //Lookup measures
        for (unsigned int i = 0; i < TEST_SIZE; i++) {
            auto key = i + 1;
            start = std::chrono::high_resolution_clock::now();
            try {
                auto tuple = pTable->getByKey(key);
            } catch (pfabric::TableException e) {
                std::cout << "Didn't find " << key << std::endl;
            }


            end = std::chrono::high_resolution_clock::now();

            auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            lookup_measures.push_back(diff);
        }
        avg = std::accumulate(lookup_measures.begin(), lookup_measures.end(), 0) / lookup_measures.size();
        minmax = std::minmax_element(std::begin(lookup_measures), std::end(lookup_measures));
        std::cout << "\nLookup Statistics in µs: "
                  << "\n\tAverage: \t" << avg
                  << "\n\tMin: \t" << *minmax.first
                  << "\n\tMax: \t" << *minmax.second << '\n';
#ifdef IDXPROFILE
        pTable->printIdxProfile();
#endif
        std::cout << "\n####################################\n";

        //Delete measures
        for (auto i = 0u; i < TEST_SIZE; i++) { keys[i] = i + 1; }
        curr_test_size = TEST_SIZE;
        for (unsigned int i = 0; i < TEST_SIZE -1; i++) {
            auto choosen_key = rand() % curr_test_size;
            auto key = keys[choosen_key];

            start = std::chrono::high_resolution_clock::now();
            pTable->deleteByKey(key);
            end = std::chrono::high_resolution_clock::now();

            auto diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            delete_measures.push_back(diff);
            if(i==TEST_SIZE/4){std::cout<<"Fortschritt: 25%"<<std::endl;}
            if(i==TEST_SIZE/2){std::cout<<"Fortschritt: 50%"<<std::endl;}
            if(i==3*TEST_SIZE/4){std::cout<<"Fortschritt: 75%"<<std::endl;}
            curr_test_size--;
            keys[choosen_key] = keys[curr_test_size];

        }
        avg = std::accumulate(delete_measures.begin(), delete_measures.end(), 0) / delete_measures.size();
        minmax = std::minmax_element(std::begin(delete_measures), std::end(delete_measures));
        std::cout << "\nDelete Statistics in µs: "
                  << "\n\tAverage: \t" << avg
                  << "\n\tMin: \t" << *minmax.first
                  << "\n\tMax: \t" << *minmax.second << '\n';
#ifdef IDXPROFILE
        pTable->printIdxProfile();
#endif

        pop.close();

}

