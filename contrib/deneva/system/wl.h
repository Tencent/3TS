/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _WORKLOAD_H_
#define _WORKLOAD_H_

#include "global.h"

class row_t;
class table_t;
class IndexHash;
class index_btree;
class Catalog;
class lock_man;
class TxnManager;
class Thread;
class index_base;
class Timestamp;
class Mvcc;

class Workload
{
public:
    // tables indexed by table name
    map<string, table_t *> tables;
    map<string, INDEX *> indexes;

    void index_delete_all();

    // FOR TPCC
    // initialize the tables and indexes.
    virtual RC init();
    virtual RC init_schema(const char * schema_file);
    virtual RC init_table()=0;
    virtual RC get_txn_man(TxnManager *& txn_manager)=0;

    uint64_t done_cnt;
    uint64_t txn_cnt;
protected:
    void index_insert(string index_name, uint64_t key, row_t * row);
    void index_insert(INDEX * index, uint64_t key, row_t * row, int64_t part_id = -1);
    void index_insert_nonunique(INDEX * index, uint64_t key, row_t * row, int64_t part_id = -1);
};

#endif
