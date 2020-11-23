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

#ifndef _TXN_TABLE_H_
#define _TXN_TABLE_H_

#include "global.h"
#include "helper.h"

class TxnManager;
class BaseQuery;
class row_t;

struct txn_node {
    txn_node() {
        next = NULL;
        prev = NULL;
    }
    ~txn_node() {}
    TxnManager * txn_man;
    uint64_t return_id; // Client ID or Home partition ID
    uint64_t client_startts; // For sequencer
    uint64_t abort_penalty;
    txn_node * next;
    txn_node * prev;

};

typedef txn_node * txn_node_t;


struct pool_node {
public:
    txn_node_t head;
    txn_node_t tail;
    volatile bool modify;
    uint64_t cnt;
    uint64_t min_ts;

};
typedef pool_node * pool_node_t;

class TxnTable {
public:
    void init();
    TxnManager* get_transaction_manager(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id);
    void dump();
    void restart_txn(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id);
    void release_transaction_manager(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id);
    void update_min_ts(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id,uint64_t ts);
    uint64_t get_min_ts(uint64_t thd_id);

private:
    bool is_matching_txn_node(txn_node_t t_node, uint64_t txn_id, uint64_t batch_id);

    //  TxnMap pool;
    uint64_t pool_size;
    pool_node ** pool;

};

#endif
