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

#ifndef _OCC_H_
#define _OCC_H_

#include "row.h"
#include "semaphore.h"

// For simplicity, the txn hisotry for OCC is oganized as follows:
// 1. history is never deleted.
// 2. hisotry forms a single directional list.
//        history head -> hist_1 -> hist_2 -> hist_3 -> ... -> hist_n
//    The head is always the latest and the tail the youngest.
//       When history is traversed, always go from head -> tail order.

class TxnManager;

enum OCCState {
    OCC_RUNNING = 0,
    OCC_VALIDATED,
    OCC_COMMITTED,
    OCC_ABORTED
};

class set_ent{
public:
    set_ent();
    UInt64 tn;
    TxnManager * txn;
    UInt32 set_size;
    row_t ** rows; //[MAX_WRITE_SET];
    set_ent * next;
};

class OptCC {
public:
    void init();
    RC validate(TxnManager * txn);
    RC find_bound(TxnManager * txn);
    void finish(RC rc, TxnManager * txn);
    volatile bool lock_all;
    uint64_t lock_txn_id;

private:
    // per row validation similar to Hekaton.
    RC per_row_validate(TxnManager * txn);

    // parallel validation in the original OCC paper.
    RC central_validate(TxnManager * txn);
    void per_row_finish(RC rc, TxnManager * txn);
    void central_finish(RC rc, TxnManager * txn);
    bool test_valid(set_ent * set1, set_ent * set2);
    RC get_rw_set(TxnManager * txni, set_ent * &rset, set_ent *& wset);

    // "history" stores write set of transactions with tn >= smallest running tn
    set_ent * history;
    set_ent * active;
    uint64_t his_len;
    uint64_t active_len;
    volatile uint64_t tnc; // transaction number counter
    pthread_mutex_t latch;
    sem_t     _semaphore;
};

struct OCCTimeTableEntry{
    uint64_t lower;
    uint64_t upper;
    uint64_t key;
    OCCState state;
    OCCTimeTableEntry * next;
    OCCTimeTableEntry * prev;
    void init(uint64_t key) {
        lower = 0;
        upper = UINT64_MAX;
        this->key = key;
        state = OCC_RUNNING;
        next = NULL;
        prev = NULL;
    }
};

struct OCCTimeTableNode {
    OCCTimeTableEntry * head;
    OCCTimeTableEntry * tail;
    pthread_mutex_t mtx;
    void init() {
        head = NULL;
        tail = NULL;
        pthread_mutex_init(&mtx,NULL);
    }
};

class OCCTimeTable {
public:
    void init();
    void init(uint64_t thd_id, uint64_t key);
    void release(uint64_t thd_id, uint64_t key);
    uint64_t get_lower(uint64_t thd_id, uint64_t key);
    uint64_t get_upper(uint64_t thd_id, uint64_t key);
    void set_lower(uint64_t thd_id, uint64_t key, uint64_t value);
    void set_upper(uint64_t thd_id, uint64_t key, uint64_t value);
    OCCState get_state(uint64_t thd_id, uint64_t key);
    void set_state(uint64_t thd_id, uint64_t key, OCCState value);
private:
    // hash table
    uint64_t hash(uint64_t key);
    uint64_t table_size;
    OCCTimeTableNode* table;
    OCCTimeTableEntry* find(uint64_t key);

    OCCTimeTableEntry * find_entry(uint64_t id);

    sem_t     _semaphore;
};

#endif
