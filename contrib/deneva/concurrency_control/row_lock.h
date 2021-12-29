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

#ifndef ROW_LOCK_H
#define ROW_LOCK_H

struct LockEntry {
    lock_t type;
    ts_t   start_ts;
    TxnManager * txn;
    LockEntry * next;
    LockEntry * prev;
};

class Row_lock {
public:
    void init(row_t * row);
    // [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
    RC lock_get(lock_t type, TxnManager * txn);
    RC lock_get(lock_t type, TxnManager * txn, uint64_t* &txnids, int &txncnt);
    RC lock_release(TxnManager * txn);
    UInt32 get_owner_cnt();

private:
    pthread_mutex_t * latch;
    bool blatch;

    bool         conflict_lock(lock_t l1, lock_t l2);
    LockEntry * get_entry();
    void         return_entry(LockEntry * entry);
    row_t * _row;
    uint64_t hash(uint64_t id) {
        return id % owners_size;
    };
    lock_t lock_type;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;

    // owners is a hash table
    // waiters is a double linked list
    // [waiters] head is the oldest txn, tail is the youngest txn.
    //   So new txns are inserted into the tail.
    LockEntry ** owners;
    uint64_t owners_size;
    LockEntry * waiters_head;
    LockEntry * waiters_tail;
    uint64_t max_owner_ts;
    uint64_t own_starttime;
};

#endif
