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

#ifndef _MAAT_H_
#define _MAAT_H_

#include "row.h"
#include "semaphore.h"

class TxnManager;

enum MAATState {
    MAAT_RUNNING = 0,
    MAAT_VALIDATED,
    MAAT_COMMITTED,
    MAAT_ABORTED
};

class Maat {
public:
    void init();
    RC validate(TxnManager * txn);
    RC find_bound(TxnManager * txn);
private:
    sem_t     _semaphore;
};

struct TimeTableEntry{
    uint64_t lower;
    uint64_t upper;
    uint64_t key;
    MAATState state;
    TimeTableEntry * next;
    TimeTableEntry * prev;
    void init(uint64_t key) {
        lower = 0;
        upper = UINT64_MAX;
        this->key = key;
        state = MAAT_RUNNING;
        next = NULL;
        prev = NULL;
    }
};

struct TimeTableNode {
    TimeTableEntry * head;
    TimeTableEntry * tail;
    pthread_mutex_t mtx;
    void init() {
        head = NULL;
        tail = NULL;
        pthread_mutex_init(&mtx,NULL);
    }
};

class TimeTable {
public:
    void init();
    void init(uint64_t thd_id, uint64_t key);
    void release(uint64_t thd_id, uint64_t key);
    uint64_t get_lower(uint64_t thd_id, uint64_t key);
    uint64_t get_upper(uint64_t thd_id, uint64_t key);
    void set_lower(uint64_t thd_id, uint64_t key, uint64_t value);
    void set_upper(uint64_t thd_id, uint64_t key, uint64_t value);
    MAATState get_state(uint64_t thd_id, uint64_t key);
    void set_state(uint64_t thd_id, uint64_t key, MAATState value);
private:
    // hash table
    uint64_t hash(uint64_t key);
    uint64_t table_size;
    TimeTableNode* table;
    TimeTableEntry* find(uint64_t key);

    TimeTableEntry * find_entry(uint64_t id);

    sem_t     _semaphore;
};

#endif
