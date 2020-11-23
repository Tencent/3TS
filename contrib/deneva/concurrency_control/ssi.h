/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */

#ifndef _SSI_H_
#define _SSI_H_

#include "row.h"
#include "semaphore.h"

class TxnManager;
enum SSIState { SSI_RUNNING=0,SSI_COMMITTED,SSI_ABORTED};
struct InOutTableEntry{
    std::set<uint64_t> inConflict;
    std::set<uint64_t> outConflict;
    SSIState state;
    uint64_t commit_ts;
    uint64_t key;

    InOutTableEntry * next;
    InOutTableEntry * prev;
    void init(uint64_t key) {
        this->key = key;
        state = SSI_RUNNING;
        commit_ts = 0;
        next = NULL;
        prev = NULL;
    }
};

struct InOutTableNode {
    InOutTableEntry * head;
    InOutTableEntry * tail;
    pthread_mutex_t mtx;
    void init() {
        head = NULL;
        tail = NULL;
        pthread_mutex_init(&mtx,NULL);
    }
};

class InOutTable {
public:
    void init();
    void init(uint64_t thd_id, uint64_t key);
    void release(uint64_t thd_id, uint64_t key);
    bool get_inConflict(uint64_t thd_id, uint64_t key);
    bool get_outConflict(uint64_t thd_id, uint64_t key);
    void set_inConflict(uint64_t thd_id, uint64_t key, uint64_t value);
    void down_inConflict(uint64_t thd_id, uint64_t key, uint64_t value);

    void set_outConflict(uint64_t thd_id, uint64_t key, uint64_t value);
    void down_outConflict(uint64_t thd_id, uint64_t key, uint64_t value);

    void clear_Conflict(uint64_t thd_id, uint64_t key);

    SSIState get_state(uint64_t thd_id, uint64_t key);
    void set_state(uint64_t thd_id, uint64_t key, SSIState value);
    uint64_t get_commit_ts(uint64_t thd_id, uint64_t key);
    void set_commit_ts(uint64_t thd_id, uint64_t key, uint64_t value);

    private:
    // hash table
    uint64_t hash(uint64_t key);
    uint64_t table_size;
    InOutTableNode* table;
    InOutTableEntry* find(uint64_t key);
    pthread_mutex_t mtx;
    InOutTableEntry * find_entry(uint64_t id);
    sem_t _semaphore;
};

class ssi_set_ent{
public:
    ssi_set_ent();
    UInt64 tn;
    TxnManager * txn;
    UInt32 set_size;
    row_t ** rows; //[MAX_WRITE_SET];
    ssi_set_ent * next;
};

class ssi {
public:
    void init();
    RC validate(TxnManager * txn);
    void gene_finish_ts(TxnManager * txn);
    RC get_rw_set(TxnManager * txn, ssi_set_ent * &rset, ssi_set_ent *& wset);
private:
    sem_t _semaphore;
};

#endif
