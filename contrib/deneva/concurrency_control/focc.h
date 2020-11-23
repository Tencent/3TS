/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#ifndef _FOCC_H_
#define _FOCC_H_

#include "row.h"
#include "semaphore.h"
#include "occ_critical_section.h"


// For simplicity, the txn hisotry for OCC is oganized as follows:
// 1. history is never deleted.
// 2. hisotry forms a single directional list.
//        history head -> hist_1 -> hist_2 -> hist_3 -> ... -> hist_n
//    The head is always the latest and the tail the youngest.
//       When history is traversed, always go from head -> tail order.

class TxnManager;

class f_set_ent{
public:
    f_set_ent();
    UInt64 tn;
    TxnManager * txn;
    UInt32 set_size;
    row_t ** rows; //[MAX_WRITE_SET];
    f_set_ent * next;
};

class Focc : public occ_cs {
public:
    void init();
    RC validate(TxnManager * txn);
    void finish(RC rc, TxnManager * txn);
    void active_storage(access_t type, TxnManager * txn, Access * access);
    volatile bool lock_all;
    uint64_t lock_txn_id;
private:
    // serial validation in the original OCC paper.
    RC central_validate(TxnManager * txn);

    void central_finish(RC rc, TxnManager * txn);
    bool test_valid(f_set_ent * set1, f_set_ent * set2);
    RC get_rw_set(TxnManager * txni, f_set_ent * &rset, f_set_ent *& wset);

    // "history" stores write set of transactions with tn >= smallest running tn

    f_set_ent * active;
    uint64_t active_len;
    volatile uint64_t tnc; // transaction number counter
    pthread_mutex_t latch;
    sem_t     _semaphore;

    sem_t    _active_semaphore;
    // Global verification lock of FOCC
    LockEntry * owners;
    LockEntry * waiters_head;
    LockEntry * waiters_tail;
};

#endif
