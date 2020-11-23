/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#ifndef _SUNDIAL_H_
#define _SUNDIAL_H_

#include "row.h"
#include "semaphore.h"


class TxnManager;

class sundial_set_ent{
public:
    sundial_set_ent();
    UInt64 tn;
    TxnManager * txn;
    UInt32 set_size;
    row_t ** rows; //[MAX_WRITE_SET];
    Access** access;
    sundial_set_ent * next;
};

class Sundial {
public:
    void init();
    RC validate(TxnManager * txn);
    void cleanup(RC rc, TxnManager * txn);
    void set_txn_ready(RC rc, TxnManager * txn);
    bool is_txn_ready(TxnManager * txn);
    static bool     _pre_abort;
private:
    RC get_rw_set(TxnManager * txni, sundial_set_ent * &rset, sundial_set_ent *& wset);
    RC validate_coor(TxnManager * txn);
    RC validate_part(TxnManager * txn);
    RC validate_write_set(sundial_set_ent * wset, TxnManager * txn, uint64_t commit_ts);
    RC validate_read_set(sundial_set_ent * rset, TxnManager * txn, uint64_t commit_ts);
    RC lock_write_set(sundial_set_ent * wset, TxnManager * txn);
    void unlock_write_set(RC rc, sundial_set_ent * wset, TxnManager * txn);
    void compute_commit_ts(TxnManager * txn);
    // sem_t     _semaphore;
};

#endif
