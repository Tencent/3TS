/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#ifndef _WSI_H_
#define _WSI_H_

#include "row.h"
#include "semaphore.h"

// For simplicity, the txn hisotry for OCC is oganized as follows:
// 1. history is never deleted.
// 2. hisotry forms a single directional list. 
//        history head -> hist_1 -> hist_2 -> hist_3 -> ... -> hist_n
//    The head is always the latest and the tail the youngest. 
//       When history is traversed, always go from head -> tail order.

class TxnManager;

class wsi_set_ent{
public:
    wsi_set_ent();
    UInt64 tn;
    TxnManager * txn;
    UInt32 set_size;
    row_t ** rows; //[MAX_WRITE_SET];
    wsi_set_ent * next;
};

class wsi {
public:
    void init();
    RC validate(TxnManager * txn);
    void gene_finish_ts(TxnManager * txn);
    void finish(RC rc, TxnManager * txn);
    void lock();
    void unlock();
private:
    // parallel validation in the original OCC paper.
    RC central_validate(TxnManager * txn);
    RC get_rw_set(TxnManager * txni, wsi_set_ent * &rset, wsi_set_ent *& wset);
    void central_finish(RC rc, TxnManager * txn);
    pthread_mutex_t latch;
    sem_t _semaphore;
};

#endif
