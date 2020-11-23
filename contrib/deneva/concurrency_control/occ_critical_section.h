/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */
/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#ifndef _OCC_CS_H_
#define _OCC_CS_H_

#include "row.h"
#include "semaphore.h"


// For simplicity, the txn hisotry for OCC is oganized as follows:
// 1. history is never deleted.
// 2. hisotry forms a single directional list.
//        history head -> hist_1 -> hist_2 -> hist_3 -> ... -> hist_n
//    The head is always the latest and the tail the youngest.
//       When history is traversed, always go from head -> tail order.

class TxnManager;

struct LockEntry {
    TxnManager * txn;
    LockEntry * next;
    LockEntry * prev;
};

class occ_cs {
public:
    void init();
    RC start_critical_section(TxnManager * txn);
    RC end_critical_section(TxnManager * txn);
    bool check_critical_section();
    void set_remote_critical_section(TxnManager * txn);
    void unset_remote_critical_section(TxnManager * txn);
private:
    sem_t     cs_semaphore;
    // Global verification lock of OCC
    LockEntry * owners;
    LockEntry * waiters_head;
    LockEntry * waiters_tail;
};

#endif
