/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#include "row_null.h"

#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "txn.h"

void Row_null::init(row_t* row) {
    _row = row;
    blatch = false;
    latch = (pthread_mutex_t*)mem_allocator.alloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(latch, NULL);
}

RC Row_null::access(TsType type, TxnManager* txn, row_t* row, uint64_t& version) {
    uint64_t starttime = get_sys_clock();
    RC rc = RCOK;
    // rc = read_and_write(type, txn, row, version);
    row = this->_row;
    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    return rc;
}

RC Row_null::abort(access_t type, TxnManager* txn) {
    return Abort;
}

RC Row_null::commit(access_t type, TxnManager* txn, row_t* data, uint64_t& version) {
    return RCOK;
}
