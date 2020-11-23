/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#include "global.h"
#if CC_ALG == BOCC
#include "helper.h"
#include "txn.h"
#include "bocc.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_occ.h"


b_set_ent::b_set_ent() {
    set_size = 0;
    txn = NULL;
    rows = NULL;
    next = NULL;
}

void Bocc::init() {
    sem_init(&_semaphore, 0, 1);
    occ_cs::init();
    tnc = 0;
    his_len = 0;
    lock_all = false;
}

RC Bocc::validate(TxnManager * txn) {
    RC rc;
    uint64_t starttime = get_sys_clock();
    rc = central_validate(txn);
    INC_STATS(txn->get_thd_id(), occ_validate_time, get_sys_clock() - starttime);
    return rc;
}

void Bocc::finish(RC rc, TxnManager * txn) {
    central_finish(rc, txn);
}

RC Bocc::central_validate(TxnManager * txn) {
    RC rc;
    uint64_t starttime = get_sys_clock();
    uint64_t total_starttime = starttime;
    uint64_t start_tn = txn->get_start_timestamp();
    uint64_t finish_tn;

    bool valid = true;
    // OptCC is centralized. No need to do per partition malloc.
    b_set_ent * wset;
    b_set_ent * rset;
    get_rw_set(txn, rset, wset);

    b_set_ent * his;

    int stop __attribute__((unused));

    DEBUG("Start Validation %ld: start_ts %lu his_len %ld\n",txn->get_txn_id(),start_tn, his_len);
    sem_wait(&_semaphore);
    INC_STATS(txn->get_thd_id(),occ_cs_wait_time,get_sys_clock() - starttime);
    starttime = get_sys_clock();
    assert(!g_ts_batch_alloc);

    finish_tn = glob_manager.get_ts(txn->get_thd_id());

    his = history;

    INC_STATS(txn->get_thd_id(),occ_cs_time,get_sys_clock() - starttime);

    starttime = get_sys_clock();
    uint64_t checked = 0;
    uint64_t hist_checked = 0;
    stop = 0;
    if (finish_tn > start_tn) {
        while (his && his->tn > finish_tn) {
            his = his->next;
        }
        while (his && his->tn > start_tn) {
            ++hist_checked;
            ++checked;
            valid = test_valid(his, rset);
#if WORKLOAD == TPCC
            if (valid) {
                valid = test_valid(his, wset);
            }
#endif
            if (!valid) {
                INC_STATS(txn->get_thd_id(),occ_hist_validate_fail_time,get_sys_clock() - starttime);
                goto final;
            }
            his = his->next;
        }
    }

    INC_STATS(txn->get_thd_id(),occ_hist_validate_time,get_sys_clock() - starttime);

    starttime = get_sys_clock();
final:
    mem_allocator.free(rset->rows, sizeof(row_t *) * rset->set_size);
    mem_allocator.free(rset, sizeof(b_set_ent));

    if (valid) {
        rc = RCOK;
        INC_STATS(txn->get_thd_id(),occ_check_cnt,checked);
    } else {
        INC_STATS(txn->get_thd_id(),occ_abort_check_cnt,checked);
        rc = Abort;
        // Optimization: If this is aborting, remove from active set now
    }
    sem_post(&_semaphore);
    DEBUG("End Validation %ld: hist# %ld\n",txn->get_txn_id(),hist_checked);
    INC_STATS(txn->get_thd_id(),occ_validate_time,get_sys_clock() - total_starttime);
    return rc;
}

void Bocc::central_finish(RC rc, TxnManager * txn) {
    b_set_ent * wset;
    b_set_ent * rset;
    get_rw_set(txn, rset, wset);
    bool readonly = (wset->set_size == 0);
    if (!readonly) {
        sem_wait(&_semaphore);
        // only update active & tnc for non-readonly transactions
        uint64_t starttime = get_sys_clock();
        if (rc == RCOK) {
            // remove the assert for performance
            wset->tn = glob_manager.get_ts(txn->get_thd_id());
            STACK_PUSH(history, wset);
            DEBUG("occ insert history");
            his_len ++;
        }
        INC_STATS(txn->get_thd_id(),occ_finish_time,get_sys_clock() - starttime);
        sem_post(&_semaphore);
    }
}

RC Bocc::get_rw_set(TxnManager * txn, b_set_ent * &rset, b_set_ent *& wset) {
    wset = (b_set_ent*) mem_allocator.alloc(sizeof(b_set_ent));
    rset = (b_set_ent*) mem_allocator.alloc(sizeof(b_set_ent));
    wset->set_size = txn->get_write_set_size();
    rset->set_size = txn->get_read_set_size();
    wset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * wset->set_size);
    rset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * rset->set_size);
    wset->txn = txn;
    rset->txn = txn;

    UInt32 n = 0, m = 0;
    for (uint64_t i = 0; i < wset->set_size + rset->set_size; i++) {
        if (txn->get_access_type(i) == WR) {
            wset->rows[n ++] = txn->get_access_original_row(i);
        } else {
            rset->rows[m ++] = txn->get_access_original_row(i);
        }
    }

    assert(n == wset->set_size);
    assert(m == rset->set_size);
    return RCOK;
}

bool Bocc::test_valid(b_set_ent * set1, b_set_ent * set2) {
    for (UInt32 i = 0; i < set1->set_size; i++) {
        for (UInt32 j = 0; j < set2->set_size; j++) {
            if (set1->rows[i] == set2->rows[j]) {
                return false;
            }
        }
    }
    return true;
}

#endif
