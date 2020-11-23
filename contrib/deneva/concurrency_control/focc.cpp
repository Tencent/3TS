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
#if CC_ALG == FOCC
#include "helper.h"
#include "txn.h"
#include "focc.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_occ.h"


f_set_ent::f_set_ent() {
    set_size = 0;
    txn = NULL;
    rows = NULL;
    next = NULL;
}

void Focc::init() {
    sem_init(&_semaphore, 0, 1);
    occ_cs::init();
    tnc = 0;
    active_len = 0;
    active = NULL;
    lock_all = false;
}

RC Focc::validate(TxnManager * txn) {
    RC rc;
    uint64_t starttime = get_sys_clock();
    rc = central_validate(txn);
    INC_STATS(txn->get_thd_id(),occ_validate_time,get_sys_clock() - starttime);
    return rc;
}

void Focc::finish(RC rc, TxnManager * txn) {
    central_finish(rc,txn);
}

void Focc::active_storage(access_t type, TxnManager * txn, Access * access) {
    f_set_ent * act = active;
    f_set_ent * wset;
    f_set_ent * rset;
    get_rw_set(txn, rset, wset);
    if (rset->set_size == 0) {
        return;
    }
    sem_wait(&_semaphore);
    while (act != NULL && act->txn != txn) {
        act = act->next;
    }
    if (act == NULL) {
        active_len ++;
        STACK_PUSH(active, rset);
    } else {
        act = rset;
    }
    sem_post(&_semaphore);
    DEBUG("FOCC active_storage %ld: active size %lu\n",txn->get_txn_id(),active_len);
}

RC Focc::central_validate(TxnManager * txn) {
    RC rc;
    uint64_t starttime = get_sys_clock();
    uint64_t total_starttime = starttime;
    uint64_t start_tn = txn->get_start_timestamp();

    bool valid = true;
    // OptCC is centralized. No need to do per partition malloc.
    f_set_ent * wset;
    f_set_ent * rset;
    get_rw_set(txn, rset, wset);
    bool readonly = (wset->set_size == 0);
    f_set_ent * ent;
    uint64_t checked = 0;
    uint64_t active_checked = 0;
    int stop __attribute__((unused));

    DEBUG("Start Validation %ld: start_ts %lu, active size %lu\n",txn->get_txn_id(),start_tn,active_len);
    sem_wait(&_semaphore);
    INC_STATS(txn->get_thd_id(),occ_cs_wait_time,get_sys_clock() - starttime);
    starttime = get_sys_clock();

    ent = active;
    // In order to prevent cross between other read sets and the current transaction write set during 
    // verification, the write set is first locked.
    if (!readonly) {
        for (uint64_t i = 0; i < wset->set_size; i++) {
            row_t * row = wset->rows[i];
            if (!row->manager->try_lock(txn->get_txn_id())) {
                rc = Abort;
            }
        }
        if (rc == Abort) {
            for (uint64_t i = 0; i < wset->set_size; i++) {
                row_t * row = wset->rows[i];
                row->manager->release_lock(txn->get_txn_id());
            }
        }
    }
    stop = 1;
    if (!readonly) {
        for (ent = active; ent != NULL; ent = ent->next) {
            f_set_ent * ract = ent;
            ++checked;
            ++active_checked;
            valid = test_valid(ract, wset);
            if (valid) {
                ++checked;
                ++active_checked;
            }
            if (!valid) {
                INC_STATS(txn->get_thd_id(),occ_act_validate_fail_time,get_sys_clock() - starttime);
                goto final;
            }
        }
    }
    INC_STATS(txn->get_thd_id(),occ_act_validate_time,get_sys_clock() - starttime);
    starttime = get_sys_clock();
final:
    mem_allocator.free(rset->rows, sizeof(row_t *) * rset->set_size);
    mem_allocator.free(rset, sizeof(f_set_ent));

    if (valid) {
        rc = RCOK;
        INC_STATS(txn->get_thd_id(),occ_check_cnt,checked);
    } else {
        INC_STATS(txn->get_thd_id(),occ_abort_check_cnt,checked);
        rc = Abort;
        // Optimization: If this is aborting, remove from active set now
        f_set_ent * act = active;
        f_set_ent * prev = NULL;
        while (act != NULL && act->txn != txn) {
            prev = act;
            act = act->next;
        }
        if (act != NULL && act->txn == txn) {
            if (prev != NULL) {
                prev->next = act->next;
            } else {
                active = act->next;
            }
            active_len --;
        }
    }
    sem_post(&_semaphore);
    DEBUG("End Validation %ld: active# %ld\n",txn->get_txn_id(),active_checked);
    INC_STATS(txn->get_thd_id(),occ_validate_time,get_sys_clock() - total_starttime);
    return rc;
}

void Focc::central_finish(RC rc, TxnManager * txn) {
    f_set_ent * wset;
    f_set_ent * rset;
    get_rw_set(txn, rset, wset);

    // only update active & tnc for non-readonly transactions
    uint64_t starttime = get_sys_clock();
    f_set_ent * act = active;
    f_set_ent * prev = NULL;
    sem_wait(&_semaphore);
    while (act != NULL && act->txn != txn) {
        prev = act;
        act = act->next;
    }
    if (act == NULL) {
        sem_post(&_semaphore);
        return;
    }
    assert(act->txn == txn);
    if (prev != NULL) {
        prev->next = act->next;
    } else {
        active = act->next;
    }
    active_len --;

    for (uint64_t i = 0; i < wset->set_size; i++) {
        row_t * row = wset->rows[i];
        row->manager->release_lock(txn->get_txn_id());
    }
    
    sem_post(&_semaphore);
    INC_STATS(txn->get_thd_id(),occ_finish_time,get_sys_clock() - starttime);

}

RC Focc::get_rw_set(TxnManager * txn, f_set_ent * &rset, f_set_ent *& wset) {
    wset = (f_set_ent*) mem_allocator.alloc(sizeof(f_set_ent));
    rset = (f_set_ent*) mem_allocator.alloc(sizeof(f_set_ent));
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

bool Focc::test_valid(f_set_ent * set1, f_set_ent * set2) {
    for (UInt32 i = 0; i < set1->set_size; i++) {
        for (UInt32 j = 0; j < set2->set_size; j++) {
            if (set1->txn == set2->txn)
                continue;
            if (set1->rows[i] == set2->rows[j]) {
                return false;
            }
        }
    }
    return true;
}

#endif