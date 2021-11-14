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
#if CC_ALG == WSI
#include "helper.h"
#include "txn.h"
#include "wsi.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_wsi.h"


wsi_set_ent::wsi_set_ent() {
    set_size = 0;
    txn = NULL;
    rows = NULL;
    next = NULL;
}

void wsi::init() {
    sem_init(&_semaphore, 0, 1);
}

RC wsi::validate(TxnManager * txn) {
    RC rc;
    rc = central_validate(txn);
    return rc;
}

RC wsi::central_validate(TxnManager * txn) {
    RC rc;
    uint64_t start_tn = txn->get_start_timestamp();
    bool valid = true;

    wsi_set_ent * wset;
    wsi_set_ent * rset;
    get_rw_set(txn, rset, wset);
    bool readonly = (wset->set_size == 0);

    int stop __attribute__((unused));
    uint64_t checked = 0;
    sem_wait(&_semaphore);

    if (!readonly) {
        for (UInt32 i = 0; i < rset->set_size; i++) {
            checked++;
            if (rset->rows[i]->manager->get_last_commit() > start_tn) {
                rc = Abort;
            }
        }
    }
    // sem_post(&_semaphore);
    mem_allocator.free(rset->rows, sizeof(row_t *) * rset->set_size);
    mem_allocator.free(rset, sizeof(wsi_set_ent));

    if (valid) {
        rc = RCOK;
        // row_t * newr = (row_t *) mem_allocator.alloc(sizeof(row_t));
        // newr->init(this->get_table(), get_part_id());
        // newr->copy(access->data);
        // access->data = newr;
        txn->set_commit_timestamp(glob_manager.get_ts(txn->get_thd_id()));
        // for (UInt32 i = 0; i < rset->set_size; i++) {
        //     rset->rows[i]->manager->update_last_commit(txn->get_commit_timestamp());
        // }
    } else {
        rc = Abort;
    }
    sem_post(&_semaphore);
    DEBUG("End Validation %ld\n",txn->get_txn_id());
    return rc;
}

void wsi::finish(RC rc, TxnManager * txn) {
    central_finish(rc,txn);
}

void wsi::central_finish(RC rc, TxnManager * txn) {
    wsi_set_ent * wset;
    wsi_set_ent * rset;
    get_rw_set(txn, rset, wset);

    for (UInt32 i = 0; i < rset->set_size; i++) {
        rset->rows[i]->manager->update_last_commit(txn->get_commit_timestamp());
    }
}

void wsi::gene_finish_ts(TxnManager * txn) {
    // txn->set_commit_timestamp(glob_manager.get_ts(txn->get_thd_id()));
}

RC wsi::get_rw_set(TxnManager * txn, wsi_set_ent * &rset, wsi_set_ent *& wset) {
    wset = (wsi_set_ent*) mem_allocator.alloc(sizeof(wsi_set_ent));
    rset = (wsi_set_ent*) mem_allocator.alloc(sizeof(wsi_set_ent));
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
#endif