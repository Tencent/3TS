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
#if CC_ALG == SUNDIAL
#include "config.h"
#include "helper.h"
#include "txn.h"
#include "row.h"
#include "sundial.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_sundial.h"
bool Sundial::_pre_abort = PRE_ABORT;

RC Sundial::get_rw_set(TxnManager * txn, sundial_set_ent * &rset, sundial_set_ent *& wset) {
    wset = (sundial_set_ent*) mem_allocator.alloc(sizeof(sundial_set_ent));
    rset = (sundial_set_ent*) mem_allocator.alloc(sizeof(sundial_set_ent));
    wset->set_size = txn->get_write_set_size();
    rset->set_size = txn->get_read_set_size();
    wset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * wset->set_size);
    rset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * rset->set_size);
    wset->access = (Access **) mem_allocator.alloc(sizeof(Access *) * wset->set_size);
    rset->access = (Access **) mem_allocator.alloc(sizeof(Access *) * rset->set_size);
    wset->txn = txn;
    rset->txn = txn;

    UInt32 n = 0, m = 0;
    for (uint64_t i = 0; i < wset->set_size + rset->set_size; i++) {
        if (txn->get_access_type(i) == WR){
            wset->access[n] = txn->txn->accesses[i];
            wset->rows[n] = txn->get_access_original_row(i);
            n++;
        }
        else {
            rset->access[m] = txn->txn->accesses[i];
            rset->rows[m] = txn->get_access_original_row(i);
            m++;
        }
    }
    assert(n == wset->set_size);
    assert(m == rset->set_size);

    return RCOK;
}

void Sundial::init() {
    return ;
}

RC Sundial::validate(TxnManager * txn)
{
    if (txn->_is_sub_txn)
        return validate_part(txn);
    else
        return validate_coor(txn);
}

RC Sundial::validate_coor(TxnManager * txn)
{
    RC rc = RCOK;
    // split _access_set into read_set and write_set
    sundial_set_ent * wset;
    sundial_set_ent * rset;
    get_rw_set(txn, rset, wset);
    // 1. lock the local write set
    // 2. compute commit ts
    // 3. validate local txn

    // [Compute the commit timestamp]
    compute_commit_ts(txn);

#if OCC_WAW_LOCK
    if (ISOLATION_LEVEL == SERIALIZABLE)
        rc = validate_read_set(rset, txn, txn->_min_commit_ts);
#else
    if (rc == RCOK) {
        rc = lock_write_set(wset, txn);
        if (rc == Abort) {
        }
    }
    if (rc != Abort) {
        RC rc2 = RCOK;
        // if any rts has been updated, update _min_commit_ts.
        compute_commit_ts(txn);
        // at this point, _min_commit_ts is the final min_commit_ts for the prepare phase. .
        if (ISOLATION_LEVEL == SERIALIZABLE) {
            rc2 = validate_read_set(rset, txn, txn->_min_commit_ts);
            if (rc2 == Abort) {
                rc = rc2;
            }
        }
    }
#endif
    if (rc == Abort) {
        unlock_write_set(rc, wset, txn);
    }
    return rc;
}

RC Sundial::validate_part(TxnManager * txn)
{
    RC rc = RCOK;
    // split _access_set into read_set and write_set
    sundial_set_ent * wset;
    sundial_set_ent * rset;
    get_rw_set(txn, rset, wset);
    // 1. lock the local write set
    // 2. compute commit ts
    // 3. validate local txn

#if OCC_WAW_LOCK
    if (ISOLATION_LEVEL == SERIALIZABLE)
        rc = validate_read_set(rset, txn, txn->_min_commit_ts);
#else
    if (rc == RCOK) {
        rc = lock_write_set(wset, txn);
        if (rc == Abort) {
        }
    }
    if (rc != Abort) {
        if (validate_write_set(wset, txn, txn->_min_commit_ts) == Abort)
            rc = Abort;
    }
    if (rc != Abort) {
        if (validate_read_set(rset, txn, txn->_min_commit_ts) == Abort) {
            rc = Abort;
        }
    }
#endif
    if (rc == Abort) {
        unlock_write_set(rc, wset, txn);
        return rc;
    } else {
    }

    return rc;
}

void Sundial::compute_commit_ts(TxnManager * txn)
{
    uint64_t size = txn->get_write_set_size();
    size += txn->get_read_set_size();
    for (uint64_t i = 0; i < size; i++) {
        if (txn->get_access_type(i) == WR){
            txn->_min_commit_ts = txn->txn->accesses[i]->orig_rts + 1 >  txn->_min_commit_ts ? txn->txn->accesses[i]->orig_rts + 1 : txn->_min_commit_ts;
        }
        else {
            txn->_min_commit_ts = txn->txn->accesses[i]->orig_wts >  txn->_min_commit_ts ? txn->txn->accesses[i]->orig_wts : txn->_min_commit_ts;
        }
    }


}

RC
Sundial::validate_read_set(sundial_set_ent * rset, TxnManager * txn, uint64_t commit_ts)
{

    // validate the read set.
    for (UInt32 i = 0; i < rset->set_size; i++) {
        if (rset->access[i]->orig_rts >= commit_ts) {
            continue;
        }
        row_t * cur_rrow = rset->rows[i];
        if (!cur_rrow->manager->try_renew(rset->access[i]->orig_wts, commit_ts, rset->access[i]->orig_rts))
        {
            return Abort;
        }
    }

    return RCOK;
}

RC
Sundial::lock_write_set(sundial_set_ent * wset, TxnManager * txn)
{
    assert(!OCC_WAW_LOCK);
    RC rc = RCOK;
    txn->_num_lock_waits = 0;

    for (UInt32 i = 0; i < wset->set_size; i++) {
        row_t * cur_wrow = wset->rows[i];
        rc = cur_wrow->manager->try_lock(txn);
        if (rc == WAIT || rc == RCOK)
            wset->access[i]->locked = true;
        if (rc == WAIT)
            ATOM_ADD_FETCH(txn->_num_lock_waits, 1);
        wset->access[i]->orig_rts = cur_wrow->manager->get_rts();
        if (wset->access[i]->orig_wts != cur_wrow->manager->get_wts()) {
            rc = Abort;
        }
        if (rc == Abort)
            return Abort;
    }
    if (rc == Abort)
        return Abort;
    else if (txn->_num_lock_waits > 0)
        return WAIT;
    return rc;
}

void
Sundial::cleanup(RC rc, TxnManager * txn)
{
    sundial_set_ent * wset;
    sundial_set_ent * rset;
    get_rw_set(txn, rset, wset);
    unlock_write_set(rc, wset, txn);
}

void
Sundial::unlock_write_set(RC rc, sundial_set_ent * wset, TxnManager * txn)
{
    for (UInt32 i = 0; i < wset->set_size; i++) {
        if (wset->access[i]->locked) {
            row_t * cur_wrow = wset->rows[i];
            cur_wrow->manager->release(txn, rc);
            wset->access[i]->locked = false;
        }
    }
}

RC
Sundial::validate_write_set(sundial_set_ent * wset, TxnManager * txn, uint64_t commit_ts)
{
    for (UInt32 i = 0; i < wset->set_size; i++) {
        if (txn->_min_commit_ts <= wset->access[i]->orig_rts){
            return Abort;
        }
    }
    return RCOK;
}

bool
Sundial::is_txn_ready(TxnManager * txn)
{
    return txn->_num_lock_waits == 0;
}

void
Sundial::set_txn_ready(RC rc, TxnManager * txn)
{
    // this function can be called by multiple threads concurrently
    if (rc == RCOK)
        ATOM_SUB_FETCH(txn->_num_lock_waits, 1);
    else {
        txn->_signal_abort = true;
    }
}

#endif