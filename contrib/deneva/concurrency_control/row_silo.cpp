/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: zhanhaozhao@ruc.edu.cn
 *
 */

#include "txn.h"
#include "row.h"
#include "row_silo.h"
#include "mem_alloc.h"
#include <jemalloc/jemalloc.h>

#if CC_ALG==SILO

void 
Row_silo::init(row_t * row) 
{
    _row = row;
#if ATOMIC_WORD
    _tid_word = 0;
#else 
    _latch = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init( _latch, NULL );
    _tid = 0;
#endif
}

RC
Row_silo::access(TxnManager * txn, TsType type, row_t * local_row) {

    uint64_t starttime = get_sys_clock();

    if (type == R_REQ) {
        DEBUG("READ %ld -- %ld, table name: %s \n",txn->get_txn_id(),_row->get_primary_key(),_row->get_table_name());
    } else if (type == P_REQ) {
        DEBUG("WRITE %ld -- %ld \n",txn->get_txn_id(),_row->get_primary_key());
    }

#if ATOMIC_WORD
    uint64_t v = 0;
    uint64_t v2 = 1;

    while (v2 != v) {
        v = _tid_word;
        while (v & LOCK_BIT) {
            PAUSE_SILO
            v = _tid_word;
        }
        local_row->copy(_row);
        COMPILER_BARRIER
        v2 = _tid_word;
    } 
    txn->last_tid = v & (~LOCK_BIT);
#else 
    if (!try_lock())
    {
        return Abort;
        // break;
    }
    DEBUG("silo %ld read lock row %ld \n", txn->get_txn_id(), _row->get_primary_key());
    local_row->copy(_row);
    txn->last_tid = _tid;
    release();
    DEBUG("silo %ld read release row %ld \n", txn->get_txn_id(), _row->get_primary_key());
#endif
    INC_STATS(txn->get_thd_id(), txn_useful_time, get_sys_clock()-starttime);
    return RCOK;
}

bool
Row_silo::validate(ts_t tid, bool in_write_set) {
#if ATOMIC_WORD
    uint64_t v = _tid_word;
    if (in_write_set)
        return tid == (v & (~LOCK_BIT));

    if (v & LOCK_BIT) 
        return false;
    else if (tid != (v & (~LOCK_BIT)))
        return false;
    else 
        return true;
#else
    if (in_write_set)    
        return tid == _tid;
    if (!try_lock())
        return false;

    DEBUG("silo %ld validate lock row %ld \n", tid, _row->get_primary_key());
    bool valid = (tid == _tid);
    release();
    DEBUG("silo %ld validate release row %ld \n", tid, _row->get_primary_key());
    return valid;
#endif
}

void
Row_silo::write(row_t * data, uint64_t tid) {
    uint64_t starttime = get_sys_clock();
    _row->copy(data);
#if ATOMIC_WORD
    uint64_t v = _tid_word;
    if (tid > (v & (~LOCK_BIT)) && (v & LOCK_BIT))
        _tid_word = (tid | LOCK_BIT); 
#else
    _tid = tid;
#endif
    INC_STATS(txn->get_thd_id(), txn_useful_time, get_sys_clock()-starttime);
}

void
Row_silo::lock() {
#if ATOMIC_WORD
    uint64_t v = _tid_word;
    while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&_tid_word, v, v | LOCK_BIT)) {
        PAUSE_SILO
        v = _tid_word;
    }
#else
    pthread_mutex_lock( _latch );
#endif
}

void
Row_silo::release() {
#if ATOMIC_WORD
    assert(_tid_word & LOCK_BIT);
    _tid_word = _tid_word & (~LOCK_BIT);
#else 
    pthread_mutex_unlock( _latch );
#endif
}

bool
Row_silo::try_lock()
{
#if ATOMIC_WORD
    uint64_t v = _tid_word;
    if (v & LOCK_BIT) // already locked
        return false;
    return __sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT));
#else
    return pthread_mutex_trylock( _latch ) != EBUSY;
    
#endif
}

uint64_t 
Row_silo::get_tid()
{
#if ATOMIC_WORD
    assert(ATOMIC_WORD);
    return _tid_word & (~LOCK_BIT);
#else
    return _tid;
#endif
}

#endif
