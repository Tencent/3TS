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

#if CC_ALG == SILO

RC
TxnManager::validate_silo()
{
    uint64_t validate_start = get_sys_clock();
    RC rc = RCOK;
    // lock write tuples in the primary key order.
    uint64_t wr_cnt = txn->write_cnt;
    // write_set = (int *) mem_allocator.alloc(sizeof(int) * wr_cnt);
    int cur_wr_idx = 0;
    int read_set[txn->row_cnt - txn->write_cnt];
    int cur_rd_idx = 0;
    for (uint64_t rid = 0; rid < txn->row_cnt; rid ++) {
        if (txn->accesses[rid]->type == WR)
            write_set[cur_wr_idx ++] = rid;
        else 
            read_set[cur_rd_idx ++] = rid;
    }

    // bubble sort the write_set, in primary key order 
    if (wr_cnt > 1)
    {
        for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
            for (uint64_t j = 0; j < i; j++) {
                if (txn->accesses[ write_set[j] ]->orig_row->get_primary_key() > 
                    txn->accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
                {
                    int tmp = write_set[j];
                    write_set[j] = write_set[j+1];
                    write_set[j+1] = tmp;
                }
            }
        }
    }
    INC_STATS(get_thd_id(), silo_sort_time, get_sys_clock()-validate_start);

    num_locks = 0;
    ts_t max_tid = 0;
    if (_pre_abort) {
        for (uint64_t i = 0; i < wr_cnt; i++) {
            row_t * row = txn->accesses[ write_set[i] ]->orig_row;
            if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid) {
                rc = Abort;
                return rc;
            }    
        }    
        for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i ++) {
            Access * access = txn->accesses[ read_set[i] ];
            if (access->orig_row->manager->get_tid() != txn->accesses[read_set[i]]->tid) {
                rc = Abort;
                return rc;
            }
        }
    }

#if LOCK_NW || LOCK_WD  
    // lock all rows in the write set.
    uint64_t lock_start = get_sys_clock();
    bool done = false;
    if (_validation_no_wait) {
        while (!done) {
            num_locks = 0;
            for (uint64_t i = 0; i < wr_cnt; i++) {
                row_t * row = txn->accesses[ write_set[i] ]->orig_row;
                #if LOCK_NW
                if (!row->manager->try_lock())
                #elif LOCK_WD
                float wait_second = 1;
                float wait_nanosecond = 0;
                if (!row->manager->try_lock_wait(wait_second, wait_nanosecond))
                #endif
                {
                    break;
                }
                DEBUG("silo %ld write lock row %ld \n", this->get_txn_id(), row->get_primary_key());
                row->manager->assert_lock();
                num_locks ++;
                if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid)
                {
                    rc = Abort;
                    INC_STATS(get_thd_id(), silo_lock_time, get_sys_clock()-lock_start);
                    return rc;
                }
            }
            if (num_locks == wr_cnt) {
                DEBUG("TRY LOCK true %ld\n", get_txn_id());
                done = true;
                INC_STATS(get_thd_id(), silo_lock_time, get_sys_clock()-lock_start);
            } else {
                rc = Abort;
                INC_STATS(get_thd_id(), silo_lock_time, get_sys_clock()-lock_start);
                return rc;
            }
        }
    } else {
        for (uint64_t i = 0; i < wr_cnt; i++) {
            row_t * row = txn->accesses[ write_set[i] ]->orig_row;
            row->manager->lock();
            DEBUG("silo %ld write lock row %ld \n", this->get_txn_id(), row->get_primary_key());
            num_locks++;
            if (row->manager->get_tid() != txn->accesses[write_set[i]]->tid) {
                rc = Abort;
                return rc;
            }
        }
    }

#endif

    COMPILER_BARRIER
#if LOCK_CS
    sem_wait(&_semaphore);
#endif 
    uint64_t check_start =get_sys_clock();
    // validate rows in the read set
    // for repeatable_read, no need to validate the read set.
    for (uint64_t i = 0; i < txn->row_cnt - wr_cnt; i ++) {
        Access * access = txn->accesses[ read_set[i] ];
        bool success = access->orig_row->manager->validate(access->tid, false);
        if (!success) {
            rc = Abort;
            return rc;
        }
        if (access->tid > max_tid)
            max_tid = access->tid;
    }
    // validate rows in the write set
    for (uint64_t i = 0; i < wr_cnt; i++) {
        Access * access = txn->accesses[ write_set[i] ];
        bool success = access->orig_row->manager->validate(access->tid, true);
        if (!success) {
            rc = Abort;
            return rc;
        }
        if (access->tid > max_tid)
            max_tid = access->tid;
    }

    this->max_tid = max_tid;
    INC_STATS(get_thd_id(), silo_check_time, get_sys_clock()-check_start);

#if LOCK_CS

    // put write in critical section
    if (rc == RCOK) 
    {
        for (uint64_t i = 0; i < txn->write_cnt; i++) {
            Access * access = txn->accesses[ write_set[i] ];
            access->orig_row->manager->write( 
                access->data, this->commit_timestamp );
        }
    }

    sem_post(&_semaphore);
#endif 

    return rc;
}

RC
TxnManager::finish(RC rc)
{
    uint64_t finish_start = get_sys_clock();
#if LOCK_NW || LOCK_WD  
    if (rc == Abort) {
        if (this->num_locks > get_access_cnt()) 
            return rc;
        for (uint64_t i = 0; i < this->num_locks; i++) {
            txn->accesses[ write_set[i] ]->orig_row->manager->release();
            DEBUG("silo %ld abort release row %ld \n", this->get_txn_id(), txn->accesses[ write_set[i] ]->orig_row->get_primary_key());
        }
    } else {
        
        for (uint64_t i = 0; i < txn->write_cnt; i++) {
            uint64_t write_start = get_sys_clock();
            Access * access = txn->accesses[ write_set[i] ];
            access->orig_row->manager->write( 
                access->data, this->commit_timestamp );
            INC_STATS(get_thd_id(), trans_access_write_insert_time, get_sys_clock() - write_start);
            INC_STATS(get_thd_id(), txn_useful_time, get_sys_clock()-write_start);
            txn->accesses[ write_set[i] ]->orig_row->manager->release();
            DEBUG("silo %ld commit release row %ld \n", this->get_txn_id(), txn->accesses[ write_set[i] ]->orig_row->get_primary_key());
        }
    }
    num_locks = 0;
#endif
    memset(write_set, 0, sizeof(write_set));

    INC_STATS(get_thd_id(), silo_finish_time, get_sys_clock()-finish_start);

    return rc;
}

RC
TxnManager::find_tid_silo(ts_t max_tid)
{
    if (max_tid > _cur_tid)
        _cur_tid = max_tid;
    return RCOK;
}

#endif
