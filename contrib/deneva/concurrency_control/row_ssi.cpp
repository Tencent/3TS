/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: anduinzhu@tencent.com hongyaozhao@tencent.com
 *
 */

#include "txn.h"
#include "row.h"
#include "manager.h"
#include "ssi.h"
#include "row_ssi.h"
#include "mem_alloc.h"
#include <jemalloc/jemalloc.h>

void Row_ssi::init(row_t * row) {
    _row = row;
    si_read_lock = NULL;
    write_lock = NULL;

    prereq_mvcc = NULL;
    readhis = NULL;
    writehis = NULL;
    readhistail = NULL;
    writehistail = NULL;
    blatch = false;
    latch = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(latch, NULL);
    whis_len = 0;
    rhis_len = 0;
    preq_len = 0;
}

row_t * Row_ssi::clear_history(TsType type, ts_t ts) {
    //uint64_t clear_history_start  = get_sys_clock();
    SSIHisEntry ** queue;
    SSIHisEntry ** tail;
    switch (type) {
    case R_REQ:
        queue = &readhis;
        tail = &readhistail;
        break;
    case W_REQ:
        queue = &writehis;
        tail = &writehistail;
        break;
    default:
        assert(false);
    }
    SSIHisEntry * his = *tail;
    SSIHisEntry * prev = NULL;
    row_t * row = NULL;
    while (his && his->prev && his->prev->ts < ts) {
        prev = his->prev;
        assert(prev->ts >= his->ts);
        if (row != NULL) {
            free(row);
        }
        row = his->row;
        his->row = NULL;
        return_his_entry(his);
        his = prev;
        if (type == R_REQ) rhis_len --;
        else whis_len --;
    }
    *tail = his;
    if (*tail) (*tail)->next = NULL;
    if (his == NULL) *queue = NULL;
    return row;
}

SSIReqEntry * Row_ssi::get_req_entry() {
    return (SSIReqEntry *) malloc(sizeof(SSIReqEntry));
}

void Row_ssi::return_req_entry(SSIReqEntry * entry) {
    free(entry);
}

SSIHisEntry * Row_ssi::get_his_entry() {
    return (SSIHisEntry *) malloc(sizeof(SSIHisEntry));
}

void Row_ssi::return_his_entry(SSIHisEntry * entry) {
    if (entry->row != NULL) {
        free(entry->row);
    }
    free(entry);
}


void Row_ssi::insert_history(ts_t ts, TxnManager * txn, row_t * row)
{
    uint64_t insert_start = get_sys_clock();
    SSIHisEntry * new_entry = get_his_entry();
    new_entry->ts = ts;
    new_entry->txnid = txn->get_txn_id();
    new_entry->row = row;
    //new_entry->txn = std::shared_ptr<TxnManager>(txn);
    new_entry->txn = txn;
    if (row != NULL) {
        whis_len ++;
    } else {
        rhis_len ++;
    }
    SSIHisEntry ** queue = (row == NULL)?
        &(readhis) : &(writehis);
    SSIHisEntry ** tail = (row == NULL)?
        &(readhistail) : &(writehistail);
    SSIHisEntry * his = *queue;
    while (his != NULL && ts < his->ts) {
        his = his->next;
    }

    if (his) {
        LIST_INSERT_BEFORE(his, new_entry,(*queue));
    } else
        LIST_PUT_TAIL((*queue), (*tail), new_entry);

    uint64_t insert_end = get_sys_clock();
    INC_STATS(txn->get_thd_id(), trans_access_write_insert_time, insert_end - insert_start);

}

SSILockEntry * Row_ssi::get_entry() {
    SSILockEntry * entry = (SSILockEntry *)
        new(SSILockEntry);
    entry->type = LOCK_NONE;
    entry->txnid = 0;

    return entry;
}

void Row_ssi::get_lock(lock_t type, TxnManager * txn) {
    SSILockEntry * entry = get_entry();
    entry->type = type;
    //entry->txn = std::shared_ptr<TxnManager>(txn);
    entry->txn = txn;
    entry->start_ts = get_sys_clock();
    entry->txnid = txn->get_txn_id();
    if (type == LOCK_SH)
        STACK_PUSH(si_read_lock, entry);
    if (type == LOCK_EX)
        STACK_PUSH(write_lock, entry);
}

void Row_ssi::release_lock(lock_t type, TxnManager * txn) {
    if (type == LOCK_SH) {
        SSILockEntry * read = si_read_lock;
        SSILockEntry * pre_read = NULL;
        bool is_delete = false;
        while (read != NULL) {
            SSILockEntry * nex = read->next;
            if (read->txnid == txn->get_txn_id()) {
                assert(read != NULL);
                is_delete = true;
                SSILockEntry * delete_p = read;
                //if (pre_read != NULL)
                //   pre_read->next = read->next;
                if(pre_read != NULL) {
                    pre_read->next = read->next;
                } 
                else {
                    assert( read == si_read_lock );
                    si_read_lock = read->next;
                }
                //read->next = NULL;
                delete_p->next = NULL;
                delete delete_p;
            }
            else 
                is_delete = false;
            if(!is_delete) pre_read = read;
            //read = read->next;
            read = nex;
        }
    }
    if (type == LOCK_EX) {
        SSILockEntry * write = write_lock;
        SSILockEntry * pre_write = NULL;
        bool is_delete = false;
	    while (write != NULL) {
            SSILockEntry * nex = write->next;
            if (write->txnid == txn->get_txn_id()) {
                assert(write != NULL);
                is_delete = true;
                SSILockEntry * delete_p = write;
                //if (pre_write != NULL)
                //    pre_write->next = write->next;
                if(pre_write != NULL) {
                    pre_write->next = write->next;
                }
                else {
                    assert( write == write_lock );
                    write_lock = write->next;
                }
                //write->next = NULL;
                delete_p->next = NULL;
                delete delete_p;
            }
            else
                is_delete = false;
            if(!is_delete) pre_write = write;
            //write = write->next;
            write = nex;
        }
    }
}

void Row_ssi::release_lock(ts_t min_ts) {
    SSILockEntry * read = si_read_lock;
    SSILockEntry * pre_read = NULL;
    bool is_delete = false;
    while (read != NULL) {
        SSILockEntry * nex = read->next;
        if (read->start_ts < min_ts) {
            assert(read != NULL);
            is_delete = true;
            SSILockEntry * delete_p = read;
            if(pre_read != NULL) {
                pre_read->next = read->next;
            } 
            else {
                assert( read == si_read_lock );
                si_read_lock = read->next;
            }
            delete_p->next = NULL;
            delete delete_p;
        }
        else 
            is_delete = false;
        if(!is_delete) pre_read = read;
        read = nex;
    }
}

RC Row_ssi::access(TxnManager * txn, TsType type, row_t * row) {
    RC rc = RCOK;
    ts_t ts = txn->get_commit_timestamp();
    ts_t start_ts = txn->get_start_timestamp();
    uint64_t starttime = get_sys_clock();
    txnid_t txnid = txn->get_txn_id();
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
    if (type == R_REQ) {
        uint64_t read_start = get_sys_clock();
        // Add the si-read lock
        // the release time for si_read_lock is in the clear_history function
        get_lock(LOCK_SH, txn);

        // check write his to the read version
        SSIHisEntry* c_his = writehis;
        SSIHisEntry* p_his = writehis;

        // if the read is not the last committed version
        if (c_his != NULL && c_his->ts > start_ts){
            p_his = c_his->next;
            // find the version (p_his) to read and the version (c_his) to build rw
            while (p_his != NULL && p_his->ts > start_ts){
                c_his = p_his;
                p_his = p_his->next;
            }

            //build rw
            if (c_his->txn->out_rw) { // Abort when exists out_rw
                rc = Abort;
                INC_STATS(txn->get_thd_id(),total_rw_abort_cnt,1);
                DEBUG("ssi txn %ld read the write_commit in %ld abort, whis_ts %ld current_start_ts %ld\n",
                  txnid, c_his->txnid, c_his->ts, start_ts);
                goto end;             
            }
            c_his->txn->in_rw = true;
            txn->out_rw = true;
            DEBUG("ssi read the write_commit in %ld out %ld\n",c_his->txnid, txnid);
        }
        // else if the read is the init version or the last committed version  
        else{ // (c_his == NULL) || (c_his != NULL && c_his->ts < start_ts)
            SSILockEntry * write = write_lock;
            while (write != NULL) {
                if (write->txnid == txnid) { //TODO I wrote the row
                    write = write->next;
                    continue;
                }
                //build rw
                if (write->txn->out_rw) { // Abort when exists out_rw
                    rc = Abort;
                    INC_STATS(txn->get_thd_id(),total_rw_abort_cnt,1);
                    DEBUG("ssi txn %ld read the write_lock in %ld abort current_start_ts %ld\n",
                    txnid, write->txnid, start_ts);
                    goto end;             
                }
                write->txn->in_rw = true;
                txn->out_rw = true;
                DEBUG("ssi read the write_lock in %ld out %ld\n",write->txnid, txnid);
                write = write->next;
            }
        }

        // TODO What if write by myself ?
        row_t * ret = (p_his == NULL) ? _row : p_his->row;
        txn->cur_row = ret;
        assert(strstr(_row->get_table_name(), ret->get_table_name()));

        uint64_t read_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_read_time, read_end - read_start);

    } else if (type == P_REQ) {
        uint64_t pre_start  = get_sys_clock();
        //WW conflict
        //if (write_lock != NULL && write_lock->txn.get() != txn) {
        if (write_lock != NULL && write_lock->txn != txn) {
            rc = Abort;
            INC_STATS(txn->get_thd_id(),total_ww_abort_cnt,1);
            INC_STATS(txn->get_thd_id(), trans_access_pre_time, get_sys_clock() - pre_start);
            goto end;
        }
        uint64_t pre_start1  = get_sys_clock();
        // WW clifict in write history
        if(writehis != NULL && start_ts < writehis->ts) {
            rc = Abort;
            INC_STATS(txn->get_thd_id(),total_ww_abort_cnt,1);
            INC_STATS(txn->get_thd_id(), trans_access_pre_time, get_sys_clock() - pre_start1);
            goto end;
        }

        // Add the write lock
        uint64_t lock_start = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_pre_before_time, lock_start - pre_start);
        get_lock(LOCK_EX, txn);
        uint64_t lock_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_pre_lock_time, lock_end - lock_start);
        // Traverse the whole read his
        SSILockEntry * si_read = si_read_lock;
        uint64_t start1 = get_sys_clock();
        while (si_read != NULL) {
            if (si_read->txnid == txnid) {
                si_read = si_read->next;
                continue;
            }
            //bool is_active = si_read_lock->txn.get()->txn_status == TxnStatus::ACTIVE;
	        //bool interleaved =  si_read_lock->txn.get()->txn_status == TxnStatus::COMMITTED &&
            //	si_read->txn.get()->get_commit_timestamp() > start_ts;
            bool is_active = si_read->txn->txn_status == TxnStatus::ACTIVE;
            bool interleaved =  si_read->txn->txn_status == TxnStatus::COMMITTED &&
                si_read->txn->get_commit_timestamp() > start_ts;
            if (is_active || interleaved) {
                //bool in = si_read->txn.get()->in_rw;
                bool in = si_read->txn->in_rw;
                if (in && interleaved) { //! Abort
                    rc = Abort;
                    //INC_STATS(txn->get_thd_id(),total_txn_abort_cnt,1);
                    INC_STATS(txn->get_thd_id(),total_rw_abort_cnt,1);
                    DEBUG("ssi txn %ld write the read_commit in %ld abort, rhis_ts %ld current_start_ts %ld\n",
                      //txnid, si_read->txnid, si_read->txn.get()->get_commit_timestamp(), start_ts);
                      txnid, si_read->txnid, si_read->txn->get_commit_timestamp(), start_ts);
                    goto end;
                } 
                
                //inout_table.set_outConflict(txn->get_thd_id(), si_read->txnid, txnid);
                //inout_table.set_inConflict(txn->get_thd_id(), txnid, si_read->txnid);
                assert(si_read->txn != NULL);
                si_read->txn->out_rw = true;
                txn->in_rw = true;
                DEBUG("ssi write the si_read_lock out %ld in %ld\n", si_read->txnid, txnid);
              
            }
            si_read = si_read->next;
        }

        uint64_t pre_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_pre_check_time, pre_end - start1);
        INC_STATS(txn->get_thd_id(), trans_access_pre_time, pre_end - pre_start);
        INC_STATS(txn->get_thd_id(), total_txn_prewrite_cnt, 1);
        
    } else if (type == W_REQ) {
        uint64_t write_start  = get_sys_clock();
        rc = RCOK;
        release_lock(LOCK_EX, txn);
        uint64_t end1 = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_write_release_time, end1 - write_start);
        //release_lock(LOCK_COM, txn);
        //TODO: here need to consider whether need to release the si-read lock.
        // release_lock(LOCK_SH, txn);

        // the corresponding prewrite request is debuffered.
        insert_history(ts, txn, row);
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        //SSIReqEntry * req = debuffer_req(P_REQ, txn);
        //assert(req != NULL);
        //return_req_entry(req);
        
        uint64_t write_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_write_time, write_end - write_start);
        
    } else if (type == XP_REQ) {
        INC_STATS(txn->get_thd_id(),total_txn_abort_cnt,1);
        uint64_t xp_start  = get_sys_clock();
        //uint64_t xp_end = get_sys_clock();
        //INC_STATS(txn->get_thd_id(), trans_access_xp_time, xp_end - xp_start);
        release_lock(LOCK_EX, txn);
        //release_lock(LOCK_COM, txn);
        //TODO: here need to consider whether need to release the si-read lock.
        release_lock(LOCK_SH, txn);
        //for(int i = 0; i < 10000000; i++);
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        //SSIReqEntry * req = debuffer_req(P_REQ, txn);
        //assert (req != NULL);
        //return_req_entry(req);
        
        uint64_t xp_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_xp_time, xp_end - xp_start);
        
    } else {
        assert(false);
    }

    if (rc == RCOK) {
        uint64_t clear_start  = get_sys_clock();
        if (whis_len > g_his_recycle_len || rhis_len > g_his_recycle_len) {
            ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
            if (readhistail && readhistail->ts < t_th) {
                clear_history(R_REQ, t_th);
            }
            // Here is a tricky bug. The oldest transaction might be
            // reading an even older version whose timestamp < t_th.
            // But we cannot recycle that version because it is still being used.
            // So the HACK here is to make sure that the first version older than
            // t_th not be recycled.
            if (whis_len > 1 && writehistail->prev->ts < t_th) {
                row_t * latest_row = clear_history(W_REQ, t_th);
                if (latest_row != NULL) {
                    assert(_row != latest_row);
                    _row->copy(latest_row);
                }
            }
            release_lock(t_th);
        }
        
        uint64_t clear_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_clear_time, clear_end - clear_start);
        
    }
end:
    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;

    

    if (g_central_man) {
        glob_manager.release_row(_row);
     } else {
        pthread_mutex_unlock(latch);
     }

    INC_STATS(txn->get_thd_id(), total_access_time, timespan);
    return rc;
}
