/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: anduinzhu, axingguchen, xenitchen, hongyaozhao@tencent.com
 *
 */

#include "txn.h"
#include "row.h"
#include "manager.h"
#include "ssi.h"
#include "row_opt_ssi.h"
#include "mem_alloc.h"
#include <jemalloc/jemalloc.h>

void Row_opt_ssi::init(row_t * row) {
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

row_t * Row_opt_ssi::clear_history(TsType type, ts_t ts) {
    OPT_SSIHisEntry ** queue;
    OPT_SSIHisEntry ** tail;
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
    OPT_SSIHisEntry * his = *tail;
    OPT_SSIHisEntry * prev = NULL;
    row_t * row = NULL;
    while (his && his->prev && his->prev->ts < ts) {
        prev = his->prev;
        assert(prev->ts >= his->ts);
        if (row != NULL) {
            free(row);
        }
        row = his->row;
        his->row = NULL;
#if ISOLATION_LEVEL == SERIALIZABLE
        //clear si_read
        OPT_SSILockEntry * read = his->si_read_lock;
        while (read != NULL) {
            assert(read != NULL);
            OPT_SSILockEntry * delete_p = read;
            //assert( read == his->si_read_lock );
            read = read->next;
            delete_p->next = NULL;
            free(delete_p);
        }
#endif
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

OPT_SSIReqEntry * Row_opt_ssi::get_req_entry() {
    return (OPT_SSIReqEntry *) malloc(sizeof(OPT_SSIReqEntry));
}

void Row_opt_ssi::return_req_entry(OPT_SSIReqEntry * entry) {
    free(entry);
}

OPT_SSIHisEntry * Row_opt_ssi::get_his_entry() {
    return (OPT_SSIHisEntry *) malloc(sizeof(OPT_SSIHisEntry));
}

void Row_opt_ssi::return_his_entry(OPT_SSIHisEntry * entry) {
    if (entry->row != NULL) {
        free(entry->row);
    }
    free(entry);
}


void Row_opt_ssi::insert_history(ts_t ts, TxnManager * txn, row_t * row) {
    OPT_SSIHisEntry * new_entry = get_his_entry();
    new_entry->ts = ts;
    new_entry->txnid = txn->get_txn_id();
    new_entry->row = row;
    new_entry->si_read_lock = NULL;
    new_entry->txn = txn;
	
    if (row != NULL) {
        whis_len ++;
    } else {
        rhis_len ++;
    }
	
    OPT_SSIHisEntry ** queue = (row == NULL)?
        &(readhis) : &(writehis);
    OPT_SSIHisEntry ** tail = (row == NULL)?
        &(readhistail) : &(writehistail);
    OPT_SSIHisEntry * his = *queue;
    while (his != NULL && ts < his->ts) {
        his = his->next;
    }
	
    if (his) {
        LIST_INSERT_BEFORE(his, new_entry,(*queue));
    } else {
        LIST_PUT_TAIL((*queue), (*tail), new_entry);
    }
}

OPT_SSILockEntry * Row_opt_ssi::get_entry() {
    OPT_SSILockEntry * entry = (OPT_SSILockEntry *)
        malloc(sizeof(OPT_SSILockEntry));
    entry->type = LOCK_NONE;
    entry->txnid = 0;
    return entry;
}

void Row_opt_ssi::get_lock(lock_t type, TxnManager * txn) {
    OPT_SSILockEntry * entry = get_entry();
    entry->type = type;
    entry->txn = txn;
    entry->start_ts = get_sys_clock();
    entry->txnid = txn->get_txn_id();
    if (type == LOCK_SH)
        STACK_PUSH(si_read_lock, entry);
    if (type == LOCK_EX)
        STACK_PUSH(write_lock, entry);
}

void Row_opt_ssi::get_lock(lock_t type, TxnManager * txn, OPT_SSIHisEntry * whis) {
    OPT_SSILockEntry * entry = get_entry();
    entry->type = type;
    entry->txn = txn;
    entry->start_ts = get_sys_clock();
    entry->txnid = txn->get_txn_id();
    if (type == LOCK_SH)
        STACK_PUSH(whis->si_read_lock, entry);
    if (type == LOCK_EX)
        STACK_PUSH(write_lock, entry);
}

void Row_opt_ssi::release_lock(lock_t type, TxnManager * txn) {
    if (type == LOCK_SH) {  
        ts_t start_ts = txn->get_start_timestamp();
        // find the read version
        OPT_SSIHisEntry* c_his = writehis;
        while (c_his != NULL && c_his->ts > start_ts){
        	c_his = c_his->next;
        }
	// get the si_read locks in the read version;
        OPT_SSILockEntry * read = c_his != NULL? c_his->si_read_lock : si_read_lock;
        OPT_SSILockEntry * pre_read = NULL;
        bool is_delete = false;
        // delete the si_read lock of this txn
        while (read != NULL) {
            OPT_SSILockEntry * nex = read->next;
            if (read->txnid == txn->get_txn_id()) {
                assert(read != NULL);
                is_delete = true;
                OPT_SSILockEntry * delete_p = read;
                if(pre_read != NULL) {
                    pre_read->next = read->next;
                } else {
                    // read is the head of write hisotry si_read_lock or the init data si_read_lock
                    assert( read == si_read_lock || read == c_his->si_read_lock );
                    if (c_his != NULL){
                        c_his->si_read_lock = read->next;
                    } else{
                        si_read_lock = read->next;
                    }
		
                }
                delete_p->next = NULL;
                free(delete_p);
                //break; // read more than once
            } else{ 
                is_delete = false;
	        }
            if(!is_delete){ 
                pre_read = read;
	    }
            read = nex;
        }
    }
    if (type == LOCK_EX) {
        OPT_SSILockEntry * write = write_lock;
        OPT_SSILockEntry * pre_write = NULL;
        bool is_delete = false;
	    while (write != NULL) {
            OPT_SSILockEntry * nex = write->next;
            if (write->txnid == txn->get_txn_id()) {
                assert(write != NULL);
                is_delete = true;
                OPT_SSILockEntry * delete_p = write;
                if(pre_write != NULL) {
                    pre_write->next = write->next;
                } else {
                    assert( write == write_lock );
                    write_lock = write->next;
                }
                delete_p->next = NULL;
                free(delete_p); 
            } else {
                is_delete = false;
	    }
            if(!is_delete) pre_write = write;
            write = nex;
        }
    }
}

void Row_opt_ssi::release_lock(ts_t min_ts) {
    // release the si_read locks in last commited history
    OPT_SSILockEntry * read = writehis->si_read_lock;
    OPT_SSILockEntry * pre_read = NULL;
    bool is_delete = false;
    while (read != NULL) {
        OPT_SSILockEntry * nex = read->next;
        if (read->start_ts < min_ts) {
            assert(read != NULL);
            is_delete = true;
            OPT_SSILockEntry * delete_p = read;
            if(pre_read != NULL) {
                pre_read->next = read->next;
            } else {
                assert( read == writehis->si_read_lock );
                writehis->si_read_lock = read->next;
            }
            delete_p->next = NULL;
            free(delete_p);
        } else {
            is_delete = false;
	}
        if(!is_delete) pre_read = read;
        read = nex;
    }
}

RC Row_opt_ssi::access(TxnManager * txn, TsType type, row_t * row) {
    RC rc = RCOK;
    ts_t ts = txn->get_commit_timestamp();
    ts_t start_ts = txn->get_start_timestamp();
    uint64_t starttime = get_sys_clock();
#if ISOLATION_LEVEL == SERIALIZABLE
    txnid_t txnid = txn->get_txn_id();
#endif
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
    if (type == R_REQ) {
        // check write his to the read version
        OPT_SSIHisEntry* c_his = writehis;
        OPT_SSIHisEntry* p_his = writehis;

#if ISOLATION_LEVEL == NOLOCK
        row_t * ret = (p_his == NULL) ? _row : p_his->row;
        txn->cur_row = ret;
#endif

        // To build rw, find the correct W which is from the next version of R version
        // if the read version is not the init data or last committed version
        if (c_his != NULL && c_his->ts > start_ts){
            p_his = c_his->next;
            // find the version (p_his) to read and the version (c_his) to build rw
            while (p_his != NULL && p_his->ts > start_ts){
                c_his = p_his;
                p_his = p_his->next;
            }
#if ISOLATION_LEVEL == SERIALIZABLE
            //build rw
            if (c_his->txn->out_rw) { // Abort when exists out_rw
                rc = Abort;
                DEBUG("ssi txn %ld read the write_commit in %ld abort, whis_ts %ld current_start_ts %ld\n",
                  txnid, c_his->txnid, c_his->ts, start_ts);
                goto end;             
            }
            c_his->txn->in_rw = true;
            txn->out_rw = true;
            DEBUG("ssi read the write_commit in %ld out %ld\n",c_his->txnid, txnid);
#endif
        } else{ 
#if ISOLATION_LEVEL == SERIALIZABLE
            // else if the read is the init version or the last committed version  
            // (c_his == NULL) || (c_his != NULL && p_his == NULL)
            OPT_SSILockEntry * write = write_lock;
            while (write != NULL) {
                if (write->txnid == txnid) { //TODO I wrote the row
                    write = write->next;
                    continue;
                }
                //build rw
                if (write->txn->out_rw) { // Abort when exists out_rw
                    rc = Abort;
                    DEBUG("ssi txn %ld read the write_lock in %ld abort current_start_ts %ld\n",
                    txnid, write->txnid, start_ts);
                    goto end;             
                }
                write->txn->in_rw = true;
                txn->out_rw = true;
                DEBUG("ssi read the write_lock in %ld out %ld\n",write->txnid, txnid);
                write = write->next;
            }
#endif
        }

#if ISOLATION_LEVEL == SERIALIZABLE
        // add si_read lock to write history
        // the release si_read_lock is in the clear_history and release_lock
        if (p_his != NULL){
            get_lock(LOCK_SH, txn, p_his);  // si_read_lock add to write history
        } else{
            get_lock(LOCK_SH, txn);         // si_read_lock add to row
        }
#endif

#if ISOLATION_LEVEL != NOLOCK
        row_t * ret = (p_his == NULL) ? _row : p_his->row;
        txn->cur_row = ret;
#endif
        assert(strstr(_row->get_table_name(), ret->get_table_name()));
    } else if (type == P_REQ) {
        //WW lock conflict
        if (write_lock != NULL && write_lock->txn != txn) {
            rc = Abort;
            goto end;         
        }
        // WCW conflict in write history
        if(writehis != NULL && start_ts < writehis->ts) {
            rc = Abort;
            goto end;         
        }

        // Add the write lock
        get_lock(LOCK_EX, txn);

#if ISOLATION_LEVEL == SERIALIZABLE
        // get si_read from last committed history or row manager(if not write history)
        OPT_SSILockEntry * si_read = writehis != NULL? writehis->si_read_lock : si_read_lock;
        while (si_read != NULL) {
            if (si_read->txnid == txnid) {
                si_read = si_read->next;
                continue;
            }
            // build rw
            bool is_active = si_read->txn->txn_status == TxnStatus::ACTIVE;
            bool interleaved =  si_read->txn->txn_status == TxnStatus::COMMITTED &&
                si_read->txn->get_commit_timestamp() > start_ts;
            if (is_active || interleaved) {
                bool in = si_read->txn->in_rw;
                if (in && interleaved) { //! Abort
                    rc = Abort;
                    DEBUG("ssi txn %ld write the read_commit in %ld abort, rhis_ts %ld current_start_ts %ld\n",
                      //txnid, si_read->txnid, si_read->txn.get()->get_commit_timestamp(), start_ts);
                      txnid, si_read->txnid, si_read->txn->get_commit_timestamp(), start_ts);
                    goto end;
                }             
                assert(si_read->txn != NULL);
                si_read->txn->out_rw = true;
                txn->in_rw = true;
                DEBUG("ssi write the si_read_lock out %ld in %ld\n", si_read->txnid, txnid);      
            }
            si_read = si_read->next;
        }
#endif
        
    } else if (type == W_REQ) {
        rc = RCOK;
        release_lock(LOCK_EX, txn);
        insert_history(ts, txn, row);
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());  
    } else if (type == XP_REQ) {
        release_lock(LOCK_EX, txn);
#if ISOLATION_LEVEL == SERIALIZABLE
        release_lock(LOCK_SH, txn);
#endif
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
    } else {
        assert(false);
    }

    if (rc == RCOK) {
        //if (whis_len > g_his_recycle_len || rhis_len > g_his_recycle_len) {
        if (whis_len > g_his_recycle_len) {
            ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
            // Here is a tricky bug. The oldest transaction might be
            // reading an even older version whose timestamp < t_th.
            // But we cannot recycle that version because it is still being used.
            // So the HACK here is to make sure that the first version older than
            // t_th not be recycled.
            if (writehistail->prev->ts < t_th) {
                // When we clear the write history, we also clear the si_read locks in that history 
                row_t * latest_row = clear_history(W_REQ, t_th);
                if (latest_row != NULL) {
                    assert(_row != latest_row);
                    _row->copy(latest_row);
                    si_read_lock = NULL; //can not read initial version once we have clear the versions.
                }
            }
#if ISOLATION_LEVEL == SERIALIZABLE
            release_lock(t_th);
#endif
        }      
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

    return rc;
}
