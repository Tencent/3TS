/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */

#include "txn.h"
#include "row.h"
#include "manager.h"
#include "ssi.h"
#include "row_ssi.h"
#include "mem_alloc.h"

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
    latch = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(latch, NULL);
    whis_len = 0;
    rhis_len = 0;
    commit_lock = 0;
    preq_len = 0;
}

row_t * Row_ssi::clear_history(TsType type, ts_t ts) {
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
            row->free_row();
            mem_allocator.free(row, sizeof(row_t));
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
    return (SSIReqEntry *) mem_allocator.alloc(sizeof(SSIReqEntry));
}

void Row_ssi::return_req_entry(SSIReqEntry * entry) {
    mem_allocator.free(entry, sizeof(SSIReqEntry));
}

SSIHisEntry * Row_ssi::get_his_entry() {
    return (SSIHisEntry *) mem_allocator.alloc(sizeof(SSIHisEntry));
}

void Row_ssi::return_his_entry(SSIHisEntry * entry) {
    if (entry->row != NULL) {
        entry->row->free_row();
        mem_allocator.free(entry->row, sizeof(row_t));
    }
    mem_allocator.free(entry, sizeof(SSIHisEntry));
}

void Row_ssi::buffer_req(TsType type, TxnManager * txn)
{
    SSIReqEntry * req_entry = get_req_entry();
    assert(req_entry != NULL);
    req_entry->txn = txn;
    req_entry->ts = txn->get_start_timestamp();
    req_entry->starttime = get_sys_clock();
    if (type == P_REQ) {
        preq_len ++;
        STACK_PUSH(prereq_mvcc, req_entry);
    }
}

// for type == R_REQ
//     debuffer all non-conflicting requests
// for type == P_REQ
//   debuffer the request with matching txn.
SSIReqEntry * Row_ssi::debuffer_req( TsType type, TxnManager * txn) {
    SSIReqEntry ** queue = &prereq_mvcc;
    SSIReqEntry * return_queue = NULL;

    SSIReqEntry * req = *queue;
    SSIReqEntry * prev_req = NULL;
    if (txn != NULL) {
        assert(type == P_REQ);
        while (req != NULL && req->txn != txn) {
            prev_req = req;
            req = req->next;
        }
        assert(req != NULL);
        if (prev_req != NULL)
            prev_req->next = req->next;
        else {
            assert( req == *queue );
            *queue = req->next;
        }
        preq_len --;
        req->next = return_queue;
        return_queue = req;
    }
    return return_queue;
}

void Row_ssi::insert_history(ts_t ts, TxnManager * txn, row_t * row)
{
    SSIHisEntry * new_entry = get_his_entry();
    new_entry->ts = ts;
    new_entry->txnid = txn->get_txn_id();
    new_entry->row = row;

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
}

SSILockEntry * Row_ssi::get_entry() {
    SSILockEntry * entry = (SSILockEntry *)
        mem_allocator.alloc(sizeof(SSILockEntry));
    entry->type = LOCK_NONE;
    entry->txn = 0;

    return entry;
}

void Row_ssi::get_lock(lock_t type, TxnManager * txn) {
    SSILockEntry * entry = get_entry();
    entry->type = type;
    entry->start_ts = get_sys_clock();
    entry->txn = txn->get_txn_id();
    if (type == LOCK_SH)
        STACK_PUSH(si_read_lock, entry);
    if (type == LOCK_EX)
        STACK_PUSH(write_lock, entry);
    if (type == LOCK_COM)
        commit_lock = txn->get_txn_id();
}

void Row_ssi::get_SIRdlock(SSILockEntry* & lhead, TxnManager * txn) {
    SSILockEntry * entry = get_entry();
    entry->start_ts = get_sys_clock();
    entry->txn = txn->get_txn_id();
    
    STACK_PUSH(lhead, entry);
    
}

void Row_ssi::release_lock(lock_t type, TxnManager * txn) {
    if (type == LOCK_SH) {
        SSILockEntry * read = si_read_lock;
        SSILockEntry * pre_read = NULL;
        while (read != NULL) {
            if (read->txn == txn->get_txn_id()) {
                assert(read != NULL);
                if (pre_read != NULL)
                    pre_read->next = read->next;
                else {
                    assert( read == si_read_lock );
                    si_read_lock = read->next;
                }
                read->next = NULL;
            }
            pre_read = read;
            read = read->next;
        }
    }
    if (type == LOCK_EX) {
        SSILockEntry * write = write_lock;
        SSILockEntry * pre_write = NULL;
        while (write != NULL) {
            if (write->txn == txn->get_txn_id()) {
                assert(write != NULL);
                if (pre_write != NULL)
                    pre_write->next = write->next;
                else {
                    assert( write == write_lock );
                    write_lock = write->next;
                }
                write->next = NULL;
            }
            pre_write = write;
            write = write->next;
        }
    }
    if (type == LOCK_COM) {
        if (commit_lock == txn->get_txn_id()) commit_lock = 0;
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
        // Add the si-read lock
        // get_lock(LOCK_SH, txn);

        // Read the row
        rc = RCOK;
        SSIHisEntry *ptr_entry, *latest_entry = writehis;
        // written by myself. OK, return directly
        if (latest_entry != nullptr && 
            txnid == latest_entry->txnid) {
            txn->cur_row = latest_entry->row;
            goto end;
        }
        // if there is an active updater, it must have put
        // the version on the head of history because our
        // algorithm will make it put the version on hisotory
        // as soon as possible.

        /**
         * for each version(xNew) of x
         * that is newer than what T read:
         *    if xNew.creator is committed
         *       and xNew.creator.out_rw is true then
         *       abort(T)
         *    set xNew.creator.rw_in = true;
         *    set T.rw_out = true;
         *
         **/

        ptr_entry = latest_entry;
        while (ptr_entry != nullptr && ptr_entry->ts > start_ts) {
            //todo is this need?
            if (ptr_entry->txnid == txnid) {
                ptr_entry = ptr_entry->next;
                continue;
            }
            //probe the txn table to find status.
            //now, the reader and creator of the version form an RW edge
            //if the creator already has an RW edge out the creater has two adjacent RW, and is
            //dangerous, abort it
            bool is_out = inout_table.get_outConflict(txn->get_thd_id(), ptr_entry->txnid); 
            if (is_out) {
               rc = Abort;
               goto end;
            }
            
            inout_table.set_inConflict(txn->get_thd_id(), ptr_entry->txnid, txnid);
            inout_table.set_outConflict(txn->get_thd_id(), txnid, ptr_entry->txnid);
            ptr_entry = ptr_entry->next;
        }
        //if we found an desirable row, store the pointer in the txn->cur_row
        //record the visitor, serving as SIReadLock.
        //otherwise we should read the original row
        if (ptr_entry != NULL) {
            txn->cur_row = ptr_entry->row;
            ptr_entry->visitors.push_back(txnid);
        } else {
            txn->cur_row = _row;
            visitors.push_back(txnid);
        }

         assert(strstr(_row->get_table_name(), txn->cur_row->get_table_name()));

    } else if (type == P_REQ) {
        // Add the write lock
        //get_lock(LOCK_EX, txn);

        SSIHisEntry  *latest_entry = writehis;
        // 1) avoid WW-conflict!!!
        // we use the strategy of 'first-updater-win'
        // if updater finds that there is a data version
        // newer that its start time, which means some txn begins
        // to update or already have finished the procedure.
        // so, abort the later txn.
        if (latest_entry != nullptr) {
            if (latest_entry->txnid != txnid &&
                latest_entry->ts > start_ts) { //ww confict
                rc = Abort;
                goto end;
            } else if (latest_entry->txnid == txnid) {
                //replace older version
                latest_entry->row = row;
            } else{// create new version
                insert_history(UINT64_MAX, txn, row);
            }
        } else {//first writer
            insert_history(UINT64_MAX, txn, row);
        }

        /**
         * if there is a SIREAD lock(r1) on x with r1.owner is running
         * or commit(r1.owner) > begin(T):
         *    if r1.owner is committed and r1.owner.in_rw:
         *       abort (T)
         *    set r1.owner.out_rw = true;
         *    set T.in_rw = true;
         **/
      
        std::vector<txnid_t> &vec = latest_entry->next ? 
                          latest_entry->next->visitors : visitors;
        for (const auto &r_txn : vec) {
            if (r_txn == txnid) continue;
            SSIState r_txn_state = inout_table.get_state(txn->get_thd_id(), r_txn);
            if (r_txn_state == SSI_RUNNING ||
               inout_table.get_commit_ts(txn->get_thd_id(), r_txn) > start_ts) {
               if (r_txn_state == SSI_COMMITTED 
                   && inout_table.get_inConflict(txn->get_thd_id(), r_txn)) {
                    rc = Abort;
                    goto end;
                }
                inout_table.set_inConflict(txn->get_thd_id(), txnid, r_txn);
                inout_table.set_outConflict(txn->get_thd_id(), r_txn, txnid);
            }//end if
        }//end while
        rc = RCOK;
        
    } else if (type == W_REQ) {
        rc = RCOK;
        //release_lock(LOCK_EX, txn);
        //release_lock(LOCK_COM, txn);
        //TODO: here need to consider whether need to release the si-read lock.
        // release_lock(LOCK_SH, txn);
        //done. set commit timestamp
        SSIHisEntry  *latest_entry = writehis;
        latest_entry->ts = ts;
        latest_entry->row = row;
      
    } else if (type == XP_REQ) {
        //release_lock(LOCK_EX, txn);
        //release_lock(LOCK_COM, txn);
        //TODO: here need to consider whether need to release the si-read lock.
        //release_lock(LOCK_SH, txn);
        SSIHisEntry *latest_entry = writehis;
        if (writehis != nullptr && latest_entry->txnid == txnid) {
            writehis = writehis->next;
            writehis->prev = nullptr;
            free(latest_entry);
        }

        // DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        // SSIReqEntry * req = debuffer_req(P_REQ, txn);
        // assert (req != NULL);
        // return_req_entry(req);
    } else {
        assert(false);
    }

    if (rc == RCOK) {
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

RC Row_ssi::validate_last_commit(TxnManager * txn) {
    RC rc = RCOK;
    SSIHisEntry *  whis = writehis;
    ts_t start_ts = txn->get_start_timestamp();
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
    // INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);

    if (commit_lock != 0 && commit_lock != txn->get_txn_id()) {
        DEBUG("si last commit lock %ld, %ld\n",commit_lock, txn->get_txn_id());
        rc = Abort;
        goto last_commit_end;
    }
    get_lock(LOCK_COM, txn);
    // Iterate over a version that is newer than the one you are currently reading.
    while (whis != NULL && whis->ts < start_ts) {
        whis = whis->next;
    }
    if (whis != NULL) {
        DEBUG("si last commit whis %ld, %ld, %ld\n",whis->ts, start_ts, txn->get_txn_id());
        release_lock(LOCK_COM, txn);
        rc = Abort;
    }    
last_commit_end:
    if (g_central_man) {
        glob_manager.release_row(_row);
    } else {
        pthread_mutex_unlock(latch);
    }
    return rc;
}
