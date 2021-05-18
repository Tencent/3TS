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

void Row_ssi::，return_req_entry(SSIReqEntry * entry) {
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
    new_entry->commit_ts = ts;
    new_entry->txn_id = txn->get_txn_id();
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

/**
 * Here, in the write operation we can only establish the rw-antidenpendcy for
 * the writer and the version on which it updated. that is no need for the writer
 * to build the rw anti-dependency with all reader on the row version before. the
 * proof following is borrowed from the Postgresql implementation.
 * https://github.com/postgres/postgres/blob/master/src/backend/storage/lmgr/README-SSI
 *
 *  1. If transaction T1 reads a row version (thus acquiring a predicate lock on it) and
 *     a second transaction T2 updates that row verson(thus creating a rw-conflict graph
 *     edge from T1 to T2), must a third transaction T3 which re-updates the new version
 *     of the row also have rw-conflict from T1 to prevent anomalies? In other words, does
 *     it matter whether we recognize the edge T1->T3?
 *    
 *     1.1 If T1 has a conflict in, it certainly doesn't. Add the edge T1->T3 would
 *         create a dangerous structure, but we already had one from the edge T1->T2
 *         , so we would have aborted something anyway.(T2 has already committed, else
 *         T3 could not have update its output; but we would have aborted either T1 
 *         or T1's predecessor(s). Hence no cycle involving T1 and T3 can survive.)
 *     1.2 Now let's consider the case where T1 doesn't have a rw-conflict in. If that's
 *         the case, for this edge T1->T3 to make difference, T3 must have a rw-conflict
 *         out that induces a cycle in the dependency graph, i.e., a rw-conflict out to
 *         some transaction preceding T1 in the graph.(A rw-conflict out to T1 itself would
 *         be problematic too, but that would mean T1 has a conflict in, the case we 
 *         already eliminated.). So we are trying to figure out if there can be an 
 *         rw-conflict edge T3->T0, where T0 is some transaction that precedes T1.
 *         For T0 to precedes T1, there has to be some edge, or sequence of edges,
 *         from T0 to T1. At least the last edge has to be a wr-dependency or 
 *         ww-dependency rather than a rw-conflict, because T1 doesn't have a
 *         rw-conflict in. And that gives us enough information about the order
 *         of transaction to see that T3 can't have rw-conflict to T0:
 *         - T0 committed before T1 start (the wr/ww-dependency implies this)
 *         - T1 started before T2 committed (the T1->T2 rw-conflict implies this)
 *         - T2 committed before T3 started (otherwise T3 would get aborted)
 *         That means T0 committed before T3 started, and therefore there can't be
 *         a rw-conflict from T3 to T0. So in all case, we don't need the T1->T3 edge
 *         to recognize cycles. Therefore it's not necessary for T1's SIREAD lock on
 *         the original tuple version to cover later versions as well.
 */

RC Row_ssi::access(TxnManager * txn, TsType type, row_t * row) {
    RC rc = RCOK;
    ts_t ts = txn->get_commit_timestamp();
    ts_t start_ts = txn->get_start_timestamp();
    uint64_t starttime = get_sys_clock();
    txnid_t txnid = txn->get_txn_id();
    //v1-->v2(s, +)===>v3
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
    if (type == R_REQ) {
        // Add the si-read lock
        get_lock(LOCK_SH, txn);

        // Read the row
        rc = RCOK;
        SSIHisEntry *ptr_entry, *latest_entry = writehis;
        //written by myself, OK. return directly, now the ycsb and tpcc have no such case
        if (lastest_entry != nullptr && 
            txnid == latest_entry->txn) { 
            txn->cur_row = latest_entry->row;
        } else {
           ptr_entry = latest_entry;
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
           while (ptr_entry != nullptr && 
                  ptr_entry->commit_ts > start_ts) {
              if (ptr_entry->txn->is_out_rw()) {
                  rc = Abort;
                  goto end;
              }
              ptr_entry->txn->set_in_rw(*(ptr_entry->txn), *txn);
              ptr_entry = ptr_entry->next;
              
           }
           
           if (ptr_entry != NULL) {
                 txn->cur_row = ptr_entry->cur_row;
                 ptr_entry->visitors.emplace_back(txn);
           } else 
                 txn->cur_row = _row;
           
           assert(strstr(_row->get_table_name(), ret->get_table_name()));
        } 
        
    } else if (type == P_REQ) {
        // Add the write lock
        get_lock(LOCK_EX, txn);
       
        /**
         * if there is a SIREAD lock(r1) on x with r1.owner is running
         * or commit(r1.owner) > begin(T):
         *    if r1.owner is committed and r1.owner.in_rw:
         *       abort (T)
         *    set r1.owner.out_rw = true;
         *    set T.in_rw = true;
         **/
        
        SSIHisEntry  *latest_entry = writehis;

        for (const auto& r_txn : lastest_entry->visitors) {
            assert(r_txn != nullptr);
            if (r_txn->get_txn_id() == txn->get_txn_id()) 
               continue;
            const auto r_txn_state = r_txn->state();
            if (r_txn_state == TxnManager::my_state::ACTIVE ||
                r_txn->get_commit_timestamp() > ts) {
               if (r_txn_state == TxnManager::my_state::COMMITTED &&
                   r_txn->is_in_rw) {
                       rc = Abort;
                       goto end;
                }
                r_txn->set_out_rw(*r_txn, *txn);

            }
           
        }

        if (preq_len < g_max_pre_req){
            DEBUG("buf P_REQ %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
            buffer_req(P_REQ, txn);
            rc = RCOK;
        } else  {
            rc = Abort;
        }
    } else if (type == W_REQ) {
        rc = RCOK;
        release_lock(LOCK_EX, txn);
        release_lock(LOCK_COM, txn);
        //TODO: here need to consider whether need to release the si-read lock.
        // release_lock(LOCK_SH, txn);

        // the corresponding prewrite request is debuffered.
        insert_history(ts, txn, row);
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        SSIReqEntry * req = debuffer_req(P_REQ, txn);
        assert(req != NULL);
        return_req_entry(req);
    } else if (type == XP_REQ) {
        release_lock(LOCK_EX, txn);
        release_lock(LOCK_COM, txn);
        //TODO: here need to consider whether need to release the si-read lock.
        release_lock(LOCK_SH, txn);

        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        SSIReqEntry * req = debuffer_req(P_REQ, txn);
        assert (req != NULL);
        return_req_entry(req);
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
