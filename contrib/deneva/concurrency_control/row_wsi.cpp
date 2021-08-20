/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#include "txn.h"
#include "row.h"
#include "helper.h"
#include "manager.h"
#include "row_wsi.h"
#include "mem_alloc.h"
#include <jemalloc/jemalloc.h>

void Row_wsi::init(row_t * row) {
    _row = row;

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
    lastCommit = 0;
    preq_len = 0;
}

void
Row_wsi::update_last_commit(uint64_t ts) {
    if (lastCommit < ts) lastCommit = ts;
}

row_t * Row_wsi::clear_history(TsType type, ts_t ts) {
    WSIHisEntry ** queue;
    WSIHisEntry ** tail;
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
    WSIHisEntry * his = *tail;
    WSIHisEntry * prev = NULL;
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
        if (type == R_REQ) {
            rhis_len --;
        } else {
            whis_len --;
        }
    }
    *tail = his;
    if (*tail) {
        (*tail)->next = NULL;
    }
    if (his == NULL) {
        *queue = NULL;
    }
    return row;
}

WSIReqEntry * Row_wsi::get_req_entry() {
    return (WSIReqEntry *) malloc(sizeof(WSIReqEntry));
}

void Row_wsi::return_req_entry(WSIReqEntry * entry) {
    free(entry);
}

WSIHisEntry * Row_wsi::get_his_entry() {
    return (WSIHisEntry *) malloc(sizeof(WSIHisEntry));
}

void Row_wsi::return_his_entry(WSIHisEntry * entry) {
    if (entry->row != NULL) {
        free(entry->row);
    }
    free(entry);
}

void Row_wsi::buffer_req(TsType type, TxnManager * txn)
{
    WSIReqEntry * req_entry = get_req_entry();
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
WSIReqEntry * Row_wsi::debuffer_req( TsType type, TxnManager * txn) {
    WSIReqEntry ** queue = &prereq_mvcc;
    WSIReqEntry * return_queue = NULL;

    WSIReqEntry * req = *queue;
    WSIReqEntry * prev_req = NULL;
    if (txn != NULL) {
        assert(type == P_REQ);
        while (req != NULL && req->txn != txn) {
            prev_req = req;
            req = req->next;
        }
        assert(req != NULL);
        if (prev_req != NULL) {
            prev_req->next = req->next;
        } else {
            assert( req == *queue );
            *queue = req->next;
        }
        preq_len --;
        req->next = return_queue;
        return_queue = req;
    }
    return return_queue;
}

void Row_wsi::insert_history(ts_t ts, TxnManager * txn, row_t * row)
{
    WSIHisEntry * new_entry = get_his_entry();
    new_entry->ts = ts;
    // new_entry->txnid = txn->get_txn_id();
    new_entry->row = row;
    if (row != NULL) {
        whis_len ++;
    } else {
        rhis_len ++;
    }
    WSIHisEntry ** queue = (row == NULL) ? &(readhis) : &(writehis);
    WSIHisEntry ** tail = (row == NULL) ? &(readhistail) : &(writehistail);
    WSIHisEntry * his = *queue;
    while (his != NULL && ts < his->ts) {
        his = his->next;
    }

    if (his) {
        LIST_INSERT_BEFORE(his, new_entry,(*queue));
    } else
        LIST_PUT_TAIL((*queue), (*tail), new_entry);
}

RC Row_wsi::access(TxnManager * txn, TsType type, row_t * row) {
    RC rc = RCOK;
    ts_t ts = txn->get_commit_timestamp();
    ts_t start_ts = txn->get_start_timestamp();
    uint64_t starttime = get_sys_clock();

    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
    INC_STATS(txn->get_thd_id(), txn_cc_manager_time, get_sys_clock() - starttime);
    if (type == R_REQ) {
        // Read the row
        rc = RCOK;
        WSIHisEntry * whis = writehis;
        while (whis != NULL && whis->ts > start_ts) {
            whis = whis->next;
        }
        row_t * ret = (whis == NULL) ? _row : whis->row;
        txn->cur_row = ret;
        insert_history(start_ts, txn, NULL);
        assert(strstr(_row->get_table_name(), ret->get_table_name()));
    } else if (type == P_REQ) {
        if (preq_len < g_max_pre_req){
            DEBUG("buf P_REQ %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
            buffer_req(P_REQ, txn);
            rc = RCOK;
        } else  {
            rc = Abort;
        }
    } else if (type == W_REQ) {
        uint64_t write_start = get_sys_clock();
        rc = RCOK;
        // the corresponding prewrite request is debuffered.
        insert_history(ts, txn, row);
        INC_STATS(txn->get_thd_id(), trans_write_time, get_sys_clock() - write_start);
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        WSIReqEntry * req = debuffer_req(P_REQ, txn);
        assert(req != NULL);
        return_req_entry(req);
    } else if (type == XP_REQ) {
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        WSIReqEntry * req = debuffer_req(P_REQ, txn);
        assert (req != NULL);
        return_req_entry(req);
    } else {
        assert(false);
    }

    if (rc == RCOK) {
        uint64_t write_start = get_sys_clock();
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
            if (whis_len > 1 &&
                writehistail->prev->ts < t_th) {
                row_t * latest_row = clear_history(W_REQ, t_th);
                if (latest_row != NULL) {
                    assert(_row != latest_row);
                    _row->copy(latest_row);
                }
            }
        }
        INC_STATS(txn->get_thd_id(), trans_access_clear_time, get_sys_clock() - write_start);
    }

    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;

    INC_STATS(txn->get_thd_id(), txn_useful_time, timespan);

    if (g_central_man) {
        glob_manager.release_row(_row);
    } else {
        pthread_mutex_unlock( latch );
    }

    return rc;
}
