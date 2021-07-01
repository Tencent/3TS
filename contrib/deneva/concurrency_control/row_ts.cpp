/*
     Copyright 2016 Massachusetts Institute of Technology

     Licensed under the Apache License, Version 2.0 (the "License");
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at

             http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
     limitations under the License.
*/

//#include "timestamp.h"
#include "txn.h"
#include "row.h"
#include "row_ts.h"
#include "mem_alloc.h"
#include "manager.h"
#include "stdint.h"
#include <jemalloc/jemalloc.h>

void Row_ts::init(row_t * row) {
    _row = row;
    wts = 0;
    rts = 0;
    min_wts = UINT64_MAX;
    min_rts = UINT64_MAX;
    min_pts = UINT64_MAX;
    readreq = NULL;
    writereq = NULL;
    prereq = NULL;
    preq_len = 0;
    //latch = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
    latch = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init( latch, NULL );
    blatch = false;
}

TsReqEntry * Row_ts::get_req_entry() {
    return (TsReqEntry *) malloc(sizeof(TsReqEntry));
}

void Row_ts::return_req_entry(TsReqEntry * entry) {
    if (entry->row != NULL) {
        // entry->row->free_row();
        // mem_allocator.free(entry->row, sizeof(row_t));
        free(entry->row);
    }
    // mem_allocator.free(entry, sizeof(TsReqEntry));
    free(entry);
}

void Row_ts::return_req_list(TsReqEntry * list) {
    TsReqEntry * req = list;
    TsReqEntry * prev = NULL;
    while (req != NULL) {
        prev = req;
        req = req->next;
        return_req_entry(prev);
    }
}

void Row_ts::buffer_req(TsType type, TxnManager *txn, row_t *row) {
    TsReqEntry * req_entry = get_req_entry();
    assert(req_entry != NULL);
    req_entry->txn = txn;
    req_entry->row = row;
    req_entry->ts = txn->get_timestamp();
    req_entry->starttime = get_sys_clock();
    if (type == R_REQ) {
        req_entry->next = readreq;
        readreq = req_entry;
        if (req_entry->ts < min_rts) min_rts = req_entry->ts;
    } else if (type == W_REQ) {
        assert(row != NULL);
        req_entry->next = writereq;
        writereq = req_entry;
        if (req_entry->ts < min_wts) min_wts = req_entry->ts;
    } else if (type == P_REQ) {
        preq_len ++;
        req_entry->next = prereq;
        prereq = req_entry;
        if (req_entry->ts < min_pts) min_pts = req_entry->ts;
    }
}

TsReqEntry * Row_ts::debuffer_req(TsType type, TxnManager * txn) {
    return debuffer_req(type, txn, UINT64_MAX);
}

TsReqEntry *Row_ts::debuffer_req(TsType type, ts_t ts) { return debuffer_req(type, NULL, ts); }

TsReqEntry * Row_ts::debuffer_req( TsType type, TxnManager * txn, ts_t ts ) {
    TsReqEntry ** queue;
    TsReqEntry * return_queue = NULL;
    switch (type) {
        case R_REQ:
            queue = &readreq;
            break;
        case P_REQ:
            queue = &prereq;
            break;
        case W_REQ:
            queue = &writereq;
            break;
        default:
            assert(false);
    }

    TsReqEntry * req = *queue;
    TsReqEntry * prev_req = NULL;
    if (txn != NULL) {
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
    } else {
        while (req != NULL) {
            if (req->ts <= ts) {
                if (prev_req == NULL) {
                    assert(req == *queue);
                    *queue = (*queue)->next;
                } else {
                    prev_req->next = req->next;
                }
                req->next = return_queue;
                return_queue = req;
                req = (prev_req == NULL)? *queue : prev_req->next;
            } else {
                prev_req = req;
                req = req->next;
            }
        }
    }
    return return_queue;
}

ts_t Row_ts::cal_min(TsType type) {
    // update the min_pts
    TsReqEntry * queue;
    switch (type) {
        case R_REQ:
            queue = readreq;
            break;
        case P_REQ:
            queue = prereq;
            break;
        case W_REQ:
            queue = writereq;
            break;
        default:
            assert(false);
    }
    ts_t new_min_pts = UINT64_MAX;
    TsReqEntry * req = queue;
    while (req != NULL) {
        if (req->ts < new_min_pts) new_min_pts = req->ts;
        req = req->next;
    }
    return new_min_pts;
}

RC Row_ts::access(TxnManager * txn, TsType type, row_t * row) {
    RC rc;
    uint64_t starttime = get_sys_clock();
    ts_t ts = txn->get_timestamp();
    if (g_central_man)
        glob_manager.lock_row(_row);
    else
        pthread_mutex_lock( latch );
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
    INC_STATS(txn->get_thd_id(), txn_cc_manager_time, get_sys_clock() - starttime);
    if (type == R_REQ) {
        uint64_t read_start = get_sys_clock();
        if (ts < wts) { // read would occur before most recent write
            rc = Abort;
            INC_STATS(txn->get_thd_id(), total_read_abort_cnt,1);
        } else if (ts > min_pts) { // read would occur after one of the prereqs already queued
            // insert the req into the read request queue
            buffer_req(R_REQ, txn, NULL);
            txn->ts_ready = false;
            rc = WAIT;
        } else { // read is ok
            // return the value.
            txn->cur_row->copy(_row);
            if (rts < ts) rts = ts;
            rc = RCOK;
        }
        
        uint64_t read_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_read_time, read_end - read_start);

    } else if (type == P_REQ) {
        uint64_t pre_start  = get_sys_clock();
        if (ts < rts) { // pre-write would occur before most recent read
            rc = Abort;
            INC_STATS(txn->get_thd_id(), total_write_abort_cnt,1);
        } else {
#if TS_TWR
            buffer_req(P_REQ, txn, NULL);
            rc = RCOK;
#else
            if (ts < wts) { //pre-write would occur before most recent write
                rc = Abort;
                INC_STATS(txn->get_thd_id(), total_write_abort_cnt,1);
            } else { // pre-write is ok
                //printf("Buffer P_REQ %ld\n",txn->txn_id);
                buffer_req(P_REQ, txn, NULL);
                rc = RCOK;
            }
            uint64_t pre_end = get_sys_clock();
            INC_STATS(txn->get_thd_id(), trans_access_pre_time, pre_end - pre_start);
#endif
        }
    } else if (type == W_REQ) {
        uint64_t write_start  = get_sys_clock();
        // write requests are always accepted.
        rc = RCOK;
#if TS_TWR
        // according to TWR, this write is already stale, ignore.
        if (ts < wts) {
            TsReqEntry * req = debuffer_req(P_REQ, txn);
            assert(req != NULL);
            update_buffer(txn->get_thd_id());
            return_req_entry(req);
            // row->free_row();
            // mem_allocator.free(row, sizeof(row_t));
            free(row);
            goto final;
        }
#else
        if (ts > min_pts) { // write should happen after older writes are processed
            buffer_req(W_REQ, txn, row);
            goto final;
        }
#endif
        if (ts > min_rts) { // write should happen after older reads are processed
            row = txn->cur_row;
            buffer_req(W_REQ, txn, row);
                        goto final;
        } else { // write is ok;
            // the write is output.
            _row->copy(row);
            if (wts < ts) wts = ts;
            // debuffer the P_REQ
            TsReqEntry * req = debuffer_req(P_REQ, txn);
            assert(req != NULL);
            update_buffer(txn->get_thd_id());
            return_req_entry(req);
            // the "row" is freed after hard copy to "_row"
            // row->free_row();
            // mem_allocator.free(row, sizeof(row_t));
            free(row);
        }
        
        uint64_t write_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_write_time, write_end - write_start);
        
    } else if (type == XP_REQ) {
        uint64_t xp_start  = get_sys_clock();
        TsReqEntry * req = debuffer_req(P_REQ, txn);
        assert (req != NULL);
        update_buffer(txn->get_thd_id());
        return_req_entry(req);
        
        uint64_t xp_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_xp_time, xp_end - xp_start);
        
    } else assert(false);

final:
        uint64_t timespan = get_sys_clock() - starttime;
        txn->txn_stats.cc_time += timespan;
        txn->txn_stats.cc_time_short += timespan;
    INC_STATS(txn->get_thd_id(), total_access_time, timespan);
    INC_STATS(txn->get_thd_id(), txn_useful_time, timespan);
    if (g_central_man)
        glob_manager.release_row(_row);
    else
        pthread_mutex_unlock( latch );

    return rc;
}

void Row_ts::update_buffer(uint64_t thd_id) {

    while (true) {
        ts_t new_min_pts = cal_min(P_REQ);
        assert(new_min_pts >= min_pts);
        if (new_min_pts > min_pts)
            min_pts = new_min_pts;
        else
            break;  // min_pts is not updated.
        // debuffer readreq. ready_read can be a list
        TsReqEntry * ready_read = debuffer_req(R_REQ, min_pts);
        if (ready_read == NULL) break;
        // for each debuffered readreq, perform read.
        TsReqEntry * req = ready_read;
        while (req != NULL) {
            if (req->txn->txn && req->ts == req->txn->get_timestamp()) {
                req->txn->cur_row->copy(_row);
                if (rts < req->ts) {
                    rts = req->ts;
                }
                req->txn->ts_ready = true;
                uint64_t timespan = get_sys_clock() - req->starttime;
                req->txn->txn_stats.cc_block_time += timespan;
                req->txn->txn_stats.cc_block_time_short += timespan;
                txn_table.restart_txn(thd_id,req->txn->get_txn_id(),0);
            }
            req = req->next;
        }
        // return all the req_entry back to freelist
        return_req_list(ready_read);
        // re-calculate min_rts
        ts_t new_min_rts = cal_min(R_REQ);
        if (new_min_rts > min_rts)
            min_rts = new_min_rts;
        else
            break;
        // debuffer writereq
        TsReqEntry * ready_write = debuffer_req(W_REQ, min_rts);
        if (ready_write == NULL) break;
        ts_t young_ts = UINT64_MAX;
        TsReqEntry * young_req = NULL;
        req = ready_write;
        while (req != NULL) {
            TsReqEntry * tmp_req = debuffer_req(P_REQ, req->txn);
            assert(tmp_req != NULL);
            return_req_entry(tmp_req);
            if (req->ts < young_ts) {
                young_ts = req->ts;
                young_req = req;
            } //else loser = req;
            req = req->next;
        }
        // perform write.
        _row->copy(young_req->row);
        if (wts < young_req->ts) wts = young_req->ts;
        return_req_list(ready_write);
    }
}
