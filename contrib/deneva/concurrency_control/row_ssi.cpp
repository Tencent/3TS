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
    // si_read = new vector<TxnManager*>;
    // wrt_list = new vector<TxnManager*>;
    si_read_lock = NULL;
    write_lock = NULL;
    _size = 32;
    si_read_lock = (SSILockEntry *) malloc(sizeof(SSILockEntry)*_size);
    memset(si_read_lock, 0, sizeof(si_read_lock));
    si_read_lock->size = _size;
    (si_read_lock+_size-1)->next = NULL;
    read_now = si_read_lock;
    read_cnt = 0;
    write_lock = (SSILockEntry *) malloc(sizeof(SSILockEntry)*_size);
    memset(write_lock, 0, sizeof(write_lock));
    write_lock->size = _size;
    (write_lock+_size-1)->next = NULL;
    write_now = write_lock;
    write_cnt = 0;
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
    entry->txnid = 0;

    return entry;
}

void Row_ssi::get_lock(lock_t type, TxnManager *& txn) {
    //  SSILockEntry * entry = get_entry();
    // entry->type = type;
    // //entry->txn = std::shared_ptr<TxnManager>(txn);
    // entry->txn = txn;
    // entry->start_ts = get_sys_clock();
    // entry->txnid = txn->get_txn_id();
    // if (type == LOCK_SH)
    //     STACK_PUSH(si_read_lock, entry);
    // if (type == LOCK_EX)
    //     STACK_PUSH(write_lock, entry);
    if(type == LOCK_SH) {
        // if(si_read_lock == NULL) {
        //     si_read_lock = (SSILockEntry *) malloc(sizeof(SSILockEntry)*_size);
        //     memset(si_read_lock, 0, sizeof(si_read_lock));
        //     si_read_lock->size = _size;
        //     (si_read_lock+_size-1)->next = NULL;
        //     read_now = si_read_lock;
        //     read_cnt = 0;
        // }
        // else if(read_cnt == _size){
        //     _size <<= 1;
        //     SSILockEntry * entry = (SSILockEntry *) malloc(sizeof(SSILockEntry)*_size);
        //     memset(entry, 0, sizeof(entry));
        //     entry->size = _size;
        //     (read_now+read_now->size-1)->next = entry;
        //     (entry+_size-1)->next = NULL;
        //     read_now = entry;
        //     read_cnt = 0;
        // }
        (read_now+read_cnt)->txn = txn;
        (read_now+read_cnt)->start_ts = get_sys_clock();
        (read_now+read_cnt)->txnid = txn->get_txn_id();
        (read_now+read_cnt)->active = true;
        ++read_cnt;
    }
    if(type == LOCK_EX) {
        // if(write_lock == NULL) {
        //     write_lock = (SSILockEntry *) malloc(sizeof(SSILockEntry)*_size);
        //     memset(write_lock, 0, sizeof(write_lock));
        //     write_lock->size = _size;
        //     (write_lock+_size-1)->next = NULL;
        //     write_now = write_lock;
        //     write_cnt = 0;
        // }
        // else if(write_cnt == _size){
        //     _size <<= 1;
        //     SSILockEntry * entry = (SSILockEntry *) malloc(sizeof(SSILockEntry)*_size);
        //     memset(entry, 0, sizeof(entry));
        //     entry->size = _size;
        //     (write_now+write_now->size-1)->next = entry;
        //     (entry+_size-1)->next = NULL;
        //     write_now = entry;
        //     write_cnt = 0;
        // }
        (write_now+write_cnt)->txn = txn;
        (write_now+write_cnt)->start_ts = get_sys_clock();
        (write_now+write_cnt)->txnid = txn->get_txn_id();
        (write_now+write_cnt)->active = true;
        ++write_cnt;
    }

    // if(type == LOCK_SH)
    //     si_read->emplace_back(txn);
    // if(type == LOCK_EX)
    //     wrt_list->emplace_back(txn);
    // vector<TxnManager*>::iterator it = si_read->begin();
    // vector<TxnManager*>::iterator it1 = wrt_list->begin();
    // it ==it1;
    // if(type == LOCK_COM)
    //     commit_lock = txn->get_txn_id();
}

void Row_ssi::release_lock(lock_t type, TxnManager * txn) {
    if (type == LOCK_SH) {
        SSILockEntry * read = si_read_lock;
        SSILockEntry * pre_read = NULL;
        // bool is_delete = false;
        // while (read != NULL) {
        //     SSILockEntry * nex = read->next;
        //     if (read->txnid == txn->get_txn_id()) {
        //         assert(read != NULL);
        //         is_delete = true;
        //         SSILockEntry * delete_p = read;
        //         //if (pre_read != NULL)
        //         //   pre_read->next = read->next;
        //         if(pre_read != NULL) {
        //             pre_read->next = read->next;
        //         } 
        //         else {
        //             assert( read == si_read_lock );
        //             si_read_lock = read->next;
        //         }
        //         //read->next = NULL;
        //         delete_p->next = NULL;
        //         delete delete_p;
        //     }
        //     else 
        //         is_delete = false;
        //     if(!is_delete) pre_read = read;
        //     //read = read->next;
        //     read = nex;
        // }
        while(read != NULL) {
            bool is_delete = true;
            uint64_t block_size = read->size;
            //SSILockEntry * nex = (read+block_size-1)->next;
            for(uint64_t i = 0; i < block_size; ++i) {
                SSILockEntry * now = read+i;
                if(now->txn == NULL) {
                    is_delete = false;
                    break;
                }
                if(now->txnid == txn->get_txn_id()) {
                    assert(now != NULL);
                    now->active = false;
                }
                //if(now->active == true) is_delete = false;
            }
            // if(is_delete) {
            //     SSILockEntry * delete_p = read;
            //     if(pre_read != NULL) 
            //         (pre_read+block_size-1)->next = nex;
            //     else
            //         si_read_lock = nex;
            //     free(delete_p);
            // }
            // if(!is_delete) pre_read = read;
            read = nex;
        }
        // for(vector<TxnManager*>::iterator it = si_read->begin(); it != si_read->end(); ) {
        //     assert(*it != NULL);
        //     if((*it)->get_txn_id() == txn->get_txn_id()) {
        //         *it = NULL;
        //         si_read->erase(it);
        //         continue;
        //     }
        //     ++it;
        // }
    }
    if (type == LOCK_EX) {
        SSILockEntry * write = write_lock;
        SSILockEntry * pre_write = NULL;
        // bool is_delete = false;
	    // while (write != NULL) {
        //     SSILockEntry * nex = write->next;
        //     if (write->txnid == txn->get_txn_id()) {
        //         assert(write != NULL);
        //         is_delete = true;
        //         SSILockEntry * delete_p = write;
        //         //if (pre_write != NULL)
        //         //    pre_write->next = write->next;
        //         if(pre_write != NULL) {
        //             pre_write->next = write->next;
        //         }
        //         else {
        //             assert( write == write_lock );
        //             write_lock = write->next;
        //         }
        //         //write->next = NULL;
        //         delete_p->next = NULL;
        //         delete delete_p;
        //     }
        //     else
        //         is_delete = false;
        //     if(!is_delete) pre_write = write;
        //     //write = write->next;
        //     write = nex;
        // }
        while(write != NULL) {
            bool is_delete = true;
            uint64_t block_size = write->size;
            SSILockEntry * nex = (write+block_size-1)->next;
            for(uint64_t i = 0; i < block_size; ++i) {
                SSILockEntry * now = write+i;
                if(now->txn == NULL) {
                    is_delete = false;
                    break;
                }
                if(now->txnid == txn->get_txn_id()) {
                    assert(now != NULL);
                    now->active = false;
                }
                //if(now->active == true) is_delete = false;
            }
            // if(is_delete) {
            //     SSILockEntry * delete_p = write;
            //     if(pre_write != NULL) 
            //         (pre_write+block_size-1)->next = nex;
            //     else
            //         write_lock = nex;
            //     free(delete_p);
            // }
            // if(!is_delete) pre_write = write;
            write = nex;
        }
        // for(vector<TxnManager*>::iterator it = wrt_list->begin(); it != wrt_list->end(); ) {
        //     assert(*it != NULL);
        //     if((*it)->get_txn_id() == txn->get_txn_id()) {
        //         *it = NULL;
        //         wrt_list->erase(it);
        //         continue;
        //     }
        //     ++it;
        // }
    }
}

void Row_ssi::release_lock(ts_t min_ts) {
    SSILockEntry * read = si_read_lock;
    SSILockEntry * pre_read = NULL;
    // bool is_delete = false;
    // while (read != NULL) {
    //     SSILockEntry * nex = read->next;
    //     if (read->start_ts < min_ts) {
    //         assert(read != NULL);
    //         is_delete = true;
    //         SSILockEntry * delete_p = read;
    //         if(pre_read != NULL) {
    //             pre_read->next = read->next;
    //         } 
    //         else {
    //             assert( read == si_read_lock );
    //             si_read_lock = read->next;
    //         }
    //         delete_p->next = NULL;
    //         delete delete_p;
    //     }
    //     else 
    //         is_delete = false;
    //     if(!is_delete) pre_read = read;
    //     read = nex;
    // }
    while(read != NULL) {
        bool is_delete = true;
        uint64_t block_size = read->size;
        SSILockEntry * nex = (read+block_size-1)->next;
        for(uint64_t i = 0; i < block_size; ++i) {
            SSILockEntry * now = read+i;
            if(now->txn == NULL) {
                    is_delete = false;
                    break;
                }
            if(now->start_ts < min_ts) {
                assert(now != NULL);
                now->active = false;
            }
            //if(now->active == true) is_delete = false;
        }
        // if(is_delete) {
        //     SSILockEntry * delete_p = read;
        //     if(pre_read != NULL) 
        //         (pre_read+block_size-1)->next = nex;
        //     else
        //         si_read_lock = nex;
        //     free(delete_p);
        // }
        // if(!is_delete) pre_read = read;
        read = nex;
    }
    // for(vector<TxnManager*>::iterator it = si_read->begin(); it != si_read->end(); ) {
    //     assert(*it != NULL);
    //     if((*it)->get_start_timestamp() < min_ts) {
    //         *it = NULL;
    //         si_read->erase(it);
    //         continue;
    //     }
    //     ++it;
    // }
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
        // Traverse the whole write lock, only one active and waiting
        // as it is more than one txn it is the WW conflict, violating serialization.
        SSILockEntry * write = write_lock;
        while (write != NULL) {
            uint64_t block_size = write->size;
            for(uint64_t i = 0; i < block_size; ++i) {
                SSILockEntry * now = write+i;
                if(now->txn == NULL) break;
                if(now->active == false) continue;
                if (now->txnid == txnid) continue;
                now->txn->in_rw = true;
                txn->out_rw = true;
                DEBUG("ssi read the write_lock in %ld out %ld\n",now->txnid, txnid);
            }
            
            //inout_table.set_inConflict(txn->get_thd_id(), write->txnid, txnid);
            //inout_table.set_outConflict(txn->get_thd_id(), txnid, write->txnid);
            //(write->txn.get())->in_rw = true;
            write = (write+block_size-1)->next;
        }
        // for(vector<TxnManager*>::iterator it = wrt_list->begin(); it != wrt_list->end(); ++it) {
        //     assert(*it != NULL);
        //     if((*it)->get_txn_id() == txnid) continue;
        //     (*it)->in_rw = true;
        //     txn->out_rw = true;
        // }
        
        // Check to see if two RW dependencies exist.
        // Add RW dependencies
        SSIHisEntry* whis = writehis;
        while (whis != NULL && whis->ts > start_ts) {
            //bool out = inout_table.get_outConflict(txn->get_thd_id(),whis->txnid);
            // if (whis->txn.get()->out_rw) { //! Abort
            if (whis->txn->out_rw) { //! Abort
                rc = Abort;
                //INC_STATS(txn->get_thd_id(),total_txn_abort_cnt,1);
                INC_STATS(txn->get_thd_id(),total_rw_abort_cnt,1);
                DEBUG("ssi txn %ld read the write_commit in %ld abort, whis_ts %ld current_start_ts %ld\n",
                  txnid, whis->txnid, whis->ts, start_ts);
                goto end;             
            }
            //inout_table.set_inConflict(txn->get_thd_id(), whis->txnid, txnid);
            //inout_table.set_outConflict(txn->get_thd_id(), txnid, whis->txnid);
            // whis->txn.get()->in_rw = true;
            whis->txn->in_rw = true;
            txn->out_rw = true;
            DEBUG("ssi read the write_commit in %ld out %ld\n",whis->txnid, txnid);
            whis = whis->next;
        }

        row_t * ret = (whis == NULL) ? _row : whis->row;
        txn->cur_row = ret;
        assert(strstr(_row->get_table_name(), ret->get_table_name()));

        uint64_t read_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_read_time, read_end - read_start);

    } else if (type == P_REQ) {
        uint64_t pre_start  = get_sys_clock();
        //WW conflict
        //if (write_lock != NULL && write_lock->txn.get() != txn) {
        // if (write_lock != NULL && write_lock->txn != txn && write_lock->start_ts != 1e18) {
        //     rc = Abort;
        //     //INC_STATS(txn->get_thd_id(),total_txn_abort_cnt,1);
        //     INC_STATS(txn->get_thd_id(),total_ww_abort_cnt,1);
        //     INC_STATS(txn->get_thd_id(), trans_access_pre_time, get_sys_clock() - pre_start);
        //     goto end;
        // }
        SSILockEntry * write = write_lock;
        while (write != NULL) {
            uint64_t block_size = write->size;
            for(uint64_t i = 0; i < block_size; ++i) {
                SSILockEntry * now = write+i;
                if(now->txn == NULL) break;
                if(now->active == false) continue;
                if (now->txnid == txnid) continue;
                rc = Abort;
                INC_STATS(txn->get_thd_id(),total_ww_abort_cnt,1);
                goto end;
            }
            
            //inout_table.set_inConflict(txn->get_thd_id(), write->txnid, txnid);
            //inout_table.set_outConflict(txn->get_thd_id(), txnid, write->txnid);
            //(write->txn.get())->in_rw = true;
            write = (write+block_size-1)->next;
        }
        // Add the write lock
        uint64_t lock_start = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_pre_before_time, lock_start - pre_start);
        get_lock(LOCK_EX, txn);
        uint64_t lock_end = get_sys_clock();
        INC_STATS(txn->get_thd_id(), trans_access_pre_lock_time, lock_end - lock_start);
        // Traverse the whole read his
        //SSILockEntry * si_read = si_read_lock;
        SSILockEntry * si_read = si_read_lock;
        uint64_t start1 = get_sys_clock();
        // while (si_read != NULL) {
        //     if (si_read->txnid == txnid) {
        //         si_read = si_read->next;
        //         continue;
        //     }
        //     //bool is_active = si_read_lock->txn.get()->txn_status == TxnStatus::ACTIVE;
	    //     //bool interleaved =  si_read_lock->txn.get()->txn_status == TxnStatus::COMMITTED &&
        //     //	si_read->txn.get()->get_commit_timestamp() > start_ts;
        //     bool is_active = si_read->txn->txn_status == TxnStatus::ACTIVE;
        //     bool interleaved =  si_read->txn->txn_status == TxnStatus::COMMITTED &&
        //         si_read->txn->get_commit_timestamp() > start_ts;
        //     if (is_active || interleaved) {
        //         //bool in = si_read->txn.get()->in_rw;
        //         bool in = si_read->txn->in_rw;
        //         if (in && interleaved) { //! Abort
        //             rc = Abort;
        //             //INC_STATS(txn->get_thd_id(),total_txn_abort_cnt,1);
        //             INC_STATS(txn->get_thd_id(),total_rw_abort_cnt,1);
        //             DEBUG("ssi txn %ld write the read_commit in %ld abort, rhis_ts %ld current_start_ts %ld\n",
        //               //txnid, si_read->txnid, si_read->txn.get()->get_commit_timestamp(), start_ts);
        //               txnid, si_read->txnid, si_read->txn->get_commit_timestamp(), start_ts);
        //             goto end;
        //         } 
        //         if (is_active) {
        //             //inout_table.set_outConflict(txn->get_thd_id(), si_read->txnid, txnid);
        //             //inout_table.set_inConflict(txn->get_thd_id(), txnid, si_read->txnid);
        //             assert(si_read->txn != NULL);
        //             si_read->txn->out_rw = true;
        //             txn->in_rw = true;
        //             DEBUG("ssi write the si_read_lock out %ld in %ld\n", si_read->txnid, txnid);
        //         }
        //     }
        //     si_read = si_read->next;
        // }

        while(si_read != NULL) {
            uint64_t block_size = si_read->size;
            for(uint64_t i = 0; i < block_size; ++i) {
                SSILockEntry * now = si_read+i;
                if(now->txn == NULL) break;
                if(now->active == false) continue;
                if (now->txnid == txnid) continue;
                bool is_active = now->txn->txn_status == TxnStatus::ACTIVE;
                bool interleaved =  now->txn->txn_status == TxnStatus::COMMITTED &&
                    now->txn->get_commit_timestamp() > start_ts;
                if (is_active || interleaved) {
                    bool in = now->txn->in_rw;
                    if (in && interleaved) { //! Abort
                        rc = Abort;
                        INC_STATS(txn->get_thd_id(),total_rw_abort_cnt,1);
                        DEBUG("ssi txn %ld write the read_commit in %ld abort, rhis_ts %ld current_start_ts %ld\n",
                        //txnid, si_read->txnid, si_read->txn.get()->get_commit_timestamp(), start_ts);
                        txnid, now->txnid, now->txn->get_commit_timestamp(), start_ts);
                        goto end;
                    } 
                    if (is_active) {
                        //inout_table.set_outConflict(txn->get_thd_id(), si_read->txnid, txnid);
                        //inout_table.set_inConflict(txn->get_thd_id(), txnid, si_read->txnid);
                        assert(now->txn != NULL);
                        now->txn->out_rw = true;
                        txn->in_rw = true;
                        DEBUG("ssi write the si_read_lock out %ld in %ld\n", si_read->txnid, txnid);
                    }
                }
            }
            si_read = (si_read+block_size-1)->next;
        }

        // for(vector<TxnManager*>::iterator it = si_read->begin(); it != si_read->end(); ++it) {
        //     assert(*it != NULL);
        //     if((*it)->get_txn_id() == txnid) continue;
        //     bool is_active = (*it)->txn_status == TxnStatus::ACTIVE;
        //     bool interleaved =  (*it)->txn_status == TxnStatus::COMMITTED &&
        //         (*it)->get_commit_timestamp() > start_ts;
        //     if(is_active || interleaved) {
        //         bool in = (*it)->in_rw;
        //         if (in && interleaved) { //! Abort
        //             rc = Abort;
        //             INC_STATS(txn->get_thd_id(),total_rw_abort_cnt,1);
        //             DEBUG("ssi txn %ld write the read_commit in %ld abort, rhis_ts %ld current_start_ts %ld\n",
        //               txnid, (*it)->get_txn_id(), (*it)->get_commit_timestamp(), start_ts);
        //             goto end;
        //         } 
        //         if (is_active) {
        //             assert(*it != NULL);
        //             (*it)->out_rw = true;
        //             txn->in_rw = true;
        //             DEBUG("ssi write the si_read_lock out %ld in %ld\n", (*it)->get_txn_id(), txnid);
        //         }
        //     }
        // }

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
        //for(int i = 0; i < 10000000; ++i);
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
