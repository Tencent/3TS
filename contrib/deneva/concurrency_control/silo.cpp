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
#include "silo.h"
#include "row_silo.h"

#if CC_ALG == SILO

void Silo::init() { sem_init(&_semaphore, 0, 1); }

RC
Silo::validate_silo(TxnManager * txnmanager)
{
    RC rc = RCOK;
    // lock write tuples in the primary key order.
    uint64_t wr_cnt = txnmanager->txn->write_cnt;
    // write_set = (int *) mem_allocator.alloc(sizeof(int) * wr_cnt);
    int cur_wr_idx = 0;
    int read_set[txnmanager->txn->row_cnt - txnmanager->txn->write_cnt];
    int cur_rd_idx = 0;
    for (uint64_t rid = 0; rid < txnmanager->txn->row_cnt; rid ++) {
        if (txnmanager->txn->accesses[rid]->type == WR)
            write_set[cur_wr_idx ++] = rid;
        else 
            read_set[cur_rd_idx ++] = rid;
    }

    // bubble sort the write_set, in primary key order 
    if (wr_cnt > 1)
    {
        for (uint64_t i = wr_cnt - 1; i >= 1; i--) {
            for (uint64_t j = 0; j < i; j++) {
                if (txnmanager->txn->accesses[ write_set[j] ]->orig_row->get_primary_key() > 
                    txnmanager->txn->accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
                {
                    int tmp = write_set[j];
                    write_set[j] = write_set[j+1];
                    write_set[j+1] = tmp;
                }
            }
        }
    }

    num_locks = 0;
    ts_t max_tid = 0;
    bool done = false;
    if (txnmanager->_pre_abort) {
        for (uint64_t i = 0; i < wr_cnt; i++) {
            row_t * row = txnmanager->txn->accesses[ write_set[i] ]->orig_row;
            if (row->manager->get_tid() != txnmanager->txn->accesses[write_set[i]]->tid) {
                rc = Abort;
                return rc;
            }    
        }    
        for (uint64_t i = 0; i < txnmanager->txn->row_cnt - wr_cnt; i ++) {
            Access * access = txnmanager->txn->accesses[ read_set[i] ];
            if (access->orig_row->manager->get_tid() != txnmanager->txn->accesses[read_set[i]]->tid) {
                rc = Abort;
                return rc;
            }
        }
    }

    // lock all rows in the write set.
    if (txnmanager->_validation_no_wait) {
        while (!done) {
            num_locks = 0;
            for (uint64_t i = 0; i < wr_cnt; i++) {
                row_t * row = txnmanager->txn->accesses[ write_set[i] ]->orig_row;
                if (!row->manager->try_lock())
                {
                    break;
                }
                DEBUG("silo %ld write lock row %ld \n", txnmanager->get_txn_id(), row->get_primary_key());
                row->manager->assert_lock();
                num_locks ++;
                if (row->manager->get_tid() != txnmanager->txn->accesses[write_set[i]]->tid)
                {
                    rc = Abort;
                    return rc;
                }
            }
            if (num_locks == wr_cnt) {
                DEBUG("TRY LOCK true %ld\n", txnmanager->get_txn_id());
                done = true;
            } else {
                rc = Abort;
                return rc;
            }
        }
    } else {
        for (uint64_t i = 0; i < wr_cnt; i++) {
            row_t * row = txnmanager->txn->accesses[ write_set[i] ]->orig_row;
            row->manager->lock();
            DEBUG("silo %ld write lock row %ld \n", txnmanager->get_txn_id(), row->get_primary_key());
            num_locks++;
            if (row->manager->get_tid() != txnmanager->txn->accesses[write_set[i]]->tid) {
                rc = Abort;
                return rc;
            }
        }
    }

    uint64_t lower = silo_time_table.get_lower(txnmanager->get_thd_id(),txnmanager->get_txn_id());
    uint64_t upper = silo_time_table.get_upper(txnmanager->get_thd_id(),txnmanager->get_txn_id());
    DEBUG("MAAT Validate Start %ld: [%lu,%lu]\n",txnmanager->get_txn_id(),lower,upper);
    std::set<uint64_t> after;
    std::set<uint64_t> before;

    // lower bound of txn greater than write timestamp
    if(lower <= txnmanager->greatest_write_timestamp) {
        lower = txnmanager->greatest_write_timestamp + 1;
        INC_STATS(txnmanager->get_thd_id(),maat_case1_cnt,1);
    }
    // lower bound of txn greater than read timestamp
    if(lower <= txnmanager->greatest_read_timestamp) {
        lower = txnmanager->greatest_read_timestamp + 1;
        INC_STATS(txnmanager->get_thd_id(),maat_case3_cnt,1);
    }

    COMPILER_BARRIER

    //RW
    // lower bound of uncommitted writes greater than upper bound of txn
    for(auto it = txnmanager->uncommitted_writes->begin(); it != txnmanager->uncommitted_writes->end();it++) {
        uint64_t it_lower = silo_time_table.get_lower(txnmanager->get_thd_id(),*it);
        if(upper >= it_lower) {
            SILOState state = silo_time_table.get_state(txnmanager->get_thd_id(),*it); //TODO
            if(state == SILO_VALIDATED || state == SILO_COMMITTED) {
                INC_STATS(txnmanager->get_thd_id(),maat_case2_cnt,1);
                if(it_lower > 0) {
                upper = it_lower - 1;
                } else {
                upper = it_lower;
                }
            }
            if(state == SILO_RUNNING) {
                after.insert(*it);
            }
        }
    }

    // // validate rows in the read set
    // // for repeatable_read, no need to validate the read set.
    // for (uint64_t i = 0; i < txnmanager->txn->row_cnt - wr_cnt; i ++) {
    //     Access * access = txnmanager->txn->accesses[ read_set[i] ];
    //     //bool success = access->orig_row->manager->validate(access->tid, false);
    //     if (!success) {
    //         rc = Abort;
    //         return rc;
    //     }
    //     if (access->tid > max_tid)
    //         max_tid = access->tid;
    // }
    // validate rows in the write set


    //WW
    // upper bound of uncommitted write writes less than lower bound of txn
    for(auto it = txnmanager->uncommitted_writes_y->begin(); it != txnmanager->uncommitted_writes_y->end();it++) {
        SILOState state = silo_time_table.get_state(txnmanager->get_thd_id(),*it);
        uint64_t it_upper = silo_time_table.get_upper(txnmanager->get_thd_id(),*it);
        if(state == SILO_ABORTED) {
            continue;
        }
        if(state == SILO_VALIDATED || state == SILO_COMMITTED) {
            if(lower <= it_upper) {
            INC_STATS(txnmanager->get_thd_id(),maat_case5_cnt,1);
            if(it_upper < UINT64_MAX) {
                lower = it_upper + 1;
            } else {
                lower = it_upper;
            }
            }
        }
        if(state == SILO_RUNNING) {
            after.insert(*it);
        }
    }

    //TODO: need to consider whether need WR check or not

    // for (uint64_t i = 0; i < wr_cnt; i++) {
    //     Access * access = txnmanager->txn->accesses[ write_set[i] ];
    //     bool success = access->orig_row->manager->validate(access->tid, true);
    //     if (!success) {
    //         rc = Abort;
    //         return rc;
    //     }
    //     if (access->tid > max_tid)
    //         max_tid = access->tid;
    // }

    // this->max_tid = max_tid;

    if(lower >= upper) {
        // Abort
        silo_time_table.set_state(txnmanager->get_thd_id(),txnmanager->get_txn_id(),SILO_ABORTED);
        rc = Abort;
    } else {
        // Validated
        silo_time_table.set_state(txnmanager->get_thd_id(),txnmanager->get_txn_id(),SILO_VALIDATED);
        rc = RCOK;

        for(auto it = before.begin(); it != before.end();it++) {
            uint64_t it_upper = silo_time_table.get_upper(txnmanager->get_thd_id(),*it);
            if(it_upper > lower && it_upper < upper-1) {
                lower = it_upper + 1;
            }
        }
        for(auto it = before.begin(); it != before.end();it++) {
            uint64_t it_upper = silo_time_table.get_upper(txnmanager->get_thd_id(),*it);
            if(it_upper >= lower) {
                if(lower > 0) {
                silo_time_table.set_upper(txnmanager->get_thd_id(),*it,lower-1);
                } else {
                silo_time_table.set_upper(txnmanager->get_thd_id(),*it,lower);
                }
            }
        }
        for(auto it = after.begin(); it != after.end();it++) {
            uint64_t it_lower = silo_time_table.get_lower(txnmanager->get_thd_id(),*it);
            uint64_t it_upper = silo_time_table.get_upper(txnmanager->get_thd_id(),*it);
            if(it_upper != UINT64_MAX && it_upper > lower + 2 && it_upper < upper ) {
                upper = it_upper - 2;
            }
            if((it_lower < upper && it_lower > lower+1)) {
                upper = it_lower - 1;
            }
        }
        // set all upper and lower bounds to meet inequality
        for(auto it = after.begin(); it != after.end();it++) {
            uint64_t it_lower = silo_time_table.get_lower(txnmanager->get_thd_id(),*it);
            if(it_lower <= upper) {
                if(upper < UINT64_MAX) {
                silo_time_table.set_lower(txnmanager->get_thd_id(),*it,upper+1);
                } else {
                silo_time_table.set_lower(txnmanager->get_thd_id(),*it,upper);
                }
            }
        }

        assert(lower < upper);
        INC_STATS(txnmanager->get_thd_id(),maat_range,upper-lower);
        INC_STATS(txnmanager->get_thd_id(),maat_commit_cnt,1);
    }
    silo_time_table.set_lower(txnmanager->get_thd_id(),txnmanager->get_txn_id(),lower);
    silo_time_table.set_upper(txnmanager->get_thd_id(),txnmanager->get_txn_id(),upper);
    DEBUG("MAAT Validate End %ld: %d [%lu,%lu]\n",txnmanager->get_txn_id(),rc==RCOK,lower,upper);

    return rc;
}

RC
Silo::finish(RC rc, TxnManager * txnmanager)
{
    if (rc == Abort) {
        if (this->num_locks > txnmanager->get_access_cnt()) 
            return rc;
        for (uint64_t i = 0; i < this->num_locks; i++) {
            txnmanager->txn->accesses[ write_set[i] ]->orig_row->manager->release();
            DEBUG("silo %ld abort release row %ld \n", txnmanager->get_txn_id(), txnmanager->txn->accesses[ write_set[i] ]->orig_row->get_primary_key());
        }
    } else {
        
        for (uint64_t i = 0; i < txnmanager->txn->write_cnt; i++) {
            Access * access = txnmanager->txn->accesses[ write_set[i] ];
            access->orig_row->manager->write( 
                access->data, txnmanager->commit_timestamp );
            txnmanager->txn->accesses[ write_set[i] ]->orig_row->manager->release();
            DEBUG("silo %ld commit release row %ld \n", txnmanager->get_txn_id(), txnmanager->txn->accesses[ write_set[i] ]->orig_row->get_primary_key());
        }
    }
    num_locks = 0;
    memset(write_set, 0, 100);

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

RC Silo::find_bound(TxnManager * txn) {
    RC rc = RCOK;
    uint64_t lower = silo_time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
    uint64_t upper = silo_time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
    if(lower >= upper) {
        silo_time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),SILO_VALIDATED);
        rc = Abort;
    } else {
        silo_time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),SILO_COMMITTED);
        // TODO: can commit_time be selected in a smarter way?
        txn->commit_timestamp = lower;
    }
    DEBUG("MAAT Bound %ld: %d [%lu,%lu] %lu\n", txn->get_txn_id(), rc, lower, upper,
            txn->commit_timestamp);
    return rc;
}

void SiloTimeTable::init() {
    //table_size = g_inflight_max * g_node_cnt * 2 + 1;
    table_size = g_inflight_max + 1;
    DEBUG_M("SiloTimeTable::init table alloc\n");
    table = (SiloTimeTableNode*) mem_allocator.alloc(sizeof(SiloTimeTableNode) * table_size);
    for(uint64_t i = 0; i < table_size;i++) {
        table[i].init();
    }
}

uint64_t SiloTimeTable::hash(uint64_t key) { return key % table_size; }

SiloTimeTableEntry* SiloTimeTable::find(uint64_t key) {
    SiloTimeTableEntry * entry = table[hash(key)].head;
    while(entry) {
        if (entry->key == key) break;
        entry = entry->next;
    }
    return entry;

}

void SiloTimeTable::init(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[34],get_sys_clock() - mtx_wait_starttime);
    SiloTimeTableEntry* entry = find(key);
    if(!entry) {
        DEBUG_M("SiloTimeTable::init entry alloc\n");
        entry = (SiloTimeTableEntry*) mem_allocator.alloc(sizeof(SiloTimeTableEntry));
        entry->init(key);
        LIST_PUT_TAIL(table[idx].head,table[idx].tail,entry);
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

void SiloTimeTable::release(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[35],get_sys_clock() - mtx_wait_starttime);
    SiloTimeTableEntry* entry = find(key);
    if(entry) {
        LIST_REMOVE_HT(entry,table[idx].head,table[idx].tail);
        DEBUG_M("SiloTimeTable::release entry free\n");
        mem_allocator.free(entry,sizeof(SiloTimeTableEntry));
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

uint64_t SiloTimeTable::get_lower(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t value = 0;
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[36],get_sys_clock() - mtx_wait_starttime);
    SiloTimeTableEntry* entry = find(key);
    if(entry) {
        value = entry->lower;
    }
    pthread_mutex_unlock(&table[idx].mtx);
    return value;
}

uint64_t SiloTimeTable::get_upper(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t value = UINT64_MAX;
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_wait_starttime);
    SiloTimeTableEntry* entry = find(key);
    if(entry) {
        value = entry->upper;
    }
    pthread_mutex_unlock(&table[idx].mtx);
    return value;
}


void SiloTimeTable::set_lower(uint64_t thd_id, uint64_t key, uint64_t value) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[38],get_sys_clock() - mtx_wait_starttime);
    SiloTimeTableEntry* entry = find(key);
    if(entry) {
        entry->lower = value;
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

void SiloTimeTable::set_upper(uint64_t thd_id, uint64_t key, uint64_t value) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[39],get_sys_clock() - mtx_wait_starttime);
    SiloTimeTableEntry* entry = find(key);
    if(entry) {
        entry->upper = value;
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

SILOState SiloTimeTable::get_state(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    SILOState state = SILO_ABORTED;
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[40],get_sys_clock() - mtx_wait_starttime);
    SiloTimeTableEntry* entry = find(key);
    if(entry) {
        state = entry->state;
    }
    pthread_mutex_unlock(&table[idx].mtx);
    return state;
}

void SiloTimeTable::set_state(uint64_t thd_id, uint64_t key, SILOState value) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[41],get_sys_clock() - mtx_wait_starttime);
    SiloTimeTableEntry* entry = find(key);
    if(entry) {
        entry->state = value;
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

