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
#include "mem_alloc.h"

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
    timestamp_last_read = 0;
    timestamp_last_write = 0;
    uncommitted_writes = new std::set<uint64_t>();
    uncommitted_reads = new std::set<uint64_t>();
    assert(uncommitted_writes->begin() == uncommitted_writes->end());
    assert(uncommitted_writes->size() == 0);
}

RC
Row_silo::access(TxnManager * txn, TsType type, row_t * local_row) {
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
#endif

    if (type == R_REQ) {
        DEBUG("READ %ld -- %ld, table name: %s \n",txn->get_txn_id(),_row->get_primary_key(),_row->get_table_name());
        // Copy uncommitted writes
        for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end(); it++) {
            uint64_t txn_id = *it;
            txn->uncommitted_writes->insert(txn_id);
            DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),txn_id);
        }

        // Copy write timestamp
        if(txn->greatest_write_timestamp < timestamp_last_write)
            txn->greatest_write_timestamp = timestamp_last_write;

        //Add to uncommitted reads (soft lock)
        uncommitted_reads->insert(txn->get_txn_id());
        
    } else if (type == P_REQ) {
        DEBUG("WRITE %ld -- %ld \n",txn->get_txn_id(),_row->get_primary_key());
        // Copy uncommitted reads
        for(auto it = uncommitted_reads->begin(); it != uncommitted_reads->end(); it++) {
            uint64_t txn_id = *it;
            txn->uncommitted_reads->insert(txn_id);
            DEBUG("    UR %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),txn_id);
        }

        // Copy uncommitted writes
        for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end(); it++) {
            uint64_t txn_id = *it;
            txn->uncommitted_writes_y->insert(txn_id);
            DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),txn_id);
        }

        // Copy read timestamp
        if(txn->greatest_read_timestamp < timestamp_last_read)
            txn->greatest_read_timestamp = timestamp_last_read;

        // Copy write timestamp
        if(txn->greatest_write_timestamp < timestamp_last_write)
            txn->greatest_write_timestamp = timestamp_last_write;

        //Add to uncommitted writes (soft lock)
        uncommitted_writes->insert(txn->get_txn_id());

    }

#if !ATOMIC_WORD
    DEBUG("silo %ld read lock row %ld \n", txn->get_txn_id(), _row->get_primary_key());
    local_row->copy(_row);
    // txn->last_tid = _tid;
    release();
    DEBUG("silo %ld read release row %ld \n", txn->get_txn_id(), _row->get_primary_key());
#endif
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
    _row->copy(data);
#if ATOMIC_WORD
    uint64_t v = _tid_word;
    if (tid > (v & (~LOCK_BIT)) && (v & LOCK_BIT))
        _tid_word = (tid | LOCK_BIT); 
#else
    _tid = tid;
#endif
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

RC Row_silo::abort(access_t type, TxnManager * txn) {
    uint64_t mtx_wait_starttime = get_sys_clock();
    INC_STATS(txn->get_thd_id(),mtx[32],get_sys_clock() - mtx_wait_starttime);
    DEBUG("Silo Abort %ld: %d -- %ld\n",txn->get_txn_id(),type,_row->get_primary_key());
#if WORKLOAD == TPCC
    uncommitted_reads->erase(txn->get_txn_id());
    uncommitted_writes->erase(txn->get_txn_id());
#else
    if(type == RD || type == SCAN) {
        uncommitted_reads->erase(txn->get_txn_id());
    }

    if(type == WR) {
        uncommitted_writes->erase(txn->get_txn_id());
    }
#endif

    return Abort;
}

RC Row_silo::commit(access_t type, TxnManager * txn, row_t * data) {
    uint64_t mtx_wait_starttime = get_sys_clock();
    INC_STATS(txn->get_thd_id(),mtx[33],get_sys_clock() - mtx_wait_starttime);
    DEBUG("Silo Commit %ld: %d,%lu -- %ld\n", txn->get_txn_id(), type, txn->get_commit_timestamp(),
            _row->get_primary_key());

#if WORKLOAD == TPCC
    if(txn->get_commit_timestamp() >  timestamp_last_read) timestamp_last_read = txn->get_commit_timestamp();
    uncommitted_reads->erase(txn->get_txn_id());
    if(txn->get_commit_timestamp() >  timestamp_last_write) timestamp_last_write = txn->get_commit_timestamp();
    uncommitted_writes->erase(txn->get_txn_id());
    // Apply write to DB
    write(data);

    uint64_t txn_commit_ts = txn->get_commit_timestamp();
    // Forward validation
    // Check uncommitted writes against this txn's
        for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end();it++) {
        if(txn->uncommitted_writes->count(*it) == 0) {
            // apply timestamps
            // these write txns need to come AFTER this txn
            uint64_t it_lower = silo_time_table.get_lower(txn->get_thd_id(),*it);
            if(it_lower <= txn_commit_ts) {
                silo_time_table.set_lower(txn->get_thd_id(),*it,txn_commit_ts+1);
                DEBUG("SILO forward val set lower %ld: %lu\n",*it,txn_commit_ts+1);
            }
        }
    }

    uint64_t lower =  silo_time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
    for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end();it++) {
        if(txn->uncommitted_writes_y->count(*it) == 0) {
            // apply timestamps
            // these write txns need to come BEFORE this txn
            uint64_t it_upper = silo_time_table.get_upper(txn->get_thd_id(),*it);
            if(it_upper >= txn_commit_ts) {
                silo_time_table.set_upper(txn->get_thd_id(),*it,txn_commit_ts-1);
                DEBUG("SILO forward val set upper %ld: %lu\n",*it,txn_commit_ts-1);
            }
        }
    }

    for(auto it = uncommitted_reads->begin(); it != uncommitted_reads->end();it++) {
        if(txn->uncommitted_reads->count(*it) == 0) {
            // apply timestamps
            // these write txns need to come BEFORE this txn
            uint64_t it_upper = silo_time_table.get_upper(txn->get_thd_id(),*it);
            if(it_upper >= lower) {
                silo_time_table.set_upper(txn->get_thd_id(),*it,lower-1);
                DEBUG("SILO forward val set upper %ld: %lu\n",*it,lower-1);
            }
        }
    }

#else
    uint64_t txn_commit_ts = txn->get_commit_timestamp();
    if (type == RD || type == SCAN) {
        if (txn_commit_ts > timestamp_last_read) timestamp_last_read = txn_commit_ts;
        uncommitted_reads->erase(txn->get_txn_id());

    // Forward validation
    // Check uncommitted writes against this txn's
        for (auto it = uncommitted_writes->begin(); it != uncommitted_writes->end();it++) {
        if (txn->uncommitted_writes->count(*it) == 0) {
            // apply timestamps
            // these write txns need to come AFTER this txn
            uint64_t it_lower = silo_time_table.get_lower(txn->get_thd_id(),*it);
            if(it_lower <= txn_commit_ts) {
            silo_time_table.set_lower(txn->get_thd_id(),*it,txn_commit_ts+1);
            DEBUG("SILO forward val set lower %ld: %lu\n",*it,txn_commit_ts+1);
            }
        }
    }

  }
  /*
#if WORKLOAD == TPCC
    if(txn_commit_ts >  timestamp_last_read)
      timestamp_last_read = txn_commit_ts;
#endif
*/

    if(type == WR) {
        if (txn_commit_ts > timestamp_last_write) timestamp_last_write = txn_commit_ts;
        uncommitted_writes->erase(txn->get_txn_id());
        // Apply write to DB
        write(data);
        uint64_t lower =  silo_time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
        for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end();it++) {
            if(txn->uncommitted_writes_y->count(*it) == 0) {
                // apply timestamps
                // these write txns need to come BEFORE this txn
                uint64_t it_upper = silo_time_table.get_upper(txn->get_thd_id(),*it);
                if(it_upper >= txn_commit_ts) {
                    silo_time_table.set_upper(txn->get_thd_id(),*it,txn_commit_ts-1);
                    DEBUG("SILO forward val set upper %ld: %lu\n",*it,txn_commit_ts-1);
                }
            }
        }

        for(auto it = uncommitted_reads->begin(); it != uncommitted_reads->end();it++) {
            if(txn->uncommitted_reads->count(*it) == 0) {
                // apply timestamps
                // these write txns need to come BEFORE this txn
                uint64_t it_upper = silo_time_table.get_upper(txn->get_thd_id(),*it);
                if(it_upper >= lower) {
                    silo_time_table.set_upper(txn->get_thd_id(),*it,lower-1);
                    DEBUG("SILO forward val set upper %ld: %lu\n",*it,lower-1);
                }
            }
        }

    }
#endif

    return RCOK;
}

void Row_silo::write(row_t* data) { _row->copy(data); }