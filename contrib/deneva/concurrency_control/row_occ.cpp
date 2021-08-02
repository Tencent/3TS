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

#include "txn.h"
#include "row.h"
#include "txn.h"
#include "manager.h"
#include "row_occ.h"
#include "mem_alloc.h"
#include "occ.h"
#include <jemalloc/jemalloc.h>

void Row_occ::init(row_t *row) {
    _row = row;
    //_latch = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
    _latch = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(_latch, NULL);
    sem_init(&_semaphore, 0, 1);
    wts = 0;
    lock_tid = 0;
    blatch = false;

    timestamp_last_read = 0;
    timestamp_last_write = 0;
    uncommitted_writes = new std::set<uint64_t>();
    uncommitted_reads = new std::set<uint64_t>();
    assert(uncommitted_writes->begin() == uncommitted_writes->end());
    assert(uncommitted_writes->size() == 0);

}

RC Row_occ::access(TxnManager *txn, TsType type) {
    RC rc = RCOK;
    //pthread_mutex_lock( _latch );
    uint64_t starttime = get_sys_clock();
    sem_wait(&_semaphore);
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
    INC_STATS(txn->get_thd_id(), txn_cc_manager_time, get_sys_clock() - starttime);
    
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
        
        
        txn->cur_row->copy(_row);

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
    
//     if (type == R_REQ) {
// #if CC_ALG == FOCC
//         if (lock_tid != 0 && lock_tid != txn->get_txn_id()) {
//             rc = Abort;
//         } else if (txn->get_start_timestamp() < wts) {
// #else
//         if (txn->get_start_timestamp() < wts) {
// #endif
//             INC_STATS(txn->get_thd_id(),occ_ts_abort_cnt,1);
//             rc = Abort;
//         } else {
//             txn->cur_row->copy(_row);
//             rc = RCOK;
//         }
//     } else assert(false);
//     // pthread_mutex_unlock( _latch );

    INC_STATS(txn->get_thd_id(), txn_useful_time, get_sys_clock()-starttime);
    sem_post(&_semaphore);
    return rc;
}

void Row_occ::latch() {
    //pthread_mutex_lock( _latch );
    sem_wait(&_semaphore);
}

bool Row_occ::validate(uint64_t ts) {
    if (ts < wts)
        return false;
    else
        return true;
}

void Row_occ::write(row_t *data, uint64_t ts) {
    _row->copy(data);
    if (PER_ROW_VALID) {
        assert(ts > wts);
        wts = ts;
    }
}

void Row_occ::release() {
    //pthread_mutex_unlock( _latch );
    sem_post(&_semaphore);
}

void Row_occ::write(row_t* data) { _row->copy(data); }

/* --------------- only used for focc -----------------------*/

bool Row_occ::try_lock(uint64_t tid) {
    bool success = true;
    sem_wait(&_semaphore);
    if (lock_tid == 0 || lock_tid == tid) {
        lock_tid = tid;
    } else success = false;
    sem_post(&_semaphore);
    return success;
}

void Row_occ::release_lock(uint64_t tid) {
    sem_wait(&_semaphore);
    if (lock_tid == tid) {
        lock_tid = 0;
    }
    sem_post(&_semaphore);
}

uint64_t Row_occ::check_lock() {
    sem_wait(&_semaphore);
    uint64_t tid = lock_tid;
    sem_post(&_semaphore);
    return tid;
}

/* --------------- only used for focc -----------------------*/

// for occ+dta

RC Row_occ::abort(access_t type, TxnManager * txn) {
    DEBUG("OCC Abort %ld: %d -- %ld\n",txn->get_txn_id(),type,_row->get_primary_key());
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

RC Row_occ::commit(access_t type, TxnManager * txn, row_t * data) {

    DEBUG("OCC Commit %ld: %d,%lu -- %ld\n", txn->get_txn_id(), type, txn->get_commit_timestamp(),
            _row->get_primary_key());

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
                uint64_t it_lower = occ_time_table.get_lower(txn->get_thd_id(),*it);
                if(it_lower <= txn_commit_ts) {
                occ_time_table.set_lower(txn->get_thd_id(),*it,txn_commit_ts+1);
                DEBUG("OCC forward val set lower %ld: %lu\n",*it,txn_commit_ts+1);
                }
            }
        }

    }

    if(type == WR) {
        if (txn_commit_ts > timestamp_last_write) timestamp_last_write = txn_commit_ts;
        uncommitted_writes->erase(txn->get_txn_id());
        // Apply write to DB
        write(data);
        uint64_t lower =  occ_time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
        for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end();it++) {
            if(txn->uncommitted_writes_y->count(*it) == 0) {
                // apply timestamps
                // these write txns need to come BEFORE this txn
                uint64_t it_upper = occ_time_table.get_upper(txn->get_thd_id(),*it);
                if(it_upper >= txn_commit_ts) {
                    occ_time_table.set_upper(txn->get_thd_id(),*it,txn_commit_ts-1);
                    DEBUG("OCC forward val set upper %ld: %lu\n",*it,txn_commit_ts-1);
                }
            }
        }

        for(auto it = uncommitted_reads->begin(); it != uncommitted_reads->end();it++) {
            if(txn->uncommitted_reads->count(*it) == 0) {
                // apply timestamps
                // these write txns need to come BEFORE this txn
                uint64_t it_upper = occ_time_table.get_upper(txn->get_thd_id(),*it);
                if(it_upper >= lower) {
                    occ_time_table.set_upper(txn->get_thd_id(),*it,lower-1);
                    DEBUG("OCC forward val set upper %ld: %lu\n",*it,lower-1);
                }
            }
        }

    }

    return RCOK;
}
