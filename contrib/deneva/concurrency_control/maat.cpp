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

#include "global.h"
#include "helper.h"
#include "txn.h"
#include "maat.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_maat.h"

void Maat::init() { sem_init(&_semaphore, 0, 1); }

RC Maat::validate(TxnManager * txn) {
    uint64_t start_time = get_sys_clock();
    uint64_t timespan;
    // sem_wait(&_semaphore);
    INC_STATS(txn->get_thd_id(),maat_validate_wait_time,get_sys_clock()-start_time);
    timespan = get_sys_clock() - start_time;
    txn->txn_stats.cc_block_time += timespan;
    txn->txn_stats.cc_block_time_short += timespan;
    
    start_time = get_sys_clock();
    RC rc = RCOK;
    uint64_t lower = time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
    uint64_t upper = time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
    DEBUG("MAAT Validate Start %ld: [%lu,%lu]\n",txn->get_txn_id(),lower,upper);
    std::set<uint64_t> after;
    std::set<uint64_t> before;

    // sort all rows accessed in primary key order.
    for (uint64_t i = txn->get_access_cnt() - 1; i > 0; i--) {
        for (uint64_t j = 0; j < i; j ++) {
            int tabcmp = strcmp(txn->get_access_original_row(j)->get_table_name(),
                txn->get_access_original_row(j+1)->get_table_name());
      if (tabcmp > 0 ||
          (tabcmp == 0 && txn->get_access_original_row(j)->get_primary_key() >
                              txn->get_access_original_row(j + 1)->get_primary_key())) {
                txn->swap_accesses(j,j+1);
            }
        }
    }

    // lock rows
    uint64_t num_locks = 0;
    for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
        row_t * row = txn->get_access_original_row(i);
        if (!row->manager->try_lock(txn->get_txn_id()))
        {
            break;
        }
        num_locks ++;
    }
    if (num_locks != txn->get_access_cnt()) {
        INC_STATS(txn->get_thd_id(),total_rw_abort_cnt,1); // use this to cnt lock violation
        rc = Abort;
        return rc;
    }

    INC_STATS(txn->get_thd_id(),maat_cs_wait_time,get_sys_clock() - start_time);

    uint64_t adjust_start = get_sys_clock();
    // lower bound of txn greater than write timestamp
    if(lower <= txn->greatest_write_timestamp) {
        lower = txn->greatest_write_timestamp + 1;
        INC_STATS(txn->get_thd_id(),maat_case1_cnt,1);
    }
    // lower bound of uncommitted writes greater than upper bound of txn
    for(auto it = txn->uncommitted_writes->begin(); it != txn->uncommitted_writes->end();it++) {
        uint64_t it_lower = time_table.get_lower(txn->get_thd_id(),*it);
        if(upper >= it_lower) {
            MAATState state = time_table.get_state(txn->get_thd_id(),*it);
            if(state == MAAT_VALIDATED || state == MAAT_COMMITTED) {
                INC_STATS(txn->get_thd_id(),maat_case2_cnt,1);
                if(it_lower > 0) {
                upper = it_lower - 1;
                } else {
                upper = it_lower;
                }
            }
            if(state == MAAT_RUNNING) {
                after.insert(*it);
            }
        }
    }
    // lower bound of txn greater than read timestamp
    if(lower <= txn->greatest_read_timestamp) {
        lower = txn->greatest_read_timestamp + 1;
        INC_STATS(txn->get_thd_id(),maat_case3_cnt,1);
    }
    // upper bound of uncommitted reads less than lower bound of txn
    for(auto it = txn->uncommitted_reads->begin(); it != txn->uncommitted_reads->end();it++) {
        uint64_t it_upper = time_table.get_upper(txn->get_thd_id(),*it);
        if(lower <= it_upper) {
            MAATState state = time_table.get_state(txn->get_thd_id(),*it);
            if(state == MAAT_VALIDATED || state == MAAT_COMMITTED) {
                INC_STATS(txn->get_thd_id(),maat_case4_cnt,1);
                if(it_upper < UINT64_MAX) {
                lower = it_upper + 1;
                } else {
                lower = it_upper;
                }
            }
            if(state == MAAT_RUNNING) {
                before.insert(*it);
            }
        }
    }
    // upper bound of uncommitted write writes less than lower bound of txn
    for(auto it = txn->uncommitted_writes_y->begin(); it != txn->uncommitted_writes_y->end();it++) {
        MAATState state = time_table.get_state(txn->get_thd_id(),*it);
        uint64_t it_upper = time_table.get_upper(txn->get_thd_id(),*it);
        if(state == MAAT_ABORTED) {
            continue;
        }
        if(state == MAAT_VALIDATED || state == MAAT_COMMITTED) {
            if(lower <= it_upper) {
            INC_STATS(txn->get_thd_id(),maat_case5_cnt,1);
            if(it_upper < UINT64_MAX) {
                lower = it_upper + 1;
            } else {
                lower = it_upper;
            }
            }
        }
        if(state == MAAT_RUNNING) {
            after.insert(*it);
        }
    }
    if(lower >= upper) {
        // Abort
        time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_ABORTED);
        rc = Abort;
    } else {
        // Validated
        time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_VALIDATED);
        rc = RCOK;

        for(auto it = before.begin(); it != before.end();it++) {
            uint64_t it_upper = time_table.get_upper(txn->get_thd_id(),*it);
            if(it_upper > lower && it_upper < upper-1) {
                lower = it_upper + 1;
            }
        }
        for(auto it = before.begin(); it != before.end();it++) {
            uint64_t it_upper = time_table.get_upper(txn->get_thd_id(),*it);
            if(it_upper >= lower) {
                if(lower > 0) {
                time_table.set_upper(txn->get_thd_id(),*it,lower-1);
                } else {
                time_table.set_upper(txn->get_thd_id(),*it,lower);
                }
            }
        }
        for(auto it = after.begin(); it != after.end();it++) {
            uint64_t it_lower = time_table.get_lower(txn->get_thd_id(),*it);
            uint64_t it_upper = time_table.get_upper(txn->get_thd_id(),*it);
            if(it_upper != UINT64_MAX && it_upper > lower + 2 && it_upper < upper ) {
                upper = it_upper - 2;
            }
            if((it_lower < upper && it_lower > lower+1)) {
                upper = it_lower - 1;
            }
        }
        // set all upper and lower bounds to meet inequality
        for(auto it = after.begin(); it != after.end();it++) {
            uint64_t it_lower = time_table.get_lower(txn->get_thd_id(),*it);
            if(it_lower <= upper) {
                if(upper < UINT64_MAX) {
                time_table.set_lower(txn->get_thd_id(),*it,upper+1);
                } else {
                time_table.set_lower(txn->get_thd_id(),*it,upper);
                }
            }
        }
    INC_STATS(txn->get_thd_id(),maat_adjust_time,get_sys_clock()-adjust_start);

        assert(lower < upper);
        INC_STATS(txn->get_thd_id(),maat_range,upper-lower);
        INC_STATS(txn->get_thd_id(),maat_commit_cnt,1);
    }
    time_table.set_lower(txn->get_thd_id(),txn->get_txn_id(),lower);
    time_table.set_upper(txn->get_thd_id(),txn->get_txn_id(),upper);
    INC_STATS(txn->get_thd_id(),maat_validate_cnt,1);
    timespan = get_sys_clock() - start_time;
    INC_STATS(txn->get_thd_id(),maat_validate_time,timespan);
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    DEBUG("MAAT Validate End %ld: %d [%lu,%lu]\n",txn->get_txn_id(),rc==RCOK,lower,upper);
    // sem_post(&_semaphore);
    return rc;

}

RC Maat::find_bound(TxnManager * txn) {
    RC rc = RCOK;
    uint64_t lower = time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
    uint64_t upper = time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
    if(lower >= upper) {
        time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_VALIDATED);
        rc = Abort;
    } else {
        time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),MAAT_COMMITTED);
        // TODO: can commit_time be selected in a smarter way?
        txn->commit_timestamp = lower;
    }
    DEBUG("MAAT Bound %ld: %d [%lu,%lu] %lu\n", txn->get_txn_id(), rc, lower, upper,
            txn->commit_timestamp);
    return rc;
}

void TimeTable::init() {
    //table_size = g_inflight_max * g_node_cnt * 2 + 1;
    table_size = g_inflight_max + 1;
    DEBUG_M("TimeTable::init table alloc\n");
    table = (TimeTableNode*) mem_allocator.alloc(sizeof(TimeTableNode) * table_size);
    for(uint64_t i = 0; i < table_size;i++) {
        table[i].init();
    }
}

uint64_t TimeTable::hash(uint64_t key) { return key % table_size; }

TimeTableEntry* TimeTable::find(uint64_t key) {
    TimeTableEntry * entry = table[hash(key)].head;
    while(entry) {
        if (entry->key == key) break;
        entry = entry->next;
    }
    return entry;

}

void TimeTable::init(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[34],get_sys_clock() - mtx_wait_starttime);
    TimeTableEntry* entry = find(key);
    if(!entry) {
        DEBUG_M("TimeTable::init entry alloc\n");
        entry = (TimeTableEntry*) mem_allocator.alloc(sizeof(TimeTableEntry));
        entry->init(key);
        LIST_PUT_TAIL(table[idx].head,table[idx].tail,entry);
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

void TimeTable::release(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[35],get_sys_clock() - mtx_wait_starttime);
    TimeTableEntry* entry = find(key);
    if(entry) {
        LIST_REMOVE_HT(entry,table[idx].head,table[idx].tail);
        DEBUG_M("TimeTable::release entry free\n");
        mem_allocator.free(entry,sizeof(TimeTableEntry));
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

uint64_t TimeTable::get_lower(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t value = 0;
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[36],get_sys_clock() - mtx_wait_starttime);
    TimeTableEntry* entry = find(key);
    if(entry) {
        value = entry->lower;
    }
    pthread_mutex_unlock(&table[idx].mtx);
    return value;
}

uint64_t TimeTable::get_upper(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t value = UINT64_MAX;
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_wait_starttime);
    TimeTableEntry* entry = find(key);
    if(entry) {
        value = entry->upper;
    }
    pthread_mutex_unlock(&table[idx].mtx);
    return value;
}


void TimeTable::set_lower(uint64_t thd_id, uint64_t key, uint64_t value) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[38],get_sys_clock() - mtx_wait_starttime);
    TimeTableEntry* entry = find(key);
    if(entry) {
        entry->lower = value;
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

void TimeTable::set_upper(uint64_t thd_id, uint64_t key, uint64_t value) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[39],get_sys_clock() - mtx_wait_starttime);
    TimeTableEntry* entry = find(key);
    if(entry) {
        entry->upper = value;
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

MAATState TimeTable::get_state(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    MAATState state = MAAT_ABORTED;
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[40],get_sys_clock() - mtx_wait_starttime);
    TimeTableEntry* entry = find(key);
    if(entry) {
        state = entry->state;
    }
    pthread_mutex_unlock(&table[idx].mtx);
    return state;
}

void TimeTable::set_state(uint64_t thd_id, uint64_t key, MAATState value) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[41],get_sys_clock() - mtx_wait_starttime);
    TimeTableEntry* entry = find(key);
    if(entry) {
        entry->state = value;
    }
    pthread_mutex_unlock(&table[idx].mtx);
}
