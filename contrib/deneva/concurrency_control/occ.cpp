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

#include "occ.h"

#include "global.h"
#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_occ.h"
#include "row.h"
#include "txn.h"

set_ent::set_ent() {
    set_size = 0;
    txn = NULL;
    rows = NULL;
    next = NULL;
}

void OptCC::init() {
  sem_init(&_semaphore, 0, 1);
    tnc = 0;
    his_len = 0;
    active_len = 0;
    active = NULL;
    lock_all = false;
}

RC OptCC::validate(TxnManager * txn) {
    RC rc;
    //uint64_t starttime = get_sys_clock();
#if PER_ROW_VALID
    rc = per_row_validate(txn);
#else
    rc = central_validate(txn);
#endif
  //INC_STATS(txn->get_thd_id(),occ_validate_time,get_sys_clock() - starttime);
    return rc;
}

void OptCC::finish(RC rc, TxnManager * txn) {
#if PER_ROW_VALID
    per_row_finish(rc,txn);
#else
    central_finish(rc,txn);
#endif
}

RC OptCC::per_row_validate(TxnManager *txn) {
    RC rc = RCOK;

    uint64_t lower = occ_time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
    uint64_t upper = occ_time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
    std::set<uint64_t> after;
    std::set<uint64_t> before;

    // lower bound of txn greater than write timestamp
    if(lower <= txn->greatest_write_timestamp) {
        lower = txn->greatest_write_timestamp + 1;
        INC_STATS(txn->get_thd_id(),maat_case1_cnt,1);
    }

    // lower bound of txn greater than read timestamp
    if(lower <= txn->greatest_read_timestamp) {
        lower = txn->greatest_read_timestamp + 1;
        INC_STATS(txn->get_thd_id(),maat_case3_cnt,1);
    }

#if CC_ALG == OCC
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
#if DEBUG_ASSERT
    for (uint64_t i = txn->get_access_cnt() - 1; i > 0; i--) {
        int tabcmp = strcmp(txn->get_access_original_row(i-1)->get_table_name(),
            txn->get_access_original_row(i)->get_table_name());
            assert(tabcmp < 0 || tabcmp == 0 && txn->get_access_original_row(i)->get_primary_key() >
            txn->get_access_original_row(i-1)->get_primary_key());
    }
#endif
    // lock all rows in the readset and writeset.
    // Validate each access
    // bool ok = true;
    // int lock_cnt = 0;
    // for (uint64_t i = 0; i < txn->get_access_cnt() && ok; i++) {
    //     lock_cnt ++;
    //     txn->get_access_original_row(i)->manager->latch();
    //     ok = txn->get_access_original_row(i)->manager->validate( txn->get_start_timestamp() );
    // }
    // rc = ok ? RCOK : Abort;

    // lock all rows, no wait for lock
    bool done = false;
    while (!done) {
        txn->num_locks = 0;
        for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
            row_t * row = txn->get_access_original_row(i);
            if (!row->manager->try_lock(txn->get_txn_id()))
            {
                break;
            }

            txn->num_locks ++;
        }
        if (txn->num_locks == txn->get_access_cnt()) {
     
            done = true;
        } else {
            rc = Abort;
            return rc;
        }
    }

    // for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
    //     txn->get_access_original_row(i)->manager->latch();
    //     validate()
    // }
    
    // RW validation
    // lower bound of uncommitted writes greater than upper bound of txn
    for(auto it = txn->uncommitted_writes->begin(); it != txn->uncommitted_writes->end();it++) {
        uint64_t it_lower = occ_time_table.get_lower(txn->get_thd_id(),*it);
        if(upper >= it_lower) {
            OCCState state = occ_time_table.get_state(txn->get_thd_id(),*it);
            if(state == OCC_VALIDATED || state == OCC_COMMITTED) {
                INC_STATS(txn->get_thd_id(),maat_case2_cnt,1);
                if(it_lower > 0) {
                upper = it_lower - 1;
                } else {
                upper = it_lower;
                }
            }
            if(state == OCC_RUNNING) {
                after.insert(*it);
            }
        }
    }

    // WR validation
    // upper bound of uncommitted reads less than lower bound of txn
    for(auto it = txn->uncommitted_reads->begin(); it != txn->uncommitted_reads->end();it++) {
        uint64_t it_upper = occ_time_table.get_upper(txn->get_thd_id(),*it);
        if(lower <= it_upper) {
            OCCState state = occ_time_table.get_state(txn->get_thd_id(),*it);
            if(state == OCC_VALIDATED || state == OCC_COMMITTED) {
                INC_STATS(txn->get_thd_id(),maat_case4_cnt,1);
                if(it_upper < UINT64_MAX) {
                lower = it_upper + 1;
                } else {
                lower = it_upper;
                }
            }
            if(state == OCC_RUNNING) {
                before.insert(*it);
            }
        }
    }

    // WW validation
    // upper bound of uncommitted write writes less than lower bound of txn
    for(auto it = txn->uncommitted_writes_y->begin(); it != txn->uncommitted_writes_y->end();it++) {
        OCCState state = occ_time_table.get_state(txn->get_thd_id(),*it);
        uint64_t it_upper = occ_time_table.get_upper(txn->get_thd_id(),*it);
        if(state == OCC_ABORTED) {
            continue;
        }
        if(state == OCC_VALIDATED || state == OCC_COMMITTED) {
            if(lower <= it_upper) {
            INC_STATS(txn->get_thd_id(),maat_case5_cnt,1);
            if(it_upper < UINT64_MAX) {
                lower = it_upper + 1;
            } else {
                lower = it_upper;
            }
            }
        }
        if(state == OCC_RUNNING) {
            after.insert(*it);
        }
    }

    if(lower >= upper) {
        // Abort
        occ_time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),OCC_ABORTED);
        rc = Abort;
    } else {
        // Validated
        occ_time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),OCC_VALIDATED);
        rc = RCOK;

        for(auto it = before.begin(); it != before.end();it++) {
            uint64_t it_upper = occ_time_table.get_upper(txn->get_thd_id(),*it);
            if(it_upper > lower && it_upper < upper-1) {
                lower = it_upper + 1;
            }
        }
        for(auto it = before.begin(); it != before.end();it++) {
            uint64_t it_upper = occ_time_table.get_upper(txn->get_thd_id(),*it);
            if(it_upper >= lower) {
                if(lower > 0) {
                occ_time_table.set_upper(txn->get_thd_id(),*it,lower-1);
                } else {
                occ_time_table.set_upper(txn->get_thd_id(),*it,lower);
                }
            }
        }
        for(auto it = after.begin(); it != after.end();it++) {
            uint64_t it_lower = occ_time_table.get_lower(txn->get_thd_id(),*it);
            uint64_t it_upper = occ_time_table.get_upper(txn->get_thd_id(),*it);
            if(it_upper != UINT64_MAX && it_upper > lower + 2 && it_upper < upper ) {
                upper = it_upper - 2;
            }
            if((it_lower < upper && it_lower > lower+1)) {
                upper = it_lower - 1;
            }
        }
        // set all upper and lower bounds to meet inequality
        for(auto it = after.begin(); it != after.end();it++) {
            uint64_t it_lower = occ_time_table.get_lower(txn->get_thd_id(),*it);
            if(it_lower <= upper) {
                if(upper < UINT64_MAX) {
                occ_time_table.set_lower(txn->get_thd_id(),*it,upper+1);
                } else {
                occ_time_table.set_lower(txn->get_thd_id(),*it,upper);
                }
            }
        }

        assert(lower < upper);
        INC_STATS(txn->get_thd_id(),maat_range,upper-lower);
        INC_STATS(txn->get_thd_id(),maat_commit_cnt,1);
    }


#endif
    return rc;
}

RC OptCC::central_validate(TxnManager * txn) {
    RC rc;
    uint64_t starttime = get_sys_clock();
    uint64_t total_starttime = starttime;
    uint64_t start_tn = txn->get_start_timestamp();
    uint64_t finish_tn;
    //set_ent ** finish_active;
    //set_ent * finish_active[f_active_len];
    uint64_t f_active_len;
    bool valid = true;
    // OptCC is centralized. No need to do per partition malloc.
    set_ent * wset;
    set_ent * rset;
    get_rw_set(txn, rset, wset);
    INC_STATS(txn->get_thd_id(),occ_rwset_get_time,get_sys_clock() - starttime);
    starttime = get_sys_clock();
    bool readonly = (wset->set_size == 0);
    if(readonly) INC_STATS(txn->get_thd_id(),occ_readonly_cnt,1);
    set_ent * his;
    set_ent * ent;
    int n = 0;
    int stop __attribute__((unused));

    //pthread_mutex_lock( &latch );
    sem_wait(&_semaphore);
    INC_STATS(txn->get_thd_id(),occ_cs_wait_time,get_sys_clock() - starttime);
    INC_STATS(txn->get_thd_id(),occ_wait_add_time,get_sys_clock() - starttime);
    starttime = get_sys_clock();
    //finish_tn = tnc;
    assert(!g_ts_batch_alloc);
    finish_tn = glob_manager.get_ts(txn->get_thd_id());
    ent = active;
    f_active_len = active_len;
    set_ent * finish_active[f_active_len];
    //finish_active = (set_ent**) mem_allocator.alloc(sizeof(set_ent *) * f_active_len);
    while (ent != NULL) {
        finish_active[n++] = ent;
        ent = ent->next;
    }
    if ( !readonly ) {
        active_len ++;
        STACK_PUSH(active, wset);
    }
    his = history;
    INC_STATS(txn->get_thd_id(),occ_add_active_time,get_sys_clock() - starttime);
    //pthread_mutex_unlock( &latch );
    DEBUG("Start Validation %ld: start_ts %ld, finish_ts %ld, active size %ld\n", txn->get_txn_id(),
        start_tn, finish_tn, f_active_len);
    sem_post(&_semaphore);
    INC_STATS(txn->get_thd_id(),occ_cs_time,get_sys_clock() - starttime);
    starttime = get_sys_clock();

    uint64_t checked = 0;
    uint64_t active_checked = 0;
    uint64_t hist_checked = 0;
    stop = 0;
    if (finish_tn > start_tn) {
    while (his && his->tn > finish_tn) his = his->next;
        while (his && his->tn > start_tn) {
              ++hist_checked;
              ++checked;
            valid = test_valid(his, rset);
#if WORKLOAD == TPCC
            if (valid)
                valid = test_valid(his, wset);
#endif
            if (!valid) {
                INC_STATS(txn->get_thd_id(),occ_rhis_abort_cnt,1);
                INC_STATS(txn->get_thd_id(),occ_hist_validate_fail_time,get_sys_clock() - starttime);
                INC_STATS(txn->get_thd_id(),occ_validate_rhis_time,get_sys_clock() - starttime);
                goto final;
              }
            his = his->next;
        }
    }
    INC_STATS(txn->get_thd_id(),occ_validate_rhis_time,get_sys_clock() - starttime);

    INC_STATS(txn->get_thd_id(),occ_hist_validate_time,get_sys_clock() - starttime);
    stop = 1;
    for (UInt32 i = 0; i < f_active_len; i++) {
        set_ent * wact = finish_active[i];
        ++checked;
        ++active_checked;
        starttime = get_sys_clock();
        valid = test_valid(wact, rset);
        INC_STATS(txn->get_thd_id(),occ_valiadate_rw_time,get_sys_clock() - starttime);
        if(!valid) INC_STATS(txn->get_thd_id(),occ_rw_abort_cnt,1);
        if (valid) {
            ++checked;
            ++active_checked;
            uint64_t wwtest_start = get_sys_clock();
            valid = test_valid(wact, wset);
            INC_STATS(txn->get_thd_id(),occ_validate_ww_time,get_sys_clock() - wwtest_start);
            if(!valid) INC_STATS(txn->get_thd_id(),occ_ww_abort_cnt,1);
        }
        if (!valid) {
            INC_STATS(txn->get_thd_id(),occ_act_validate_fail_time,get_sys_clock() - starttime);
            goto final;
        }
    }
    INC_STATS(txn->get_thd_id(),occ_act_validate_time,get_sys_clock() - starttime);
    starttime = get_sys_clock();
final:
  /*
    if (valid)
        txn->cleanup(RCOK);
    */
    mem_allocator.free(rset->rows, sizeof(row_t *) * rset->set_size);
    mem_allocator.free(rset, sizeof(set_ent));
    //mem_allocator.free(finish_active, sizeof(set_ent*)* f_active_len);


    if (STATS_ENABLE && simulation->is_warmup_done()) stats._stats[txn->get_thd_id()]->occ_max_len = max(stats._stats[txn->get_thd_id()]->occ_max_len, checked);
    if (valid) {
        rc = RCOK;
        INC_STATS(txn->get_thd_id(),occ_check_cnt,checked);
    } else {
        //txn->cleanup(Abort);
        INC_STATS(txn->get_thd_id(),occ_abort_check_cnt,checked);
        rc = Abort;
    // Optimization: If this is aborting, remove from active set now
        starttime = get_sys_clock();
          sem_wait(&_semaphore);
        INC_STATS(txn->get_thd_id(),occ_wait_rm_time,get_sys_clock() - starttime);
        starttime = get_sys_clock();
        set_ent * act = active;
        set_ent * prev = NULL;
        while (act != NULL && act->txn != txn) {
          prev = act;
          act = act->next;
        }
        if(act != NULL && act->txn == txn) {
            if (prev != NULL) prev->next = act->next;
            else active = act->next;
            active_len --;
        }
        INC_STATS(txn->get_thd_id(),occ_rm_active_time,get_sys_clock() - starttime);
        sem_post(&_semaphore);
    }
    DEBUG("End Validation %ld: active# %ld, hist# %ld\n", txn->get_txn_id(), active_checked,
        hist_checked);
    INC_STATS(txn->get_thd_id(),occ_validate_time,get_sys_clock() - total_starttime);
    return rc;
}

void OptCC::per_row_finish(RC rc, TxnManager * txn) {
    // if(rc == RCOK) {
    //     // advance the global timestamp and get the end_ts
    //     txn->set_end_timestamp(glob_manager.get_ts( txn->get_thd_id() ));
    // }
    // if(rc == RCOK) {
    //     // release uncommitted read and write when commit
    //     for (uint64_t i = 0; i < txn->get_access_cnt(); i++) {
    //         if(txn->get_access_type(i) == WR){
    //             txn->get_access_original_row(i)->manager->commit(WR, txn, txn->get_access_original_row(i));
    //         }
    //         else{
    //             txn->get_access_original_row(i)->manager->commit(RD, txn, txn->get_access_original_row(i));
    //         }
    //         txn->get_access_original_row(i)->manager->release();
    //     }
    // }
    // else{
    //     // release uncommitted read and write when abort
    //     for (uint64_t i = 0; i < txn->num_locks; i++) {
    //         if(txn->get_access_type(i) == WR){
    //             txn->get_access_original_row(i)->manager->abort(WR, txn);
    //         }
    //         else{
    //             txn->get_access_original_row(i)->manager->abort(RD, txn);
    //         }
    //         txn->get_access_original_row(i)->manager->release();
    //     }

    //     // release uncommitted read and write when abort
    //     for (uint64_t i = txn->num_locks; i < txn->get_access_cnt(); i++) {
    //         if(txn->get_access_type(i) == WR){
    //             txn->get_access_original_row(i)->manager->abort_no_lock(WR, txn);
    //         }
    //         else{
    //             txn->get_access_original_row(i)->manager->abort_no_lock(RD, txn);
    //         }
    //     }

    // }

}

void OptCC::central_finish(RC rc, TxnManager * txn) {
    set_ent * wset;
    set_ent * rset;
    uint64_t starttime = get_sys_clock();
    get_rw_set(txn, rset, wset);
    INC_STATS(txn->get_thd_id(),occ_rwset_get_time,get_sys_clock() - starttime);
    bool readonly = (wset->set_size == 0);

    if (!readonly) {
        // only update active & tnc for non-readonly transactions
        uint64_t wait_start = get_sys_clock();
        //        pthread_mutex_lock( &latch );
        sem_wait(&_semaphore);
        INC_STATS(txn->get_thd_id(),occ_wait_rm_time,get_sys_clock() - wait_start);
        uint64_t rm_start = get_sys_clock();
        set_ent * act = active;
        set_ent * prev = NULL;
        while (act != NULL && act->txn != txn) {
            prev = act;
            act = act->next;
        }
        if(act == NULL) {
            assert(rc == Abort);
            //pthread_mutex_unlock( &latch );
            INC_STATS(txn->get_thd_id(),occ_rm_active_time,get_sys_clock() - rm_start);
            sem_post(&_semaphore);
            return;
        }
        assert(act->txn == txn);
        if (prev != NULL)
            prev->next = act->next;
        else
            active = act->next;
        active_len --;
        if (rc == RCOK) {
            // remove the assert for performance
              /*
            if (history)
                assert(history->tn == tnc);
              */
            // tnc ++;
            wset->tn = glob_manager.get_ts(txn->get_thd_id());
            STACK_PUSH(history, wset);
            DEBUG("occ insert history");
            his_len ++;
            //mem_allocator.free(wset->rows, sizeof(row_t *) * wset->set_size);
            //mem_allocator.free(wset, sizeof(set_ent));
        }
        INC_STATS(txn->get_thd_id(),occ_rm_active_time,get_sys_clock() - rm_start);
        //    pthread_mutex_unlock( &latch );
        sem_post(&_semaphore);
    }
    INC_STATS(txn->get_thd_id(),occ_finish_time,get_sys_clock() - starttime);
}

RC OptCC::get_rw_set(TxnManager * txn, set_ent * &rset, set_ent *& wset) {
    wset = (set_ent*) mem_allocator.alloc(sizeof(set_ent));
    rset = (set_ent*) mem_allocator.alloc(sizeof(set_ent));
    wset->set_size = txn->get_write_set_size();
    rset->set_size = txn->get_read_set_size();
    wset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * wset->set_size);
    rset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * rset->set_size);
    wset->txn = txn;
    rset->txn = txn;

    UInt32 n = 0, m = 0;
    for (uint64_t i = 0; i < wset->set_size + rset->set_size; i++) {
        if (txn->get_access_type(i) == WR)
            wset->rows[n ++] = txn->get_access_original_row(i);
        else
            rset->rows[m ++] = txn->get_access_original_row(i);
    }

    assert(n == wset->set_size);
    assert(m == rset->set_size);
    return RCOK;
}

bool OptCC::test_valid(set_ent * set1, set_ent * set2) {
    for (UInt32 i = 0; i < set1->set_size; i++)
        for (UInt32 j = 0; j < set2->set_size; j++) {
            if (set1->rows[i] == set2->rows[j]) {
                return false;
            }
        }
    return true;
}



RC OptCC::find_bound(TxnManager * txn) {
    RC rc = RCOK;
    uint64_t lower = occ_time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
    uint64_t upper = occ_time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
    if(lower >= upper) {
        occ_time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),OCC_VALIDATED);
        rc = Abort;
    } else {
        occ_time_table.set_state(txn->get_thd_id(),txn->get_txn_id(),OCC_COMMITTED);
        // TODO: can commit_time be selected in a smarter way?
        txn->commit_timestamp = lower;
    }
    DEBUG("OCC Bound %ld: %d [%lu,%lu] %lu\n", txn->get_txn_id(), rc, lower, upper,
            txn->commit_timestamp);
    return rc;
}

void OCCTimeTable::init() {
    //table_size = g_inflight_max * g_node_cnt * 2 + 1;
    table_size = g_inflight_max + 1;
    DEBUG_M("OCCTimeTable::init table alloc\n");
    table = (OCCTimeTableNode*) mem_allocator.alloc(sizeof(OCCTimeTableNode) * table_size);
    for(uint64_t i = 0; i < table_size;i++) {
        table[i].init();
    }
}

uint64_t OCCTimeTable::hash(uint64_t key) { return key % table_size; }

OCCTimeTableEntry* OCCTimeTable::find(uint64_t key) {
    OCCTimeTableEntry * entry = table[hash(key)].head;
    while(entry) {
        if (entry->key == key) break;
        entry = entry->next;
    }
    return entry;

}

void OCCTimeTable::init(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[34],get_sys_clock() - mtx_wait_starttime);
    OCCTimeTableEntry* entry = find(key);
    if(!entry) {
        DEBUG_M("OCCTimeTable::init entry alloc\n");
        entry = (OCCTimeTableEntry*) mem_allocator.alloc(sizeof(OCCTimeTableEntry));
        entry->init(key);
        LIST_PUT_TAIL(table[idx].head,table[idx].tail,entry);
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

void OCCTimeTable::release(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[35],get_sys_clock() - mtx_wait_starttime);
    OCCTimeTableEntry* entry = find(key);
    if(entry) {
        LIST_REMOVE_HT(entry,table[idx].head,table[idx].tail);
        DEBUG_M("OCCTimeTable::release entry free\n");
        mem_allocator.free(entry,sizeof(OCCTimeTableEntry));
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

uint64_t OCCTimeTable::get_lower(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t value = 0;
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[36],get_sys_clock() - mtx_wait_starttime);
    OCCTimeTableEntry* entry = find(key);
    if(entry) {
        value = entry->lower;
    }
    pthread_mutex_unlock(&table[idx].mtx);
    return value;
}

uint64_t OCCTimeTable::get_upper(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    uint64_t value = UINT64_MAX;
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_wait_starttime);
    OCCTimeTableEntry* entry = find(key);
    if(entry) {
        value = entry->upper;
    }
    pthread_mutex_unlock(&table[idx].mtx);
    return value;
}


void OCCTimeTable::set_lower(uint64_t thd_id, uint64_t key, uint64_t value) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[38],get_sys_clock() - mtx_wait_starttime);
    OCCTimeTableEntry* entry = find(key);
    if(entry) {
        entry->lower = value;
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

void OCCTimeTable::set_upper(uint64_t thd_id, uint64_t key, uint64_t value) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[39],get_sys_clock() - mtx_wait_starttime);
    OCCTimeTableEntry* entry = find(key);
    if(entry) {
        entry->upper = value;
    }
    pthread_mutex_unlock(&table[idx].mtx);
}

OCCState OCCTimeTable::get_state(uint64_t thd_id, uint64_t key) {
    uint64_t idx = hash(key);
    OCCState state = OCC_ABORTED;
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[40],get_sys_clock() - mtx_wait_starttime);
    OCCTimeTableEntry* entry = find(key);
    if(entry) {
        state = entry->state;
    }
    pthread_mutex_unlock(&table[idx].mtx);
    return state;
}

void OCCTimeTable::set_state(uint64_t thd_id, uint64_t key, OCCState value) {
    uint64_t idx = hash(key);
    uint64_t mtx_wait_starttime = get_sys_clock();
    pthread_mutex_lock(&table[idx].mtx);
    INC_STATS(thd_id,mtx[41],get_sys_clock() - mtx_wait_starttime);
    OCCTimeTableEntry* entry = find(key);
    if(entry) {
        entry->state = value;
    }
    pthread_mutex_unlock(&table[idx].mtx);
}