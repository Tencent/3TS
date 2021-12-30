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
    uint64_t starttime = get_sys_clock();
#if PER_ROW_VALID
    rc = per_row_validate(txn);
#else
    rc = central_validate(txn);
#endif
  INC_STATS(txn->get_thd_id(),occ_validate_time,get_sys_clock() - starttime);
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
    bool ok = true;
    int lock_cnt = 0;
    for (uint64_t i = 0; i < txn->get_access_cnt() && ok; i++) {
        lock_cnt ++;
        txn->get_access_original_row(i)->manager->latch();
        ok = txn->get_access_original_row(i)->manager->validate( txn->get_start_timestamp() );
    }
    rc = ok ? RCOK : Abort;
#endif
    return rc;
}

RC OptCC::central_validate(TxnManager * txn) {
#if ISOLATION_LEVEL == NOLOCK
    return RCOK;
#endif
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
    bool readonly = (wset->set_size == 0);
    set_ent * his;
    set_ent * ent;
    int n = 0;
    int stop __attribute__((unused));

    //pthread_mutex_lock( &latch );
    sem_wait(&_semaphore);
    INC_STATS(txn->get_thd_id(),occ_cs_wait_time,get_sys_clock() - starttime);
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
    //pthread_mutex_unlock( &latch );
    DEBUG("Start Validation %ld: start_ts %ld, finish_ts %ld, active size %ld\n", txn->get_txn_id(),
        start_tn, finish_tn, f_active_len);
    sem_post(&_semaphore);
    INC_STATS(txn->get_thd_id(),occ_cs_time,get_sys_clock() - starttime);
    starttime = get_sys_clock();
    starttime = get_sys_clock();

    uint64_t checked = 0;
    uint64_t active_checked = 0;
    uint64_t hist_checked = 0;
    stop = 0;
#if ISOLATION_LEVEL == SERIALIZABLE
    if (finish_tn > start_tn) {
    while (his && his->tn > finish_tn) his = his->next;
        while (his && his->tn > start_tn) {
              ++hist_checked;
              ++checked;
            valid = test_valid(his, rset);
// #if WORKLOAD == TPCC
//             if (valid)
//                 valid = test_valid(his, wset);
// #endif
            if (valid)
                valid = test_valid(his, wset);
            if (!valid) {
                INC_STATS(txn->get_thd_id(),occ_hist_validate_fail_time,get_sys_clock() - starttime);
                goto final;
              }
            his = his->next;
        }
    }

    INC_STATS(txn->get_thd_id(),occ_hist_validate_time,get_sys_clock() - starttime);
#endif
    starttime = get_sys_clock();
    stop = 1;
    for (UInt32 i = 0; i < f_active_len; i++) {
        set_ent * wact = finish_active[i];
#if ISOLATION_LEVEL == SERIALIZABLE || ISOLATION_LEVEL == READ_COMMITTED
        ++checked;
        ++active_checked;
        valid = test_valid(wact, rset);
#endif
        if (valid) {
            ++checked;
            ++active_checked;
            valid = test_valid(wact, wset);
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


    if (valid) {
        rc = RCOK;
        INC_STATS(txn->get_thd_id(),occ_check_cnt,checked);
    } else {
        //txn->cleanup(Abort);
        INC_STATS(txn->get_thd_id(),occ_abort_check_cnt,checked);
        rc = Abort;
    // Optimization: If this is aborting, remove from active set now
          sem_wait(&_semaphore);
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
        sem_post(&_semaphore);
    }
    DEBUG("End Validation %ld: active# %ld, hist# %ld\n", txn->get_txn_id(), active_checked,
        hist_checked);
    INC_STATS(txn->get_thd_id(),occ_validate_time,get_sys_clock() - total_starttime);
    return rc;
}

void OptCC::per_row_finish(RC rc, TxnManager * txn) {
    if(rc == RCOK) {
        // advance the global timestamp and get the end_ts
        txn->set_end_timestamp(glob_manager.get_ts( txn->get_thd_id() ));
    }
}

void OptCC::central_finish(RC rc, TxnManager * txn) {
    set_ent * wset;
    set_ent * rset;
    get_rw_set(txn, rset, wset);
    bool readonly = (wset->set_size == 0);

    if (!readonly) {
        // only update active & tnc for non-readonly transactions
        uint64_t starttime = get_sys_clock();
        //        pthread_mutex_lock( &latch );
        sem_wait(&_semaphore);
        set_ent * act = active;
        set_ent * prev = NULL;
        while (act != NULL && act->txn != txn) {
            prev = act;
            act = act->next;
        }
        if(act == NULL) {
            // assert(rc == Abort);
            //pthread_mutex_unlock( &latch );
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
        //    pthread_mutex_unlock( &latch );
        sem_post(&_semaphore);
        INC_STATS(txn->get_thd_id(),occ_finish_time,get_sys_clock() - starttime);
    }
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
