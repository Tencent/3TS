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

#include "dta.h"

#include "../system/global.h"
#include "../system/helper.h"
#include "../system/manager.h"
#include "../system/mem_alloc.h"
#include "../system/txn.h"
#include "dli.h"
#include "row_dta.h"

void get_rw_set(TxnManager* txn, std::list<dta_item>& rset, std::list<dta_item>& wset) {
  uint64_t len = txn->get_access_cnt();
  for (uint64_t i = 0; i < len; i++) {
    if (txn->get_access_type(i) == WR)
      wset.emplace_back(txn->get_access_original_row(i), txn->get_access_version(i));
    else
      rset.emplace_back(txn->get_access_original_row(i), txn->get_access_version(i));
  }
}

dta_set_ent::dta_set_ent() {
  set_size = 0;
  txn = NULL;
  rows = NULL;
  next = NULL;
}

void Dta::init() {
  sem_init(&_semaphore, 0, 1);
  sem_init(&sem_rwset_, 0, 1);
}

void Dta::finish(RC rc, TxnManager* txn) {
  std::list<dta_item> rset, wset;
  ::get_rw_set(txn, rset, wset);
  wset.sort();
  sem_wait(&sem_rwset_);
  auto it = rwset_it_[txn];
  sem_post(&sem_rwset_);

  it->latch();
  if (rc == RCOK) {
    assert(it->wset.size() == wset.size());
    it->wset = std::move(wset);
    it->lower = txn->get_commit_timestamp();
    it->upper = it->lower + 1;
    it->state = DTA_COMMITTED;
  } else {
    it->state = DTA_ABORTED;
  }
  it->release();
}
RC Dta::validate(TxnManager* txn) {
  RC rc = RCOK;
#if CC_ALG == DTA
  uint64_t start_time = get_sys_clock();
  uint64_t timespan;
  sem_wait(&_semaphore);

  timespan = get_sys_clock() - start_time;
  txn->txn_stats.cc_block_time += timespan;
  txn->txn_stats.cc_block_time_short += timespan;
  INC_STATS(txn->get_thd_id(), dta_cs_wait_time, timespan);
  start_time = get_sys_clock();
  uint64_t lower = dta_time_table.get_lower(txn->get_thd_id(), txn->get_txn_id());
  uint64_t upper = dta_time_table.get_upper(txn->get_thd_id(), txn->get_txn_id());
  DEBUG("DTA Validate Start %ld: [%lu,%lu]\n", txn->get_txn_id(), lower, upper);

  dta_set_ent* wset;
  dta_set_ent* rset;
  get_rw_set(txn, rset, wset);

  DEBUG("DTA write set size %ld: %u \n", txn->get_txn_id(), wset->set_size);
  if (!wset->set_size) {
    goto VALID_END;
  }

  // lower bound of txn greater than write timestamp
  if (lower <= txn->greatest_write_timestamp) {
    lower = txn->greatest_write_timestamp + 1;
    INC_STATS(txn->get_thd_id(), dta_case1_cnt, 1);
  }

  // Examine each element in the write set
  for (UInt32 i = 0; i < wset->set_size; i++) {
    // 1. get the max read timestamp, and just the lower
    row_t* cur_wrow = wset->rows[i];
    if (lower <= cur_wrow->manager->timestamp_last_read) {
      lower = cur_wrow->manager->timestamp_last_read + 1;
    }
    if (lower >= upper) goto VALID_END;

    // 2. write in the key's write xid
    if (cur_wrow->manager->write_trans) {
      if (cur_wrow->manager->write_trans != txn->get_txn_id()) {
        dta_time_table.set_state(txn->get_thd_id(), txn->get_txn_id(), DTA_ABORTED);
        rc = Abort;
        goto FINISH;
      }
    } else {
      cur_wrow->manager->write_trans = txn->get_txn_id();
    }

    // 3. find the key's read xids, adjust their lower and upper
    std::set<uint64_t>* readxid_list = cur_wrow->manager->uncommitted_reads;

    for (auto it = readxid_list->begin(); it != readxid_list->end(); it++) {
      if (lower >= upper) goto VALID_END;

      uint64_t txn_id = *it;
      DEBUG("    UR %ld -- %ld: %ld\n", txn->get_txn_id(), cur_wrow->get_primary_key(), txn_id);
      if (txn_id == txn->get_txn_id()) continue;
      uint64_t it_upper = dta_time_table.get_upper(txn->get_thd_id(), *it);
      uint64_t it_lower = dta_time_table.get_lower(txn->get_thd_id(), *it);
      if (it_lower >= it_upper) continue;
      DTAState state = dta_time_table.get_state(txn->get_thd_id(), *it);
      if (state == DTA_VALIDATED || state == DTA_COMMITTED) {
        INC_STATS(txn->get_thd_id(), dta_case4_cnt, 1);
        if (lower < it_upper) {
          lower = it_upper;
        }
      } else if (state == DTA_RUNNING) {
        if (lower <= it_lower) {
          // TRANS_LOG_WARN("DTAvalidation set lower = dta_txn->lower + 1, transid:%lu
          // running_txn_id:%lu lower:%lu upper:%lu running_txn_id.lower:%lu
          // running_txn_id.upper:%lu", ctx, part_ctx->GetTransID(), lower, upper, dta_txn->lower,
          // dta_txn->upper);
          lower = it_lower + 1;
          it_upper = lower < it_upper ? lower : it_upper;
          dta_time_table.set_upper(txn->get_thd_id(), *it, it_upper);
        } else if (lower < it_upper) {
          // TRANS_LOG_WARN("DTAvalidation set running_txn.upper < ctx.lower, transid:%lu
          // running_txn_id:%lu lower:%lu upper:%lu running_txn_id.lower:%lu
          // running_txn_id.upper:%lu", ctx, part_ctx->GetTransID(), lower, upper, dta_txn->lower,
          // dta_txn->upper);
          it_upper = lower;
          dta_time_table.set_upper(txn->get_thd_id(), *it, it_upper);
        }
      }
    }
  }

VALID_END:
  if (lower >= upper) {
    dta_time_table.set_state(txn->get_thd_id(), txn->get_txn_id(), DTA_ABORTED);
    rc = Abort;
  } else {
    dta_time_table.set_state(txn->get_thd_id(), txn->get_txn_id(), DTA_VALIDATED);
    rc = RCOK;
    assert(lower < upper);
    INC_STATS(txn->get_thd_id(), dta_range, upper - lower);
    INC_STATS(txn->get_thd_id(), dta_commit_cnt, 1);
  }

  dta_time_table.set_lower(txn->get_thd_id(), txn->get_txn_id(), lower);
  dta_time_table.set_upper(txn->get_thd_id(), txn->get_txn_id(), upper);

FINISH:
  INC_STATS(txn->get_thd_id(), dta_validate_cnt, 1);
  timespan = get_sys_clock() - start_time;
  INC_STATS(txn->get_thd_id(), dta_validate_time, timespan);
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;

  DEBUG("DTA Validate End %ld: %d [%lu,%lu]\n", txn->get_txn_id(), rc == RCOK, lower, upper);
  sem_post(&_semaphore);
#endif
  return rc;
}

RC Dta::get_rw_set(TxnManager* txn, dta_set_ent*& rset, dta_set_ent*& wset) {
  wset = (dta_set_ent*)mem_allocator.alloc(sizeof(dta_set_ent));
  rset = (dta_set_ent*)mem_allocator.alloc(sizeof(dta_set_ent));
  wset->set_size = txn->get_write_set_size();
  rset->set_size = txn->get_read_set_size();
  wset->rows = (row_t**)mem_allocator.alloc(sizeof(row_t*) * wset->set_size);
  rset->rows = (row_t**)mem_allocator.alloc(sizeof(row_t*) * rset->set_size);
  wset->txn = txn;
  rset->txn = txn;

  UInt32 n = 0, m = 0;
  for (uint64_t i = 0; i < wset->set_size + rset->set_size; i++) {
    if (txn->get_access_type(i) == WR)
      wset->rows[n++] = txn->get_access_original_row(i);
    else
      rset->rows[m++] = txn->get_access_original_row(i);
  }

  assert(n == wset->set_size);
  assert(m == rset->set_size);

  return RCOK;
}

RC Dta::find_bound(TxnManager* txn) {
  RC rc = RCOK;
#if CC_ALG == DTA
  uint64_t lower = dta_time_table.get_lower(txn->get_thd_id(), txn->get_txn_id());
  uint64_t upper = dta_time_table.get_upper(txn->get_thd_id(), txn->get_txn_id());
  if (lower >= upper) {
    dta_time_table.set_state(txn->get_thd_id(), txn->get_txn_id(), DTA_VALIDATED);
    rc = Abort;
  } else {
    dta_time_table.set_state(txn->get_thd_id(), txn->get_txn_id(), DTA_COMMITTED);
    // TODO: can commit_time be selected in a smarter way?
    txn->commit_timestamp = lower;
    // dta_time_table.set_upper(txn->get_thd_id(),txn->get_txn_id(),txn->commit_timestamp+1);
  }
  DEBUG("DTA Bound %ld: %d [%lu,%lu] %lu\n", txn->get_txn_id(), rc, lower, upper,
        txn->commit_timestamp);
#endif
  return rc;
}

void DtaTimeTable::init() {
  // table_size = g_inflight_max * g_node_cnt * 2 + 1;
  table_size = g_inflight_max + 1;
  DEBUG_M("DtaTimeTable::init table alloc\n");
  table = (DtaTimeTableNode*)mem_allocator.alloc(sizeof(DtaTimeTableNode) * table_size);
  for (uint64_t i = 0; i < table_size; i++) {
    table[i].init();
  }
}

uint64_t DtaTimeTable::hash(uint64_t key) { return key % table_size; }

DtaTimeTableEntry* DtaTimeTable::find(uint64_t key) {
  DtaTimeTableEntry* entry = table[hash(key)].head;
  while (entry) {
    if (entry->key == key) break;
    entry = entry->next;
  }
  return entry;
}

void DtaTimeTable::init(uint64_t thd_id, uint64_t key, uint64_t ts) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id, mtx[34], get_sys_clock() - mtx_wait_starttime);
  DtaTimeTableEntry* entry = find(key);
  if (!entry) {
    DEBUG_M("DtaTimeTable::init entry alloc\n");
    entry = (DtaTimeTableEntry*)mem_allocator.alloc(sizeof(DtaTimeTableEntry));
    entry->init(key, ts);
    LIST_PUT_TAIL(table[idx].head, table[idx].tail, entry);
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

void DtaTimeTable::release(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id, mtx[35], get_sys_clock() - mtx_wait_starttime);
  DtaTimeTableEntry* entry = find(key);
  if (entry) {
    LIST_REMOVE_HT(entry, table[idx].head, table[idx].tail);
    DEBUG_M("DtaTimeTable::release entry free\n");
    mem_allocator.free(entry, sizeof(DtaTimeTableEntry));
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

uint64_t DtaTimeTable::get_lower(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t value = 0;
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id, mtx[36], get_sys_clock() - mtx_wait_starttime);
  DtaTimeTableEntry* entry = find(key);
  if (entry) {
    value = entry->lower;
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return value;
}

uint64_t DtaTimeTable::get_upper(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  uint64_t value = UINT64_MAX;
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id, mtx[37], get_sys_clock() - mtx_wait_starttime);
  DtaTimeTableEntry* entry = find(key);
  if (entry) {
    value = entry->upper;
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return value;
}

void DtaTimeTable::set_lower(uint64_t thd_id, uint64_t key, uint64_t value) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id, mtx[38], get_sys_clock() - mtx_wait_starttime);
  DtaTimeTableEntry* entry = find(key);
  if (entry) {
    entry->lower = value;
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

void DtaTimeTable::set_upper(uint64_t thd_id, uint64_t key, uint64_t value) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id, mtx[39], get_sys_clock() - mtx_wait_starttime);
  DtaTimeTableEntry* entry = find(key);
  if (entry) {
    entry->upper = value;
  }
  pthread_mutex_unlock(&table[idx].mtx);
}

DTAState DtaTimeTable::get_state(uint64_t thd_id, uint64_t key) {
  uint64_t idx = hash(key);
  DTAState state = DTA_ABORTED;
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id, mtx[40], get_sys_clock() - mtx_wait_starttime);
  DtaTimeTableEntry* entry = find(key);
  if (entry) {
    state = entry->state;
  }
  pthread_mutex_unlock(&table[idx].mtx);
  return state;
}

void DtaTimeTable::set_state(uint64_t thd_id, uint64_t key, DTAState value) {
  uint64_t idx = hash(key);
  uint64_t mtx_wait_starttime = get_sys_clock();
  pthread_mutex_lock(&table[idx].mtx);
  INC_STATS(thd_id, mtx[41], get_sys_clock() - mtx_wait_starttime);
  DtaTimeTableEntry* entry = find(key);
  if (entry) {
    entry->state = value;
  }
  pthread_mutex_unlock(&table[idx].mtx);
}
