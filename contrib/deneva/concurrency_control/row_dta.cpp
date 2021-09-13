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

#include "row_dta.h"

#include "dta.h"
#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include "txn.h"

void Row_dta::init(row_t* row) {
  _row = row;
  max_version_ = 1;
  timestamp_last_read = 0;
  timestamp_last_write = 0;
  dta_avail = true;
  // uncommitted_writes = new std::set<uint64_t>();
  uncommitted_reads = new std::set<uint64_t>();
  write_trans = 0;
  //   assert(uncommitted_writes->begin() == uncommitted_writes->end());
  //   assert(uncommitted_writes->size() == 0);

  // multi-version part
  readreq_mvcc = NULL;
  prereq_mvcc = NULL;
  // readhis = NULL;
  writehis = NULL;
  // readhistail = NULL;
  writehistail = NULL;
  blatch = false;
  latch = (pthread_mutex_t*)mem_allocator.alloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(latch, NULL);
  whis_len = 0;
  // rhis_len = 0;
  rreq_len = 0;
  preq_len = 0;
}

RC Row_dta::access(TsType type, TxnManager* txn, row_t* row, uint64_t& version) {
  uint64_t starttime = get_sys_clock();
  RC rc = RCOK;

  rc = read_and_write(type, txn, row, version);

  uint64_t timespan = get_sys_clock() - starttime;
  txn->txn_stats.cc_time += timespan;
  txn->txn_stats.cc_time_short += timespan;
  return rc;
}

RC Row_dta::read_and_write(TsType type, TxnManager* txn, row_t* row, uint64_t& version) {
  assert(CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3);
  RC rc = RCOK;

  uint64_t mtx_wait_starttime = get_sys_clock();
  while (!ATOM_CAS(dta_avail, true, false)) {
  }
  INC_STATS(txn->get_thd_id(), mtx[30], get_sys_clock() - mtx_wait_starttime);

  if (type == 0) {
    DEBUG("READ %ld -- %ld: lw %ld\n", txn->get_txn_id(), _row->get_primary_key(),
          timestamp_last_read);
  } else if (type == 1) {
    DEBUG("COMMIT-WRITE %ld -- %ld: lw %ld\n", txn->get_txn_id(), _row->get_primary_key(),
          timestamp_last_read);
  } else if (type == 2) {
    DEBUG("WRITE (P_REQ) %ld -- %ld: lw %ld\n", txn->get_txn_id(), _row->get_primary_key(),
          timestamp_last_read);
  } else if (type == 3) {
    DEBUG("XP-REQ %ld -- %ld: lw %ld\n", txn->get_txn_id(), _row->get_primary_key(),
          timestamp_last_read);
  }

  // Add to uncommitted reads (soft lock)
  uncommitted_reads->insert(txn->get_txn_id());

  // =======TODO: Do we need using write_txn to adjust it's upper?

  // Fetch the previous version
  ts_t ts = txn->get_timestamp();

  if (g_central_man)
    glob_manager.lock_row(_row);
  else
    pthread_mutex_lock(latch);

  if (type == R_REQ) {
    // figure out if ts is in interval(prewrite(x))
    // bool conf = conflict(type, ts);
    bool conf = false;
    if (conf && rreq_len < g_max_read_req) {
      rc = WAIT;
      // txn->wait_starttime = get_sys_clock();
      DEBUG("buf R_REQ %ld %ld\n", txn->get_txn_id(), _row->get_primary_key());
      buffer_req(R_REQ, txn);
      txn->ts_ready = false;
    } else {
      // return results immediately.
      rc = RCOK;
      DTAMVHisEntry* whis = writehis;
      while (whis_len && whis != NULL && whis->ts > ts) whis = whis->next;
      row_t* ret = (whis == NULL) ? _row : whis->row;
      version = (whis == NULL) ? 0 : whis->version;
      txn->cur_row = ret;
      // insert_history(ts, NULL);
      assert(strstr(_row->get_table_name(), ret->get_table_name()));
    }

    // Adjust txn.lower
    // uint64_t lower =  dta_time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
    // if (lower < timestamp_last_read)
    // 	dta_time_table.set_lower(txn->get_thd_id(),txn->get_txn_id(), timestamp_last_read + 1);

  } else if (type == P_REQ) {
    /*if ( conflict(type, ts) ) {
            rc = Abort;
    } else*/
    if (preq_len < g_max_pre_req) {
      DEBUG("buf P_REQ %ld %ld\n", txn->get_txn_id(), _row->get_primary_key());
      buffer_req(P_REQ, txn);
      rc = RCOK;
    } else {
      rc = Abort;
    }
  } else
    assert(false);

  if (rc == RCOK) {
    if (whis_len > g_his_recycle_len) {
      // if (whis_len > g_his_recycle_len || rhis_len > g_his_recycle_len) {
      ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
      // if (readhistail && readhistail->ts < t_th)
      // 	clear_history(R_REQ, t_th);
      // Here is a tricky bug. The oldest transaction might be
      // reading an even older version whose timestamp < t_th.
      // But we cannot recycle that version because it is still being used.
      // So the HACK here is to make sure that the first version older than
      // t_th not be recycled.
      if (whis_len > 1 && writehistail->prev->ts < t_th) {
        row_t* latest_row = clear_history(W_REQ, t_th);
        if (latest_row != NULL) {
          assert(_row != latest_row);
          _row->copy(latest_row);
        }
      }
    }
  }

  if (g_central_man)
    glob_manager.release_row(_row);
  else
    pthread_mutex_unlock(latch);

  ATOM_CAS(dta_avail, false, true);

  return rc;
}

RC Row_dta::prewrite(TxnManager* txn) {
  assert(CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3);
  RC rc = RCOK;

  uint64_t mtx_wait_starttime = get_sys_clock();
  while (!ATOM_CAS(dta_avail, true, false)) {
  }
  INC_STATS(txn->get_thd_id(), mtx[31], get_sys_clock() - mtx_wait_starttime);
  DEBUG("PREWRITE %ld -- %ld: lw %ld, lr %ld\n", txn->get_txn_id(), _row->get_primary_key(),
        timestamp_last_write, timestamp_last_read);

  // // Copy uncommitted reads
  // for(auto it = uncommitted_reads->begin(); it != uncommitted_reads->end(); it++) {
  //   uint64_t txn_id = *it;
  //   txn->uncommitted_reads->insert(txn_id);
  //   DEBUG("    UR %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),txn_id);
  // }

  // // Copy uncommitted writes
  // for(auto it = uncommitted_writes->begin(); it != uncommitted_writes->end(); it++) {
  //   uint64_t txn_id = *it;
  //   txn->uncommitted_writes_y->insert(txn_id);
  //   DEBUG("    UW %ld -- %ld: %ld\n",txn->get_txn_id(),_row->get_primary_key(),txn_id);
  // }

  // // Copy read timestamp
  // if(txn->greatest_read_timestamp < timestamp_last_read)
  //   txn->greatest_read_timestamp = timestamp_last_read;

  // // Copy write timestamp
  // if(txn->greatest_write_timestamp < timestamp_last_write)
  //   txn->greatest_write_timestamp = timestamp_last_write;

  // Add to uncommitted writes (soft lock)
  // uncommitted_writes->insert(txn->get_txn_id());

  ATOM_CAS(dta_avail, false, true);

  return rc;
}

RC Row_dta::abort(access_t type, TxnManager* txn) {
  uint64_t mtx_wait_starttime = get_sys_clock();
  while (!ATOM_CAS(dta_avail, true, false)) {
  }
  INC_STATS(txn->get_thd_id(), mtx[32], get_sys_clock() - mtx_wait_starttime);
  DEBUG("dta Abort %ld: %d -- %ld\n", txn->get_txn_id(), type, _row->get_primary_key());

  uncommitted_reads->erase(txn->get_txn_id());

  if (type == WR && write_trans == txn->get_txn_id()) {
    write_trans = 0;
  }

  if (type == XP) {
    write_trans = 0;
    DEBUG("debuf %ld %ld\n", txn->get_txn_id(), _row->get_primary_key());
    DTAMVReqEntry* req = debuffer_req(P_REQ, txn);
    assert(req != NULL);
    return_req_entry(req);
    // update_buffer(txn);
  }

  ATOM_CAS(dta_avail, false, true);
  return Abort;
}

RC Row_dta::commit(access_t type, TxnManager* txn, row_t* data, uint64_t& version) {
  uint64_t mtx_wait_starttime = get_sys_clock();
  while (!ATOM_CAS(dta_avail, true, false)) {
  }
  INC_STATS(txn->get_thd_id(), mtx[33], get_sys_clock() - mtx_wait_starttime);
  DEBUG("dta Commit %ld: %d,%lu -- %ld\n", txn->get_txn_id(), type, txn->get_commit_timestamp(),
        _row->get_primary_key());

  uint64_t txn_commit_ts = txn->get_commit_timestamp();

  if (txn_commit_ts > timestamp_last_read) timestamp_last_read = txn_commit_ts;
  uncommitted_reads->erase(txn->get_txn_id());

  if (type == WR) {
    // ts_t ts = txn->get_timestamp();

    // the corresponding prewrite request is debuffered.
    version = insert_history(txn_commit_ts, data);
    DEBUG("dta insert histroy %ld: %lu -- %ld\n", txn->get_txn_id(), txn->get_commit_timestamp(),
          data->get_primary_key());
    DEBUG("debuf %ld %ld\n", txn->get_txn_id(), _row->get_primary_key());
    DTAMVReqEntry* req = debuffer_req(P_REQ, txn);
    assert(req != NULL);
    return_req_entry(req);
    // update_buffer(txn);

    write_trans = 0;
  }

  ATOM_CAS(dta_avail, false, true);
  return RCOK;
}

row_t* Row_dta::clear_history(TsType type, ts_t ts) {
  DTAMVHisEntry** queue;
  DTAMVHisEntry** tail;
  switch (type) {
    // case R_REQ : queue = &readhis; tail = &readhistail; break;
    case W_REQ:
      queue = &writehis;
      tail = &writehistail;
      break;
    default:
      assert(false);
  }
  DTAMVHisEntry* his = *tail;
  DTAMVHisEntry* prev = NULL;
  row_t* row = NULL;
  while (his && his->prev && his->prev->ts < ts) {
    prev = his->prev;
    assert(prev->ts >= his->ts);
    if (row != NULL) {
      row->free_row();
      mem_allocator.free(row, sizeof(row_t));
    }
    row = his->row;
    his->row = NULL;
    return_his_entry(his);
    his = prev;
    // if (type == R_REQ) rhis_len --;
    // else
    whis_len--;
  }
  *tail = his;
  if (*tail) (*tail)->next = NULL;
  if (his == NULL) *queue = NULL;
  return row;
}

DTAMVReqEntry* Row_dta::get_req_entry() {
  return (DTAMVReqEntry*)mem_allocator.alloc(sizeof(DTAMVReqEntry));
}

void Row_dta::return_req_entry(DTAMVReqEntry* entry) {
  mem_allocator.free(entry, sizeof(DTAMVReqEntry));
}

DTAMVHisEntry* Row_dta::get_his_entry() {
  return (DTAMVHisEntry*)mem_allocator.alloc(sizeof(DTAMVHisEntry));
}

void Row_dta::return_his_entry(DTAMVHisEntry* entry) {
  if (entry->row != NULL) {
    entry->row->free_row();
    mem_allocator.free(entry->row, sizeof(row_t));
  }
  mem_allocator.free(entry, sizeof(DTAMVHisEntry));
}

void Row_dta::buffer_req(TsType type, TxnManager* txn) {
  DTAMVReqEntry* req_entry = get_req_entry();
  assert(req_entry != NULL);
  req_entry->txn = txn;
  req_entry->ts = txn->get_timestamp();
  req_entry->starttime = get_sys_clock();
  if (type == R_REQ) {
    rreq_len++;
    STACK_PUSH(readreq_mvcc, req_entry);
  } else if (type == P_REQ) {
    preq_len++;
    STACK_PUSH(prereq_mvcc, req_entry);
  }
}

// for type == R_REQ
//	 debuffer all non-conflicting requests
// for type == P_REQ
//   debuffer the request with matching txn.
DTAMVReqEntry* Row_dta::debuffer_req(TsType type, TxnManager* txn) {
  DTAMVReqEntry** queue;
  DTAMVReqEntry* return_queue = NULL;
  switch (type) {
    case R_REQ:
      queue = &readreq_mvcc;
      break;
    case P_REQ:
      queue = &prereq_mvcc;
      break;
    default:
      assert(false);
  }

  DTAMVReqEntry* req = *queue;
  DTAMVReqEntry* prev_req = NULL;
  if (txn != NULL) {
    assert(type == P_REQ);
    while (req != NULL && req->txn != txn) {
      prev_req = req;
      req = req->next;
    }
    assert(req != NULL);
    if (prev_req != NULL)
      prev_req->next = req->next;
    else {
      assert(req == *queue);
      *queue = req->next;
    }
    preq_len--;
    req->next = return_queue;
    return_queue = req;
  } else {
    assert(type == R_REQ);
    // should return all non-conflicting read requests
    // The following code makes the assumption that each write op
    // must read the row first. i.e., there is no write-only operation.
    uint64_t min_pts = UINT64_MAX;
    // uint64_t min_pts = (1UL << 32);
    for (DTAMVReqEntry* preq = prereq_mvcc; preq != NULL; preq = preq->next)
      if (preq->ts < min_pts) min_pts = preq->ts;
    while (req != NULL) {
      if (req->ts <= min_pts) {
        if (prev_req == NULL) {
          assert(req == *queue);
          *queue = (*queue)->next;
        } else
          prev_req->next = req->next;
        rreq_len--;
        req->next = return_queue;
        return_queue = req;
        req = (prev_req == NULL) ? *queue : prev_req->next;
      } else {
        prev_req = req;
        req = req->next;
      }
    }
  }

  return return_queue;
}

uint64_t Row_dta::insert_history(ts_t ts, row_t* row) {
  DTAMVHisEntry* new_entry = get_his_entry();
  new_entry->version = max_version_;
  ++max_version_;
  new_entry->ts = ts;
  new_entry->row = row;
  if (row != NULL) whis_len++;
  // else rhis_len ++;
  // DTAMVHisEntry ** queue = (row == NULL)?
  // 	&(readhis) : &(writehis);
  // DTAMVHisEntry ** tail = (row == NULL)?
  // 	&(readhistail) : &(writehistail);

  DTAMVHisEntry** queue = &(writehis);
  DTAMVHisEntry** tail = &(writehistail);
  DTAMVHisEntry* his = *queue;
  while (his != NULL && ts < his->ts) {
    his = his->next;
  }

  if (his) {
    LIST_INSERT_BEFORE(his, new_entry, (*queue));
    // if (his == *queue)
    //	*queue = new_entry;
  } else
    LIST_PUT_TAIL((*queue), (*tail), new_entry);
  return new_entry->version;
}

bool Row_dta::conflict(TsType type, ts_t ts) {
  // find the unique prewrite-read couple (prewrite before read)
  // if no such couple found, no conflict.
  // else
  // 	 if exists writehis between them, NO conflict!!!!
  // 	 else, CONFLICT!!!
  ts_t rts;
  ts_t pts;
  if (type == R_REQ) {
    rts = ts;
    pts = 0;
    DTAMVReqEntry* req = prereq_mvcc;
    while (req != NULL) {
      if (req->ts < ts && req->ts > pts) {
        pts = req->ts;
      }
      req = req->next;
    }
    if (pts == 0)  // no such couple exists
      return false;
  } else if (type == P_REQ) {
    rts = 0;
    pts = ts;
    // DTAMVHisEntry * his = readhis;
    // while (his != NULL) {
    // 	if (his->ts > ts) {
    // 		rts = his->ts;
    // 	} else
    // 		break;
    // 	his = his->next;
    // }
    if (rts == 0)  // no couple exists
      return false;
    assert(rts > pts);
  }
  DTAMVHisEntry* whis = writehis;
  while (whis != NULL && whis->ts > pts) {
    if (whis->ts < rts) return false;
    whis = whis->next;
  }
  return true;
}

void Row_dta::update_buffer(TxnManager* txn) {
  DTAMVReqEntry* ready_read = debuffer_req(R_REQ, NULL);
  DTAMVReqEntry* req = ready_read;
  DTAMVReqEntry* tofree = NULL;

  while (req != NULL) {
    // find the version for the request
    DTAMVHisEntry* whis = writehis;
    while (whis != NULL && whis->ts > req->ts) whis = whis->next;
    row_t* row = (whis == NULL) ? _row : whis->row;
    req->txn->cur_row = row;
    // insert_history(req->ts, NULL);
    assert(row->get_data() != NULL);
    assert(row->get_table() != NULL);
    assert(row->get_schema() == _row->get_schema());

    req->txn->ts_ready = true;
    uint64_t timespan = get_sys_clock() - req->starttime;
    req->txn->txn_stats.cc_block_time += timespan;
    req->txn->txn_stats.cc_block_time_short += timespan;
    txn_table.restart_txn(txn->get_thd_id(), req->txn->get_txn_id(), 0);
    tofree = req;
    req = req->next;
    // free ready_read
    return_req_entry(tofree);
  }
}
