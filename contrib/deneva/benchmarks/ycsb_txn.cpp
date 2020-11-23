/*
    Tencent is pleased to support the open source community by making 3TS available.

    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
    in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
    Tencent Modifications are Copyright (C) THL A29 Limited.

    Author: hongyaozhao@ruc.edu.cn

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
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"
#include "message.h"

void YCSBTxnManager::init(uint64_t thd_id, Workload * h_wl) {
    TxnManager::init(thd_id, h_wl);
    _wl = (YCSBWorkload *) h_wl;
    reset();
}

void YCSBTxnManager::reset() {
  state = YCSB_0;
  next_record_id = 0;
    TxnManager::reset();
}

RC YCSBTxnManager::acquire_locks() {
    uint64_t starttime = get_sys_clock();
    assert(CC_ALG == CALVIN);
    YCSBQuery* ycsb_query = (YCSBQuery*) query;
    locking_done = false;
    RC rc = RCOK;
    incr_lr();
    assert(ycsb_query->requests.size() == g_req_per_query);
    assert(phase == CALVIN_RW_ANALYSIS);
    for (uint32_t rid = 0; rid < ycsb_query->requests.size(); rid ++) {
        ycsb_request * req = ycsb_query->requests[rid];
        uint64_t part_id = _wl->key_to_part( req->key );
    DEBUG("LK Acquire (%ld,%ld) %d,%ld -> %ld\n", get_txn_id(), get_batch_id(), req->acctype,
          req->key, GET_NODE_ID(part_id));
    if (GET_NODE_ID(part_id) != g_node_id) continue;
        INDEX * index = _wl->the_index;
        itemid_t * item;
        item = index_read(index, req->key, part_id);
        row_t * row = ((row_t *)item->location);
        RC rc2 = get_lock(row,req->acctype);
    if(rc2 != RCOK) {
      rc = rc2;
    }
    }
    if(decr_lr() == 0) {
        if (ATOM_CAS(lock_ready, false, true)) rc = RCOK;
    }
    txn_stats.wait_starttime = get_sys_clock();
    /*
    if(rc == WAIT && lock_ready_cnt == 0) {
        if(ATOM_CAS(lock_ready,false,true))
        //lock_ready = true;
        rc = RCOK;
    }
    */
    INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
    locking_done = true;
    return rc;
}


RC YCSBTxnManager::run_txn() {
  RC rc = RCOK;
  assert(CC_ALG != CALVIN);

  if(IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
    DEBUG("Running txn %ld\n",txn->txn_id);
    //query->print();
    query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
  }

  uint64_t starttime = get_sys_clock();

  while(rc == RCOK && !is_done()) {
    rc = run_txn_state();
  }

  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();

  if(IS_LOCAL(get_txn_id())) {
    if(is_done() && rc == RCOK)
      rc = start_commit();
    else if(rc == Abort)
      rc = start_abort();
  } else if(rc == Abort){
    rc = abort();
  }

  return rc;

}

RC YCSBTxnManager::run_txn_post_wait() {
    get_row_post_wait(row);
    next_ycsb_state();
    return RCOK;
}

bool YCSBTxnManager::is_done() { return next_record_id >= ((YCSBQuery*)query)->requests.size(); }

void YCSBTxnManager::next_ycsb_state() {
  switch(state) {
    case YCSB_0:
      state = YCSB_1;
      break;
    case YCSB_1:
      next_record_id++;
      if(!IS_LOCAL(txn->txn_id) || !is_done()) {
        state = YCSB_0;
      } else {
        state = YCSB_FIN;
      }
      break;
    case YCSB_FIN:
      break;
    default:
      assert(false);
  }
}

bool YCSBTxnManager::is_local_request(uint64_t idx) {
  return GET_NODE_ID(_wl->key_to_part(((YCSBQuery*)query)->requests[idx]->key)) == g_node_id;
}

RC YCSBTxnManager::send_remote_request() {
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
  ycsb_query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
  msg_queue.enqueue(get_thd_id(),Message::create_message(this,RQRY),dest_node_id);
  return WAIT_REM;
}

void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage * msg) {
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  //msg->requests.init(ycsb_query->requests.size());
  uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
  while (next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) &&
         GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
    YCSBQuery::copy_request_to_msg(ycsb_query,msg,next_record_id++);
  }
}

RC YCSBTxnManager::run_txn_state() {
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
    ycsb_request * req = ycsb_query->requests[next_record_id];
    uint64_t part_id = _wl->key_to_part( req->key );
  bool loc = GET_NODE_ID(part_id) == g_node_id;

    RC rc = RCOK;

    switch (state) {
        case YCSB_0 :
      if(loc) {
        rc = run_ycsb_0(req,row);
      } else {
        rc = send_remote_request();

      }

      break;
        case YCSB_1 :
      rc = run_ycsb_1(req->acctype,row);
      break;
    case YCSB_FIN :
      state = YCSB_FIN;
      break;
    default:
            assert(false);
  }

  if (rc == RCOK) next_ycsb_state();

  return rc;
}

RC YCSBTxnManager::run_ycsb_0(ycsb_request * req,row_t *& row_local) {
    RC rc = RCOK;
        int part_id = _wl->key_to_part( req->key );
        access_t type = req->acctype;
      itemid_t * m_item;

        m_item = index_read(_wl->the_index, req->key, part_id);

        row_t * row = ((row_t *)m_item->location);

        rc = get_row(row, type,row_local);

    return rc;

}

RC YCSBTxnManager::run_ycsb_1(access_t acctype, row_t * row_local) {
  if (acctype == RD || acctype == SCAN) {
    int fid = 0;
        char * data = row_local->get_data();
        uint64_t fval __attribute__ ((unused));
    fval = *(uint64_t *)(&data[fid * 100]);
#if ISOLATION_LEVEL == READ_COMMITTED || ISOLATION_LEVEL == READ_UNCOMMITTED
    // Release lock after read
    release_last_row_lock();
#endif

  } else {
    assert(acctype == WR);
        int fid = 0;
      char * data = row_local->get_data();
      *(uint64_t *)(&data[fid * 100]) = 0;
#if YCSB_ABORT_MODE
    if (data[0] == 'a') return RCOK;
#endif

#if ISOLATION_LEVEL == READ_UNCOMMITTED
    // Release lock after write
    release_last_row_lock();
#endif
  }
  return RCOK;
}
RC YCSBTxnManager::run_calvin_txn() {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
  while(!calvin_exec_phase_done() && rc == RCOK) {
    DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
    switch(this->phase) {
      case CALVIN_RW_ANALYSIS:

        // Phase 1: Read/write set analysis
        calvin_expected_rsp_cnt = ycsb_query->get_participants(_wl);
#if YCSB_ABORT_MODE
        if(query->participant_nodes[g_node_id] == 1) {
          calvin_expected_rsp_cnt--;
        }
#else
        calvin_expected_rsp_cnt = 0;
#endif
        DEBUG("(%ld,%ld) expects %d responses;\n", txn->txn_id, txn->batch_id,
              calvin_expected_rsp_cnt);

        this->phase = CALVIN_LOC_RD;
        break;
      case CALVIN_LOC_RD:
        // Phase 2: Perform local reads
        DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
        rc = run_ycsb();
        //release_read_locks(query);

        this->phase = CALVIN_SERVE_RD;
        break;
      case CALVIN_SERVE_RD:
        // Phase 3: Serve remote reads
        // If there is any abort logic, relevant reads need to be sent to all active nodes...
        if(query->participant_nodes[g_node_id] == 1) {
          rc = send_remote_reads();
        }
        if(query->active_nodes[g_node_id] == 1) {
          this->phase = CALVIN_COLLECT_RD;
          if(calvin_collect_phase_done()) {
            rc = RCOK;
          } else {
            DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n", txn->txn_id,
                  txn->batch_id, rsp_cnt, calvin_expected_rsp_cnt);
            rc = WAIT;
          }
        } else { // Done
          rc = RCOK;
          this->phase = CALVIN_DONE;
        }

        break;
      case CALVIN_COLLECT_RD:
        // Phase 4: Collect remote reads
        this->phase = CALVIN_EXEC_WR;
        break;
      case CALVIN_EXEC_WR:
        // Phase 5: Execute transaction / perform local writes
        DEBUG("(%ld,%ld) execute writes\n",txn->txn_id,txn->batch_id);
        rc = run_ycsb();
        this->phase = CALVIN_DONE;
        break;
      default:
        assert(false);
    }
  }
  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();
  return rc;
}

RC YCSBTxnManager::run_ycsb() {
  RC rc = RCOK;
  assert(CC_ALG == CALVIN);
  YCSBQuery* ycsb_query = (YCSBQuery*) query;

  for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
      ycsb_request * req = ycsb_query->requests[i];
    if (this->phase == CALVIN_LOC_RD && req->acctype == WR) continue;
    if (this->phase == CALVIN_EXEC_WR && req->acctype == RD) continue;

        uint64_t part_id = _wl->key_to_part( req->key );
    bool loc = GET_NODE_ID(part_id) == g_node_id;

    if (!loc) continue;

    rc = run_ycsb_0(req,row);
    assert(rc == RCOK);

    rc = run_ycsb_1(req->acctype,row);
    assert(rc == RCOK);
  }
  return rc;

}

