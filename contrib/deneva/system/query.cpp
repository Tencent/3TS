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

#include "query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include "da_query.h"

/*************************************************/
//     class Query_queue
/*************************************************/

void Query_queue::init(Workload *h_wl) {
    all_queries = new Query_thd * [g_thread_cnt];
    _wl = h_wl;
    for (UInt32 tid = 0; tid < g_thread_cnt; tid++) init(tid);
}

void Query_queue::init(int thread_id) {
    all_queries[thread_id] = (Query_thd *) mem_allocator.alloc(sizeof(Query_thd));
    all_queries[thread_id]->init(_wl, thread_id);
}

BaseQuery *Query_queue::get_next_query(uint64_t thd_id) {
    BaseQuery * query = all_queries[thd_id]->get_next_query();
    return query;
}

void Query_thd::init(Workload *h_wl, int thread_id) {
    uint64_t request_cnt;
    q_idx = 0;
    request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
#if WORKLOAD == YCSB    
    queries = (YCSBQuery *)mem_allocator.alloc(sizeof(YCSBQuery) * request_cnt);
#elif WORKLOAD == TPCC
    queries = (TPCCQuery *)mem_allocator.alloc(sizeof(TPCCQuery) * request_cnt);
#elif WORKLOAD == PPS
    queries = (PPSQuery *)mem_allocator.alloc(sizeof(PPSQuery) * request_cnt);
#endif
    for (UInt32 qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB    
        new(&queries[qid]) YCSBQuery();
#elif WORKLOAD == TPCC
        new(&queries[qid]) TPCCQuery();
#elif WORKLOAD == PPS
        new(&queries[qid]) PPSQuery();
#elif WORKLOAD == DA
        new(&queries[qid]) DAQuery();
#endif
        queries[qid].init(thread_id, h_wl);
    }
}

BaseQuery *Query_thd::get_next_query() {
    BaseQuery * query = &queries[q_idx++];
    return query;
}

void BaseQuery::init() { 
    DEBUG_M("BaseQuery::init array partitions\n");
    partitions.init(g_part_cnt);
    DEBUG_M("BaseQuery::init array partitions_touched\n");
    partitions_touched.init(g_part_cnt);
    DEBUG_M("BaseQuery::init array active_nodes\n");
    active_nodes.init(g_node_cnt);
    DEBUG_M("BaseQuery::init array participant_nodes\n");
    participant_nodes.init(g_node_cnt);
}

void BaseQuery::clear() { 
    partitions.clear();
    partitions_touched.clear();
    active_nodes.clear();
    participant_nodes.clear();
} 

void BaseQuery::release() { 
    partitions.release();
    partitions_touched.release();
    active_nodes.release();
    participant_nodes.release();
} 
