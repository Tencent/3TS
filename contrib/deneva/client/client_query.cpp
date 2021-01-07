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

#include "client_query.h"
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

typedef struct
{
    void* context;
    int thd_id;
}FUNC_ARGS;

void
Client_query_queue::init(Workload * h_wl) {
    _wl = h_wl;


#if SERVER_GENERATE_QUERIES
    if (ISCLIENT) return;
    size = g_thread_cnt;
#else
    size = g_servers_per_client;
#endif
    query_cnt = new uint64_t * [size];
    for ( UInt32 id = 0; id < size; id ++) {
        std::vector<BaseQuery*> new_queries(g_max_txn_per_part+4,NULL);
        queries.push_back(new_queries);
        query_cnt[id] = (uint64_t*)mem_allocator.align_alloc(sizeof(uint64_t));
    }
    next_tid = 0;
#if WORKLOAD == DA
    FUNC_ARGS *arg=(FUNC_ARGS*)mem_allocator.align_alloc(sizeof(FUNC_ARGS));
    arg->context=this;
    arg->thd_id=g_init_parallelism - 1;
    pthread_t  p_thds_main;
    pthread_create(&p_thds_main, NULL, initQueriesHelper, (void*)arg );
    pthread_detach(p_thds_main);
#else
    pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
    for (uint64_t i = 0; i < g_init_parallelism - 1; i++) {
        FUNC_ARGS *arg=(FUNC_ARGS*)mem_allocator.align_alloc(sizeof(FUNC_ARGS));
        arg->context=this;
        arg->thd_id=i;
        pthread_create(&p_thds[i], NULL, initQueriesHelper, (void*)arg );
    }
    FUNC_ARGS *arg=(FUNC_ARGS*)mem_allocator.align_alloc(sizeof(FUNC_ARGS));
    arg->context=this;
    arg->thd_id=g_init_parallelism - 1;

    initQueriesHelper(arg);

    for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
        pthread_join(p_thds[i], NULL);
    }
#endif
}

void *
Client_query_queue::initQueriesHelper(void * args) {
    ((Client_query_queue*)((FUNC_ARGS*)args)->context)->initQueriesParallel(((FUNC_ARGS*)args)->thd_id);

    return NULL;
}

void
Client_query_queue::initQueriesParallel(uint64_t thd_id) {
#if WORKLOAD != DA
    UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
    uint64_t request_cnt;
    request_cnt = g_max_txn_per_part + 4;

    uint32_t final_request;
#if CC_ALG == BOCC || CC_ALG == FOCC
    if (tid == g_init_parallelism-1) {
        final_request = request_cnt * g_servers_per_client;
    } else {
        final_request = request_cnt * g_servers_per_client / g_init_parallelism * (tid+1);
    }
#else
    if (tid == g_init_parallelism-1) {
        final_request = request_cnt;
    } else {
        final_request = request_cnt / g_init_parallelism * (tid+1);
    }
#endif
#endif
#if WORKLOAD == YCSB
    YCSBQueryGenerator * gen = new YCSBQueryGenerator;
    gen->init();
#elif WORKLOAD == TPCC
    TPCCQueryGenerator * gen = new TPCCQueryGenerator;
#elif WORKLOAD == PPS
    PPSQueryGenerator * gen = new PPSQueryGenerator;
#elif WORKLOAD == DA
    DAQueryGenerator  * gen = new DAQueryGenerator;
#endif
#if SERVER_GENERATE_QUERIES
    #if CC_ALG == BOCC || CC_ALG == FOCC
    for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
        queries[thread_id][query_id] = gen->create_query(_wl,g_node_id);
    }
    #else
    for ( UInt32 thread_id = 0; thread_id < g_thread_cnt; thread_id ++) {
        for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request;
            query_id++) {
        queries[thread_id][query_id] = gen->create_query(_wl,g_node_id);
        }
    }
    #endif
#elif WORKLOAD == DA
    gen->create_query(_wl,thd_id);
#else
#if CC_ALG == BOCC || CC_ALG == FOCC
    for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request; query_id ++) {
        queries[0][query_id] = gen->create_query(_wl,g_server_start_node);
    }
#else
    for ( UInt32 server_id = 0; server_id < g_servers_per_client; server_id ++) {
        for (UInt32 query_id = request_cnt / g_init_parallelism * tid; query_id < final_request;
            query_id++) {
        queries[server_id][query_id] = gen->create_query(_wl,server_id+g_server_start_node);
        }
    }
#endif
#endif
}

bool Client_query_queue::done() { return false; }

BaseQuery *
Client_query_queue::get_next_query(uint64_t server_id,uint64_t thread_id) {
#if WORKLOAD == DA
    BaseQuery * query;
    query=da_gen_qry_queue.pop_data();
    //while(!da_query_queue.pop(query));
    return query;
#else
    assert(server_id < size);
    uint64_t query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
    if(query_id > g_max_txn_per_part) {
        __sync_bool_compare_and_swap(query_cnt[server_id],query_id+1,0);
        query_id = __sync_fetch_and_add(query_cnt[server_id], 1);
    }
    BaseQuery * query = queries[server_id][query_id];
    return query;
#endif
}
