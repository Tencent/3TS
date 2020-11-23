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
#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"
#include "http.h"
//#include <jemallloc.h>
__thread uint64_t Manager::_max_cts = 1;
void Manager::init() {
    timestamp = 1;
    last_min_ts_time = 0;
    min_ts = 0;
    all_ts = (ts_t *) malloc(sizeof(ts_t) * (g_thread_cnt * g_node_cnt));
    _all_txns = new TxnManager * [g_thread_cnt + g_rem_thread_cnt];
    for (UInt32 i = 0; i < g_thread_cnt + g_rem_thread_cnt; i++) {
        _all_txns[i] = NULL;
    }
    for (UInt32 i = 0; i < BUCKET_CNT; i++) pthread_mutex_init(&mutexes[i], NULL);
    for (UInt32 i = 0; i < g_thread_cnt * g_node_cnt; ++i) all_ts[i] = 0;
}

uint64_t Manager::get_ts(uint64_t thread_id) {
  if (g_ts_batch_alloc) assert(g_ts_alloc == TS_CAS);
    uint64_t time;
    uint64_t starttime = get_sys_clock();
    switch(g_ts_alloc) {
    case TS_MUTEX :
        pthread_mutex_lock( &ts_mutex );
        time = ++timestamp;
        pthread_mutex_unlock( &ts_mutex );
        break;
    case TS_CAS :
        if (g_ts_batch_alloc)
            time = ATOM_FETCH_ADD(timestamp, g_ts_batch_num);
        else
            time = ATOM_FETCH_ADD(timestamp, 1);
        break;
    case TS_HW :
        assert(false);
        break;
    case TS_CLOCK :
        time = get_wall_clock() * (g_node_cnt + g_thread_cnt) + (g_node_id * g_thread_cnt + thread_id);
        break;
    case LTS_CURL_CLOCK:
        time = CurlGetTimeStamp();
        break;
    case LTS_TCP_CLOCK:
        time = tcp_ts.TcpGetTimeStamp(thread_id);

        break;
    default :
        assert(false);
    }
    INC_STATS(thread_id, ts_alloc_time, get_sys_clock() - starttime);
    return time;
}

ts_t Manager::get_min_ts(uint64_t tid) {
    uint64_t now = get_sys_clock();
    if (now - last_min_ts_time > MIN_TS_INTVL) {
        last_min_ts_time = now;
    uint64_t min = txn_table.get_min_ts(tid);
    if (min > min_ts) min_ts = min;
    }
    return min_ts;
}

void Manager::set_txn_man(TxnManager * txn) {
    int thd_id = txn->get_thd_id();
    _all_txns[thd_id] = txn;
}


uint64_t Manager::hash(row_t * row) {
    uint64_t addr = (uint64_t)row / MEM_ALLIGN;
    return (addr * 1103515247 + 12345) % BUCKET_CNT;
}

void Manager::lock_row(row_t * row) {
    int bid = hash(row);
    uint64_t mtx_time_start = get_sys_clock();
    pthread_mutex_lock( &mutexes[bid] );
    INC_STATS(0,mtx[2],get_sys_clock() - mtx_time_start);
}

void Manager::release_row(row_t * row) {
    int bid = hash(row);
    pthread_mutex_unlock( &mutexes[bid] );
}
