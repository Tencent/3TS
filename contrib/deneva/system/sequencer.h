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

#ifndef _SEQUENCER_H_
#define _SEQUENCER_H_

#include "global.h"
#include "query.h"
#include <boost/lockfree/queue.hpp>

class Workload;
class BaseQuery;
class Message;

typedef struct qlite_entry {
    BaseQuery * qry;
    uint32_t client_id;
    uint64_t client_startts;
    uint64_t seq_startts;
    uint64_t seq_first_startts;
    uint64_t skew_startts;
    uint64_t total_batch_time;
    uint32_t server_ack_cnt;
    uint32_t abort_cnt;
    Message * msg;
} qlite;

typedef struct qlite_ll_entry {
    qlite * list;
    uint64_t size;
    uint32_t max_size;
    uint32_t txns_left;
    uint64_t epoch;
    uint64_t batch_send_time;
    qlite_ll_entry * next;
    qlite_ll_entry * prev;
} qlite_ll;

class Sequencer {
 public:
    void init(Workload * wl);
    void process_ack(Message * msg, uint64_t thd_id);
    void process_txn(Message* msg, uint64_t thd_id, uint64_t early_start, uint64_t last_start,
                                     uint64_t wait_time, uint32_t abort_cnt);
    void send_next_batch(uint64_t thd_id);

 private:
    void reset_participating_nodes(bool * part_nodes);

    boost::lockfree::queue<Message*, boost::lockfree::capacity<65526> > * fill_queue;
#if WORKLOAD == YCSB
    YCSBQuery* node_queries;
#elif WORKLOAD == TPCC
    TPCCQuery* node_queries;
#elif WORKLOAD == PPS
    PPSQuery* node_queries;
#endif
    volatile uint64_t total_txns_finished;
    volatile uint64_t total_txns_received;
    volatile uint32_t rsp_cnt;
    uint64_t last_time_batch;
    qlite_ll * wl_head;        // list of txns in batch being executed
    qlite_ll * wl_tail;        // list of txns in batch being executed
    volatile uint32_t next_txn_id;
    Workload * _wl;
};

class Seq_thread_t {
public:
    uint64_t _thd_id;
    uint64_t _node_id;
    Workload * _wl;

    uint64_t     get_thd_id();
    uint64_t     get_node_id();


    void         init(uint64_t thd_id, uint64_t node_id, Workload * workload);
    // the following function must be in the form void* (*)(void*)
    // to run with pthread.
    // conversion is done within the function.
    RC             run_remote();
    RC             run_recv();
    RC             run_send();
};
#endif
