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

#ifndef _QUERY_H_
#define _QUERY_H_

#include "global.h"
#include "helper.h"
#include "array.h"

class Workload;
class YCSBQuery;
class TPCCQuery;
class PPSQuery;

class BaseQuery {
public:
    virtual ~BaseQuery() {}
    virtual bool readonly() = 0;
    virtual void print() = 0;
    virtual void init();
    uint64_t waiting_time;
    void clear();
    void release();
    virtual bool isReconQuery() {return false;}

    // Prevent unnecessary remote messages
    Array<uint64_t> partitions;
    Array<uint64_t> partitions_touched;
    Array<uint64_t> active_nodes;
    Array<uint64_t> participant_nodes;

};

class QueryGenerator {
public:
    virtual ~QueryGenerator() {}
    virtual BaseQuery * create_query(Workload * h_wl, uint64_t home_partition_id) = 0;
};

// All the queries for a particular thread.
class Query_thd {
public:
    void init(Workload * h_wl, int thread_id);
    BaseQuery * get_next_query();
    int q_idx;
#if WORKLOAD == YCSB
    YCSBQuery * queries;
#elif WORKLOAD == TPCC
    TPCCQuery * queries;
#elif WORKLOAD == PPS
    PPSQuery * queries;
#elif WORKLOAD == DA
    DAQuery * queries;
#endif
    char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
};

class Query_queue {
public:
    void init(Workload * h_wl);
    void init(int thread_id);
    BaseQuery * get_next_query(uint64_t thd_id);

private:
    Query_thd ** all_queries;
    Workload * _wl;
};

#endif
