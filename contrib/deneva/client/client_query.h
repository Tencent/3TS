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

#ifndef _CLIENT_QUERY_H_
#define _CLIENT_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class Workload;
class YCSBQuery;
class YCSBClientQuery;
class TPCCQuery;
class tpcc_client_query;

// We assume a separate task queue for each thread in order to avoid
// contention in a centralized query queue. In reality, more sophisticated
// queue model might be implemented.
class Client_query_queue {
public:
    void init(Workload * h_wl);
    bool done();
    BaseQuery * get_next_query(uint64_t server_id,uint64_t thread_id);
    void initQueriesParallel(uint64_t thd_id);
    static void * initQueriesHelper(void * context);

private:
    Workload * _wl;
    uint64_t size;
    std::vector<std::vector<BaseQuery*>> queries;
    uint64_t ** query_cnt;
    volatile uint64_t next_tid;
};

#endif
