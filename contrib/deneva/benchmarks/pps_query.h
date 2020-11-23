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

#ifndef _PPSQUERY_H_
#define _PPSQUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class Workload;
class Message;
class PPSQueryMessage;
class PPSClientQueryMessage;

class PPSQueryGenerator : public QueryGenerator {
public:
  BaseQuery * create_query(Workload * h_wl, uint64_t home_partition_id);

private:
    BaseQuery * gen_requests_parts(uint64_t home_partition_id);
    BaseQuery * gen_requests_suppliers(uint64_t home_partition_id);
    BaseQuery * gen_requests_products(uint64_t home_partition_id);
    BaseQuery * gen_requests_partsbysupplier(uint64_t home_partition_id);
    BaseQuery * gen_requests_partsbyproduct(uint64_t home_partition_id);
    BaseQuery * gen_requests_orderproduct(uint64_t home_partition_id);
    BaseQuery * gen_requests_updateproductpart(uint64_t home_partition_id);
    BaseQuery * gen_requests_updatepart(uint64_t home_partition_id);
    myrand * mrand;
};

class PPSQuery : public BaseQuery {
public:
    void init(uint64_t thd_id, Workload * h_wl);
    void init();
    void reset();
    void release();
    void print();
    static std::set<uint64_t> participants(Message * msg, Workload * wl);
    uint64_t participants(bool *& pps,Workload * wl);
    uint64_t get_participants(Workload * wl);
    bool readonly();
    virtual bool isReconQuery() {
    bool result = (txn_type == PPS_GETPARTBYSUPPLIER) || (txn_type == PPS_GETPARTBYPRODUCT) ||
                (txn_type == PPS_ORDERPRODUCT);
        return result;
    }

    PPSTxnType txn_type;
    // txn input

    uint64_t rqry_req_cnt;
    uint64_t part_key;
    uint64_t supplier_key;
    uint64_t product_key;

    // part keys from secondary lookup
    Array<uint64_t> part_keys;

    // track number of parts we've read
    // in a getpartsbyX query

};

#endif
