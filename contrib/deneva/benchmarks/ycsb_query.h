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

#ifndef _YCSBQuery_H_
#define _YCSBQuery_H_

#include "global.h"
#include "helper.h"
#include "query.h"
#include "array.h"

class Workload;
class Message;
class YCSBQueryMessage;
class YCSBClientQueryMessage;


// Each YCSBQuery contains several ycsb_requests,
// each of which is a RD, WR or SCAN
// to a single table

class ycsb_request {
public:
  ycsb_request() {}
  ycsb_request(const ycsb_request& req) : acctype(req.acctype), key(req.key), value(req.value) { }
  void copy(ycsb_request * req) {
    this->acctype = req->acctype;
    this->key = req->key;
    this->value = req->value;
  }
//    char table_name[80];
    access_t acctype;
    uint64_t key;
    char value;
    // only for (qtype == SCAN)
    //UInt32 scan_len;
};

class YCSBQueryGenerator : public QueryGenerator {
public:
  void init();
  BaseQuery * create_query(Workload * h_wl, uint64_t home_partition_id);

private:
    BaseQuery * gen_requests_hot(uint64_t home_partition_id, Workload * h_wl);
    BaseQuery * gen_requests_zipf(uint64_t home_partition_id, Workload * h_wl);
    // for Zipfian distribution
    double zeta(uint64_t n, double theta);
    uint64_t zipf(uint64_t n, double theta);

    myrand * mrand;
    static uint64_t the_n;
    static double denom;
    double zeta_2_theta;
};

class YCSBQuery : public BaseQuery {
public:
  YCSBQuery() {}
  ~YCSBQuery() {}

  void print();

    void init(uint64_t thd_id, Workload * h_wl) {};
  void init();
  void release();
  void release_requests();
  void reset();
  uint64_t get_participants(Workload * wl);
  static std::set<uint64_t> participants(Message * msg, Workload * wl);
  static void copy_request_to_msg(YCSBQuery * ycsb_query, YCSBQueryMessage * msg, uint64_t id);
  uint64_t participants(bool *& pps,Workload * wl);
  bool readonly();

  //std::vector<ycsb_request> requests;
  Array<ycsb_request*> requests;
  void* orig_request;
  /*
  uint64_t rid;
  uint64_t access_cnt;
    uint64_t request_cnt;
    uint64_t req_i;
  ycsb_request req;
  uint64_t rqry_req_cnt;
  */

};

#endif
