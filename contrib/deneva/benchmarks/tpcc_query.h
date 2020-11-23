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

#ifndef _TPCCQuery_H_
#define _TPCCQuery_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class Workload;
class Message;
class TPCCQueryMessage;
class TPCCClientQueryMessage;

// items of new order transaction
struct Item_no {
    uint64_t ol_i_id;
    uint64_t ol_supply_w_id;
    uint64_t ol_quantity;
  void copy(Item_no * item) {
    ol_i_id = item->ol_i_id;
    ol_supply_w_id = item->ol_supply_w_id;
    ol_quantity = item->ol_quantity;
  }
};


class TPCCQueryGenerator : public QueryGenerator {
public:
  BaseQuery * create_query(Workload * h_wl, uint64_t home_partition_id);

private:
    BaseQuery * gen_requests(uint64_t home_partition_id, Workload * h_wl);
  BaseQuery * gen_payment(uint64_t home_partition);
  BaseQuery * gen_new_order(uint64_t home_partition);
    myrand * mrand;
};

class TPCCQuery : public BaseQuery {
public:
    void init(uint64_t thd_id, Workload * h_wl);
  void init();
  void reset();
  void release();
  void release_items();
  void print();
  static std::set<uint64_t> participants(Message * msg, Workload * wl);
  uint64_t participants(bool *& pps,Workload * wl);
  uint64_t get_participants(Workload * wl);
  bool readonly();

    TPCCTxnType txn_type;
    // common txn input for both payment & new-order
    uint64_t w_id;
    uint64_t d_id;
    uint64_t c_id;
    // txn input for payment
    uint64_t d_w_id;
    uint64_t c_w_id;
    uint64_t c_d_id;
    char c_last[LASTNAME_LEN];
    double h_amount;
    bool by_last_name;
    // txn input for new-order
    //Item_no * items;
  Array<Item_no*> items;
    bool rbk;
    bool remote;
    uint64_t ol_cnt;
    uint64_t o_entry_d;
    // Input for delivery
    uint64_t o_carrier_id;
    uint64_t ol_delivery_d;
    // for order-status

    // Other
    uint64_t ol_i_id;
    uint64_t ol_supply_w_id;
    uint64_t ol_quantity;
    uint64_t ol_number;
    uint64_t ol_amount;
    uint64_t o_id;
  uint64_t rqry_req_cnt;

};

#endif
