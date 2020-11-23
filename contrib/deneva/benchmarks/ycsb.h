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

#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "wl.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

class YCSBQuery;
class YCSBQueryMessage;
class ycsb_request;

enum YCSBRemTxnType {
  YCSB_0,
  YCSB_1,
  YCSB_FIN,
  YCSB_RDONE
};

class YCSBWorkload : public Workload {
public :
    RC init();
    RC init_table();
    RC init_schema(const char * schema_file);
    RC get_txn_man(TxnManager *& txn_manager);
    int key_to_part(uint64_t key);
    INDEX * the_index;
    table_t * the_table;
private:
    void init_table_parallel();
    void * init_table_slice();
    static void * threadInitTable(void * This) {
        ((YCSBWorkload *)This)->init_table_slice();
        return NULL;
    }
    pthread_mutex_t insert_lock;
    //  For parallel initialization
    static int next_tid;
};

class YCSBTxnManager : public TxnManager {
public:
    void init(uint64_t thd_id, Workload * h_wl);
    void reset();
    void partial_reset();
  RC acquire_locks();
    RC run_txn();
  RC run_txn_post_wait();
    RC run_calvin_txn();
  void copy_remote_requests(YCSBQueryMessage * msg);
private:
  void next_ycsb_state();
  RC run_txn_state();
  RC run_ycsb_0(ycsb_request * req,row_t *& row_local);
  RC run_ycsb_1(access_t acctype, row_t * row_local);
  RC run_ycsb();
  bool is_done() ;
  bool is_local_request(uint64_t idx) ;
  RC send_remote_request() ;

  row_t * row;
    YCSBWorkload * _wl;
    YCSBRemTxnType state;
  uint64_t next_record_id;
};

#endif
