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

#ifndef ROW_DTA_H
#define ROW_DTA_H
#include "../storage/row.h"
class table_t;
class Catalog;
class TxnManager;

struct DTAMVReqEntry {
  TxnManager* txn;
  ts_t ts;
  ts_t starttime;
  DTAMVReqEntry* next;
};

struct DTAMVHisEntry {
  ts_t ts;
  // only for write history. The value needs to be stored.
  //	char * data;
  row_t* row;
  uint64_t version;
  DTAMVHisEntry* next;
  DTAMVHisEntry* prev;
};

class Row_dta {
 public:
  void init(row_t* row);
  RC access(TsType type, TxnManager* txn, row_t* row, uint64_t& version);
  RC read_and_write(TsType type, TxnManager* txn, row_t* row, uint64_t& version);
  RC prewrite(TxnManager* txn);
  RC abort(access_t type, TxnManager* txn);
  RC commit(access_t type, TxnManager* txn, row_t* data, uint64_t& version);
  void write(row_t* data);

 private:
  volatile bool dta_avail;
  uint64_t max_version_;
  row_t* _row;

 public:
  std::set<uint64_t>* uncommitted_reads;
  // std::set<uint64_t> * uncommitted_writes;

  uint64_t write_trans;
  uint64_t timestamp_last_read;
  uint64_t timestamp_last_write;

  // multi-verison part
 private:
  pthread_mutex_t* latch;
  bool blatch;

  DTAMVReqEntry* get_req_entry();
  void return_req_entry(DTAMVReqEntry* entry);
  DTAMVHisEntry* get_his_entry();
  void return_his_entry(DTAMVHisEntry* entry);

  bool conflict(TsType type, ts_t ts);
  void buffer_req(TsType type, TxnManager* txn);
  DTAMVReqEntry* debuffer_req(TsType type, TxnManager* txn = NULL);
  void update_buffer(TxnManager* txn);
  uint64_t insert_history(ts_t ts, row_t* row);

  row_t* clear_history(TsType type, ts_t ts);

  DTAMVReqEntry* readreq_mvcc;
  DTAMVReqEntry* prereq_mvcc;
  // DTAMVHisEntry * readhis;
  DTAMVHisEntry* writehis;
  // DTAMVHisEntry * readhistail;
  DTAMVHisEntry* writehistail;
  uint64_t whis_len;
  // uint64_t rhis_len;
  uint64_t rreq_len;
  uint64_t preq_len;
};

#endif
