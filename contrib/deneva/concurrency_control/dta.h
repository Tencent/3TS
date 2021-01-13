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

#ifndef _DTA_H_
#define _DTA_H_

#include "../storage/row.h"
#include "semaphore.h"

class TxnManager;

enum DTAState {
  DTA_RUNNING = 0,
  DTA_VALIDATED,
  DTA_COMMITTED,
  DTA_ABORTED
};
struct dta_item {
  row_t* data;
  uint64_t version;
  dta_item(row_t* data, uint64_t version) : data(data), version(version) {}
  bool operator<(const dta_item& rhs) { return data < rhs.data; }
};
struct dta_rwset {
  std::list<dta_item> rset;
  std::list<dta_item> wset;
  DTAState state;
  pthread_mutex_t mtx;
  ts_t commit_ts;
  uint64_t lower, upper;
  uint64_t thd_id;
  dta_rwset(const std::list<dta_item>& rset, const std::list<dta_item>& wset, DTAState state,
             const ts_t commit_ts, const uint64_t thd_id)
      : rset(rset),
        wset(wset),
        state(state),
        commit_ts(commit_ts),
        lower(0),
        upper(0),
        thd_id(thd_id) {
    pthread_mutex_init(&mtx, NULL);
  }

  void latch() { pthread_mutex_lock(&mtx); }
  void release() { pthread_mutex_unlock(&mtx); }
};

class dta_set_ent {
 public:
  dta_set_ent();
  UInt64 tn;
  TxnManager* txn;
  UInt32 set_size;
  row_t** rows;  //[MAX_WRITE_SET];
  dta_set_ent* next;
};

class Dta {
 public:
  void init();
  RC validate(TxnManager* txn);
  RC find_bound(TxnManager* txn);
  void finish(RC rc, TxnManager* txn);

 private:
  RC get_rw_set(TxnManager* txni, dta_set_ent*& rset, dta_set_ent*& wset);
  sem_t _semaphore;

  std::list<dta_rwset> rwset_;
  std::map<TxnManager*, std::list<dta_rwset>::iterator> rwset_it_;
  sem_t sem_rwset_;
};

struct DtaTimeTableEntry {
  uint64_t lower;
  uint64_t upper;
  uint64_t key;
  DTAState state;
  DtaTimeTableEntry* next;
  DtaTimeTableEntry* prev;
  void init(uint64_t key, uint64_t ts) {
    lower = ts;
    upper = UINT64_MAX;
    this->key = key;
    state = DTA_RUNNING;
    next = NULL;
    prev = NULL;
  }
};

struct DtaTimeTableNode {
  DtaTimeTableEntry* head;
  DtaTimeTableEntry* tail;
  pthread_mutex_t mtx;
  void init() {
    head = NULL;
    tail = NULL;
    pthread_mutex_init(&mtx, NULL);
  }
};

class DtaTimeTable {
 public:
  void init();
  void init(uint64_t thd_id, uint64_t key, uint64_t ts);
  void release(uint64_t thd_id, uint64_t key);
  uint64_t get_lower(uint64_t thd_id, uint64_t key);
  uint64_t get_upper(uint64_t thd_id, uint64_t key);
  void set_lower(uint64_t thd_id, uint64_t key, uint64_t value);
  void set_upper(uint64_t thd_id, uint64_t key, uint64_t value);
  DTAState get_state(uint64_t thd_id, uint64_t key);
  void set_state(uint64_t thd_id, uint64_t key, DTAState value);

 private:
  // hash table
  uint64_t hash(uint64_t key);
  uint64_t table_size;
  DtaTimeTableNode* table;
  DtaTimeTableEntry* find(uint64_t key);

  DtaTimeTableEntry* find_entry(uint64_t id);

  sem_t _semaphore;
};

#endif
