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

#ifndef ROW_DLI1_H
#define ROW_DLI1_H
#include <semaphore.h>

#include "../storage/row.h"

class table_t;
class Catalog;
class TxnManager;
struct TsReqEntry;

class Row_dli_base {
 public:
  struct Entry {
    Entry(const ts_t w_ts) : w_ts(w_ts), r_trans_ts() {}
    ts_t w_ts;
    std::set<txnid_t> r_trans_ts;
  };
  void init(row_t* row);
  RC access(TxnManager* txn, TsType type, uint64_t& version);
  void latch();
  uint64_t write(row_t* data, TxnManager* txn, const access_t type);
  void release();
  bool has_version(uint64_t version) const { return version < _rw_transs->size(); }
  Entry* get_version(uint64_t version) {
    assert(has_version(version));
    return &(_rw_transs->at(version));
  }

  std::atomic<uint64_t> w_trans;

 private:
  pthread_mutex_t* _latch;
  sem_t _semaphore;
  row_t* _row;
  uint64_t _cur_version;
  std::vector<Entry>* _rw_transs;
};

#endif
