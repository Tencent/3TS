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

#include "row_dli_based.h"

#include "mem_alloc.h"
#include "row.h"
#include "txn.h"

void Row_dli_base::init(row_t *row) {
  _row = row;
  _latch = (pthread_mutex_t *)mem_allocator.alloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(_latch, NULL);
  sem_init(&_semaphore, 0, 1);
  _cur_version = 0;
  _rw_transs = new std::vector<Entry>();
  _rw_transs->emplace_back(0);
  w_trans = 0;
}

RC Row_dli_base::access(TxnManager *txn, TsType type, uint64_t &version) {
  RC rc = RCOK;
  // pthread_mutex_lock( _latch );
  sem_wait(&_semaphore);
  if (type == R_REQ) {
    txn->cur_row->copy(_row);
    version = _cur_version;
    _rw_transs->back().r_trans_ts.insert(txn->get_start_timestamp());
  } else if (type == P_REQ) {
    txn->cur_row->copy(_row);
    version = UINT64_MAX;
  } else
    assert(false);
  // pthread_mutex_unlock( _latch );
  sem_post(&_semaphore);
  return rc;
}

void Row_dli_base::latch() {
  // pthread_mutex_lock( _latch );
  sem_wait(&_semaphore);
}

uint64_t Row_dli_base::write(row_t *data, TxnManager* txn, const access_t type) {
  uint64_t res = 0;
  if (type == WR) {
    latch();
    res = _rw_transs->size();
    //assert(txn->get_commit_timestamp() >= _rw_transs->at(res - 1).w_ts);
    _rw_transs->emplace_back(txn->get_commit_timestamp());
    release();
  }
  if (w_trans == txn->get_start_timestamp()) w_trans = 0;
  return res;
}

void Row_dli_base::release() {
  // pthread_mutex_unlock( _latch );
  sem_post(&_semaphore);
}
