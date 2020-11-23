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

#ifndef _MANAGER_H_
#define _MANAGER_H_

#include "helper.h"
#include "global.h"

class row_t;
class TxnManager;

class Manager {
public:
    void             init();
    // returns the next timestamp.
    ts_t            get_ts(uint64_t thread_id);

    // For MVCC. To calculate the min active ts in the system
    ts_t             get_min_ts(uint64_t tid = 0);

    // HACK! the following mutexes are used to model a centralized
    // lock/timestamp manager.
     void             lock_row(row_t * row);
    void             release_row(row_t * row);

    // SUNDIAL, max_cts
    void set_max_cts(uint64_t cts) { _max_cts = cts; }
    uint64_t get_max_cts() { return _max_cts; }

    TxnManager* get_txn_man(int thd_id) {
        return _all_txns[thd_id];
    };
    void             set_txn_man(TxnManager * txn);
    // For SILO
    uint64_t         get_epoch() { return *_epoch; };
    void              update_epoch();
private:
    // For SILO
    volatile uint64_t * _epoch;
    ts_t *             _last_epoch_update_time;

    pthread_mutex_t ts_mutex;
    uint64_t         timestamp;
    pthread_mutex_t mutexes[BUCKET_CNT];
    uint64_t         hash(row_t * row);
    ts_t * volatile all_ts;
    TxnManager **         _all_txns;
    ts_t            last_min_ts_time;
    ts_t            min_ts;

    static __thread uint64_t _max_cts; // max commit timestamp seen by the thread so far.
};

#endif
