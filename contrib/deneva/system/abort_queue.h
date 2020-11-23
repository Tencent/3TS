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

#ifndef _ABORT_QUEUE_H_
#define _ABORT_QUEUE_H_


#include "global.h"
#include "helper.h"

struct abort_entry {
    uint64_t penalty_end;
    uint64_t txn_id;
    abort_entry() {}
    abort_entry(uint64_t penalty_end, uint64_t txn_id) {
        this->penalty_end = penalty_end;
        this->txn_id = txn_id;
    }
};


struct CompareAbortEntry {
    bool operator()(const abort_entry* lhs, const abort_entry* rhs) {
        return lhs->penalty_end > rhs->penalty_end;
    }
};

class AbortQueue {
public:
    void init();
    uint64_t enqueue(uint64_t thd_id, uint64_t txn_id, uint64_t abort_cnt);
    void process(uint64_t thd_id);
private:
    std::priority_queue<abort_entry*,std::vector<abort_entry*>,CompareAbortEntry> queue;
    pthread_mutex_t mtx;
};



#endif
