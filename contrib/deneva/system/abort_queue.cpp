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

#include "mem_alloc.h"
#include "abort_queue.h"
#include "message.h"
#include "work_queue.h"

void AbortQueue::init() { pthread_mutex_init(&mtx, NULL); }

uint64_t AbortQueue::enqueue(uint64_t thd_id, uint64_t txn_id, uint64_t abort_cnt) {
    uint64_t starttime = get_sys_clock();
    uint64_t penalty = g_abort_penalty;
#if BACKOFF
    penalty = max(penalty * 2^abort_cnt,g_abort_penalty_max);
#endif
    penalty += starttime;
    //abort_entry * entry = new abort_entry(penalty,txn_id);
    DEBUG_M("AbortQueue::enqueue entry alloc\n");
    abort_entry * entry = (abort_entry*)mem_allocator.alloc(sizeof(abort_entry));
    entry->penalty_end = penalty;
    entry->txn_id = txn_id;
    uint64_t mtx_time_start = get_sys_clock();
    pthread_mutex_lock(&mtx);
    INC_STATS(thd_id,mtx[0],get_sys_clock() - mtx_time_start);
    DEBUG("AQ Enqueue %ld %f -- %f\n", entry->txn_id, float(penalty - starttime) / BILLION,
        simulation->seconds_from_start(starttime));
    INC_STATS(thd_id,abort_queue_penalty,penalty - starttime);
    INC_STATS(thd_id,abort_queue_enqueue_cnt,1);
    queue.push(entry);
    pthread_mutex_unlock(&mtx);

    INC_STATS(thd_id,abort_queue_enqueue_time,get_sys_clock() - starttime);

    return penalty - starttime;
}

void AbortQueue::process(uint64_t thd_id) {
    if (queue.empty()) return;
    abort_entry * entry;
    uint64_t mtx_time_start = get_sys_clock();
    pthread_mutex_lock(&mtx);
    INC_STATS(thd_id,mtx[1],get_sys_clock() - mtx_time_start);
    uint64_t starttime = get_sys_clock();
    while(!queue.empty()) {
        entry = queue.top();
        if(entry->penalty_end < starttime) {
            queue.pop();
            DEBUG("AQ Dequeue %ld %f -- %f\n", entry->txn_id,
                float(starttime - entry->penalty_end) / BILLION,
                simulation->seconds_from_start(starttime));
            INC_STATS(thd_id,abort_queue_penalty_extra,starttime - entry->penalty_end);
            INC_STATS(thd_id,abort_queue_dequeue_cnt,1);
            Message * msg = Message::create_message(RTXN);
            msg->txn_id = entry->txn_id;
            work_queue.enqueue(thd_id,msg,false);
            //entry = queue.top();
            DEBUG_M("AbortQueue::dequeue entry free\n");
            mem_allocator.free(entry,sizeof(abort_entry));
        } else {
            break;
        }
    }
    pthread_mutex_unlock(&mtx);

    INC_STATS(thd_id,abort_queue_dequeue_time,get_sys_clock() - starttime);

}

