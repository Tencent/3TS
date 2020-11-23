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

#include "msg_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "pool.h"
#include "message.h"
#include <boost/lockfree/queue.hpp>

void MessageQueue::init() {
  //m_queue = new boost::lockfree::queue<msg_entry* > (0);
    m_queue = new boost::lockfree::queue<msg_entry* > * [g_this_send_thread_cnt];
#if NETWORK_DELAY_TEST
    cl_m_queue = new boost::lockfree::queue<msg_entry* > * [g_this_send_thread_cnt];
#endif
    for(uint64_t i = 0; i < g_this_send_thread_cnt; i++) {
        m_queue[i] = new boost::lockfree::queue<msg_entry* > (0);
#if NETWORK_DELAY_TEST
        cl_m_queue[i] = new boost::lockfree::queue<msg_entry* > (0);
#endif
    }
    ctr = new  uint64_t * [g_this_send_thread_cnt];
    for(uint64_t i = 0; i < g_this_send_thread_cnt; i++) {
        ctr[i] = (uint64_t*) mem_allocator.align_alloc(sizeof(uint64_t));
        *ctr[i] = i % g_thread_cnt;
    }
    for (uint64_t i = 0; i < g_this_send_thread_cnt; i++) sthd_m_cache.push_back(NULL);
}

void MessageQueue::enqueue(uint64_t thd_id, Message * msg,uint64_t dest) {
    DEBUG("MQ Enqueue %ld\n",dest)
    assert(dest < g_total_node_cnt);
    assert(dest != g_node_id);
    DEBUG_M("MessageQueue::enqueue msg_entry alloc\n");
    msg_entry * entry = (msg_entry*) mem_allocator.alloc(sizeof(struct msg_entry));
    //msg_pool.get(entry);
    entry->msg = msg;
    entry->dest = dest;
    entry->starttime = get_sys_clock();
    assert(entry->dest < g_total_node_cnt);
    uint64_t mtx_time_start = get_sys_clock();
#if CC_ALG == CALVIN
    // Need to have strict message ordering for sequencer thread
    uint64_t rand = thd_id % g_this_send_thread_cnt;
#elif WORKLOAD == DA
    uint64_t rand = 0;
#else
    uint64_t rand = mtx_time_start % g_this_send_thread_cnt;
#endif
#if NETWORK_DELAY_TEST
    if(ISCLIENTN(dest)) {
        while (!cl_m_queue[rand]->push(entry) && !simulation->is_done()) {
        }
        return;
    }
#endif
    while (!m_queue[rand]->push(entry) && !simulation->is_done()) {
    }
    INC_STATS(thd_id,mtx[3],get_sys_clock() - mtx_time_start);
    INC_STATS(thd_id,msg_queue_enq_cnt,1);
}

uint64_t MessageQueue::dequeue(uint64_t thd_id, Message *& msg) {
    msg_entry * entry = NULL;
    uint64_t dest = UINT64_MAX;
    uint64_t mtx_time_start = get_sys_clock();
    bool valid = false;
#if NETWORK_DELAY_TEST
    valid = cl_m_queue[thd_id%g_this_send_thread_cnt]->pop(entry);
    if(!valid) {
        entry = sthd_m_cache[thd_id % g_this_send_thread_cnt];
        if(entry)
        valid = true;
        else
        valid = m_queue[thd_id%g_this_send_thread_cnt]->pop(entry);
    }
#elif WORKLOAD == DA
    valid = m_queue[0]->pop(entry);
#else

    valid = m_queue[thd_id%g_this_send_thread_cnt]->pop(entry);
#endif

    INC_STATS(thd_id,mtx[4],get_sys_clock() - mtx_time_start);
    uint64_t curr_time = get_sys_clock();
    if(valid) {
        assert(entry);
#if NETWORK_DELAY_TEST
        if(!ISCLIENTN(entry->dest)) {
            if(ISSERVER && (get_sys_clock() - entry->starttime) < g_network_delay) {
                sthd_m_cache[thd_id%g_this_send_thread_cnt] = entry;
                INC_STATS(thd_id,mtx[5],get_sys_clock() - curr_time);
                return UINT64_MAX;
            } else {
                sthd_m_cache[thd_id%g_this_send_thread_cnt] = NULL;
            }
            if(ISSERVER) {
                INC_STATS(thd_id,mtx[38],1);
                INC_STATS(thd_id,mtx[39],curr_time - entry->starttime);
            }
        }

#endif
        dest = entry->dest;
        assert(dest < g_total_node_cnt);
        msg = entry->msg;
        DEBUG("MQ Dequeue %ld\n",dest)
        INC_STATS(thd_id,msg_queue_delay_time,curr_time - entry->starttime);
        INC_STATS(thd_id,msg_queue_cnt,1);
        msg->mq_time = curr_time - entry->starttime;
        //msg_pool.put(entry);
        DEBUG_M("MessageQueue::enqueue msg_entry free\n");
        mem_allocator.free(entry,sizeof(struct msg_entry));
    } else {
        msg = NULL;
        dest = UINT64_MAX;
    }
    INC_STATS(thd_id,mtx[5],get_sys_clock() - curr_time);
    return dest;
}
