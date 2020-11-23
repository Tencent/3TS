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

#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "mem_alloc.h"
#include "transport.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pool.h"
#include "global.h"

void MessageThread::init(uint64_t thd_id) {
    buffer_cnt = g_total_node_cnt;
#if CC_ALG == CALVIN
    buffer_cnt++;
#endif
    DEBUG_M("MessageThread::init buffer[] alloc\n");
    buffer = (mbuf **) mem_allocator.align_alloc(sizeof(mbuf*) * buffer_cnt);
    for(uint64_t n = 0; n < buffer_cnt; n++) {
        DEBUG_M("MessageThread::init mbuf alloc\n");
        buffer[n] = (mbuf *)mem_allocator.align_alloc(sizeof(mbuf));
        buffer[n]->init(n);
        buffer[n]->reset(n);
    }
    _thd_id = thd_id;
}

void MessageThread::check_and_send_batches() {
    uint64_t starttime = get_sys_clock();
    for(uint64_t dest_node_id = 0; dest_node_id < buffer_cnt; dest_node_id++) {
        if(buffer[dest_node_id]->ready()) {
            send_batch(dest_node_id);
        }
    }
    INC_STATS(_thd_id,mtx[11],get_sys_clock() - starttime);
}

void MessageThread::send_batch(uint64_t dest_node_id) {
    uint64_t starttime = get_sys_clock();
    mbuf * sbuf = buffer[dest_node_id];
    assert(sbuf->cnt > 0);
    ((uint32_t*)sbuf->buffer)[2] = sbuf->cnt;
    INC_STATS(_thd_id,mbuf_send_intv_time,get_sys_clock() - sbuf->starttime);

    DEBUG("Send batch of %ld msgs to %ld\n",sbuf->cnt,dest_node_id);
    fflush(stdout);
    tport_man.send_msg(_thd_id,dest_node_id,sbuf->buffer,sbuf->ptr);
    INC_STATS(_thd_id,msg_batch_size_msgs,sbuf->cnt);
    INC_STATS(_thd_id,msg_batch_size_bytes,sbuf->ptr);
    if(ISSERVERN(dest_node_id)) {
        INC_STATS(_thd_id,msg_batch_size_bytes_to_server,sbuf->ptr);
    } else if (ISCLIENTN(dest_node_id)){
        INC_STATS(_thd_id,msg_batch_size_bytes_to_client,sbuf->ptr);
    }
    INC_STATS(_thd_id,msg_batch_cnt,1);
    sbuf->reset(dest_node_id);
    INC_STATS(_thd_id,mtx[12],get_sys_clock() - starttime);
}
char type2char1(DATxnType txn_type)
{
    switch (txn_type)
    {
        case DA_READ:
            return 'R';
        case DA_WRITE:
            return 'W';
        case DA_COMMIT:
            return 'C';
        case DA_ABORT:
            return 'A';
        case DA_SCAN:
            return 'S';
        default:
            return 'U';
    }
}

void MessageThread::run() {

    uint64_t starttime = get_sys_clock();
    Message * msg = NULL;
    uint64_t dest_node_id;
    mbuf * sbuf;


    dest_node_id = msg_queue.dequeue(get_thd_id(), msg);
    if(!msg) {
        check_and_send_batches();
        INC_STATS(_thd_id,mtx[9],get_sys_clock() - starttime);
        return;
    }
    assert(msg);
    assert(dest_node_id < g_total_node_cnt);
    assert(dest_node_id != g_node_id);

    sbuf = buffer[dest_node_id];

    if(!sbuf->fits(msg->get_size())) {
        assert(sbuf->cnt > 0);
        send_batch(dest_node_id);
    }

#if WORKLOAD == DA
    if(!is_server&&false)
        printf("cl seq_id:%lu type:%c trans_id:%lu item:%c state:%lu next_state:%lu\n",
            ((DAClientQueryMessage*)msg)->seq_id,
            type2char1(((DAClientQueryMessage*)msg)->txn_type),
            ((DAClientQueryMessage*)msg)->trans_id,
            static_cast<char>('x'+((DAClientQueryMessage*)msg)->item_id),
            ((DAClientQueryMessage*)msg)->state,
            (((DAClientQueryMessage*)msg)->next_state));
            fflush(stdout);
#endif

    uint64_t copy_starttime = get_sys_clock();
    msg->copy_to_buf(&(sbuf->buffer[sbuf->ptr]));
    INC_STATS(_thd_id,msg_copy_output_time,get_sys_clock() - copy_starttime);
    DEBUG("%ld Buffered Msg %d, (%ld,%ld) to %ld\n", _thd_id, msg->rtype, msg->txn_id, msg->batch_id,
            dest_node_id);
    sbuf->cnt += 1;
    sbuf->ptr += msg->get_size();
    // Free message here, no longer needed unless CALVIN sequencer
    if(CC_ALG != CALVIN) {
        Message::release_message(msg);
    }
    if (sbuf->starttime == 0) sbuf->starttime = get_sys_clock();

    check_and_send_batches();
    INC_STATS(_thd_id,mtx[10],get_sys_clock() - starttime);

}

