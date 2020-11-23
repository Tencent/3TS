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

#include "global.h"
#include "manager.h"
#include "thread.h"
#include "calvin_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"

void CalvinLockThread::setup() {}

RC CalvinLockThread::run() {
    tsetup();

    RC rc = RCOK;
    TxnManager * txn_man;
    uint64_t prof_starttime = get_sys_clock();
    uint64_t idle_starttime = 0;

    while(!simulation->is_done()) {
        txn_man = NULL;

        Message * msg = work_queue.sched_dequeue(_thd_id);

        if(!msg) {
            if (idle_starttime == 0) idle_starttime = get_sys_clock();
            continue;
        }
        if(idle_starttime > 0) {
                INC_STATS(_thd_id,sched_idle_time,get_sys_clock() - idle_starttime);
                idle_starttime = 0;
        }

        prof_starttime = get_sys_clock();
        assert(msg->get_rtype() == CL_QRY);
        assert(msg->get_txn_id() != UINT64_MAX);

        txn_man =
                txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id());
        while (!txn_man->unset_ready()) {
        }
        assert(ISSERVERN(msg->get_return_id()));
        txn_man->txn_stats.starttime = get_sys_clock();

        txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
        txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;

        msg->copy_to_txn(txn_man);
        txn_man->register_thread(this);
        assert(ISSERVERN(txn_man->return_id));

        INC_STATS(get_thd_id(),sched_txn_table_time,get_sys_clock() - prof_starttime);
        prof_starttime = get_sys_clock();

        rc = RCOK;
        // Acquire locks
        if (!txn_man->isRecon()) {
                rc = txn_man->acquire_locks();
        }

        if(rc == RCOK) {
                work_queue.enqueue(_thd_id,msg,false);
        }
        txn_man->set_ready();

        INC_STATS(_thd_id,mtx[33],get_sys_clock() - prof_starttime);
        prof_starttime = get_sys_clock();
    }
    printf("FINISH %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;
}

void CalvinSequencerThread::setup() {}

bool CalvinSequencerThread::is_batch_ready() {
    bool ready = get_wall_clock() - simulation->last_seq_epoch_time >= g_seq_batch_time_limit;
    return ready;
}

RC CalvinSequencerThread::run() {
    tsetup();

    Message * msg;
    uint64_t idle_starttime = 0;
    uint64_t prof_starttime = 0;

    while(!simulation->is_done()) {

        prof_starttime = get_sys_clock();

        if(is_batch_ready()) {
            simulation->advance_seq_epoch();
            //last_batchtime = get_wall_clock();
            seq_man.send_next_batch(_thd_id);
        }

        INC_STATS(_thd_id,mtx[30],get_sys_clock() - prof_starttime);
        prof_starttime = get_sys_clock();

        msg = work_queue.sequencer_dequeue(_thd_id);

        INC_STATS(_thd_id,mtx[31],get_sys_clock() - prof_starttime);
        prof_starttime = get_sys_clock();

        if(!msg) {
            if (idle_starttime == 0) idle_starttime = get_sys_clock();
                continue;
        }
        if(idle_starttime > 0) {
            INC_STATS(_thd_id,seq_idle_time,get_sys_clock() - idle_starttime);
            idle_starttime = 0;
        }

        switch (msg->get_rtype()) {
            case CL_QRY:
                // Query from client
                DEBUG("SEQ process_txn\n");
                seq_man.process_txn(msg,get_thd_id(),0,0,0,0);
                // Don't free message yet
                break;
            case CALVIN_ACK:
                // Ack from server
                DEBUG("SEQ process_ack (%ld,%ld) from %ld\n", msg->get_txn_id(), msg->get_batch_id(),
                            msg->get_return_id());
                seq_man.process_ack(msg,get_thd_id());
                // Free message here
                msg->release();
                break;
            default:
                assert(false);
        }

        INC_STATS(_thd_id,mtx[32],get_sys_clock() - prof_starttime);
        prof_starttime = get_sys_clock();
    }
    printf("FINISH %ld:%ld\n",_node_id,_thd_id);
    fflush(stdout);
    return FINISH;

}
