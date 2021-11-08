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

#include "txn_table.h"

#include "global.h"
#include "mem_alloc.h"
#include "message.h"
#include "pool.h"
#include "query.h"
#include "row.h"
#include "tpcc.h"
#include "tpcc_query.h"
#include "txn.h"
#include "work_queue.h"
#include "ycsb.h"
#include "ycsb_query.h"

void TxnTable::init() {
    //pool_size = g_inflight_max * g_node_cnt * 2 + 1;
    pool_size = g_inflight_max + 1;
    DEBUG_M("TxnTable::init pool_node alloc\n");
    pool = (pool_node **) mem_allocator.align_alloc(sizeof(pool_node*) * pool_size);
    for(uint32_t i = 0; i < pool_size;i++) {
        pool[i] = (pool_node *) mem_allocator.align_alloc(sizeof(struct pool_node));
        pool[i]->head = NULL;
        pool[i]->tail = NULL;
        pool[i]->cnt = 0;
        pool[i]->modify = false;
        pool[i]->min_ts = UINT64_MAX;
    }
}

void TxnTable::dump() {
    for(uint64_t i = 0; i < pool_size;i++) {
        if (pool[i]->cnt == 0) continue;
        txn_node_t t_node = pool[i]->head;

        while (t_node != NULL) {
            printf("TT (%ld,%ld)\n", t_node->txn_man->get_txn_id(), t_node->txn_man->get_batch_id());
                t_node = t_node->next;
        }
    }
}

bool TxnTable::is_matching_txn_node(txn_node_t t_node, uint64_t txn_id, uint64_t batch_id){
    assert(t_node);
#if CC_ALG == CALVIN
    return (t_node->txn_man->get_txn_id() == txn_id && t_node->txn_man->get_batch_id() == batch_id);
#else
    return (t_node->txn_man->get_txn_id() == txn_id);
#endif
}

void TxnTable::update_min_ts(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id,uint64_t ts){
    uint64_t pool_id = txn_id % pool_size;
    while (!ATOM_CAS(pool[pool_id]->modify, false, true)) {
    };
    if (ts < pool[pool_id]->min_ts) pool[pool_id]->min_ts = ts;
    ATOM_CAS(pool[pool_id]->modify,true,false);
}

TxnManager * TxnTable::get_transaction_manager(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
    DEBUG("TxnTable::get_txn_manager %ld / %ld\n",txn_id,pool_size);
    uint64_t starttime = get_sys_clock();
    uint64_t pool_id = txn_id % pool_size;

    uint64_t mtx_starttime = starttime;
    // set modify bit for this pool: txn_id % pool_size
    while (!ATOM_CAS(pool[pool_id]->modify, false, true)) {
    };
    INC_STATS(thd_id,mtx[7],get_sys_clock()-mtx_starttime);

    txn_node_t t_node = pool[pool_id]->head;
    TxnManager * txn_man = NULL;

    uint64_t prof_starttime = get_sys_clock();
    while (t_node != NULL) {
        if(is_matching_txn_node(t_node,txn_id,batch_id)) {
            txn_man = t_node->txn_man;
            break;
        }
        t_node = t_node->next;
    }
    INC_STATS(thd_id,mtx[20],get_sys_clock()-prof_starttime);


    if(!txn_man) {
        prof_starttime = get_sys_clock();

        txn_table_pool.get(thd_id,t_node);

        INC_STATS(thd_id,mtx[21],get_sys_clock()-prof_starttime);
        prof_starttime = get_sys_clock();

        txn_man_pool.get(thd_id,txn_man);

        INC_STATS(thd_id,mtx[22],get_sys_clock()-prof_starttime);
        prof_starttime = get_sys_clock();

        txn_man->set_txn_id(txn_id);
        txn_man->set_batch_id(batch_id);
        t_node->txn_man = txn_man;
        txn_man->txn_stats.starttime = get_sys_clock();
        txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
        LIST_PUT_TAIL(pool[pool_id]->head,pool[pool_id]->tail,t_node);

        INC_STATS(thd_id,mtx[23],get_sys_clock()-prof_starttime);
        prof_starttime = get_sys_clock();

        ++pool[pool_id]->cnt;
        if(pool[pool_id]->cnt > 1) {
            INC_STATS(thd_id,txn_table_cflt_cnt,1);
            INC_STATS(thd_id,txn_table_cflt_size,pool[pool_id]->cnt-1);
        }
        INC_STATS(thd_id,txn_table_new_cnt,1);
        INC_STATS(thd_id,mtx[24],get_sys_clock()-prof_starttime);
    }

#if CC_ALG == MVCC
    if(txn_man->get_timestamp() < pool[pool_id]->min_ts)
        pool[pool_id]->min_ts = txn_man->get_timestamp();
#endif


    // unset modify bit for this pool: txn_id % pool_size
    ATOM_CAS(pool[pool_id]->modify,true,false);

    INC_STATS(thd_id,txn_table_get_time,get_sys_clock() - starttime);
    INC_STATS(thd_id,txn_table_get_cnt,1);
    return txn_man;
}

#if WORKLOAD == DA
extern std::vector<Message*> DA_delayed_operations;
#endif

void TxnTable::restart_txn(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
    uint64_t pool_id = txn_id % pool_size;
    // set modify bit for this pool: txn_id % pool_size
    while (!ATOM_CAS(pool[pool_id]->modify, false, true)) {
    };

    txn_node_t t_node = pool[pool_id]->head;

    while (t_node != NULL) {
        if(is_matching_txn_node(t_node,txn_id,batch_id)) {
#if CC_ALG == CALVIN
            work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RTXN),false);
#elif WORKLOAD == DA
            //TODO: if DA run distributedly, we need judge IS_LOCAL
            DA_delayed_operations.push_back(Message::create_message(t_node->txn_man,RTXN_CONT));
#else
            if(IS_LOCAL(txn_id))
                work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RTXN_CONT),false);
            else
                work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RQRY_CONT),false);
#endif
            break;
        }
        t_node = t_node->next;
    }

    // unset modify bit for this pool: txn_id % pool_size
    ATOM_CAS(pool[pool_id]->modify,true,false);

}

void TxnTable::release_transaction_manager(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id){
    uint64_t starttime = get_sys_clock();

    uint64_t pool_id = txn_id % pool_size;
    uint64_t mtx_starttime = starttime;
    // set modify bit for this pool: txn_id % pool_size
    while (!ATOM_CAS(pool[pool_id]->modify, false, true)) {
    };
    INC_STATS(thd_id,mtx[8],get_sys_clock()-mtx_starttime);

    txn_node_t t_node = pool[pool_id]->head;

#if CC_ALG == MVCC || IS_GENERIC_ALG
    uint64_t min_ts = UINT64_MAX;
    txn_node_t saved_t_node = NULL;
#endif

    uint64_t prof_starttime = get_sys_clock();
    while (t_node != NULL) {
        if(is_matching_txn_node(t_node,txn_id,batch_id)) {
            LIST_REMOVE_HT(t_node,pool[txn_id % pool_size]->head,pool[txn_id % pool_size]->tail);
            --pool[pool_id]->cnt;
#if CC_ALG == MVCC || IS_GENERIC_ALG
            saved_t_node = t_node;
            t_node = t_node->next;
            continue;
#else
            break;
#endif
        }
#if CC_ALG == MVCC || IS_GENERIC_ALG
        if (t_node->txn_man->get_timestamp() < min_ts) min_ts = t_node->txn_man->get_timestamp();
#endif
        t_node = t_node->next;
    }
    INC_STATS(thd_id,mtx[25],get_sys_clock()-prof_starttime);
    prof_starttime = get_sys_clock();

#if CC_ALG == MVCC || IS_GENERIC_ALG
    t_node = saved_t_node;
    pool[pool_id]->min_ts = min_ts;
#endif

    // unset modify bit for this pool: txn_id % pool_size
    ATOM_CAS(pool[pool_id]->modify,true,false);

    prof_starttime = get_sys_clock();
    assert(t_node);
    assert(t_node->txn_man);

    txn_man_pool.put(thd_id,t_node->txn_man);

    INC_STATS(thd_id,mtx[26],get_sys_clock()-prof_starttime);
    prof_starttime = get_sys_clock();

    txn_table_pool.put(thd_id,t_node);
    INC_STATS(thd_id,mtx[27],get_sys_clock()-prof_starttime);


    INC_STATS(thd_id,txn_table_release_time,get_sys_clock() - starttime);
    INC_STATS(thd_id,txn_table_release_cnt,1);

}

uint64_t TxnTable::get_min_ts(uint64_t thd_id) {
    uint64_t starttime = get_sys_clock();
    uint64_t min_ts = UINT64_MAX;
    for(uint64_t i = 0 ; i < pool_size; i++) {
        uint64_t pool_min_ts = pool[i]->min_ts;
        if (pool_min_ts < min_ts) min_ts = pool_min_ts;
    }

    INC_STATS(thd_id,txn_table_min_ts_time,get_sys_clock() - starttime);
    return min_ts;

}

