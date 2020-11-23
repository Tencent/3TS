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

#include "pool.h"
#include "global.h"
#include "helper.h"
#include "txn.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "tpcc_query.h"
#include "pps_query.h"
#include "da.h"
#include "da_query.h"
#include "query.h"
#include "msg_queue.h"
#include "row.h"

#define TRY_LIMIT 10

void TxnManPool::init(Workload * wl, uint64_t size) {
    _wl = wl;
#if CC_ALG == CALVIN
    pool = new boost::lockfree::queue<TxnManager* > (size);
#else
    pool = new boost::lockfree::queue<TxnManager* > * [g_total_thread_cnt];
#endif
    TxnManager * txn;
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
#if CC_ALG != CALVIN
        pool[thd_id] = new boost::lockfree::queue<TxnManager* > (size);
#endif
        for(uint64_t i = 0; i < size; i++) {
        //put(items[i]);
        _wl->get_txn_man(txn);
        txn->init(thd_id,_wl);
        put(thd_id, txn);
        }
    }
}

void TxnManPool::get(uint64_t thd_id, TxnManager *& item) {
#if CC_ALG == CALVIN
    bool r = pool->pop(item);
#else
    bool r = pool[thd_id]->pop(item);
#endif
    if(!r) {
        _wl->get_txn_man(item);
    }
    item->init(thd_id,_wl);
}

void TxnManPool::put(uint64_t thd_id, TxnManager * item) {
    item->release();
    int tries = 0;
#if CC_ALG == CALVIN
    while (!pool->push(item) && tries++ < TRY_LIMIT) {
    }
#else
    while (!pool[thd_id]->push(item) && tries++ < TRY_LIMIT) {
    }
#endif
    if(tries >= TRY_LIMIT) {
        mem_allocator.free(item,sizeof(TxnManager));
    }
}

void TxnManPool::free_all() {
    TxnManager * item;
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
#if CC_ALG == CALVIN
    while(pool->pop(item)) {
#else
        while(pool[thd_id]->pop(item)) {
#endif
            mem_allocator.free(item,sizeof(TxnManager));
        }
    }
}

void TxnPool::init(Workload * wl, uint64_t size) {
    _wl = wl;
#if CC_ALG == CALVIN
    pool = new boost::lockfree::queue<Transaction*  > (size);
#else
    pool = new boost::lockfree::queue<Transaction* > * [g_total_thread_cnt];
#endif
    Transaction * txn;
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
#if CC_ALG != CALVIN
        pool[thd_id] = new boost::lockfree::queue<Transaction*  > (size);
#endif
        for(uint64_t i = 0; i < size; i++) {
            //put(items[i]);
            txn = (Transaction*) mem_allocator.alloc(sizeof(Transaction));
            txn->init();
            put(thd_id,txn);
        }
    }
}

void TxnPool::get(uint64_t thd_id, Transaction *& item) {
#if CC_ALG == CALVIN
    bool r = pool->pop(item);
#else
    bool r = pool[thd_id]->pop(item);
#endif
    if(!r) {
        item = (Transaction*) mem_allocator.alloc(sizeof(Transaction));
        item->init();
    }
}

void TxnPool::put(uint64_t thd_id,Transaction * item) {
    //item->release();
    item->reset(thd_id);
    int tries = 0;
#if CC_ALG == CALVIN
    while (!pool->push(item) && tries++ < TRY_LIMIT) {
    }
#else
    while (!pool[thd_id]->push(item) && tries++ < TRY_LIMIT) {
    }
#endif
    if(tries >= TRY_LIMIT) {
        item->release(thd_id);
        mem_allocator.free(item,sizeof(Transaction));
    }
}

void TxnPool::free_all() {
    TxnManager * item;
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
#if CC_ALG == CALVIN
        while(pool->pop(item)) {
#else
        while(pool[thd_id]->pop(item)) {
#endif
            mem_allocator.free(item,sizeof(item));

        }
    }
}

void QryPool::init(Workload * wl, uint64_t size) {
    _wl = wl;
#if CC_ALG == CALVIN
    pool = new boost::lockfree::queue<BaseQuery* > (size);
#else
    pool = new boost::lockfree::queue<BaseQuery*> * [g_total_thread_cnt];
#endif
    BaseQuery * qry=NULL;
    DEBUG_M("QryPool alloc init\n");
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
#if CC_ALG != CALVIN
        pool[thd_id] = new boost::lockfree::queue<BaseQuery* > (size);
#endif
        for(uint64_t i = 0; i < size; i++) {
            //put(items[i]);
#if WORKLOAD==TPCC
            TPCCQuery * m_qry = (TPCCQuery *) mem_allocator.alloc(sizeof(TPCCQuery));
            m_qry = new TPCCQuery();
#elif WORKLOAD==PPS
            PPSQuery * m_qry = (PPSQuery *) mem_allocator.alloc(sizeof(PPSQuery));
            m_qry = new PPSQuery();
#elif WORKLOAD==YCSB
            YCSBQuery * m_qry = (YCSBQuery *) mem_allocator.alloc(sizeof(YCSBQuery));
            m_qry = new YCSBQuery();
#elif WORKLOAD==DA
            DAQuery * m_qry = (DAQuery *) mem_allocator.alloc(sizeof(DAQuery));
            m_qry = new DAQuery();
#endif
            m_qry->init();
            qry = m_qry;
            put(thd_id,qry);
        }
    }
}

void QryPool::get(uint64_t thd_id, BaseQuery *& item) {
#if CC_ALG == CALVIN
    bool r = pool->pop(item);
#else
    bool r = pool[thd_id]->pop(item);
#endif
    if(!r) {
        DEBUG_M("query_pool alloc\n");
#if WORKLOAD==TPCC
        TPCCQuery * qry = (TPCCQuery *) mem_allocator.alloc(sizeof(TPCCQuery));
        qry = new TPCCQuery();
#elif WORKLOAD==PPS
        PPSQuery * qry = (PPSQuery *) mem_allocator.alloc(sizeof(PPSQuery));
        qry = new PPSQuery();
#elif WORKLOAD==YCSB
        YCSBQuery * qry = NULL;
        qry = (YCSBQuery *) mem_allocator.alloc(sizeof(YCSBQuery));
        qry = new YCSBQuery();
#elif WORKLOAD==DA
        DAQuery * qry = NULL;
        qry = (DAQuery *) mem_allocator.alloc(sizeof(DAQuery));
        qry = new DAQuery();
#endif
        qry->init();
        item = (BaseQuery*)qry;
    }
    DEBUG_R("get 0x%lx\n",(uint64_t)item);
}

void QryPool::put(uint64_t thd_id, BaseQuery * item) {
    assert(item);
#if WORKLOAD == YCSB
    ((YCSBQuery*)item)->reset();
#elif WORKLOAD == TPCC
    ((TPCCQuery*)item)->reset();
#elif WORKLOAD == PPS
    ((PPSQuery*)item)->reset();
#endif
    //DEBUG_M("put 0x%lx\n",(uint64_t)item);
    DEBUG_R("put 0x%lx\n",(uint64_t)item);
    //mem_allocator.free(item,sizeof(item));
    int tries = 0;
#if CC_ALG == CALVIN
    while (!pool->push(item) && tries++ < TRY_LIMIT) {
    }
#else
    while (!pool[thd_id]->push(item) && tries++ < TRY_LIMIT) {
    }
#endif
    if(tries >= TRY_LIMIT) {
#if WORKLOAD == YCSB
        ((YCSBQuery*)item)->release();
#elif WORKLOAD == TPCC
        ((TPCCQuery*)item)->release();
#elif WORKLOAD == PPS
        ((PPSQuery*)item)->release();
#endif
        mem_allocator.free(item,sizeof(BaseQuery));
    }
}

void QryPool::free_all() {
    BaseQuery * item;
    DEBUG_M("query_pool free\n");
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
#if CC_ALG == CALVIN
        while(pool->pop(item)) {
#else
        while(pool[thd_id]->pop(item)) {
#endif
            mem_allocator.free(item,sizeof(item));
        }
    }
}


void AccessPool::init(Workload * wl, uint64_t size) {
    _wl = wl;
    pool = new boost::lockfree::queue<Access* > * [g_total_thread_cnt];
    DEBUG_M("AccessPool alloc init\n");
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
        pool[thd_id] = new boost::lockfree::queue<Access* > (size);
        for(uint64_t i = 0; i < size; i++) {
            Access * item = (Access*)mem_allocator.alloc(sizeof(Access));
            put(thd_id,item);
        }
    }
}

void AccessPool::get(uint64_t thd_id, Access *& item) {
  //bool r = pool->pop(item);
    bool r = pool[thd_id]->pop(item);
    if(!r) {
        DEBUG_M("access_pool alloc\n");
        item = (Access*)mem_allocator.alloc(sizeof(Access));
        item->orig_row = NULL;
        item->data = NULL;
        item->orig_data = NULL;
    #if CC_ALG == SUNDIAL
        item->orig_rts = 0;
        item->orig_wts = 0;
        item->locked = false;
    #endif
    }
}

void AccessPool::put(uint64_t thd_id, Access * item) {
    pool[thd_id]->push(item);
    /*
    int tries = 0;
    while(!pool->push(item) && tries++ < TRY_LIMIT) { }
    if(tries >= TRY_LIMIT) {
        mem_allocator.free(item,sizeof(Access));
    }
    */
}

void AccessPool::free_all() {
  Access * item;
  DEBUG_M("access_pool free\n");
  //while(pool->pop(item)) {
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
        while(pool[thd_id]->pop(item)) {
            mem_allocator.free(item,sizeof(item));
        }
    }
}

void TxnTablePool::init(Workload * wl, uint64_t size) {
    _wl = wl;
    pool = new boost::lockfree::queue<txn_node* > * [g_total_thread_cnt];
    DEBUG_M("TxnTablePool alloc init\n");
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
        pool[thd_id] = new boost::lockfree::queue<txn_node* > (size);
        for(uint64_t i = 0; i < size; i++) {
            txn_node * t_node = (txn_node *) mem_allocator.align_alloc(sizeof(struct txn_node));
            //put(new txn_node());
            put(thd_id,t_node);
        }
    }
}

void TxnTablePool::get(uint64_t thd_id, txn_node *& item) {
    bool r = pool[thd_id]->pop(item);
    if(!r) {
        DEBUG_M("txn_table_pool alloc\n");
        item = (txn_node *) mem_allocator.align_alloc(sizeof(struct txn_node));
    }
}

void TxnTablePool::put(uint64_t thd_id, txn_node * item) {
    int tries = 0;
    while (!pool[thd_id]->push(item) && tries++ < TRY_LIMIT) {
    }
    if(tries >= TRY_LIMIT) {
        mem_allocator.free(item,sizeof(txn_node));
    }
}

void TxnTablePool::free_all() {
    txn_node * item;
    DEBUG_M("txn_table_pool free\n");
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
        while(pool[thd_id]->pop(item)) {
            mem_allocator.free(item,sizeof(item));
        }
    }
}
void MsgPool::init(Workload * wl, uint64_t size) {
    _wl = wl;
    pool = new boost::lockfree::queue<msg_entry* > (size);
    msg_entry* entry;
    DEBUG_M("MsgPool alloc init\n");
    for(uint64_t i = 0; i < size; i++) {
        entry = (msg_entry*) mem_allocator.alloc(sizeof(struct msg_entry));
        put(entry);
    }
}

void MsgPool::get(msg_entry* & item) {
    bool r = pool->pop(item);
    if(!r) {
        DEBUG_M("msg_pool alloc\n");
        item = (msg_entry*) mem_allocator.alloc(sizeof(struct msg_entry));
    }
}

void MsgPool::put(msg_entry* item) {
    item->msg = NULL;
    item->dest = UINT64_MAX;
    item->starttime = UINT64_MAX;
    int tries = 0;
    while (!pool->push(item) && tries++ < TRY_LIMIT) {
    }
    if(tries >= TRY_LIMIT) {
        mem_allocator.free(item,sizeof(msg_entry));
    }
}

void MsgPool::free_all() {
    msg_entry * item;
    DEBUG_M("msg_pool free\n");
    while(pool->pop(item)) {
        mem_allocator.free(item,sizeof(item));
    }
}

void RowPool::init(Workload * wl, uint64_t size) {
    _wl = wl;
    pool = new boost::lockfree::queue<row_t*> * [g_total_thread_cnt];
    row_t* entry;
    DEBUG_M("RowPool alloc init\n");
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
        pool[thd_id] = new boost::lockfree::queue<row_t* > (size);
        for(uint64_t i = 0; i < size; i++) {
            entry = (row_t*) mem_allocator.alloc(sizeof(struct row_t));
            put(thd_id,entry);
        }
    }
}

void RowPool::get(uint64_t thd_id, row_t* & item) {
    bool r = pool[thd_id]->pop(item);
    if(!r) {
        DEBUG_M("msg_pool alloc\n");
        item = (row_t*) mem_allocator.alloc(sizeof(struct row_t));
    }
}

void RowPool::put(uint64_t thd_id, row_t* item) {
    int tries = 0;
    while (!pool[thd_id]->push(item) && tries++ < TRY_LIMIT) {
    }
    if(tries >= TRY_LIMIT) {
        mem_allocator.free(item,sizeof(row_t));
    }
}

void RowPool::free_all() {
    row_t * item;
    for(uint64_t thd_id = 0; thd_id < g_total_thread_cnt; thd_id++) {
        while(pool[thd_id]->pop(item)) {
            DEBUG_M("row_pool free\n");
            mem_allocator.free(item,sizeof(row_t));
        }
    }
}

