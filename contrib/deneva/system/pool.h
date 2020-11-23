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

#ifndef _TXN_POOL_H_
#define _TXN_POOL_H_


#include "global.h"
#include "helper.h"
#include <boost/lockfree/queue.hpp>
#include "concurrentqueue.h"

class TxnManager;
class BaseQuery;
class Workload;
struct msg_entry;
struct txn_node;
class Access;
class Transaction;
class row_t;


class TxnManPool {
public:
    void init(Workload * wl, uint64_t size);
    void get(uint64_t thd_id, TxnManager *& item);
    void put(uint64_t thd_id, TxnManager * items);
    void free_all();

private:
#if CC_ALG == CALVIN
    boost::lockfree::queue<TxnManager*> * pool;
#else
    boost::lockfree::queue<TxnManager*> ** pool;
#endif
    Workload * _wl;

};


class TxnPool {
public:
    void init(Workload * wl, uint64_t size);
    void get(uint64_t thd_id, Transaction *& item);
    void put(uint64_t thd_id,Transaction * items);
    void free_all();

private:
#if CC_ALG == CALVIN
    boost::lockfree::queue<Transaction*> * pool;
#else
    boost::lockfree::queue<Transaction*> ** pool;
#endif
    Workload * _wl;

};

class QryPool {
public:
    void init(Workload * wl, uint64_t size);
    void get(uint64_t thd_id, BaseQuery *& item);
    void put(uint64_t thd_id, BaseQuery * items);
    void free_all();

private:
#if CC_ALG == CALVIN
    boost::lockfree::queue<BaseQuery* > * pool;
#else
    boost::lockfree::queue<BaseQuery* > ** pool;
#endif
    Workload * _wl;

};


class AccessPool {
public:
    void init(Workload * wl, uint64_t size);
    void get(uint64_t thd_id, Access *& item);
    void put(uint64_t thd_id, Access * items);
    void free_all();

private:
    boost::lockfree::queue<Access* > ** pool;
    Workload * _wl;

};


class TxnTablePool {
public:
    void init(Workload * wl, uint64_t size);
    void get(uint64_t thd_id, txn_node *& item);
    void put(uint64_t thd_id, txn_node * items);
    void free_all();

private:
    boost::lockfree::queue<txn_node*> ** pool;
    Workload * _wl;

};
class MsgPool {
public:
    void init(Workload * wl, uint64_t size);
    void get(msg_entry *& item);
    void put(msg_entry * items);
    void free_all();

private:
    boost::lockfree::queue<msg_entry* > * pool;
    Workload * _wl;

};

class RowPool {
public:
    void init(Workload * wl, uint64_t size);
    void get(uint64_t thd_id, row_t *& item);
    void put(uint64_t thd_id, row_t * items);
    void free_all();

private:
    boost::lockfree::queue<row_t* > ** pool;
    Workload * _wl;

};


#endif
