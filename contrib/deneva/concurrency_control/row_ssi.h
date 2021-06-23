/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */

#ifndef ROW_SSI_H
#define ROW_SSI_H

class table_t;
class Catalog;
class TxnManager;

struct SSIReqEntry {
    TxnManager * txn;
    ts_t ts;
    ts_t starttime;
    SSIReqEntry * next;
};

struct SSIHisEntry {
    //std::shared_ptr<TxnManager> txn;
    TxnManager * txn;
    txnid_t txnid;
    ts_t ts;
    // only for write history. The value needs to be stored.
    // char * data;
    row_t * row;
    SSIHisEntry * next;
    SSIHisEntry * prev;
};

struct SSILockEntry {
    lock_t type;
    ts_t   start_ts;
    //std::shared_ptr<TxnManager> txn;
    TxnManager * txn;
    txnid_t txnid;
    bool active;
    SSILockEntry * next;
    SSILockEntry * prev;
};

class Row_ssi {
public:
    void init(row_t * row);
    RC access(TxnManager * txn, TsType type, row_t * row);
    RC validate_last_commit(TxnManager * txn);
private:
    pthread_mutex_t * latch;

    SSILockEntry * si_read_lock;
    SSILockEntry * write_lock;
    SSILockEntry * read_now;
    SSILockEntry * write_now;
    uint64_t read_cnt;
    uint64_t write_cnt;

    enum{SIZE = 10};

    //std::vector<TxnManager*> *si_read, *wrt_list;

    bool blatch;

    row_t * _row;
    void get_lock(lock_t type, TxnManager * txn);
    void release_lock(lock_t type, TxnManager * txn);
    void release_lock(ts_t min_ts);

    void insert_history(ts_t ts, TxnManager * txn, row_t * row);

    SSIReqEntry * get_req_entry() { return (SSIReqEntry *) mem_allocator.alloc(sizeof(SSIReqEntry)); }
    void return_req_entry(SSIReqEntry * entry) { mem_allocator.free(entry, sizeof(SSIReqEntry)); }
    SSIHisEntry * get_his_entry() { return new (SSIHisEntry); }
    void return_his_entry(SSIHisEntry * entry);

    bool conflict(TsType type, ts_t ts);


    SSILockEntry * get_entry();
    row_t * clear_history(TsType type, ts_t ts);

    SSIReqEntry * prereq_mvcc;
    SSIHisEntry * readhis;
    SSIHisEntry * writehis;
    SSIHisEntry * readhistail;
    SSIHisEntry * writehistail;

    uint64_t whis_len;
    uint64_t rhis_len;
    uint64_t preq_len;
};

#endif
