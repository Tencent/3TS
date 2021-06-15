/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: anduinzhu@tencent.com
 *         axingguchen@tencent.com
 *         xenitchen@tencent.com
 *
 */

#ifndef ROW_OPT_SSI_H
#define ROW_OPT_SSI_H

class table_t;
class Catalog;
class TxnManager;

struct OPT_SSIReqEntry {
    TxnManager * txn;
    ts_t ts;
    ts_t starttime;
    OPT_SSIReqEntry * next;
};

struct OPT_SSIHisEntry {
    TxnManager *txn;
    txnid_t txnid;
    ts_t ts;
    row_t *row;
    OPT_SSIHisEntry *next;
    OPT_SSIHisEntry *prev;
};

struct OPT_SSILockEntry {
    lock_t type;
    ts_t   start_ts;
    TxnManager * txn;
    txnid_t txnid;
    OPT_SSILockEntry * next;
    OPT_SSILockEntry * prev;
};

class Row_opt_ssi {
public:
    void init(row_t * row);
    RC   access(TxnManager * txn, TsType type, row_t * row);
private:
    pthread_mutex_t * latch;

    OPT_SSILockEntry * si_read_lock;
    OPT_SSILockEntry * write_lock;

    bool blatch;

    row_t * _row;
    void get_lock(lock_t type, TxnManager * txn);
    void release_lock(lock_t type, TxnManager * txn);

    void insert_history(ts_t ts, TxnManager * txn, row_t * row);

    OPT_SSIReqEntry * get_req_entry();
    void return_req_entry(OPT_SSIReqEntry * entry);
    OPT_SSIHisEntry * get_his_entry();
    void return_his_entry(OPT_SSIHisEntry * entry);

    void buffer_req(TsType type, TxnManager * txn);
    OPT_SSIReqEntry * debuffer_req( TsType type, TxnManager * txn = NULL);


    OPT_SSILockEntry * get_entry();
    row_t * clear_history(TsType type, ts_t ts);

    OPT_SSIReqEntry * prereq_mvcc;
    OPT_SSIHisEntry * readhis;
    OPT_SSIHisEntry * writehis;
    OPT_SSIHisEntry * readhistail;
    OPT_SSIHisEntry * writehistail;

    uint64_t whis_len;
    uint64_t rhis_len;
    uint64_t preq_len;
};

#endif
