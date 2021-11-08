/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: anduinzhu@tencent.com
 *
 */

#ifndef ROW_SGRAPH_H
#define ROW_SGRAPH_H

#include "../global.h"

class table_t;
class Catalog;
class TxnManager;



class Row_sgraph {
public:
    void init(row_t * row);
    RC   access(TxnManager * txn, TsType type, row_t * row);
private:
    pthread_mutex_t * latch;

    bool blatch;

    row_t * _row;
    void get_lock(lock_t type, TxnManager * txn);
    void get_lock(lock_t type, TxnManager * txn, OPT_SSIHisEntry * whis);
    void release_lock(lock_t type, TxnManager * txn);
    void release_lock(ts_t min_ts);
    
    
    
};

#endif
