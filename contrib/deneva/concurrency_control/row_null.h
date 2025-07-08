/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 Tencent.  All rights reserved.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#ifndef ROW_NULL_H
#define ROW_NULL_H
#include "../storage/row.h"
class table_t;
class Catalog;
class TxnManager;

class Row_null {
public:
    void init(row_t* row);
    RC access(TsType type, TxnManager* txn, row_t* row, uint64_t& version);
    RC abort(access_t type, TxnManager* txn);
    RC commit(access_t type, TxnManager* txn, row_t* data, uint64_t& version);

private:
    row_t* _row;

    pthread_mutex_t* latch;
    bool blatch;
};

#endif
