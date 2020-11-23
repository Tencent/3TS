/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@ruc.edu.cn
 *
 */

#pragma once

#include "global.h"
#ifndef ROW_SUNDIAL_H
#define ROW_SUNDIAL_H

#if CC_ALG == SUNDIAL
#if WRITE_PERMISSION_LOCK

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 62)
#define RTS_LEN (15)
#define WTS_LEN (62 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#else

#define LOCK_BIT (1UL << 63)

#endif

class TxnManager;

class Row_sundial {
public:

    Row_sundial() : Row_sundial(NULL) {};
    Row_sundial(row_t * row);
    void    init(row_t * row);
    RC      access(access_t type, TxnManager * txn, row_t *& row,
                uint64_t &wts, uint64_t &rts);
    RC      read(TxnManager * txn, uint64_t &wts, uint64_t &rts, bool latch = true, bool remote = false);
    RC      write(TxnManager * txn, uint64_t &wts, uint64_t &rts, bool latch = true);

    void    latch();
    void    unlatch();

    RC      commit(access_t type, TxnManager * txn, row_t * data);
    RC      abort(access_t type, TxnManager * txn);

    void    lock();
    bool    try_lock();
    RC      try_lock(TxnManager * txn);
    void    release(TxnManager * txn, RC rc);



    void    update_ts(uint64_t cts);


    bool    try_renew(ts_t wts, ts_t rts, ts_t &new_rts);
    bool    renew(ts_t wts, ts_t rts, ts_t &new_rts);


    void    get_last_ts(ts_t & last_rts, ts_t & last_wts);

    ts_t    get_last_rts();
    ts_t    get_last_wts();

    uint64_t    get_wts() {return _wts;}
    uint64_t    get_rts() {return _rts;}
    void        get_ts(uint64_t &wts, uint64_t &rts);
    void        set_ts(uint64_t wts, uint64_t rts);

    TxnManager *        _lock_owner;
#if OCC_LOCK_TYPE == WAIT_DIE || OCC_WAW_LOCK

    struct CompareWait {
        bool operator() (TxnManager * en1, TxnManager * en2) const;
    };
    std::set<TxnManager *, CompareWait> _waiting_set;
    uint32_t _max_num_waits;
#endif


    bool                 is_deleted() { return _deleted; }
    void                 delete_row(uint64_t del_ts);
    enum State {
        SHARED,
        EXCLUSIVE
    };
    State               _state;
    uint32_t            _owner; // host node ID

    row_t *             _row;

    uint64_t            _wts; // last write timestamp
    uint64_t            _rts; // end lease timestamp

    pthread_mutex_t *   _latch;       // to guarantee read/write consistency
    bool                _blatch;      // to guarantee read/write consistency
    bool                _ts_lock;     // wts/rts cannot be changed if _ts_lock is true.

    bool                _deleted;
    uint64_t            _delete_timestamp;

    // for locality predictor
    uint32_t            _num_remote_reads; // should cache a local copy if this number is too large.
};


#endif
#endif