#include "row_si.h"

#include "storage/row.h"
#include "system/manager.h"
#include "system/mem_alloc.h"
#include "system/txn.h"
#include "dli.h"

inline bool TupleSatisfiesMVCC(const SIEntry& tuple, const ts_t start_ts) {
    return tuple.commit_ts < start_ts;
}
void Row_si::init(row_t* row) {
    row_ = row;
    versions_ = new TSNode<SIEntry>(row_, 0, 0);
    latch_ = (pthread_mutex_t*)mem_allocator.alloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(latch_, NULL);
    w_trans = 0;
    timestamp_last_write = 0;
}
RC Row_si::access(TxnManager* txn, TsType type, uint64_t& version) {
    uint64_t start_time = get_sys_clock();
    ts_t ts = txn->get_start_timestamp();
    if (type == R_REQ) {
        txn->cur_row = row_;
        version = 0;
        for (TSNode<SIEntry>* v = versions_.load(); v != nullptr; v = v->next_) {
            if (v->commit_ts < ts) {
                txn->cur_row = v->row;
                version = v->commit_ts;
                //std::lock_guard<std::mutex> l(v->mutex);
                //v->r_trans_ts.push_back(ts);
                break;
            }
        }
    } else {
        assert(false);
    }
    uint64_t timespan = get_sys_clock() - start_time;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    return RCOK;
}
void Row_si::latch() { pthread_mutex_lock(latch_); }
void Row_si::release() { pthread_mutex_unlock(latch_); }

uint64_t Row_si::write(row_t* data, TxnManager* txn, access_t type) {
    uint64_t res = 0;
    if (type == WR) {
        res = TSNode<SIEntry>::push_front(versions_, data, txn->get_start_timestamp(), txn->get_commit_timestamp())->commit_ts;
        timestamp_last_write = txn->get_commit_timestamp();
    }
    if (w_trans == txn->get_start_timestamp()) w_trans = 0;
    return res;
}
