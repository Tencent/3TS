#ifndef SI_H
#define SI_H
#include <unordered_map>
#include <unordered_set>

#include "../storage/row.h"
#include "../system/global.h"
#include "semaphore.h"

template <typename T> class TSNode;

struct DliValidatedTxn;

class Dli {
  public:
    using RWSet = std::map<row_t*, uint64_t>;

    void init();
    RC validate(TxnManager* txn, const bool lock_rows = true);
    void finish_trans(RC rc, TxnManager* txn);
    RC find_bound(TxnManager* txn);
    static void get_rw_set(TxnManager* txn, RWSet& rset,
                          RWSet& wset);
    void latch();
    void release();

    std::atomic<TSNode<DliValidatedTxn>*> validated_txns_;

    // pthread_mutex_t validated_trans_mutex_;
    // pthread_mutex_t commit_trans_rset_mutex_;
    // pthread_mutex_t commit_trans_wset_mutex_;
    // pthread_mutex_t commit_trans_lowup_mutex_;
    pthread_mutex_t mtx_;
};

struct DliValidatedTxn {
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
    DliValidatedTxn(const Dli::RWSet& rset, const Dli::RWSet& wset, const ts_t start_ts, const uint64_t lower, const uint64_t upper) : is_abort_(false), rset_(rset), wset_(wset), start_ts_(start_ts), lower_(lower), upper_(upper) {}
#else
    DliValidatedTxn(const Dli::RWSet& rset, const Dli::RWSet& wset, const ts_t start_ts) : is_abort_(false), rset_(rset), wset_(wset), start_ts_(start_ts) {}
#endif
    DliValidatedTxn(DliValidatedTxn&&) = default;
    std::atomic<bool> is_abort_;
    Dli::RWSet rset_;
    Dli::RWSet wset_;
    const ts_t start_ts_;
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
    std::atomic<uint64_t> lower_;
    std::atomic<uint64_t> upper_;
#endif
};

#endif
