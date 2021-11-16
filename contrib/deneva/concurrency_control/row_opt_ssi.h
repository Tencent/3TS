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

#include "../system/txn.h"
#include <unordered_set>

class table_t;
class Catalog;
class TxnManager;


/* The lowest bit is used to determine whether an abort is needed */
inline static TxnManager* accessEdge(const TxnManager* node, const bool rw) {
  constexpr uint64_t lowestSet = 1;
  constexpr uint64_t lowestNotSet = ~(lowestSet);
  return reinterpret_cast<TxnManager*>(rw ? lowestSet | reinterpret_cast<uintptr_t>(node)
                                    : lowestNotSet & reinterpret_cast<uintptr_t>(node));
}

inline static std::tuple<TxnManager*, bool> findEdge(const TxnManager* encoded_id) {
  constexpr uint64_t lowestSet = 1;
  constexpr uint64_t lowestNotSet = ~(lowestSet);
  return std::make_tuple(reinterpret_cast<TxnManager*>(lowestNotSet & reinterpret_cast<uintptr_t>(encoded_id)),
                         reinterpret_cast<uintptr_t>(encoded_id) & lowestSet);
}

struct RecycledTxnManagerSets {
  std::unique_ptr<std::vector<std::unique_ptr<TxnManager::NodeSet>>> rns;

  RecycledTxnManagerSets() : rns(new std::vector<std::unique_ptr<TxnManager::NodeSet>>{}) {}
};

struct OPT_SSIReqEntry {
    TxnManager * txn;
    ts_t ts;
    ts_t starttime;
    OPT_SSIReqEntry * next;
};

struct OPT_SSILockEntry {
    lock_t type;
    ts_t   start_ts;
    TxnManager * txn;
    txnid_t txnid;
    OPT_SSILockEntry * next;
    OPT_SSILockEntry * prev;
};

struct OPT_SSIHisEntry {
    TxnManager *txn;
    txnid_t txnid;
    ts_t ts;
    row_t *row;
    OPT_SSIHisEntry *next;
    OPT_SSIHisEntry *prev;
    OPT_SSILockEntry * si_read_lock;
};

class Row_opt_ssi {
    using Allocator = common::ChunkAllocator;
    using EMB = atom::EpochManagerBase<Allocator>;
    using EM = atom::EpochManager<Allocator>;
    using NEMB = atom::EpochManagerBase<common::NoAllocator>;
    using NEM = atom::EpochManager<common::NoAllocator>;
public:
   
    void init(row_t * row);
    // RC   access(TxnManager * txn, TsType type, row_t * row);
   
    const uint64_t size() const;
    uintptr_t createNode();
    void setInactive();
    void waitAndTidy();
    void cleanup();
    bool insert_and_check(uintptr_t from_node, bool read_write_edge);
    bool lookupEdge(uintptr_t from_node) const;
    void removeEdge(uintptr_t from_node);

    bool find(TxnManager::NodeSet& nodes, TxnManager* txn) const;
    bool cycleCheckNaive();
    bool cycleCheckNaive(TxnManager* cur) const;
    bool needsAbort(uintptr_t node);
    bool isCommited(uintptr_t node);
    void abort(std::unordered_set<uint64_t>& uset);
    bool checkCommited();
    bool erase_graph_constraints();
    std::string generateString();
    void print();
    void log(const common::LogInfo log_info);
    void log(const std::string log_info);

    bool cycleCheckOnline(const uintptr_t from_node);
    bool dfsF(uintptr_t n, uint64_t ub) const;
    bool dfsB(uintptr_t n, uint64_t lb) const;
    void reorder() const;
    void setEMB(EMB* emb);
    void setAlloc(Allocator* alloc);

private:
    pthread_mutex_t * latch;

    OPT_SSILockEntry * si_read_lock;
    OPT_SSILockEntry * write_lock;
    atom::AtomicSinglyLinkedList<uint64_t> rw_history;

    bool blatch;

    row_t * _row;
    void get_lock(lock_t type, TxnManager * txn);
    void get_lock(lock_t type, TxnManager * txn, OPT_SSIHisEntry * whis);
    //void release_lock(lock_t type, TxnManager * txn);
    //void release_lock(ts_t min_ts);

    // void insert_history(ts_t ts, TxnManager * txn, row_t * row);

    OPT_SSIReqEntry * get_req_entry();
    void return_req_entry(OPT_SSIReqEntry * entry);
    OPT_SSIHisEntry * get_his_entry();
    void return_his_entry(OPT_SSIHisEntry * entry);

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
    //the following is used for graphcc
    tbb::spin_mutex mut;
    common::GlobalLogger logger;
    Allocator* alloc_;
    EMB* em_;
    common::NoAllocator noalloc_;
    NEMB nem_;
    std::atomic<uint64_t> created_sets_;
    atom::AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                             common::ChunkAllocator, false>
        order_map_;
    std::atomic<uint64_t> order_version_;
    const bool online_ = false;

    static thread_local std::unordered_set<TxnManager*> visited;
    static thread_local std::unordered_set<TxnManager*> visit_path;
    static thread_local RecycledTxnManagerSets empty_sets;
    static thread_local TxnManager* this_node;
    static thread_local atom::EpochGuard<NEMB, NEM>* neg_;
    

    std::vector<std::pair<uint64_t, uint64_t>> dF;
    std::vector<std::pair<uint64_t, uint64_t>> dB;

    std::vector<uint64_t> L;
    std::vector<std::pair<uint64_t, uint64_t>> R;
};

#endif
