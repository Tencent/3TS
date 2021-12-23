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
#include <atomic>

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


class Row_opt_ssi {
    using Allocator = common::ChunkAllocator;
    using EMB = atom::EpochManagerBase<Allocator>;
    using EM = atom::EpochManager<Allocator>;
    using NEMB = atom::EpochManagerBase<common::NoAllocator>;
    using NEM = atom::EpochManager<common::NoAllocator>;
public:
   
    void init(row_t * row);
    RC   access(TxnManager * txn, TsType type, row_t * row);
   
    const uint64_t size() const;
    uintptr_t createNode(TxnManager* cur_node);
    void setInactive();
    void waitAndTidy();
    void cleanup(TxnManager* cur_node);
    //bool insert_and_check(uintptr_t from_node, bool read_write_edge);
    bool insert_and_check1(TxnManager* cur_node, uintptr_t from_node, bool readwrite);
    bool lookupEdge(uintptr_t from_node) const;
    void removeEdge(uintptr_t from_node);

    bool find(TxnManager::NodeSet& nodes, TxnManager* txn) const;
    bool cycleCheckNaiveWrapper(TxnManager* cur_node);
    bool cycleCheckNaive(TxnManager* cur) const;
    bool needsAbort(uintptr_t node);
    bool isCommited(uintptr_t node);
    void abort(TxnManager* cur_node, std::unordered_set<uint64_t>& uset);
    bool checkCommited(TxnManager* cur_node);
    bool erase_graph_constraints(TxnManager* cur_node);
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
    atom::AtomicSinglyLinkedList<uint64_t> *rw_history, tmp_hisotry;
    bool blatch;

    row_t * _row;

    inline static constexpr uint64_t encode_txnid(const uint64_t txnid, const bool rw) {
        return rw ? 0x8000000000000000 | txnid : 0x7FFFFFFFFFFFFFFF & txnid;
    }

    /* Returns the transaction and the used transaction given an encoded bitstring */
    inline static constexpr std::tuple<uint64_t, bool> find(const uint64_t encoded_id) {
      return std::make_tuple(0x7FFFFFFFFFFFFFFF & encoded_id, encoded_id >> 63);
    }

   
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
    TxnManager* cur_node;
    static thread_local atom::EpochGuard<NEMB, NEM>* neg_;
    std::atomic<uint64_t> lsn;
    
    
    std::vector<std::pair<uint64_t, uint64_t>> dF;
    std::vector<std::pair<uint64_t, uint64_t>> dB;

    std::vector<uint64_t> L;
    std::vector<std::pair<uint64_t, uint64_t>> R;
};

#endif
