/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: williamcliu@tencent.com
 *
 */

#pragma once

#include "../util/dli_identify_util.h"
#include "../row/row_prece.h"
#include <mutex>
#include <type_traits>
#include <list>
#include <algorithm>

namespace ttts {

template <UniAlgs ALG, typename Data>
class AlgManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_CYCLE>>
{
  public:
    using Txn = TxnManager<ALG, Data>;

    void Start(const uint64_t txn_id)
    {
        return std::make_unique<Txn>(txn_id);
    }

    bool Validate(Txn& txn)
    {
        txn.state() = Txn::State::PREPARING;

        //txn-.lock(m_); // release after build WC, WA or RA precedence
        const auto cc_txns = [this, &txn] {
            std::scoped_lock l(m_);
            cc_txns_.emplace_back(txn.shared_from_this());
            return RefreshAndLockTxns_();
        }();
        const auto txn_num = cc_txns.size();
        const auto this_idx = txn_num - 1; // this txn node is just emplaced to the end of cc_txns_

        auto matrix = InitPathMatrix_(cc_txns);
        UpdatePathMatrix_(matrix);

        Path<Txn> cycle = MinCycle_(matrix, this_idx);

        if (!cycle.Passable()) {
            cycle = DirtyCycle<Txn, true /*is_commit*/>(txn);
        }

        if (!cycle.Passable()) {
            return true;
        } else {
            txn.cycle_ = std::make_unique<Path<Txn>>(std::move(cycle));
            return false;
        }
    }

    void Commit(Txn& txn, const uint64_t /*commit_ts*/)
    {
        txn.Commit();
    }

    void Abort(Txn& txn)
    {
        if (!txn.cycle_) {
            // we can only identify the dirty write/read anomaly rather than avoiding it
            Path<Txn> cycle = DirtyCycle<Txn, false /*is_commit*/>(txn);
            if (cycle.Passable()) {
                txn.cycle_ = std::make_unique<Path<Txn>>(std::move(cycle));
            }
        }
        if (const std::unique_ptr<Path<Txn>>& cycle = txn.cycle_) {
            txn.Abort(true /*clear_to_txns*/); // break cycles to prevent memory leak
        } else {
            txn.Abort(false /*clear_to_txns*/);
        }
    }

    void CheckConcurrencyTxnEmpty()
    {
        std::scoped_lock l(m_);
        for (auto it = cc_txns_.begin(); it != cc_txns_.end(); ) {
            if (const auto txn = it->lock()) {
                std::cerr << "** Txn Leak ** " << *txn;
                ++it;
            } else {
                it = cc_txns_.erase(it);
            }
        }
        // assert failed here means there is actually a cycle but we miss it
        assert(cc_txns_.empty());
    }

    static AnomalyType IdentifyAnomaly(const std::vector<PreceInfo<Txn>>& preces)
    {
        assert(preces.size() >= 2);
        const auto& p1 = preces.front();
        const auto& p2 = preces.back();
        if (std::any_of(preces.begin(), preces.end(),
                    [](const PreceInfo<Txn>& prece) { return prece.type() == PreceType::WA ||
                                                        prece.type() == PreceType::WC; })) {
            return AnomalyType::WAT_1_DIRTY_WRITE;
        } else if (std::any_of(preces.begin(), preces.end(),
                    [](const PreceInfo<Txn>& prece) { return prece.type() == PreceType::RA; })) {
            return AnomalyType::RAT_1_DIRTY_READ;
        } else if (preces.size() >= 3) {
            return IdentifyAnomalyMultiple(preces);
        // [Note] When build path, later happened precedence is sorted to back, which is DIFFERENT from 3TS-DA
        } else if (p1.row_id() != p2.row_id()) {
            return IdentifyAnomalyDouble(p1.type(), p2.type());
        } else {
            return IdentifyAnomalySingle(p1.type(), p2.type());
        }
    }

  private:
    std::vector<std::shared_ptr<Txn>> RefreshAndLockTxns_()
    {
        std::vector<std::shared_ptr<Txn>> txns;
        for (auto it = cc_txns_.begin(); it != cc_txns_.end(); ) {
            if (auto txn = it->lock()) {
                txns.emplace_back(std::move(txn));
                ++it;
            } else {
                it = cc_txns_.erase(it);
            }
        }
        return txns;
    }

    std::vector<std::vector<Path<Txn>>> InitPathMatrix_(
        const std::vector<std::shared_ptr<Txn>>& cc_txns) const
    {
        const uint64_t txn_num = cc_txns.size();
        std::vector<std::vector<Path<Txn>>> matrix(txn_num);
        for (uint64_t from_idx = 0; from_idx < txn_num; ++from_idx) {
            matrix[from_idx].resize(txn_num);
            Txn& from_txn_node = *cc_txns[from_idx];
            std::lock_guard<std::mutex> l(from_txn_node.mutex());
            const auto& to_preces = from_txn_node.UnsafeGetToPreces();
            for (uint64_t to_idx = 0; to_idx < txn_num; ++to_idx) {
                const uint64_t to_txn_id = cc_txns[to_idx]->txn_id();
                if (const auto it = to_preces.find(to_txn_id); it != to_preces.end()) {
                    matrix[from_idx][to_idx] = *it->second;
                }
            }
        }
        return matrix;
    }

    static void UpdatePath_(Path<Txn>& path, Path<Txn>&& new_path) {
        if (new_path < path) {
            path = std::move(new_path); // do not use std::min because there is a copy cost when assign self
        }
    };

    void UpdatePathMatrix_(std::vector<std::vector<Path<Txn>>>& matrix) const
    {
        const uint64_t txn_num = matrix.size();
        for (uint64_t mid = 0; mid < txn_num; ++mid) {
            for (uint64_t start = 0; start < txn_num; ++start) {
                if (start == mid) {
                    continue;
                }
                for (uint64_t end = 0; end < txn_num; ++end) {
                    if (end != mid && start != end) {
                        UpdatePath_(matrix[start][end], matrix[start][mid] + matrix[mid][end]);
                    }
                }
            }
        }
    }

    Path<Txn> MinCycle_(std::vector<std::vector<Path<Txn>>>& matrix, const uint64_t this_idx) const
    {
        const uint64_t txn_num = matrix.size();
        Path<Txn> min_cycle;
        for (uint64_t start = 0; start < txn_num; ++start) {
            if (start == this_idx) {
                continue;
            }
            // We should try different order because the path may be imPassable when order is wrong.
            UpdatePath_(min_cycle, matrix[start][this_idx] + matrix[this_idx][start]);
            UpdatePath_(min_cycle, matrix[this_idx][start] + matrix[start][this_idx]);
            assert(!min_cycle.Passable() || min_cycle.IsCycle());
            for (uint64_t end = 0; end < txn_num; ++end) {
                if (end != this_idx && start != end) {
                    // Here try once is ok because start and end will exchange later.
                    UpdatePath_(min_cycle, matrix[start][end] + matrix[end][this_idx] + matrix[this_idx][start]);
                    assert(!min_cycle.Passable() || min_cycle.IsCycle());
                }
            }
        }
        return min_cycle;
    }

    std::mutex m_;
    std::list<std::weak_ptr<Txn>> cc_txns_;
};

}
