#pragma once

#include "dli_identify_util.h"
#include "row_prece.h"
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

    bool Validate(Txn& txn)
    {
        txn.node_->state() = TxnNode::State::PREPARING;

        //txn-.lock(m_); // release after build WC, WA or RA precedence
        const auto cc_txns = [this, &txn] {
            std::scoped_lock l(m_);
            cc_txns_.emplace_back(txn.node_);
            return RefreshAndLockTxns_();
        }();
        const auto txn_num = cc_txns.size();
        const auto this_idx = txn_num - 1; // this txn node is just emplaced to the end of cc_txns_

        auto matrix = InitPathMatrix_(cc_txns);
        UpdatePathMatrix_(matrix);

        Path cycle = MinCycle_(matrix, this_idx);

        if (!cycle.Passable()) {
            cycle = DirtyCycle<true /*is_commit*/>(*txn.node_);
        }

        if (!cycle.Passable()) {
            return true;
        } else {
            txn.cycle_ = std::make_unique<Path>(std::move(cycle));
            return false;
        }
    }

    void Commit(Txn& txn)
    {
        txn.node_->Commit();
    }

    void Abort(Txn& txn)
    {
        if (!txn.cycle_) {
            // we can only identify the dirty write/read anomaly rather than avoiding it
            Path cycle = DirtyCycle<false /*is_commit*/>(*txn.node_);
            txn.cycle_ = std::make_unique<Path>(std::move(cycle));
        }
        if (const std::unique_ptr<Path>& cycle = txn.cycle_) {
            txn.node_->Abort(true /*clear_to_txns*/); // break cycles to prevent memory leak
        } else {
            txn.node_->Abort(false /*clear_to_txns*/);
        }
    }

    void CheckConcurrencyTxnEmpty()
    {
        RefreshAndLockTxns_();
        if (!cc_txns_.empty()) {
            // assert failed here means there is actually a cycle but we miss it
            std::cerr << "**** Has Txn Leak ****" << std::endl;
            for (const auto txn_weak : cc_txns_) {
                if (const auto txn = txn_weak.lock()) {
                    std::cerr << "Leak Txn: " << txn->txn_id() << std::endl;
                    std::scoped_lock l(txn->mutex());
                    std::cerr << "From Txns: ";
                    for (const auto& [from_txn_id, from_prece_weak] : txn->UnsafeGetFromPreces()) {
                        if (const auto from_prece = from_prece_weak.lock()) {
                            std::cerr << from_txn_id << "(" << *from_prece << ") ";
                        } else {
                            std::cerr << from_txn_id << "(null prece) ";
                        }
                    }
                    std::cerr << std::endl;
                    std::cerr << "To Txns: ";
                    for (const auto& [to_txn_id, to_prece] : txn->UnsafeGetToPreces()) {
                        std::cerr << to_txn_id << "(" << *to_prece << ") ";
                    }
                    std::cerr << std::endl;
                }
            }
            std::cerr << "**** Txn Leak Info End ****" << std::endl;
            assert(false);
        }
    }

    static AnomalyType IdentifyAnomaly(const std::vector<PreceInfo>& preces)
    {
        assert(preces.size() >= 2);
        if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WA || prece.type() == PreceType::WC; })) {
            // WA and WC precedence han only appear
            return AnomalyType::WAT_1_DIRTY_WRITE;
        } else if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::RA; })) {
            return AnomalyType::RAT_1_DIRTY_READ;
        } else if (preces.size() >= 3) {
            return IdentifyAnomalyMultiple_(preces);
        // [Note] When build path, later happened precedence is sorted to back, which is DIFFERENT from 3TS-DA
        } else if (preces.back().row_id() != preces.front().row_id()) {
            return IdentifyAnomalyDouble_(preces.front().type(), preces.back().type());
        } else {
            return IdentifyAnomalySingle_(preces.front().type(), preces.back().type());
        }
    }

  private:
    std::vector<std::shared_ptr<TxnNode>> RefreshAndLockTxns_()
    {
        std::vector<std::shared_ptr<TxnNode>> txns;
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

    std::vector<std::vector<Path>> InitPathMatrix_(
        const std::vector<std::shared_ptr<TxnNode>>& cc_txns) const
    {
        const uint64_t txn_num = cc_txns.size();
        std::vector<std::vector<Path>> matrix(txn_num);
        for (uint64_t from_idx = 0; from_idx < txn_num; ++from_idx) {
            matrix[from_idx].resize(txn_num);
            TxnNode& from_txn_node = *cc_txns[from_idx];
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

    static void UpdatePath_(Path& path, Path&& new_path) {
        if (new_path < path) {
            path = std::move(new_path); // do not use std::min because there is a copy cost when assign self
        }
    };

    void UpdatePathMatrix_(std::vector<std::vector<Path>>& matrix) const
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

    Path MinCycle_(std::vector<std::vector<Path>>& matrix, const uint64_t this_idx) const
    {
        const uint64_t txn_num = matrix.size();
        Path min_cycle;
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

    // require type1 precedence happens before type2 precedence
    static AnomalyType IdentifyAnomalySingle_(const PreceType early_type, const PreceType later_type) {
        if ((early_type == PreceType::WW || early_type == PreceType::WR) && (later_type == PreceType::WW || later_type == PreceType::WCW)) {
            return AnomalyType::WAT_1_FULL_WRITE; // WW-WW | WR-WW = WWW
        } else if (early_type == PreceType::WR && early_type == PreceType::WW) {
            return AnomalyType::WAT_1_FULL_WRITE; // WR-WW = WWW
        } else if ((early_type == PreceType::WW || early_type == PreceType::WR) && (later_type == PreceType::WR || later_type == PreceType::WCR)) {
            return AnomalyType::WAT_1_LOST_SELF_UPDATE; // WW-WR = WWR
        } else if (early_type == PreceType::RW && later_type == PreceType::WW) {
            return AnomalyType::WAT_1_LOST_UPDATE; // RW-WW | RW-RW = RWW
        } else if (early_type == PreceType::RW && later_type == PreceType::RW) {
            return AnomalyType::WAT_1_LOST_UPDATE; // RW-WW | RW-RW = RWW
        } else if (early_type == PreceType::WR && later_type == PreceType::RW) {
            return AnomalyType::RAT_1_INTERMEDIATE_READ; // WR-RW = WRW
        } else if (early_type == PreceType::RW && (later_type == PreceType::WR || later_type == PreceType::WCR)) {
            return AnomalyType::RAT_1_NON_REPEATABLE_READ; // RW-WR = RWR
        } else if (early_type == PreceType::RW && later_type == PreceType::WCW) {
            return AnomalyType::IAT_1_LOST_UPDATE_COMMITTED; // RW-WW(WCW) = RWW
        } else {
            return AnomalyType::UNKNOWN_1;
        }
    }

    static AnomalyType IdentifyAnomalyDouble_(const PreceType early_type, const PreceType later_type) {
        const auto any_order = [early_type, later_type](const PreceType type1, const PreceType type2) -> std::optional<bool> {
            if (early_type == type1 && later_type == type2) {
                return true;
            } else if (early_type == type2 && later_type == type1) {
                return false;
            } else {
                return {};
            }
        };
        if (const auto order = any_order(PreceType::WR, PreceType::WW); order.has_value()) {
            return *order ? AnomalyType::WAT_2_DOUBLE_WRITE_SKEW_1 : AnomalyType::WAT_2_DOUBLE_WRITE_SKEW_2;
        } else if (any_order(PreceType::WW, PreceType::WCR)) {
            return AnomalyType::WAT_2_DOUBLE_WRITE_SKEW_2;
        } else if (const auto order = any_order(PreceType::RW, PreceType::WW); order.has_value()) {
            return *order ? AnomalyType::WAT_2_READ_WRITE_SKEW_1 : AnomalyType::WAT_2_READ_WRITE_SKEW_2;
        } else if (any_order(PreceType::WW, PreceType::WW)) {
            return AnomalyType::WAT_2_FULL_WRITE_SKEW;
        } else if (any_order(PreceType::WW, PreceType::WCW)) {
            return AnomalyType::WAT_2_FULL_WRITE_SKEW;
        } else if (any_order(PreceType::WR, PreceType::WR)) {
            return AnomalyType::RAT_2_WRITE_READ_SKEW;
        } else if (any_order(PreceType::WR, PreceType::WCR)) {
            return AnomalyType::RAT_2_WRITE_READ_SKEW;
        } else if (any_order(PreceType::WR, PreceType::WCW)) {
            return AnomalyType::RAT_2_DOUBLE_WRITE_SKEW_COMMITTED;
        } else if (const auto order = any_order(PreceType::RW, PreceType::WR); order.has_value()) {
            return *order ? AnomalyType::RAT_2_READ_SKEW : AnomalyType::RAT_2_READ_SKEW_2;
        } else if (any_order(PreceType::RW, PreceType::WCR)) {
            return AnomalyType::RAT_2_READ_SKEW;
        } else if (any_order(PreceType::RW, PreceType::WCW)) {
            return AnomalyType::IAT_2_READ_WRITE_SKEW_COMMITTED;
        } else if (any_order(PreceType::RW, PreceType::RW)) {
            return AnomalyType::IAT_2_WRITE_SKEW;
        } else {
            return AnomalyType::UNKNOWN_2;
        }
    }

    static AnomalyType IdentifyAnomalyMultiple_(const std::vector<PreceInfo>& preces) {
        if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WW; })) {
            return AnomalyType::WAT_STEP;
        }
        if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WR || prece.type() == PreceType::WCR; })) {
            return AnomalyType::RAT_STEP;
        }
        return AnomalyType::IAT_STEP;
    }

    std::mutex m_;
    std::list<std::weak_ptr<TxnNode>> cc_txns_;
};

}

