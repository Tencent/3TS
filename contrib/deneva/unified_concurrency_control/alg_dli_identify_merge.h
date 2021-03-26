#pragma once

#include "dli_identify_util.h"
#include "row_prece.h"
#include <mutex>
#include <type_traits>

namespace ttts {

template <UniAlgs ALG, typename Data>
class AlgManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_MERGE>>
{
  public:
    using Txn = TxnManager<ALG, Data>;

    bool Validate(Txn& txn)
    {
        txn.node_->state() = TxnNode::State::PREPARING;

        {
            std::scoped_lock l(m_);
            cc_txns_.emplace_back(txn.node_);
        }

        Path cycle_part = DirtyCycle<true /*is_commit*/>(*txn.node_);

        if (!cycle_part.Passable()) {
            cycle_part = CyclePart_(txn);
        }

        if (!cycle_part.Passable()) {
            return true;
        } else {
            txn.cycle_ = std::make_unique<Path>(std::move(cycle_part));
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

    void CheckConcurrencyTxnEmpty() {
        std::scoped_lock l(m_);
        bool is_empty = true;
        for (const auto& weak_txn : cc_txns_) {
            if (const auto txn = weak_txn.lock()) {
                std::cerr << "** Txn Leak ** " << *txn;
                is_empty = false;
            }
        }
        // assert failed here means there is actually a cycle but we miss it
        assert(is_empty);
        cc_txns_.clear();
    }

    static AnomalyType IdentifyAnomaly(const std::vector<PreceInfo>& preces)
    {
        assert(preces.size() == 2);
        const auto& p1 = preces.front();
        const auto& p2 = preces.back();
        if (std::any_of(preces.begin(), preces.end(),
                    [](const PreceInfo& prece) { return prece.type() == PreceType::WA ||
                                                        prece.type() == PreceType::WC; })) {
            return AnomalyType::WAT_1_DIRTY_WRITE;
        } else if (std::any_of(preces.begin(), preces.end(),
                    [](const PreceInfo& prece) { return prece.type() == PreceType::RA; })) {
            return AnomalyType::RAT_1_DIRTY_READ;
        } else if (p1.from_txn_id() != p2.to_txn_id()) {
            return IdentifyAnomalyMultiple(preces);
        // [Note] When build path, later happened precedence is sorted to back, which is DIFFERENT from 3TS-DA
        } else if (p1.row_id() != p2.row_id()) {
            return IdentifyAnomalyDouble(p1.type(), p2.type());
        } else if (p1.to_ver_id() < p2.to_ver_id() ||
                  (p1.to_ver_id() == p2.to_ver_id() && p1.from_ver_id() < p2.from_ver_id())) {
            return IdentifyAnomalySingle(p1.type(), p2.type());
        } else {
            return IdentifyAnomalySingle(p2.type(), p1.type());
        }
    }

  private:
    Path CyclePart_(Txn& txn)
    {
        // Validate failed if has a from_txn and a to_txn which are both finished but not released.
        std::scoped_lock l(txn.node_->mutex());
        std::shared_ptr<PreceInfo> from_prece;
        for (const auto& [_, weak_from_prece] : txn.node_->UnsafeGetFromPreces()) {
            from_prece = weak_from_prece.lock();
            if (from_prece == nullptr) {
                continue;
            }
            const auto& from_txn = from_prece->from_txn();
            if (from_txn == nullptr || from_txn->state() == TxnNode::State::ACTIVE) {
                from_prece = nullptr;
                continue;
            }
            break;
        }

        std::shared_ptr<PreceInfo> to_prece;
        for (const auto& [_, to_prece_tmp] : txn.node_->UnsafeGetToPreces()) {
            if (to_prece_tmp->to_txn()->state() != TxnNode::State::ACTIVE) {
                to_prece = to_prece_tmp;
                break;
            }
        }

        if (from_prece && to_prece) {
            return Path(std::vector<PreceInfo>{*from_prece, *to_prece});
        }
        return {};
    }

    std::mutex m_;
    std::vector<std::weak_ptr<TxnNode>> cc_txns_; // only for debug
};

}

