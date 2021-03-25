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

        std::scoped_lock l(m_);

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
        for (const auto& [txn_id, prece] : cc_txns_) {
            // assert failed here means there is actually a cycle but we miss it
            assert(prece.expired());
            cc_txns_.erase(txn_id);
        }
    }

  private:
    Path CyclePart_(Txn& txn)
    {
        // Validate failed if has a from_txn and a to_txn which are both finished but not released.
        std::scoped_lock l(txn.node_->mutex());
        std::shared_ptr<PreceInfo> from_prece;
        for (const auto& [from_txn_id, weak_from_prece] : txn.node_->UnsafeGetFromPreces()) {
            const auto it = cc_txns_.find(from_txn_id);
            if (it != cc_txns_.end()) {
                if (it->second.expired()) {
                    cc_txns_.erase(it);
                } else {
                    from_prece = weak_from_prece.lock();
                    assert(from_prece != nullptr);
                    break;
                }
            }
        }

        std::shared_ptr<PreceInfo> to_prece;
        for (const auto& [to_txn_id, to_prece_tmp] : txn.node_->UnsafeGetToPreces()) {
            const auto it = cc_txns_.find(to_txn_id);
            if (it != cc_txns_.end()) {
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
    std::unordered_map<uint64_t, std::weak_ptr<TxnNode>> cc_txns_;
};

}

