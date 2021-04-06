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

#include <unordered_map>
#include <memory>
#include <mutex>
#include <type_traits>
#include "util.h"
#include "dli_identify_util.h"

namespace ttts {

template <typename Txn> class Path;
template <typename Txn> class PreceInfo;

template <UniAlgs ALG, typename Data>
class TxnManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_CYCLE ||
                                                      ALG == UniAlgs::UNI_DLI_IDENTIFY_CHAIN>>
        : public std::enable_shared_from_this<TxnManager<ALG, Data>>
{
  public:
    using Txn = TxnManager<ALG, Data>;

    template <typename ...Args>
    static std::shared_ptr<TxnManager> Construct(Args&&... args)
    {
        // release condition (1) for TxnManager
        return std::make_shared<TxnManager>(std::forward<Args>(args)...);
    }

    using Ver = VerManager<ALG, Data>;

    enum class State { ACTIVE, PREPARING, COMMITTED, ABORTED };

    TxnManager(const uint64_t txn_id, const uint64_t /*start_ts*/) : txn_id_(txn_id), state_(State::ACTIVE) {}

    // We use ver_id but not the count of operation to support snapshot read.
    // [Note] Should ensure to_txn_node thread safe.
    void AddToTxn(TxnManager& to_txn, const uint64_t row_id, const uint64_t from_ver_id,
            const uint64_t to_ver_id, const PreceType type)
    {
        const uint64_t to_txn_id = to_txn.txn_id();
        if (const auto& real_type = RealPreceType_(type); txn_id_ != to_txn_id && real_type.has_value()) {
            std::lock_guard<std::mutex> l(m_);
            std::shared_ptr<PreceInfo<Txn>> prece = std::make_shared<PreceInfo<Txn>>(this->shared_from_this(), to_txn.shared_from_this(),
                    *real_type, row_id, from_ver_id, to_ver_id);
            // For read/write precedence, only record the first precedence between the two transactions
            to_preces_.try_emplace(to_txn_id, prece);
            to_txn.from_preces_.try_emplace(txn_id_, prece);
            // For dirty precedence, W1W2 has higher priority than W1R2 because W1R2C1 is not dirty read
            if (real_type == PreceType::WR || real_type == PreceType::WW) {
                dirty_to_preces_.emplace_back(prece);
            }
        }
    }

    uint64_t txn_id() const { return txn_id_; }

    const auto& UnsafeGetToPreces() const { return to_preces_; }
    const auto& UnsafeGetFromPreces() const { return from_preces_; }
    const auto& UnsafeGetDirtyToPreces() const { return dirty_to_preces_; }

    std::mutex& mutex() const { return m_; }

    void Commit()
    {
        std::lock_guard<std::mutex> l(m_);
        state_ = State::COMMITTED;
        dirty_to_preces_.clear();
    }

    void Abort(const bool clear_to_preces)
    {
        std::lock_guard<std::mutex> l(m_);
        state_ = State::ABORTED;
        dirty_to_preces_.clear();
        if (clear_to_preces) {
            to_preces_.clear();
        }
    }

    const auto& state() const { return state_; }
    auto& state() { return state_; }

    friend std::ostream& operator<<(std::ostream& os, const TxnManager& txn)
    {
        os << "[Leak Txn] T" << txn.txn_id();
        std::scoped_lock l(txn.mutex());
        os << " [From Txns] T";
        for (const auto& [from_txn_id, from_prece_weak] : txn.UnsafeGetFromPreces()) {
            if (const auto from_prece = from_prece_weak.lock()) {
                os << from_txn_id << "(" << *from_prece << ") ";
            } else {
                os << from_txn_id << "(null prece) ";
            }
        }
        os << " [To Txns] T";
        for (const auto& [to_txn_id, to_prece] : txn.UnsafeGetToPreces()) {
            os << to_txn_id << "(" << *to_prece << ") ";
        }
        return os;
    }

    std::unique_ptr<Path<Txn>> cycle_;
    std::unordered_map<uint64_t, std::shared_ptr<Ver>> pre_versions_; // record for revoke

  private:
    std::optional<PreceType> RealPreceType_(const std::optional<PreceType>& active_prece_type,
            const std::optional<PreceType>& committed_prece_type,
            const std::optional<PreceType>& aborted_prece_type) const
    {
        const auto state = state_.load();
        if (state == State::ACTIVE || state == State::PREPARING) {
            return active_prece_type;
        } else if (state == State::COMMITTED) {
            return committed_prece_type;
        } else {
            assert(state == State::ABORTED);
            return aborted_prece_type;
        }
    }

    std::optional<PreceType> RealPreceType_(const PreceType type) const
    {
        if (type == PreceType::WW) {
            return RealPreceType_(PreceType::WW, PreceType::WCW, {});
        } else if (type == PreceType::WR) {
            return RealPreceType_(PreceType::WR, PreceType::WCR, {});
        } else {
            return RealPreceType_(type, type, {});
        }
    }

    mutable std::mutex m_;
    uint64_t txn_id_;
    std::atomic<State> state_;
    // Key is txn_id.
    std::unordered_map<uint64_t, std::shared_ptr<PreceInfo<Txn>>> to_preces_;
    // We can not use std::shared_ptr because PreceInfo has a use count to this which will cause cycle
    // reference. Key is txn_id;
    std::unordered_map<uint64_t, std::weak_ptr<PreceInfo<Txn>>> from_preces_;
    // The prece may also stored in to_preces_.
    std::vector<std::shared_ptr<PreceInfo<Txn>>> dirty_to_preces_;
};

}
