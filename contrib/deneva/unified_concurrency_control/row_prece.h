/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: williamcliu@tencent.com
 *
 */

#ifdef ENUM_BEGIN
#ifdef ENUM_MEMBER
#ifdef ENUM_END

ENUM_BEGIN(OperationType)
ENUM_MEMBER(OperationType, W)
ENUM_MEMBER(OperationType, R)
ENUM_MEMBER(OperationType, C)
ENUM_MEMBER(OperationType, A)
ENUM_END(OperationType)

ENUM_BEGIN(PreceType)
ENUM_MEMBER(PreceType, RW)
ENUM_MEMBER(PreceType, WR)
ENUM_MEMBER(PreceType, WCR)
ENUM_MEMBER(PreceType, WW)
ENUM_MEMBER(PreceType, WCW)
ENUM_MEMBER(PreceType, RA)
ENUM_MEMBER(PreceType, WC)
ENUM_MEMBER(PreceType, WA)
ENUM_END(PreceType)

ENUM_BEGIN(AnomalyType)
// ======== WAT - 1 =========
ENUM_MEMBER(AnomalyType, WAT_1_DIRTY_WRITE)
ENUM_MEMBER(AnomalyType, WAT_1_FULL_WRITE)
ENUM_MEMBER(AnomalyType, WAT_1_LOST_SELF_UPDATE)
ENUM_MEMBER(AnomalyType, WAT_1_LOST_UPDATE)
// ======== WAT - 2 =========
ENUM_MEMBER(AnomalyType, WAT_2_DOUBLE_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, WAT_2_DOUBLE_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, WAT_2_READ_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, WAT_2_READ_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, WAT_2_FULL_WRITE_SKEW)
// ======== WAT - 3 =========
ENUM_MEMBER(AnomalyType, WAT_STEP)
// ======== RAT - 1 =========
ENUM_MEMBER(AnomalyType, RAT_1_DIRTY_READ)
ENUM_MEMBER(AnomalyType, RAT_1_INTERMEDIATE_READ)
ENUM_MEMBER(AnomalyType, RAT_1_NON_REPEATABLE_READ)
// ======== RAT - 2 =========
ENUM_MEMBER(AnomalyType, RAT_2_WRITE_READ_SKEW)
ENUM_MEMBER(AnomalyType, RAT_2_DOUBLE_WRITE_SKEW_COMMITTED)
ENUM_MEMBER(AnomalyType, RAT_2_READ_SKEW)
ENUM_MEMBER(AnomalyType, RAT_2_READ_SKEW_2)
// ======== RAT - 3 =========
ENUM_MEMBER(AnomalyType, RAT_STEP)
// ======== IAT - 1 =========
ENUM_MEMBER(AnomalyType, IAT_1_LOST_UPDATE_COMMITTED)
// ======== IAT - 2 =========
ENUM_MEMBER(AnomalyType, IAT_2_READ_WRITE_SKEW_COMMITTED)
ENUM_MEMBER(AnomalyType, IAT_2_WRITE_SKEW)
// ======== IAT - 3 =========
ENUM_MEMBER(AnomalyType, IAT_STEP)
// ======== Unknown =========
ENUM_MEMBER(AnomalyType, UNKNOWN_1)
ENUM_MEMBER(AnomalyType, UNKNOWN_2)
ENUM_END(AnomalyType)

#endif
#endif
#endif

#ifndef ROW_PRECE_H
#define ROW_PRECE_H

#include <mutex>
#include <unordered_map>
#include <atomic>
#include <cassert>
#include <memory>
#include <iostream>
#include <vector>
#include "util.h"

namespace ttts {

#define ENUM_FILE "../unified_concurrency_control/row_prece.h"
#include "../system/extend_enum.h"

class TxnNode;

inline std::pair<OperationType, OperationType> DividePreceType(const PreceType prece)
{
    if (prece == PreceType::WR) {
        return {OperationType::W, OperationType::R};
    } else if (prece == PreceType::WCR) {
        return {OperationType::C, OperationType::R};
    } else if (prece == PreceType::WW) {
        return {OperationType::W, OperationType::W};
    } else if (prece == PreceType::WCW) {
        return {OperationType::C, OperationType::W};
    } else if (prece == PreceType::RW) {
        return {OperationType::R, OperationType::W};
    } else {
        assert(false);
        return {};
    }
}

inline PreceType MergeOperationType(const OperationType o1, const OperationType o2)
{
    if (o1 == OperationType::W && o2 == OperationType::R) {
        return PreceType::WR;
    } else if (o1 == OperationType::C && o2 == OperationType::R) {
        return PreceType::WCR;
    } else if (o1 == OperationType::W && o2 == OperationType::W) {
        return PreceType::WW;
    } else if (o1 == OperationType::C && o2 == OperationType::W) {
        return PreceType::WCW;
    } else if (o1 == OperationType::R && o2 == OperationType::W) {
        return PreceType::RW;
    } else {
        assert(false);
        return {};
    }
}

class PreceInfo {
  public:
    inline PreceInfo(std::shared_ptr<TxnNode> from_txn, std::shared_ptr<TxnNode> to_txn, const PreceType type,
            const uint64_t row_id, const uint64_t from_ver_id, const uint64_t to_ver_id);
    PreceInfo(const PreceInfo&) = default;
    PreceInfo(PreceInfo&&) = default;

    friend std::ostream& operator<<(std::ostream& os, const PreceInfo prece) {
        return os << 'T' << prece.from_txn_id() << "--" << prece.type_ << "(row=" << prece.row_id_ << ")->T" << prece.to_txn_id();
    }

    uint64_t from_txn_id() const { return from_txn_id_; }
    inline uint64_t to_txn_id() const;
    uint64_t from_ver_id() const { return from_ver_id_; }
    uint64_t to_ver_id() const { return to_ver_id_; }
    OperationType from_op_type() const { return DividePreceType(type_).first; }
    OperationType to_op_type() const { return DividePreceType(type_).second; }
    uint64_t row_id() const { return row_id_; }
    PreceType type() const { return type_; }
    std::shared_ptr<TxnNode> from_txn() const { return from_txn_.lock(); }
    std::shared_ptr<TxnNode> to_txn() const { return to_txn_; }

  private:
    const uint64_t from_txn_id_;
    const std::weak_ptr<TxnNode> from_txn_;
    const std::shared_ptr<TxnNode> to_txn_; // release condition (2) for TxnNode
    const PreceType type_;
    const uint64_t row_id_;
    const uint64_t from_ver_id_;
    const uint64_t to_ver_id_;
};

// A TxnNode can be released only when no more transactions build precedence before it. In this case, the
// transaction cannot be a part of cycle anymore.
//
// For latest read, it should satisfies:
// (1) The transaction is already finished (for latest read, no future transactions build RW precedence before
// it).
// (2) There are no transactions built precedence before it.
// We use std::shared_ptr to realize it. When the pointer expired, the two conditions are satisified.
//
// For snapshot read, it should also satisfies:
// (3) Minimum active transaction snapshot timestamp (start timestamp) > this transaction's largest write
// timestamp (commit timestamp). (no future transactions build RW precedence before it)
class TxnNode : public std::enable_shared_from_this<TxnNode>
{
  public:
    enum class State { ACTIVE, PREPARING, COMMITTED, ABORTED };

    TxnNode(const uint64_t txn_id) : txn_id_(txn_id), state_(State::ACTIVE) {}

    // We use ver_id but not the count of operation to support snapshot read.
    // [Note] Should ensure to_txn_node thread safe.
    template <PreceType TYPE>
    void AddToTxn(std::shared_ptr<TxnNode> to_txn_node, const uint64_t row_id, const uint64_t from_ver_id,
            const uint64_t to_ver_id)
    {
        const uint64_t to_txn_id = to_txn_node->txn_id();
        if (const auto& type = RealPreceType_<TYPE>(); txn_id_ != to_txn_id && type.has_value()) {
            std::lock_guard<std::mutex> l(m_);
            std::shared_ptr<PreceInfo> prece = std::make_shared<PreceInfo>(shared_from_this(), to_txn_node,
                    *type, row_id, from_ver_id, to_ver_id);
            // For read/write precedence, only record the first precedence between the two transactions
            to_preces_.try_emplace(to_txn_id, prece);
            to_txn_node->from_preces_.try_emplace(txn_id_, prece);
            // For dirty precedence, W1W2 has higher priority than W1R2 because W1R2C1 is not dirty read
            if ((type == PreceType::WR || type == PreceType::WW) &&
                (!dirty_to_prece_ || (dirty_to_prece_->type() == PreceType::WR &&
                                                type == PreceType::WW))) {
                dirty_to_prece_ = prece;
            }
        }
    }

    uint64_t txn_id() const { return txn_id_; }

    const auto& UnsafeGetToPreces() const { return to_preces_; }
    const auto& UnsafeGetFromPreces() const { return from_preces_; }
    const std::shared_ptr<PreceInfo>& UnsafeGetDirtyToTxn() const { return dirty_to_prece_; }

    std::mutex& mutex() const { return m_; }

    void Commit()
    {
        std::lock_guard<std::mutex> l(m_);
        state_ = State::COMMITTED;
        dirty_to_prece_ = nullptr;
    }

    void Abort(const bool clear_to_preces)
    {
        std::lock_guard<std::mutex> l(m_);
        state_ = State::ABORTED;
        dirty_to_prece_ = nullptr;
        if (clear_to_preces) {
            to_preces_.clear();
        }
    }

    const auto& state() const { return state_; }
    auto& state() { return state_; }

    friend std::ostream& operator<<(std::ostream& os, const TxnNode& txn)
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

    template <PreceType TYPE,
             typename = typename std::enable_if_t<TYPE != PreceType::WCW && TYPE != PreceType::WCR>>
    std::optional<PreceType> RealPreceType_() const
    {
        if constexpr (TYPE == PreceType::WW) {
            return RealPreceType_(PreceType::WW, PreceType::WCW, {});
        } else if constexpr (TYPE == PreceType::WR) {
            return RealPreceType_(PreceType::WR, PreceType::WCR, {});
        } else {
            return RealPreceType_(TYPE, TYPE, {});
        }
    }

    mutable std::mutex m_;
    uint64_t txn_id_;
    std::atomic<State> state_;
    // Key is txn_id.
    std::unordered_map<uint64_t, std::shared_ptr<PreceInfo>> to_preces_;
    // We can not use std::shared_ptr because PreceInfo has a use count to this which will cause cycle
    // reference. Key is txn_id;
    std::unordered_map<uint64_t, std::weak_ptr<PreceInfo>> from_preces_;
    // The prece may also stored in to_preces_.
    std::shared_ptr<PreceInfo> dirty_to_prece_;
};

inline uint64_t PreceInfo::to_txn_id() const { return to_txn_->txn_id(); }
inline PreceInfo::PreceInfo(std::shared_ptr<TxnNode> from_txn, std::shared_ptr<TxnNode> to_txn,
        const PreceType type, const uint64_t row_id, const uint64_t from_ver_id, const uint64_t to_ver_id)
    : from_txn_id_(from_txn->txn_id()), from_txn_(std::move(from_txn)), to_txn_(std::move(to_txn)), type_(type), row_id_(row_id),
        from_ver_id_(from_ver_id), to_ver_id_(to_ver_id) {}
// Not thread safe. Protected by RowManager<DLI_IDENTIFY>::m_
//
// A VersionInfo can be released when it will not be read anymore.
//
// For latest read, it should satisfies:
// (1) It is not the latest version.
// (2) The later version's write transaction is finished. (to prevent version revoke)
//
// For snapshot read, it should also satisfies:
// (3) Minimum active transaction snapshot timestamp (start timestamp) > the later version's write timestamp.
template <typename Data>
class VersionInfo
{
  public:
    VersionInfo(std::weak_ptr<TxnNode> w_txn, Data&& data, const uint64_t ver_id)
        : w_txn_(std::move(w_txn)), data_(std::move(data)), ver_id_(ver_id) {}
    VersionInfo(const VersionInfo&) = default;
    VersionInfo(VersionInfo&&) = default;
    ~VersionInfo() {}

    std::shared_ptr<TxnNode> w_txn() const { return w_txn_.lock(); }

    template <typename Task>
    void foreach_r_txn(Task&& task) const
    {
        for (const auto& r_txn_weak : r_txns_) {
            if (const auto r_txn = r_txn_weak.lock()) {
                task(*r_txn);
            }
        }
    }

    void add_r_txn(std::weak_ptr<TxnNode> txn) { r_txns_.emplace_back(std::move(txn)); }

    const Data& data() const { return data_; }

    uint64_t ver_id() const { return ver_id_; }

  private:
    const std::weak_ptr<TxnNode> w_txn_;
    Data const data_;
    // There may be two versions on same the row which have the same ver_id due to version revoking
    const uint64_t ver_id_;
    std::vector<std::weak_ptr<TxnNode>> r_txns_;
};

template <UniAlgs ALG, typename Data>
class RowManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_CYCLE ||
                                                      ALG == UniAlgs::UNI_DLI_IDENTIFY_MERGE>>
{
  public:
    using Txn = TxnManager<ALG, Data>;

    RowManager(const uint64_t row_id, Data init_data)
        : row_id_(row_id), cur_ver_id_(0),
          latest_version_(std::make_shared<VersionInfo<Data>>(std::weak_ptr<TxnNode>(), std::move(init_data),
                      0))
    {}

    std::optional<Data> Read(Txn& txn)
    {
        std::lock_guard<std::mutex> l(m_);
        const uint64_t to_ver_id = latest_version_->ver_id();
        build_prece_from_w_txn_<PreceType::WR>(*latest_version_, txn, to_ver_id);
        latest_version_->add_r_txn(txn.node_);
        return latest_version_->data();
    }

    bool Prewrite(Data data, Txn& txn)
    {
        std::lock_guard<std::mutex> l(m_);
        const uint64_t to_ver_id = (++cur_ver_id_);
        const auto pre_version = std::exchange(latest_version_,
                std::make_shared<VersionInfo<Data>>(txn.node_, std::move(data),
                    to_ver_id));
        build_prece_from_w_txn_<PreceType::WW>(*pre_version, txn, to_ver_id);
        build_preces_from_r_txns_<PreceType::RW>(*pre_version, txn, to_ver_id);
        // If transaction writes multiple versions for the same row, only record the first pre_version
        txn.pre_versions_.emplace(row_id_, std::move(pre_version));
        return true;
    }

    void Write(Data data, Txn& txn)
    {
        // do nothing
    }

    void Revoke(Data data, Txn& txn)
    {
        std::lock_guard<std::mutex> l(m_);
        const auto it = txn.pre_versions_.find(row_id_);
        assert(it != txn.pre_versions_.end());
        latest_version_ = it->second; // revoke version
    }

  private:
    template <PreceType TYPE>
    void build_prece_from_w_txn_(VersionInfo<Data>& version, const Txn& to_txn,
            const uint64_t to_ver_id) const
    {
        if (const std::shared_ptr<TxnNode> from_txn = version.w_txn()) {
            build_prece<TYPE>(*from_txn, to_txn, version.ver_id(), to_ver_id);
        }
    }

    template <PreceType TYPE>
    void build_preces_from_r_txns_(VersionInfo<Data>& version, const Txn& to_txn,
            const uint64_t to_ver_id) const
    {
        version.foreach_r_txn([&to_txn, to_ver_id, from_ver_id = version.ver_id(), this](TxnNode& from_txn) {
            build_prece<TYPE>(from_txn, to_txn, from_ver_id, to_ver_id);
        });
    }

    template <PreceType TYPE>
    void build_prece(TxnNode& from_txn, const Txn& to_txn, const uint64_t from_ver_id,
            const uint64_t to_ver_id) const
    {
        from_txn.AddToTxn<TYPE>(to_txn.node_, row_id_, from_ver_id, to_ver_id);
    }

  private:
    const uint64_t row_id_;
    std::mutex m_;
    uint64_t cur_ver_id_;
    //std::map<uint64_t, std::shared_ptr<VersionInfo>> versions_; // key is write timestamp (snapshot read)
    std::shared_ptr<VersionInfo<Data>> latest_version_;
};

}

#endif
