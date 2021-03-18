/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifdef ENUM_BEGIN
#ifdef ENUM_MEMBER
#ifdef ENUM_END

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
#include "../storage/row.h"

class TxnManager;

#define ENUM_FILE "concurrency_control/row_prece.h"
#include "system/extend_enum.h"

class TxnNode;

class PreceInfo {
  public:
    PreceInfo(const uint64_t from_txn_id, std::shared_ptr<TxnNode> to_txn, const PreceType type,
            const uint64_t row_id)
        : from_txn_id_(from_txn_id), to_txn_(std::move(to_txn)), type_(type), row_id_(row_id) {}
    PreceInfo(const PreceInfo&) = default;
    PreceInfo(PreceInfo&&) = default;

    friend std::ostream& operator<<(std::ostream& os, const PreceInfo prece) {
        return os << prece.type_ << "(row=" << prece.row_id_ << ")";
    }

    uint64_t from_txn_id() const { return from_txn_id_; }
    uint64_t to_txn_id() const;
    uint64_t row_id() const { return row_id_; }
    PreceType type() const { return type_; }

  private:
    const uint64_t from_txn_id_;
    const std::shared_ptr<TxnNode> to_txn_;
    const PreceType type_;
    const uint64_t row_id_;
};

// Not thread safe. Protected by alg_man<DLI_IDENTIFY>::m_
class TxnNode : public std::enable_shared_from_this<TxnNode>
{
  public:
    TxnNode(const uint64_t txn_id) : txn_id_(txn_id) {}

    template <PreceType TYPE>
    void AddToTxn(const uint64_t to_txn_id, std::shared_ptr<TxnNode> to_txn_node,
            const uint64_t row_id) {
        if (const auto& type = RealPreceType_<TYPE>(); txn_id_ != to_txn_id && type.has_value()) {
            std::lock_guard<std::mutex> l(m_);
            to_txns_.emplace(to_txn_id, PreceInfo(txn_id_, std::move(to_txn_node), *type, row_id));
        }
    }

    uint64_t txn_id() const { return txn_id(); }

    const std::unordered_map<uint64_t, PreceInfo>& UnsafeGetToTxns() const { return to_txns_; }

    std::mutex& mutex() const { return m_; }

    void commit() {
        std::lock_guard<std::mutex> l(m_);
        is_committed_ = true;
    }

    void abort(bool clear_to_txns) {
        std::lock_guard<std::mutex> l(m_);
        is_committed_ = false;
        if (clear_to_txns) {
            to_txns_.clear();
        }
    }

  private:
    const std::optional<PreceType> RealPreceType_(const std::optional<PreceType>& active_prece_type,
            const std::optional<PreceType>& committed_prece_type,
            const std::optional<PreceType>& aborted_prece_type) const {
        if (!is_committed_.has_value()) {
            return active_prece_type;
        } else if (is_committed_.value()) {
            return committed_prece_type;
        } else {
            return aborted_prece_type;
        }
    }

    template <PreceType TYPE,
             typename = typename std::enable_if_t<TYPE != PreceType::WCW && TYPE != PreceType::WCR>>
    const std::optional<PreceType> RealPreceType_() const {
        if constexpr (TYPE == PreceType::WW) {
            return RealPreceType_(PreceType::WW, PreceType::WCW, {});
        } else if constexpr (TYPE == PreceType::WR) {
            return RealPreceType_(PreceType::WR, PreceType::WCR, {});
        } else {
            return RealPreceType_(TYPE, TYPE, {});
        }
    }

    mutable std::mutex m_;
    const uint64_t txn_id_;
    std::optional<bool> is_committed_;
    std::unordered_map<uint64_t, PreceInfo> to_txns_; // key is txn_id
    //std::vector<std::weak_ptr<TxnNode>> from_txns_;
};

// Not thread safe. Protected by alg_man<DLI_IDENTIFY>::m_
class VersionInfo
{
  public:
    VersionInfo(std::weak_ptr<TxnNode> w_txn, row_t* const ver_row)
        : w_txn_(std::move(w_txn)), ver_row_(ver_row) {}
    VersionInfo(const VersionInfo&) = default;
    VersionInfo(VersionInfo&&) = default;
    VersionInfo& operator=(const VersionInfo&) = default;
    VersionInfo& operator=(VersionInfo&&) = default;
    std::shared_ptr<TxnNode> w_txn() const { return w_txn_.lock(); }
    template <typename Task>
    void foreach_r_txn(Task&& task) const {
        for (const auto& r_txn_weak : r_txns_) {
            if (const auto r_txn = r_txn_weak.lock()) {
                task(*r_txn);
            }
        }
    }
    void add_r_txn(std::weak_ptr<TxnNode> txn) { r_txns_.emplace_back(std::move(txn)); }
    row_t* ver_row() const { return ver_row_; }

  private:
    std::weak_ptr<TxnNode> w_txn_;
    row_t* ver_row_;
    std::vector<std::weak_ptr<TxnNode>> r_txns_;
};

template <int ALG> class RowManager;

template <>
class RowManager<DLI_IDENTIFY>
{
  public:
    void init(row_t* const orig_row);
    RC access(TxnManager* const txn, const TsType type, row_t* const ver_row = nullptr);

  private:
    void read_(TxnManager* txn);
    void prewrite_(row_t* const ver_row, TxnManager* const txn);
    void write_(row_t* const ver_row, TxnManager* const txn);
    void revoke_(row_t* const ver_row, TxnManager* const txn);
    uint64_t row_id_() const;
    template <PreceType TYPE> void build_prece_from_w_txn_(const TxnManager& txn);
    template <PreceType TYPE> void build_preces_from_r_txns_(const TxnManager& txn);
    template <PreceType TYPE> void build_prece(TxnNode& from_txn, const TxnManager& txn);

  private:
    row_t* orig_row_;
    std::mutex m_;
    VersionInfo version_; // TODO: use std::vector<VersionInfo> to support abort
};

#endif
