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

#include <vector>
#include <iostream>
#include <string>
#include <sstream>
#include <iterator>
#include <algorithm>
#include "operation_type.h"
#include "../../../src/3ts/backend/cca/anomaly_type.h"
#include "../../../src/3ts/backend/cca/prece_type.h"

#define CYCLE_ORDER 1

namespace ttts {

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

template <typename Txn>
class PreceInfo {
  public:
    inline PreceInfo(std::shared_ptr<Txn> from_txn, std::shared_ptr<Txn> to_txn, const PreceType type,
            const uint64_t row_id, const uint64_t from_ver_id, const uint64_t to_ver_id)
      : from_txn_id_(from_txn->txn_id()), from_txn_(std::move(from_txn)), to_txn_(std::move(to_txn)), type_(type), row_id_(row_id),
          from_ver_id_(from_ver_id), to_ver_id_(to_ver_id) {}
    PreceInfo(const PreceInfo&) = default;
    PreceInfo(PreceInfo&&) = default;

    friend std::ostream& operator<<(std::ostream& os, const PreceInfo prece) {
        return os << 'T' << prece.from_txn_id() << "--" << prece.type_ << "(row=" << prece.row_id_ << ")->T" << prece.to_txn_id();
    }

    uint64_t from_txn_id() const { return from_txn_id_; }
    inline uint64_t to_txn_id() const { return to_txn_->txn_id(); }
    uint64_t from_ver_id() const { return from_ver_id_; }
    uint64_t to_ver_id() const { return to_ver_id_; }
    OperationType from_op_type() const { return DividePreceType(type_).first; }
    OperationType to_op_type() const { return DividePreceType(type_).second; }
    uint64_t row_id() const { return row_id_; }
    PreceType type() const { return type_; }
    std::shared_ptr<Txn> from_txn() const { return from_txn_.lock(); }
    std::shared_ptr<Txn> to_txn() const { return to_txn_; }

  private:
    const uint64_t from_txn_id_;
    const std::weak_ptr<Txn> from_txn_;
    const std::shared_ptr<Txn> to_txn_; // release condition (2) for TxnManager
    const PreceType type_;
    const uint64_t row_id_;
    const uint64_t from_ver_id_;
    const uint64_t to_ver_id_;
};

template <typename Txn>
class Path {
 public:
    inline Path();
    inline Path(std::vector<PreceInfo<Txn>>&& preces);
    inline Path(const PreceInfo<Txn>& prece);
    inline Path(PreceInfo<Txn>&& prece);

    Path(const Path&) = default;
    Path(Path&&) = default;
    Path& operator=(const Path&) = default;
    Path& operator=(Path&&) = default;

    inline bool operator<(const Path& p) const;
    inline Path& operator+=(const Path& p);
    inline Path operator+(const Path& p) const;
    friend std::ostream& operator<<(std::ostream& os, const Path<Txn>& path) {
      if (path.preces_.empty()) {
          os << "Empty path";
      } else {
          std::copy(path.preces_.begin(), path.preces_.end(), std::ostream_iterator<PreceInfo<Txn>>(os, ", "));
      }
      return os;
    }

    inline std::string ToString() const;
    inline bool Passable() const { return !preces_.empty(); }
    inline const std::vector<PreceInfo<Txn>>& Preces() const { return preces_; }
    bool IsCycle() const {
        return preces_.size() >= 2 && preces_.front().from_txn_id() == preces_.back().to_txn_id();
    }

  private:
    std::vector<PreceInfo<Txn>> preces_;
};

template <typename Txn>
static Path<Txn> DirtyPath_(const PreceInfo<Txn>& rw_prece, Txn& txn_to_finish, const PreceType type) {
    PreceInfo<Txn> dirty_prece(rw_prece.to_txn(), txn_to_finish.shared_from_this(), type, rw_prece.row_id(),
            rw_prece.to_ver_id(), UINT64_MAX);
    return Path<Txn>(std::vector<PreceInfo<Txn>>{rw_prece, std::move(dirty_prece)});
}

template <typename Txn, bool IS_COMMIT>
inline Path<Txn> DirtyCycle(Txn& txn_to_finish) {
    std::lock_guard<std::mutex> l(txn_to_finish.mutex());
    const auto& preces = txn_to_finish.UnsafeGetDirtyToPreces();
    for (const auto& prece : preces) {
        if (!prece || prece->to_txn()->state() == Txn::State::ABORTED) {
            // do nothing and continue
        } else if (prece->type() == PreceType::WW) {
            return DirtyPath_(*prece, txn_to_finish, IS_COMMIT ? PreceType::WC : PreceType::WA);
        } else if (prece->type() == PreceType::WR) {
            if (!IS_COMMIT) {
                return DirtyPath_(*prece, txn_to_finish, PreceType::RA);
            }
        } else {
            assert(false);
            return {};
        }
    }
    return {}; // no dirty cycle
}

template <typename Txn>
Path<Txn>::Path() {}

template <typename Txn>
Path<Txn>::Path(std::vector<PreceInfo<Txn>>&& preces) : preces_(
#if CYCLE_ORDER
    std::move(preces))
#else
    (sort(preces, std::greater<PreceInfo<Txn>>()), std::move(preces)))
#endif
{}

template <typename Txn>
Path<Txn>::Path(const PreceInfo<Txn>& prece) : preces_{prece} {}

template <typename Txn>
Path<Txn>::Path(PreceInfo<Txn>&& prece) : preces_{std::move(prece)} {}

template <typename Txn>
bool Path<Txn>::operator<(const Path<Txn>& p) const {
    // imPassable has the greatest weight
    if (!Passable()) {
        return false;
    }
    if (!p.Passable()) {
        return true;
    }
#if CYCLE_ORDER
    return preces_.size() < p.preces_.size();
#else
    return std::lexicographical_compare(preces_.begin(), preces_.end(), p.preces_.begin(), p.preces_.end());
#endif
}

template <typename Txn>
Path<Txn>& Path<Txn>::operator+=(const Path<Txn>& p) {
    if (!Passable() || !p.Passable()) {
        preces_.clear();
        return *this;
    }
#if CYCLE_ORDER
    const auto& current_back = preces_.back();
    const auto& append_front = p.preces_.front();
    assert(current_back.to_txn_id() == append_front.from_txn_id());

    const auto cat_preces = [&](auto&& begin) {
        for (auto it = begin; it != p.preces_.end(); ++it) {
            preces_.emplace_back(*it);
        }
    };

    const uint64_t new_from_ver_id = current_back.from_ver_id();
    const uint64_t new_to_ver_id = append_front.to_ver_id();
    const OperationType new_from_op_type = current_back.from_op_type();
    const OperationType new_to_op_type = append_front.to_op_type();
    bool merged = false;
    if (current_back.row_id() == append_front.row_id()) {
        if (new_from_ver_id >= new_to_ver_id) {
            // it breaks the operation order on the same row
            preces_.clear();
            return *this;
        }
        // merge two precedence on the same row
        if (current_back.from_txn_id() != append_front.to_txn_id() &&
                (new_from_op_type != OperationType::R || new_to_op_type != OperationType::R)) {
            preces_.pop_back();
            const auto new_prece_type = MergeOperationType(new_from_op_type, new_to_op_type);
            preces_.emplace_back(current_back.from_txn(), append_front.to_txn(), new_prece_type,
                current_back.row_id(), new_from_ver_id, new_to_ver_id);
            // the first precedence in p.prece has already merged
            cat_preces(std::next(p.preces_.begin()));
            merged = true;
        }
    }
    if (!merged) {
        cat_preces(p.preces_.begin());
    }
#else
    std::vector<PreceInfo<Txn>> preces;
    std::merge(preces_.begin(), preces_.end(), p.preces_.begin(), p.preces_.end(), std::back_inserter(preces), std::greater<PreceInfo<Txn>>());
#endif
    return *this;
}

template <typename Txn>
Path<Txn> Path<Txn>::operator+(const Path<Txn>& p) const {
    return Path<Txn>(*this) += p;
}

template <typename Txn>
std::string Path<Txn>::ToString() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

// require type1 precedence happens before type2 precedence
[[maybe_unused]] static AnomalyType IdentifyAnomalySingle(const PreceType early_type, const PreceType later_type) {
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

[[maybe_unused]] static AnomalyType IdentifyAnomalyDouble(const PreceType early_type, const PreceType later_type) {
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

template <typename Txn>
[[maybe_unused]] static AnomalyType IdentifyAnomalyMultiple(const std::vector<PreceInfo<Txn>>& preces) {
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo<Txn>& prece) { return prece.type() == PreceType::WW; })) {
        return AnomalyType::WAT_STEP;
    }
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo<Txn>& prece) { return prece.type() == PreceType::WR || prece.type() == PreceType::WCR; })) {
        return AnomalyType::RAT_STEP;
    }
    return AnomalyType::IAT_STEP;
}

}
