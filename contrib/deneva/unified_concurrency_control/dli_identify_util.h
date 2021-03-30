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
#include "row_prece.h"
#include <vector>
#include <iostream>
#include <string>
#include <sstream>
#include <iterator>
#include <algorithm>

#define CYCLE_ORDER 1

namespace ttts {

class Path {
 public:
    inline Path();
    inline Path(std::vector<PreceInfo>&& preces);
    inline Path(const PreceInfo& prece);
    inline Path(PreceInfo&& prece);

    Path(const Path&) = default;
    Path(Path&&) = default;
    Path& operator=(const Path&) = default;
    Path& operator=(Path&&) = default;

    inline bool operator<(const Path& p) const;
    inline Path& operator+=(const Path& p);
    inline Path operator+(const Path& p) const;
    inline friend std::ostream& operator<<(std::ostream& os, const Path& path);

    inline std::string ToString() const;
    inline bool Passable() const { return !preces_.empty(); }
    inline const std::vector<PreceInfo>& Preces() const { return preces_; }
    bool IsCycle() const {
        return preces_.size() >= 2 && preces_.front().from_txn_id() == preces_.back().to_txn_id();
    }

  private:
    std::vector<PreceInfo> preces_;
};

template <bool is_commit> inline Path DirtyCycle(TxnNode& txn_node);

static Path DirtyPath_(const PreceInfo& rw_prece, TxnNode& txn_to_finish, const PreceType type) {
    PreceInfo dirty_prece(rw_prece.to_txn(), txn_to_finish.shared_from_this(), type, rw_prece.row_id(),
            rw_prece.to_ver_id(), UINT64_MAX);
    return Path(std::vector<PreceInfo>{rw_prece, std::move(dirty_prece)});
}

template <>
inline Path DirtyCycle<true>(TxnNode& txn_to_finish) {
    std::lock_guard<std::mutex> l(txn_to_finish.mutex());
    const auto& prece = txn_to_finish.UnsafeGetDirtyToTxn();
    if (!prece) {
        return {};
    } else if (prece->type() == PreceType::WW) {
        return DirtyPath_(*prece, txn_to_finish, PreceType::WC);
    } else {
        assert(prece->type() == PreceType::WR);
        return {};
    }
}

template <>
inline Path DirtyCycle<false>(TxnNode& txn_to_finish) {
    std::lock_guard<std::mutex> l(txn_to_finish.mutex());
    const auto& prece = txn_to_finish.UnsafeGetDirtyToTxn();
    if (!prece) {
        return {};
    } else if (prece->type() == PreceType::WW) {
        return DirtyPath_(*prece, txn_to_finish, PreceType::WA);
    } else if (prece->type() == PreceType::WR) {
        return DirtyPath_(*prece, txn_to_finish, PreceType::RA);
    } else {
        assert(false);
        return {};
    }
}

Path::Path() {}

Path::Path(std::vector<PreceInfo>&& preces) : preces_(
#if CYCLE_ORDER
    std::move(preces))
#else
    (sort(preces, std::greater<PreceInfo>()), std::move(preces)))
#endif
{}

Path::Path(const PreceInfo& prece) : preces_{prece} {}

Path::Path(PreceInfo&& prece) : preces_{std::move(prece)} {}

bool Path::operator<(const Path& p) const {
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

Path& Path::operator+=(const Path& p) {
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
    std::vector<PreceInfo> preces;
    std::merge(preces_.begin(), preces_.end(), p.preces_.begin(), p.preces_.end(), std::back_inserter(preces), std::greater<PreceInfo>());
#endif
    return *this;
}

Path Path::operator+(const Path& p) const {
    return Path(*this) += p;
}

std::string Path::ToString() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const Path& path) {
    if (path.preces_.empty()) {
        os << "Empty path";
    } else {
        std::copy(path.preces_.begin(), path.preces_.end(), std::ostream_iterator<PreceInfo>(os, ", "));
    }
    return os;
}

// require type1 precedence happens before type2 precedence
static AnomalyType IdentifyAnomalySingle(const PreceType early_type, const PreceType later_type) {
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

static AnomalyType IdentifyAnomalyDouble(const PreceType early_type, const PreceType later_type) {
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

static AnomalyType IdentifyAnomalyMultiple(const std::vector<PreceInfo>& preces) {
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WW; })) {
        return AnomalyType::WAT_STEP;
    }
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WR || prece.type() == PreceType::WCR; })) {
        return AnomalyType::RAT_STEP;
    }
    return AnomalyType::IAT_STEP;
}

}
