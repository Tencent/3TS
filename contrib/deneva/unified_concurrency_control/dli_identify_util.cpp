#include "dli_identify_util.h"
#include <sstream>
#include <iterator>

#define CYCLE_ORDER 1

namespace ttts {

static Path DirtyPath_(const PreceInfo& rw_prece, TxnNode& txn_to_finish, const PreceType type) {
    PreceInfo dirty_prece(rw_prece.to_txn_id(), txn_to_finish.shared_from_this(), type, rw_prece.row_id(),
            rw_prece.to_ver_id(), UINT64_MAX);
    return Path(std::vector<PreceInfo>{rw_prece, std::move(dirty_prece)});
}

template <>
Path DirtyCycle<true>(TxnNode& txn_to_finish) {
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
Path DirtyCycle<false>(TxnNode& txn_to_finish) {
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
            preces_.emplace_back(current_back.from_txn_id(), append_front.to_txn(), new_prece_type,
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

}
