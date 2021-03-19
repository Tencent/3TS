#include "concurrency_control/dli_identify.h"
#include "concurrency_control/row_prece.h"
#include "system/txn.h"

static Path dirty_path(const PreceInfo& rw_prece, TxnNode& txn_to_finish, const PreceType type) {
    PreceInfo dirty_prece(rw_prece.to_txn_id(), txn_to_finish.shared_from_this(), type, rw_prece.row_id(),
            rw_prece.to_ver_id(), UINT64_MAX);
    return Path(std::vector<PreceInfo>{rw_prece, std::move(dirty_prece)});
}

template <>
Path AlgManager<DLI_IDENTIFY>::dirty_cycle_<true>(TxnNode& txn_to_finish) const {
    std::lock_guard<std::mutex> l(txn_to_finish.mutex());
    const auto& prece = txn_to_finish.UnsafeGetDirtyToTxn();
    if (!prece.has_value()) {
        return {};
    } else if (prece->type() == PreceType::WW) {
        return dirty_path(*prece, txn_to_finish, PreceType::WC);
    } else {
        assert(prece->type() == PreceType::WR);
        return {};
    }
}

template <>
Path AlgManager<DLI_IDENTIFY>::dirty_cycle_<false>(TxnNode& txn_to_finish) const {
    std::lock_guard<std::mutex> l(txn_to_finish.mutex());
    const auto& prece = txn_to_finish.UnsafeGetDirtyToTxn();
    if (!prece.has_value()) {
        return {};
    } else if (prece->type() == PreceType::WW) {
        return dirty_path(*prece, txn_to_finish, PreceType::WA);
    } else if (prece->type() == PreceType::WR) {
        return dirty_path(*prece, txn_to_finish, PreceType::RA);
    } else {
        assert(false);
        return {};
    }
}

std::vector<std::vector<Path>> AlgManager<DLI_IDENTIFY>::init_path_matrix_(
        const std::vector<std::shared_ptr<TxnNode>>& concurrent_txns) const {
    const uint64_t txn_num = concurrent_txns.size();
    std::vector<std::vector<Path>> matrix(txn_num);
    for (uint64_t from_idx = 0; from_idx < txn_num; ++from_idx) {
        matrix[from_idx].resize(txn_num);
        TxnNode& from_txn_node = *concurrent_txns[from_idx];
        std::lock_guard<std::mutex> l(from_txn_node.mutex());
        const auto& to_txns = from_txn_node.UnsafeGetToTxns();
        for (uint64_t to_idx = 0; to_idx < txn_num; ++to_idx) {
            const uint64_t to_txn_id = concurrent_txns[to_idx]->txn_id();
            if (const auto it = to_txns.find(to_txn_id); it != to_txns.end()) {
                matrix[from_idx][to_idx] = it->second;
            }
        }
    }
    return matrix;
}

static void update_path(Path& path, Path&& new_path) {
    if (new_path < path) {
        path = std::move(new_path); // do not use std::min because there is a copy cost when assign self
    }
};

void AlgManager<DLI_IDENTIFY>::update_path_matrix_(std::vector<std::vector<Path>>& matrix) const {
    const uint64_t txn_num = matrix.size();
    for (uint64_t mid = 0; mid < txn_num; ++mid) {
        for (uint64_t start = 0; start < txn_num; ++start) {
            for (uint64_t end = 0; end < txn_num; ++end) {
                update_path(matrix[start][end], matrix[start][mid] + matrix[mid][end]);
            }
        }
    }
}

Path AlgManager<DLI_IDENTIFY>::min_cycle_(std::vector<std::vector<Path>>& matrix, const uint64_t this_idx) const {
    const uint64_t txn_num = matrix.size();
    Path min_cycle;
    for (uint64_t start = 0; start < txn_num; ++start) {
        if (start == this_idx) {
            continue;
        }
        update_path(min_cycle, matrix[start][this_idx] + matrix[this_idx][start]);
        assert(!min_cycle.passable() || min_cycle.is_cycle());
        for (uint64_t end = 0; end < txn_num; ++end) {
            if (end != this_idx && start != end) {
                update_path(min_cycle, matrix[start][end] + matrix[end][this_idx] + matrix[this_idx][start]);
                assert(!min_cycle.passable() || min_cycle.is_cycle());
            }
        }
    }
    return min_cycle;
}
void AlgManager<DLI_IDENTIFY>::init() {}

RC AlgManager<DLI_IDENTIFY>::validate(TxnManager* txn)
{
    //txn->dli_identify_man_.lock(m_); // release after build WC, WA or RA precedence
    const auto concurrent_txns = [this, txn] {
        std::lock_guard<std::mutex> l(m_);
        txns_.emplace_back(txn->dli_identify_man_.node());
        return refresh_and_lock_txns_();
    }();
    const auto txn_num = concurrent_txns.size();
    const auto this_idx = txn_num - 1; // this txn node is just emplaced to the end of txns_

    auto matrix = init_path_matrix_(concurrent_txns);
    update_path_matrix_(matrix);

    Path cycle = min_cycle_(matrix, this_idx);

    if (!cycle.passable()) {
        cycle = dirty_cycle_<true /*is_commit*/>(*txn->dli_identify_man_.node());
    }

    if (!cycle.passable()) {
        return RCOK;
    } else {
        txn->dli_identify_man_.set_cycle(std::move(cycle));
        return Abort;
    }
}

void AlgManager<DLI_IDENTIFY>::finish(RC rc, TxnManager* txn)
{
    if (rc == RCOK) {
        txn->dli_identify_man_.node()->commit();
        return;
    }
    assert(rc == Abort);
    if (!txn->dli_identify_man_.cycle()) {
        // we can only identify the dirty write/read anomaly rather than avoiding it
        Path cycle = dirty_cycle_<false /*is_commit*/>(*txn->dli_identify_man_.node());
        txn->dli_identify_man_.set_cycle(std::move(cycle));
    }
    if (const std::unique_ptr<Path>& cycle = txn->dli_identify_man_.cycle()) {
#if WORKLOAD == DA
        g_da_cycle_info = cycle->to_string();
#endif
        txn->dli_identify_man_.node()->abort(true /*clear_to_txns*/); // break cycles to prevent memory leak
    } else {
        txn->dli_identify_man_.node()->abort(false /*clear_to_txns*/);
    }
}

std::vector<std::shared_ptr<TxnNode>> AlgManager<DLI_IDENTIFY>::refresh_and_lock_txns_() {
    std::vector<std::shared_ptr<TxnNode>> txns;
    for (auto it = txns_.begin(); it != txns_.end(); ) {
        if (auto txn = it->lock()) {
            txns.emplace_back(std::move(txn));
            ++it;
        } else {
            it = txns_.erase(it);
        }
    }
    return txns;
}


