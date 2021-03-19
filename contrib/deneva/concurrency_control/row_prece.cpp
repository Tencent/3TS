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

#include "row_prece.h"

#include "mem_alloc.h"
#include "row.h"
#include "txn.h"
#include "dli_identify.h"

void AlgTxnManager<DLI_IDENTIFY>::set_cycle(Path&& cycle) {
    cycle_ = std::make_unique<Path>(std::move(cycle));
}

uint64_t PreceInfo::to_txn_id() const { return to_txn_->txn_id(); }

void RowManager<DLI_IDENTIFY>::init(row_t* const orig_row) {
    orig_row_ = orig_row;
    latest_version_ = std::make_shared<VersionInfo>(std::weak_ptr<TxnNode>(), orig_row, 0);
}

RC RowManager<DLI_IDENTIFY>::read(row_t& ver_row, TxnManager& txn) {
    std::lock_guard<std::mutex> l(m_);
    const uint64_t to_ver_id = latest_version_->ver_id();
    build_prece_from_w_txn_<PreceType::WR>(*latest_version_, txn, to_ver_id);
    latest_version_->add_r_txn(txn.dli_identify_man_.node());
    ver_row.copy(latest_version_->ver_row());
    return RCOK;
}

RC RowManager<DLI_IDENTIFY>::prewrite(row_t& ver_row, TxnManager& txn) {
    std::lock_guard<std::mutex> l(m_);
    const uint64_t to_ver_id = latest_version_->ver_id() + 1;
    std::shared_ptr<VersionInfo> pre_version = std::exchange(latest_version_,
            std::make_shared<VersionInfo>(txn.dli_identify_man_.node(), &ver_row, to_ver_id));
    build_prece_from_w_txn_<PreceType::WW>(*pre_version, txn, to_ver_id);
    build_preces_from_r_txns_<PreceType::RW>(*pre_version, txn, to_ver_id);
    // If transaction writes multiple versions for the same row, only record the first pre_version
    txn.dli_identify_man_.pre_versions_.emplace(orig_row_, std::move(pre_version));
    return RCOK;
}

// We have already written versions when prewrite, do nothing here.
void RowManager<DLI_IDENTIFY>::write(row_t& ver_row, TxnManager& txn) {}

void RowManager<DLI_IDENTIFY>::revoke(row_t& ver_row, TxnManager& txn) {
    std::lock_guard<std::mutex> l(m_);
    const auto it = txn.dli_identify_man_.pre_versions_.find(orig_row_);
    assert(it != txn.dli_identify_man_.pre_versions_.end());
    latest_version_ = it->second; // revoke version
}

uint64_t RowManager<DLI_IDENTIFY>::row_id_() const { return orig_row_->get_row_id(); }

template <PreceType TYPE>
void RowManager<DLI_IDENTIFY>::build_prece_from_w_txn_(VersionInfo& version, const TxnManager& to_txn,
        const uint64_t to_ver_id) const {
    if (const std::shared_ptr<TxnNode> from_txn = version.w_txn()) {
        build_prece<TYPE>(*from_txn, to_txn, version.ver_id(), to_ver_id);
    }
}

template <PreceType TYPE>
void RowManager<DLI_IDENTIFY>::build_preces_from_r_txns_(VersionInfo& version, const TxnManager& to_txn,
        const uint64_t to_ver_id) const {
    version.foreach_r_txn([&to_txn, to_ver_id, from_ver_id = version.ver_id(), this](TxnNode& from_txn) {
        build_prece<TYPE>(from_txn, to_txn, from_ver_id, to_ver_id);
    });
}

template <PreceType TYPE>
void RowManager<DLI_IDENTIFY>::build_prece(TxnNode& from_txn, const TxnManager& to_txn,
        const uint64_t from_ver_id, const uint64_t to_ver_id) const {
    from_txn.AddToTxn<TYPE>(to_txn.get_txn_id(), to_txn.dli_identify_man_.node(), row_id_(), from_ver_id,
            to_ver_id);
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

AnomalyType IdentifyAnomaly(const std::vector<PreceInfo>& preces) {
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

