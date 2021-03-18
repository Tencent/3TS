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
    version_ = VersionInfo({}, orig_row);
}

RC RowManager<DLI_IDENTIFY>::access(TxnManager* const txn, const TsType type, row_t* const ver_row) {
    assert((type == R_REQ) ^ (ver_row != nullptr));
    std::lock_guard<std::mutex> l(m_);
    if (type == R_REQ) {
        read_(txn);
    } else if (type == P_REQ) {
        prewrite_(ver_row, txn);
    } else if (type == W_REQ) {
        write_(ver_row, txn);
    } else if (type == XP_REQ) {
        revoke_(ver_row, txn);
    } else {
        assert(false);
    }
    return RCOK;
}

void RowManager<DLI_IDENTIFY>::read_(TxnManager* txn) {
    build_prece_from_w_txn_<PreceType::WR>(*txn);
    txn->cur_row->copy(version_.ver_row()); // txn->cur_row need be initted
    version_.add_r_txn(txn->dli_identify_man_.node());
}

void RowManager<DLI_IDENTIFY>::prewrite_(row_t* const ver_row, TxnManager* const txn) {
    build_prece_from_w_txn_<PreceType::WW>(*txn);
    build_preces_from_r_txns_<PreceType::RW>(*txn);
    version_ = VersionInfo(txn->dli_identify_man_.node(), ver_row);
}

void RowManager<DLI_IDENTIFY>::write_(row_t* const ver_row, TxnManager* const txn) {
    // not WC precedence here because the cycle with WC has been checked in validation
}

void RowManager<DLI_IDENTIFY>::revoke_(row_t* const ver_row, TxnManager* const txn) {
    // not WA/RA precedence here because the cycle with WA/RA has been checked in validation
    // TODO: keep remove versions until meet the version written by itself
}

uint64_t RowManager<DLI_IDENTIFY>::row_id_() const { return orig_row_->get_row_id(); }

template <PreceType TYPE>
void RowManager<DLI_IDENTIFY>::build_prece_from_w_txn_(const TxnManager& txn) {
    if (const std::shared_ptr<TxnNode> from_txn = version_.w_txn()) {
        build_prece<TYPE>(*from_txn, txn);
    }
}

template <PreceType TYPE>
void RowManager<DLI_IDENTIFY>::build_preces_from_r_txns_(const TxnManager& txn) {
    version_.foreach_r_txn([&txn, this](TxnNode& from_txn) { build_prece<TYPE>(from_txn, txn); });
}

template <PreceType TYPE>
void RowManager<DLI_IDENTIFY>::build_prece(TxnNode& from_txn, const TxnManager& txn) {
    from_txn.AddToTxn<TYPE>(txn.get_txn_id(), txn.dli_identify_man_.node(), row_id_());
}

/*
    // direct precedence
    template <PreceType TYPE>
    void AddToTxn(const uint64_t to_txn_id, const std::shared_ptr<TxnNode>& to_txn_node,
            const uint64_t row_id) {
        if (const auto& type = RealPreceType_<TYPE>(); txn_id_ != to_txn_id && type.has_value()) {
            AddToTxn(to_txn_id, to_txn_node, Path(PreceInfo(to_txn_node, *type, row_id));
        }
    }

    void AddToTxn(const uint64_t to_txn_id, const std::shared_ptr<TxnNode>& to_txn_node,
            const Path& path) {
        if (const auto [it, ok] = to_txns_.try_emplace(to_txn_id, path); ok) {
            to_txn_node->from_txns_.emplace_back(shared_from_this());
        } else if (it->second < path) {
            it->second = path;
        } else {
            return; // path is not updated, not do recursively update
        }
        for (const auto& from_txn_weak : from_txns_) {
            if (const auto from_txn = from_txns_.lock()) {
                from_txn->AddToTxn(to_txn_id, to_txn_node);
            }
        }
    }
*/
