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

#include "../util/dli_identify_util.h"
#include "../row/row_prece.h"
#include <mutex>
#include <type_traits>

namespace ttts {

template <UniAlgs ALG, typename Data>
class AlgManager<ALG, Data, typename std::enable_if_t<ALG == UniAlgs::UNI_DLI_IDENTIFY_CHAIN>> {
 public:
  using Txn = TxnManager<ALG, Data>;

  bool Validate(Txn& txn) {
    txn.state() = Txn::State::PREPARING;

    {
      std::scoped_lock l(m_);
      cc_txns_.emplace_back(txn.shared_from_this());
    }

    Path<Txn> cycle_part = DirtyCycle<Txn, true /*is_commit*/>(txn);

    if (!cycle_part.Passable()) {
      cycle_part = CyclePart_(txn);
    }

    if (!cycle_part.Passable()) {
      return true;
    } else {
      txn.cycle_ = std::make_unique<Path<Txn>>(std::move(cycle_part));
      return false;
    }
  }

  void Commit(Txn& txn, const uint64_t /*commit_ts*/) { txn.Commit(); }

  void Abort(Txn& txn) {
    if (!txn.cycle_) {
      // we can only identify the dirty write/read anomaly rather than avoiding it
      Path<Txn> cycle = DirtyCycle<Txn, false /*is_commit*/>(txn);
      if (cycle.Passable()) {
        txn.cycle_ = std::make_unique<Path<Txn>>(std::move(cycle));
      }
    }
    if (const std::unique_ptr<Path<Txn>>& cycle = txn.cycle_) {
      txn.Abort(true /*clear_to_txns*/);  // break cycles to prevent memory leak
    } else {
      txn.Abort(false /*clear_to_txns*/);
    }
  }

  void CheckConcurrencyTxnEmpty() {
    std::scoped_lock l(m_);
    bool is_empty = true;
    for (const auto& weak_txn : cc_txns_) {
      if (const auto txn = weak_txn.lock()) {
        std::cerr << "** Txn Leak ** " << *txn;
        is_empty = false;
      }
    }
    // assert failed here means there is actually a cycle but we miss it
    assert(is_empty);
    cc_txns_.clear();
  }

  static AnomalyType IdentifyAnomaly(const std::vector<PreceInfo<Txn>>& preces) {
    assert(preces.size() == 2);
    const auto& p1 = preces.front();
    const auto& p2 = preces.back();
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo<Txn>& prece) {
          return prece.type() == PreceType::WA || prece.type() == PreceType::WC;
        })) {
      return AnomalyType::WAT_1_DIRTY_WRITE;
    } else if (std::any_of(preces.begin(), preces.end(),
                           [](const PreceInfo<Txn>& prece) { return prece.type() == PreceType::RA; })) {
      return AnomalyType::RAT_1_DIRTY_READ;
    } else if (p1.from_txn_id() != p2.to_txn_id()) {
      return IdentifyAnomalyMultiple(preces);
      // [Note] When build path, later happened precedence is sorted to back, which is DIFFERENT from 3TS-DA
    } else if (p1.row_id() != p2.row_id()) {
      return IdentifyAnomalyDouble(p1.type(), p2.type());
    } else if (p1.to_ver_id() < p2.to_ver_id() ||
               (p1.to_ver_id() == p2.to_ver_id() && p1.from_ver_id() < p2.from_ver_id())) {
      return IdentifyAnomalySingle(p1.type(), p2.type());
    } else {
      return IdentifyAnomalySingle(p2.type(), p1.type());
    }
  }

 private:
  Path<Txn> CyclePart_(Txn& txn) {
    // Validate failed if has a from_txn and a to_txn which are both finished but not released.
    std::scoped_lock l(txn.mutex());
    std::shared_ptr<PreceInfo<Txn>> from_prece;
    for (const auto & [ _, weak_from_prece ] : txn.UnsafeGetFromPreces()) {
      from_prece = weak_from_prece.lock();
      if (from_prece == nullptr) {
        continue;
      }
      const auto& from_txn = from_prece->from_txn();
      if (from_txn == nullptr || from_txn->state() == Txn::State::ACTIVE) {
        from_prece = nullptr;
        continue;
      }
      break;
    }

    std::shared_ptr<PreceInfo<Txn>> to_prece;
    for (const auto & [ _, to_prece_tmp ] : txn.UnsafeGetToPreces()) {
      if (to_prece_tmp->to_txn()->state() != Txn::State::ACTIVE) {
        to_prece = to_prece_tmp;
        break;
      }
    }

    if (from_prece && to_prece) {
      return Path<Txn>(std::vector<PreceInfo<Txn>>{*from_prece, *to_prece});
    }
    return {};
  }

  std::mutex m_;
  std::vector<std::weak_ptr<Txn>> cc_txns_;  // only for debug
};
}
