/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: williamcliu@tencent.com
 *
 */
#pragma once
#include <set>

#include "../occ_algorithm.h"

namespace ttts {
namespace occ_algorithm {
struct Snapshot {
  uint64_t t_min;
  uint64_t t_max;
  uint64_t t_id;
  std::set<uint64_t> t_active_id;
};

template <typename TransDesc, template <typename, typename> class EnvDesc, typename AnomalyType>
class SITransactionDesc : public TransactionDescBase<TransDesc, EnvDesc, AnomalyType> {
 public:
  using env_desc_type = EnvDesc<TransDesc, AnomalyType>;
  SITransactionDesc(const uint64_t trans_id, const uint64_t start_ts, Snapshot&& snapshot, env_desc_type& env_desc)
      : TransactionDescBase<TransDesc, EnvDesc, AnomalyType>(trans_id, env_desc),
        start_ts_(start_ts),
        commit_ts_(0),
        back_check_(false),
        snapshot_(snapshot) {}
  virtual std::optional<AnomalyType> CheckConflict(const uint64_t commit_ts) = 0;
  virtual std::optional<AnomalyType> Commit(const uint64_t commit_ts) {
    commit_ts_ = commit_ts;
    THROW_ANOMALY(CheckConflict(commit_ts));
    TransactionDescBase<TransDesc, EnvDesc, AnomalyType>::Commit();
    return {};
  }
  const Snapshot& snapshot() const { return snapshot_; }
  uint64_t start_ts() const { return start_ts_; }
  uint64_t commit_ts() const { return commit_ts_; }
  void set_back_check() {
    back_check_ = true;
  };
  bool back_check() { return back_check_; }

 private:
  const uint64_t start_ts_;
  uint64_t commit_ts_;
  bool back_check_;  // used by focc, active trans r_set is not complete. so we re-check it later by
  const Snapshot snapshot_;
};

template <typename TransDesc, typename AnomalyType>
class SIEnvironmentDesc : public EnvironmentBase<TransDesc, AnomalyType> {
  static_assert(std::is_base_of_v<SITransactionDesc<TransDesc, SIEnvironmentDesc, AnomalyType>, TransDesc>,
                "TransDesc with SIEnvironmentDesc should base of SITransactionDesc");

 public:
  using EnvironmentBase<TransDesc, AnomalyType>::item_vers_;
  using EnvironmentBase<TransDesc, AnomalyType>::history_;
  using EnvironmentBase<TransDesc, AnomalyType>::os_;

 public:
  using trans_desc_type = TransDesc;

  SIEnvironmentDesc(const History& history, std::ostream* const os)
      : EnvironmentBase<TransDesc, AnomalyType>(history, os), trans_id_cnt_(1), act_cnt_(1) {}

  Snapshot valueSnapShot(const uint64_t trans_id) const {
    Snapshot res;
    res.t_min = active_trans_.empty() ? 0 : active_trans_.begin()->first;
    res.t_max = commit_trans_.empty() ? 0 : commit_trans_.rbegin()->first + 1;
    res.t_id = trans_id;
    for (const auto& i : active_trans_) {
      res.t_active_id.insert(i.first);
    }
    return res;
  }
  uint64_t valueRealTransId(const uint64_t trans_id) {
    if (!real_tran_id_.count(trans_id)) {
      real_tran_id_[trans_id] = trans_id_cnt_;
      ++trans_id_cnt_;
    }
    return real_tran_id_[trans_id];
  }
  std::vector<int> DoCheck() {
    // initial first version.
    // add by ym: In SI we init first version here. And in RC, we init it at construction.
    const uint64_t id = valueRealTransId(history_.trans_num() + 1);
    commit_trans_[id] = std::make_unique<TransDesc>(id, act_cnt_, valueSnapShot(id), *this);
    for (uint64_t item_id = 0; item_id < history_.item_num(); ++item_id) {
      this->CommitVersion(item_id, commit_trans_[id].get());
      ++act_cnt_;
    }
    commit_trans_[id]->Commit(act_cnt_);
    ++act_cnt_;
    // uint32_t anomaly_count = 0;
    std::vector<int> ret_anomally;
    for (const Operation& op : history_.operations()) {
      const uint64_t real_id = valueRealTransId(op.trans_id());
      if (abort_trans_.count(real_id)) continue;
      if (active_trans_.count(real_id) == 0) {
        active_trans_[real_id] = std::make_unique<TransDesc>(real_id, act_cnt_, valueSnapShot(real_id), *this);
      }
      auto& trans = *active_trans_[real_id];
      switch (op.type()) {
        case Operation::Type::READ: {
          trans.Read(op.item_id());
          break;
        }
        case Operation::Type::WRITE: {
          trans.Write(op.item_id());
          break;
        }
        case Operation::Type::SCAN_ODD: {
          for (uint64_t item_id = 0; item_id < item_vers_.size(); ++item_id) {
            trans.Read(item_id, [](const uint64_t version) { return version % 2 == 0; });
          }
          break;
        }
        case Operation::Type::COMMIT: {
          if (std::optional<AnomalyType> a = trans.Commit(act_cnt_); a.has_value()) {
            ret_anomally.push_back(a.value());
            TRY_LOG(os_) << a.value() << " " << op.trans_id() << std::endl;
            //++anomaly_count;
            trans.Abort();
            abort_trans_[real_id] = std::move(active_trans_[real_id]);
            active_trans_.erase(real_id);
          } else {
            commit_trans_[real_id] = std::move(active_trans_[real_id]);
            active_trans_.erase(real_id);
          }
          break;
        }
        case Operation::Type::ABORT: {
          trans.Abort();
          abort_trans_[real_id] = std::move(active_trans_[real_id]);
          active_trans_.erase(real_id);
          break;
        }
        default:
          assert(false);
      }
      ++act_cnt_;
    }
    return ret_anomally;
  }

  virtual uint64_t GetVisiableVersion(const uint64_t item_id, TransDesc* const r_trans) {
    for (auto it = item_vers_[item_id].rbegin(); it != item_vers_[item_id].rend(); ++it) {
      if (TupleSatisfiesMVCC(*it, r_trans->snapshot())) {
        return (*it)->version_;
      }
    }
    assert(false);
    return 0;
  }

  std::optional<uint64_t> GetLatestCommitTime(const uint64_t item_id) const {
    const auto it = latest_commit_time_.find(item_id);
    return it == latest_commit_time_.end() ? std::optional<uint64_t>() : std::optional<uint64_t>(it->second);
  }
  void UpdateCommitTime(const uint64_t item_id, uint64_t ts) { latest_commit_time_[item_id] = ts; }
  static bool TupleSatisfiesMVCC(std::unique_ptr<ItemVersionDesc<TransDesc>>& tuple, const Snapshot& snapshot) {
    if (tuple->w_trans_->is_aborted()) {
      return false;
    }
    if (tuple->w_trans_->is_committed()) {
      if (tuple->w_trans_->trans_id() < snapshot.t_min) return true;
      if (tuple->w_trans_->trans_id() >= snapshot.t_max) return false;
      return snapshot.t_active_id.count(tuple->w_trans_->trans_id()) == 0;
    }
    return tuple->w_trans_->trans_id() == snapshot.t_id;
  }

 public:
  std::map<uint64_t, std::unique_ptr<TransDesc>> active_trans_;
  std::map<uint64_t, std::unique_ptr<TransDesc>> commit_trans_;
  std::map<uint64_t, std::unique_ptr<TransDesc>> abort_trans_;

 private:
  uint64_t trans_id_cnt_;
  uint64_t act_cnt_;
  std::map<uint64_t, uint64_t> real_tran_id_;  // not assume that trans ids are incremental
  std::map<uint64_t, uint64_t> latest_commit_time_;
};
}  // namespace occ_algorithm
}  // namespace ttts
