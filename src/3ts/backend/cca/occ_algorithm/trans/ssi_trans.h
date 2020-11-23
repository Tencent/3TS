/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: moggma@tencent.com
 *
 */
#pragma once

#include "../env/si_env.h"

namespace ttts {
namespace occ_algorithm {

class SSITransactionDesc
    : public SITransactionDesc<SSITransactionDesc, SIEnvironmentDesc, Anomally> {
 public:
  SSITransactionDesc(const uint64_t trans_id, const uint64_t start_ts, Snapshot&& snapshot,
                     env_desc_type& env_desc)
      : SITransactionDesc<SSITransactionDesc, SIEnvironmentDesc, Anomally>(
            trans_id, start_ts, std::move(snapshot), env_desc) {}
  static std::string name;
  std::optional<Anomally> CheckConflict(uint64_t commit_ts) {
    std::optional<Anomally> a;
    for (const auto& i : r_items_) {
      a = ReadConflict(i.first, i.second->version_);
      if (a.has_value()) return a;
    }
    for (const auto& i : w_items_) {
      a = WriteConflict(i.first);
      if (a.has_value()) return a;
    }
    if (!in_conflict().empty() && !out_conflict().empty()) {
      return std::optional<Anomally>(Anomally::WRITE_SKEW);
    }
    for (const auto& i : w_items_) {
      const auto tmp = env_desc_.GetLatestCommitTime(i.first);
      if (tmp.has_value() && tmp.value() <= commit_ts && tmp.value() >= start_ts()) {
        return std::optional<Anomally>(Anomally::WW_CONFLICT);
      }
    }
    for (const auto& i : w_items_) {
      env_desc_.UpdateCommitTime(i.first, commit_ts);
    }
    commit_ts_ = commit_ts;
    return {};
  }

  std::optional<Anomally> ReadConflict(const uint64_t item_id, const uint64_t read_version) {
    for (uint64_t version = 0; env_desc_.HasVersion(item_id, version); ++version) {
      const auto& it = env_desc_.GetVersion(item_id, version);
      if (it.w_trans_->trans_id() == trans_id()) continue;
      if (it.w_trans_->is_running()) {
        it.w_trans_->UpInConflict(this);
        UpOutConflict(it.w_trans_);
      }
      if (version > read_version) {
        if (it.w_trans_->is_committed() && !it.w_trans_->out_conflict().empty()) {
          return std::optional<Anomally>(Anomally::WRITE_SKEW);
        }
        if (!it.w_trans_->is_aborted()) {
          it.w_trans_->UpInConflict(this);
          UpOutConflict(it.w_trans_);
        }
      }
    }
    return {};
  }
  std::optional<Anomally> WriteConflict(const uint64_t item_id) {
    for (uint64_t version = 0; env_desc_.HasVersion(item_id, version); ++version) {
      const auto& it = env_desc_.GetVersion(item_id, version);
      for (const auto& i : it.r_transs_) {
        if (i.second->trans_id() == trans_id()) continue;
        if (i.second->is_running() ||
            (i.second->is_committed() && i.second->commit_ts() > start_ts())) {
          if (i.second->is_committed() && !i.second->in_conflict().empty()) {
            return std::optional<Anomally>(Anomally::WRITE_SKEW);
          }
          i.second->UpOutConflict(this);
          UpInConflict(i.second);
        }
      }
    }
    return {};
  }

  virtual std::optional<Anomally> Abort() override {
    if (is_aborted()) return {};
    committed_ = false;
    for (const auto& i : out_conflict()) {
      i->DownInConflict(this);
    }
    for (const auto& i : in_conflict()) {
      i->DownOutConflict(this);
    }
    return {};
  }

  void UpInConflict(SSITransactionDesc* trans_id) { in_conflict_.insert(trans_id); }
  void UpOutConflict(SSITransactionDesc* trans_id) { out_conflict_.insert(trans_id); }
  void DownInConflict(SSITransactionDesc* trans_id) { in_conflict_.erase(trans_id); }
  void DownOutConflict(SSITransactionDesc* trans_id) { out_conflict_.erase(trans_id); }
  const std::set<SSITransactionDesc*>& in_conflict() const { return in_conflict_; }
  const std::set<SSITransactionDesc*>& out_conflict() const { return out_conflict_; }
  uint64_t commit_ts() const {
    assert(commit_ts_.has_value());
    return commit_ts_.value();
  }

 private:
  std::optional<uint64_t> commit_ts_;
  std::set<SSITransactionDesc*> in_conflict_;
  std::set<SSITransactionDesc*> out_conflict_;
};

std::string SSITransactionDesc::name = "SSI";
}  // namespace occ_algorithm
}  // namespace ttts
