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

class WSITransactionDesc : public SITransactionDesc<WSITransactionDesc, SIEnvironmentDesc, Anomally> {
 public:
  WSITransactionDesc(const uint64_t trans_id, const uint64_t start_ts, Snapshot&& snapshot, env_desc_type& env_desc)
      : SITransactionDesc<WSITransactionDesc, SIEnvironmentDesc, Anomally>(trans_id, start_ts, std::move(snapshot),
                                                                           env_desc) {}
  static std::string name;
  std::optional<Anomally> CheckConflict(uint64_t commit_ts) override {
    if (!w_items_.empty())
      for (const auto& i : r_items_) {
        const auto tmp = env_desc_.GetLatestCommitTime(i.first);
        if (tmp.has_value() && tmp.value() <= commit_ts && tmp.value() >= start_ts()) {
          return std::optional<Anomally>(Anomally::RW_CONFLICT);
        }
      }
    for (const auto& i : w_items_) {
      env_desc_.UpdateCommitTime(i.first, commit_ts);
    }
    return {};
  }
};

std::string WSITransactionDesc::name = "WSI";
}  // namespace occ_algorithm
}  // namespace ttts
