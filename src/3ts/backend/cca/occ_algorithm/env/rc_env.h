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
#include "occ_algorithm.h"

namespace ttts {
namespace occ_algorithm {

template <typename TransDesc, template <typename, typename> class EnvDesc, typename AnomalyType>
class TransactionDesc : public TransactionDescBase<TransDesc, EnvDesc, AnomalyType> {
 public:
  using TransactionDescBase<TransDesc, EnvDesc, AnomalyType>::TransactionDescBase;
  virtual std::optional<AnomalyType> CheckConflict() = 0;
  virtual std::optional<AnomalyType> Commit() {
    THROW_ANOMALY(CheckConflict());
    TransactionDescBase<TransDesc, EnvDesc, AnomalyType>::Commit();
    return {};
  }
};

template <typename TransDesc, typename AnomalyType>
class EnvironmentDesc : public EnvironmentBase<TransDesc, AnomalyType> {
  static_assert(std::is_base_of_v<TransactionDesc<TransDesc, EnvironmentDesc, AnomalyType>, TransDesc>,
                "TransDesc with EnvironmentDesc should base of TransactionDesc");

 private:
  using EnvironmentBase<TransDesc, AnomalyType>::item_vers_;
  using EnvironmentBase<TransDesc, AnomalyType>::history_;
  using EnvironmentBase<TransDesc, AnomalyType>::os_;

 public:
  using trans_desc_type = TransDesc;
  EnvironmentDesc(const History& history, std::ostream* const os)
      : EnvironmentBase<TransDesc, AnomalyType>(history, os), transs_(history.trans_num()) {
    // initial first version
    for (uint64_t item_id = 0; item_id < history_.item_num(); ++item_id) {
      this->CommitVersion(item_id, nullptr);
    }
    for (uint64_t trans_id = 0; trans_id < history_.trans_num(); ++trans_id) {
      transs_[trans_id] = std::make_unique<TransDesc>(trans_id, *this);
    }
  }

  uint32_t DoCheck() {
    uint32_t anomaly_count = 0;
    for (const Operation& op : history_.operations()) {
      auto& trans = *transs_[op.trans_id()];
      switch (op.type()) {
        case Operation::Type::READ:
          trans.Read(op.item_id());
          break;
        case Operation::Type::WRITE:
          trans.Write(op.item_id());
          break;
        case Operation::Type::COMMIT: {
          std::optional<AnomalyType> a = trans.Commit();
          if (a.has_value()) {
            TRY_LOG(os_) << a.value() << " " << op.trans_id() << std::endl;
            ++anomaly_count;
            trans.Abort();
          }
          break;
        }
        case Operation::Type::ABORT:
          trans.Abort();
          break;
        default:
          assert(false);
      }
    }
    return anomaly_count;
  }

  std::vector<std::unique_ptr<TransDesc>> transs_;

  virtual uint64_t GetVisiableVersion(const uint64_t item_id, TransDesc* const r_trans) {
    return item_vers_[item_id].size() - 1;
  }
};

}  // namespace occ_algorithm
}  // namespace ttts
