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
#include "../env/si_env.h"
#include "../occ_algorithm.h"

namespace ttts {
namespace occ_algorithm {

class DLITransactionDesc
    : public SITransactionDesc<DLITransactionDesc, SIEnvironmentDesc, Anomally> {
 public:
  static std::string name;

  DLITransactionDesc(const uint64_t trans_id, const uint64_t start_ts,
                                    Snapshot&& snapshot, env_desc_type& env_desc)
      : SITransactionDesc(trans_id, start_ts, std::move(snapshot), env_desc) {}

  virtual std::optional<Anomally> CheckConflict(const uint64_t) override {
    // It is indeed unnecessary to do such a copy for each TransctionDesc. But in actual database
    // system based on OCC, we usually copy the current environment to transaction with which
    // transaction can do conflict check without considering of the thread safe problem when other
    // transaction access the gloval environment concurrently.
    std::list<DLITransactionDesc*> l;
    const auto add_to_link = [trans_id = trans_id_,
                              &l](const std::unique_ptr<DLITransactionDesc>& ptr) {
      if (ptr->trans_id() != trans_id && !ptr->is_aborted()) {
        l.emplace_back(ptr.get());
      }
    };
    for (const auto& ptr : env_desc_.active_trans_) {
      add_to_link(ptr.second);
    }
    for (const auto& ptr : env_desc_.commit_trans_) {
      add_to_link(ptr.second);  // add by ym: Q why we detect DLI with commit_trans_?
    }
    return CheckConflict_(l);
  }

 private:
  std::optional<Anomally> CheckConflict_(
      const std::list<DLITransactionDesc*>& transs) {
    DLITransactionDesc combined_trans = *this;
    for (const auto& tl : transs) {
      THROW_ANOMALY(combined_trans.CheckDynamicEdgeCross(*tl));
      combined_trans.MergeTransaction(*tl);
    }
    return {};
  }

  void MergeTransaction(const SITransactionDesc& tl) {
    const auto merge = [](const set_type& src, set_type& dst) {
      for (const auto& pair : src) {
        if (!Has(dst, pair.first)) {
          dst[pair.first] = pair.second;  // if version is different, don't change it
        }
      }
    };
    merge(tl.r_items(), r_items_);
    merge(tl.w_items(), w_items_);
  }

  struct IntersectionItem {
    IntersectionItem(const uint64_t item_id, const uint64_t version_1, const uint64_t version_2)
        : item_id_(item_id), version_1_(version_1), version_2_(version_2) {}
    const uint64_t item_id_;
    const uint64_t version_1_;
    const uint64_t version_2_;
  };

  std::optional<Anomally> CheckDynamicEdgeCross(const DLITransactionDesc& tl) const {
    bool upper = 0, tl_upper = 0;
    THROW_ANOMALY(CheckWWOrRRIntersection(r_items_, upper, tl.r_items(), tl_upper));
    THROW_ANOMALY(CheckWWOrRRIntersection(w_items_, upper, tl.w_items(), tl_upper));
    THROW_ANOMALY(CheckWRIntersection(w_items_, upper, tl.r_items(), tl_upper));
    THROW_ANOMALY(CheckWRIntersection(tl.w_items(), tl_upper, r_items_, upper));
    return {};
  };

  std::optional<Anomally> CheckWWOrRRIntersection(const set_type& s1, bool& upper1,
                                                  const set_type& s2, bool& upper2) const {
    for (const auto& inter_item : GetIntersection(s1, s2)) {
      if (inter_item.version_1_ > inter_item.version_2_) {
        upper1 = true;
      } else if (inter_item.version_1_ < inter_item.version_2_) {
        upper2 = true;
      }
    }
    if (upper1 && upper2) {
      return Anomally::EDGE_CROESS;
    }
    return {};
  }

  std::optional<Anomally> CheckWRIntersection(const set_type& w_items, bool& w_upper,
                                              const set_type& r_items, bool& r_upper) const {
    for (const auto& inter_item : GetIntersection(w_items, r_items)) {
      if (inter_item.version_1_ > inter_item.version_2_) {
        w_upper = true;
      } else {
        r_upper = true;
      }
    }
    if (w_upper && r_upper) {
      return Anomally::EDGE_CROESS;
    }
    return {};
  }

  static std::vector<IntersectionItem> GetIntersection(const set_type& s1, const set_type& s2) {
    static const auto get_version =
        [](const ItemVersionDesc<DLITransactionDesc>* ver) {
          return ver ? ver->version_
                     : UINT64_MAX;  // ver is empty only when it is written by current transction
        };
    std::vector<IntersectionItem> ret;
    for (const auto& pair : s1) {
      if (Has(s2, pair.first)) {
        ret.emplace_back(pair.first, get_version(pair.second),
                         get_version(AssertGet(s2, pair.first)));
      }
    }
    return ret;
  }
};

std::string DLITransactionDesc::name = "DLI";

}  // namespace occ_algorithm
}  // namespace ttts
