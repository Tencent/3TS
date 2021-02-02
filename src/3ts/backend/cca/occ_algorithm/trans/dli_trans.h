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

  virtual std::optional<Anomally> Commit(const uint64_t commit_ts) override {
    THROW_ANOMALY(SITransactionDesc::Commit(commit_ts));
    MergeToHistoryTransRecursive();
    return {};
  }

 private:
  virtual std::optional<Anomally> CheckConflict(const uint64_t) override {
    for (const auto& pair : r_items_) {
      if (env_desc_.HasVersion(pair.first, pair.second->version_ + 1)) {
        const auto& later_item = env_desc_.GetVersion(pair.first, pair.second->version_ + 1);
        if (Has(w_items_, pair.first)) {
          return Anomally::LOST_UPDATE;
        }
        const DLITransactionDesc* const tl = later_item.w_trans_;
        if (tl) {
          THROW_ANOMALY(CheckCrossWithTrans(*tl));
          tl->MergeTo(*this);
        }
      }
    }
    return {};
  }

  std::optional<Anomally> CheckCrossWithTrans(const DLITransactionDesc& tl) const {
    assert(tl.is_committed());
    for (const auto& pair : tl.r_items_) {
      if (Has(w_items_, pair.first)) {
        return Anomally::WRITE_SKEW;
      } else if (Has(r_items_, pair.first) &&
                 pair.second->version_ < AssertGet(r_items_, pair.first)->version_) {
        return Anomally::THREE_TRANS_WRITE_SKEW;
      }
    }
    for (const auto& pair : tl.i_items_) {
      if (Has(w_items_, pair.first)) {
        return Anomally::MULTI_TRANS_ANOMALY;
      } else if (Has(r_items_, pair.first) &&
                 pair.second->version_ < AssertGet(r_items_, pair.first)->version_) {
        return Anomally::MULTI_TRANS_ANOMALY;
      }
    }
    for (const auto& pair : tl.w_items_) {
      if (Has(w_items_, pair.first)) {
        return Anomally::READ_WRITE_SKEW;
      } else if (Has(r_items_, pair.first) &&
                 pair.second->version_ <= AssertGet(r_items_, pair.first)->version_) {
        return Anomally::READ_SKEW;
      }
    }
    return {};
  }

  void MergeToHistoryTransRecursive() const {
    std::vector<item_type*> empty_items_to_merge;
    std::unordered_set<uint64_t> empty_transs_merged;
    MergeToHistoryTransRecursive(empty_items_to_merge, empty_transs_merged);
  }

  void MergeToHistoryTransRecursive(const std::vector<item_type*>& items_to_merge,
                                    std::unordered_set<uint64_t>& transs_merged) const {
    const auto merge = [&transs_merged, &items_to_merge,
                        this](DLITransactionDesc& tl) {
      if (!Has(transs_merged, tl.trans_id_)) {
        transs_merged.emplace(tl.trans_id_);
        const auto items_merged =
            items_to_merge.empty() ? MergeTo(tl) : MergeTo(items_to_merge, tl);
        if (!items_merged.empty()) {
          tl.MergeToHistoryTransRecursive(items_merged, transs_merged);
        }
      }
    };
    for (const auto& pair_item : w_items_) {
      const item_type& last_item =
          env_desc_.GetVersion(pair_item.first, pair_item.second->version_ - 1);
      bool merged = false;
      for (const auto& pair_r_trans : last_item.r_transs_) {
        if (pair_r_trans.second->trans_id_ != trans_id_ &&
            pair_r_trans.second->is_committed()) {  // eg: "R1x W1x C1" will cause same trans_id
          merge(*pair_r_trans.second);
          merged = true;
        }
      }
      if (!merged && last_item.w_trans_) {
        merge(*last_item.w_trans_);
      }
    }
    for (const auto& pair : r_items_) {
      DLITransactionDesc* const w_trans = pair.second->w_trans_;
      if (w_trans) {  // maybe it is a initial version
        merge(*w_trans);
      }
    }
  }

  void MergeOneItemTo(DLITransactionDesc& tl, item_type& item,
                      std::vector<item_type*>& items_merged) const {
    if (Has(tl.r_items_, item.item_id_)) {
      assert(AssertGet(tl.r_items_, item.item_id_)->version_ <= item.version_);
    } else if (Has(tl.w_items_, item.item_id_)) {
      assert(AssertGet(tl.w_items_, item.item_id_)->version_ <= item.version_);
    } else if (!Has(tl.i_items_, item.item_id_) ||
               AssertGet(tl.i_items_, item.item_id_)->version_ > item.version_) {
      tl.i_items_[item.item_id_] = &item;
      items_merged.emplace_back(&item);
    }
  }

  std::vector<item_type*> MergeTo(DLITransactionDesc& tl) const {
    assert(is_committed());
    std::vector<ItemVersionDesc<DLITransactionDesc>*> merged_items;
    for (auto& pair : r_items_) {
      MergeOneItemTo(tl, *pair.second, merged_items);
    }
    for (const auto& pair : i_items_) {
      MergeOneItemTo(tl, *pair.second, merged_items);
    }
    for (const auto& pair : w_items_) {
      MergeOneItemTo(tl, env_desc_.GetVersion(pair.first, pair.second->version_ - 1), merged_items);
    }
    return merged_items;
  }

  std::vector<item_type*> MergeTo(const std::vector<item_type*>& items_to_merge,
                                  DLITransactionDesc& tl) const {
    assert(is_committed());
    std::vector<ItemVersionDesc<DLITransactionDesc>*> merged_items;
    for (item_type* const item : items_to_merge) {
      MergeOneItemTo(tl, *item, merged_items);
    }
    return merged_items;
  }

  set_type i_items_;
};

std::string DLITransactionDesc::name = "DLI";

}  // namespace occ_algorithm
}  // namespace ttts
