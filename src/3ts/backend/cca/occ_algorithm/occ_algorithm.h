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
#include "../algorithm.h"

namespace ttts {
namespace occ_algorithm {

template <typename TransDesc>
struct ItemVersionDesc {
  ItemVersionDesc(const uint64_t item_id, const uint64_t version, TransDesc* const w_trans)
      : item_id_(item_id), version_(version), w_trans_(w_trans) {}
  ItemVersionDesc(const ItemVersionDesc&) = delete;
  ItemVersionDesc(ItemVersionDesc&&) = delete;
  const uint64_t item_id_;
  const uint64_t version_;
  TransDesc* w_trans_;
  std::unordered_map<uint64_t, TransDesc*> r_transs_;
};

#define THROW_ANOMALY(expression) \
  do {                            \
    const auto a = (expression);  \
    if (a.has_value()) {          \
      return a;                   \
    }                             \
  } while (0)

template <typename Container>
bool Has(const Container& container, const typename Container::key_type& key) {
  return container.find(key) != container.end();
}

template <typename Container>
typename Container::mapped_type AssertGet(const Container& container,
                                          const typename Container::key_type& key) {
  assert(Has(container, key));
  return container.find(key)->second;
}
template <typename TransDesc, typename AnomalyType>
class EnvironmentBase {
 public:
  EnvironmentBase(const History& history, std::ostream* const os)
      : item_vers_(history.item_num()), history_(history), os_(os) {}
  virtual ~EnvironmentBase() = default;
  virtual std::vector<int> DoCheck() = 0;
  bool HasVersion(const uint64_t item_id, const uint64_t version) {
    return item_vers_[item_id].size() > version;
  }

  ItemVersionDesc<TransDesc>& GetVersion(const uint64_t item_id, const uint64_t version) {
    assert(HasVersion(item_id, version));
    return *item_vers_[item_id][version];
  }
  // put an version in the item_vers_
  virtual ItemVersionDesc<TransDesc>& CommitVersion(const uint64_t item_id,
                                                    TransDesc* const w_trans) {
    const uint64_t version = item_vers_[item_id].size();
    item_vers_[item_id].push_back(
        std::make_unique<ItemVersionDesc<TransDesc>>(item_id, version, w_trans));
    return *item_vers_[item_id].back();
  }

  virtual uint64_t GetVisiableVersion(const uint64_t item_id, TransDesc* const r_trans) = 0;

  virtual ItemVersionDesc<TransDesc>& ReadVersion(const uint64_t item_id, const uint64_t version,
                                                  TransDesc* const r_trans) {
    const auto& r_ver = item_vers_[item_id][version];
    r_ver->r_transs_.emplace(r_trans->trans_id(), r_trans);
    assert(r_ver->version_ == version);
    return *r_ver;
  }

 protected:
  std::vector<std::vector<std::unique_ptr<ItemVersionDesc<TransDesc>>>> item_vers_;
  const History& history_;
  std::ostream* const os_;
};

template <typename TransDesc, template <typename, typename> class EnvDesc, typename AnomalyType>
class TransactionDescBase {
 public:
  using item_type = ItemVersionDesc<TransDesc>;
  using env_desc_type = EnvDesc<TransDesc, AnomalyType>;
  using set_type = std::unordered_map<uint64_t, ItemVersionDesc<TransDesc>*>;
  TransactionDescBase(const uint64_t trans_id, env_desc_type& env_desc)
      : env_desc_(env_desc), trans_id_(trans_id), committed_() {}
  virtual ~TransactionDescBase() {}
  TransactionDescBase(const TransactionDescBase&) = default;
  TransactionDescBase(TransactionDescBase&&) = delete;

  bool is_running() const { return !committed_.has_value(); }
  bool is_committed() const { return committed_.has_value() && committed_.value(); }
  bool is_aborted() const { return committed_.has_value() && !committed_.value(); }
  uint64_t trans_id() const { return trans_id_; }
  virtual const set_type& r_items() const { return r_items_; }
  virtual const set_type& w_items() const { return w_items_; }

  virtual void Read(const uint64_t item_id, std::function<bool(const uint64_t)>&& predicate = {}) {
    if (!Has(w_items_, item_id) && !Has(r_items_, item_id)) {
      const uint64_t version = env_desc_.GetVisiableVersion(item_id, static_cast<TransDesc*>(this));
      if (!predicate /* without predicate */ || predicate(version) /* predicate satisfied */) {
        r_items_[item_id] = &env_desc_.ReadVersion(item_id, version, static_cast<TransDesc*>(this));
      }
    }
  }

  virtual void Write(const uint64_t item_id) {
    // std::cout<<"base write";
    w_items_[item_id] = nullptr;  // occupy a location
  }

  virtual std::optional<AnomalyType> Commit() {
    for (auto& pair :
         w_items_) {  // add by ym : while using RU, w_items_ is empty. So jump this for-loop
      pair.second = &env_desc_.CommitVersion(
          pair.first, static_cast<typename env_desc_type::trans_desc_type*>(this));
    }
    committed_ = true;
    return {};
  }

  virtual std::optional<Anomally> Abort() {
    committed_ = false;
    return {};
  }

 protected:
  env_desc_type& env_desc_;
  const uint64_t trans_id_;
  std::optional<bool> committed_;
  set_type r_items_;
  set_type w_items_;
};

}  // namespace occ_algorithm

template <typename TransDesc>
class OCCAlgorithm : public RollbackRateAlgorithm {
 public:
  OCCAlgorithm() : RollbackRateAlgorithm(TransDesc::name + " with OCC") {}
  virtual ~OCCAlgorithm() {}
  virtual bool Check(const History& history, std::ostream* const os) const override {
    return RollbackNum(history, os).size() == 0;
  }
  std::vector<int> RollbackNum(const History& history, std::ostream* const os = nullptr) const {
    typename TransDesc::env_desc_type c(history, os);
    std::vector<int> ret_anomally = c.DoCheck();
    TRY_LOG(os) << "aborted: " << ret_anomally.size();
    return ret_anomally;
  }
};

}  // namespace ttts
