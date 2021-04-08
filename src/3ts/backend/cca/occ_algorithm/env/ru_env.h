#pragma once
#include "../occ_algorithm.h"

namespace ttts {
namespace occ_algorithm {

template <typename TransDesc, template <typename, typename> class EnvDesc, typename AnomalyType>
class RUTransactionDesc : public TransactionDescBase<TransDesc, EnvDesc, AnomalyType> {
 public:
  using TransactionDescBase<TransDesc, EnvDesc, AnomalyType>::TransactionDescBase;
  using env_desc_type = EnvDesc<TransDesc, AnomalyType>;
  using TransactionDescBase<TransDesc, EnvDesc, AnomalyType>::env_desc_;
  using RU_set_type = std::unordered_map<uint64_t, std::vector<ItemVersionDesc<TransDesc>*>>;
  RUTransactionDesc(const uint64_t trans_id, const uint64_t start_ts, env_desc_type& env_desc)
      : TransactionDescBase<TransDesc, EnvDesc, AnomalyType>(trans_id, env_desc),
        start_ts_(start_ts),
        commit_ts_(0),
        last_op_ts_(0),
        unrepeatable_read(false),
        intermediate_read(false) {}
  virtual std::optional<AnomalyType> CheckConflict(const uint64_t commit_ts) = 0;

  virtual void Write(const uint64_t item_id) {
    RU_w_items_[item_id]
        .push_back(&env_desc_.CommitVersion(item_id, static_cast<typename env_desc_type::trans_desc_type*>(this)));
  }

  virtual void Read(const uint64_t item_id, std::function<bool(const uint64_t)>&& predicate = {}) {
    const uint64_t version = env_desc_.GetVisiableVersion(item_id, static_cast<TransDesc*>(this));
    if (!predicate || predicate(version)) {  // without predicate || predicate satisfied
      RU_r_items_[item_id].push_back(&env_desc_.ReadVersion(item_id, version, static_cast<TransDesc*>(this)));
    }
  }
  void set_last_op_ts(const uint64_t last_op_ts) { last_op_ts_ = last_op_ts; }
  virtual std::optional<AnomalyType> Commit(const uint64_t commit_ts) {
    commit_ts_ = commit_ts;
    THROW_ANOMALY(CheckConflict(commit_ts));
    TransactionDescBase<TransDesc, EnvDesc, AnomalyType>::Commit();
    return {};
  }

  uint64_t get_start_ts() const { return start_ts_; }

  uint64_t get_last_op_ts() const { return last_op_ts_; }

  bool unrepeatable_read;
  bool intermediate_read;
  virtual const RU_set_type& RU_r_items() const { return RU_r_items_; }
  virtual const RU_set_type& RU_w_items() const { return RU_w_items_; }

 protected:
  const uint64_t start_ts_;
  uint64_t commit_ts_;
  uint64_t last_op_ts_;
  RU_set_type RU_r_items_;
  RU_set_type RU_w_items_;
};

template <typename TransDesc, typename AnomalyType>
class RUEnvironmentDesc : public EnvironmentBase<TransDesc, AnomalyType> {
  static_assert(std::is_base_of_v<RUTransactionDesc<TransDesc, RUEnvironmentDesc, AnomalyType>, TransDesc>,
                "TransDesc with EnvironmentDesc should base of RUTransactionDesc");

 private:
  using EnvironmentBase<TransDesc, AnomalyType>::item_vers_;
  using EnvironmentBase<TransDesc, AnomalyType>::history_;
  using EnvironmentBase<TransDesc, AnomalyType>::os_;

 public:
  using trans_desc_type = TransDesc;
  RUEnvironmentDesc(const History& history, std::ostream* const os)
      : EnvironmentBase<TransDesc, AnomalyType>(history, os) {
    // initial first version
    for (uint64_t item_id = 0; item_id < history_.item_num(); ++item_id) {
      this->CommitVersion(item_id, nullptr);
    }
  }

  std::vector<int> DoCheck() {
    std::vector<int> ret_anomally;

    for (const Operation& op : history_.operations()) {
      // std::cout << op << "  ";
      if (abort_trans_.count(op.trans_id())) continue;
      if (active_trans_.count(op.trans_id()) == 0) {
        active_trans_[op.trans_id()] = std::make_unique<TransDesc>(op.trans_id(), act_cnt_, *this);
      }
      auto& trans = *active_trans_[op.trans_id()];
      switch (op.type()) {
        case Operation::Type::READ:
          trans.Read(op.item_id());
          trans.set_last_op_ts(act_cnt_);
          break;
        case Operation::Type::WRITE:
          trans.Write(op.item_id());
          trans.set_last_op_ts(act_cnt_);
          break;
        case Operation::Type::SCAN_ODD: {
          for (uint64_t item_id = 0; item_id < item_vers_.size(); ++item_id) {
            trans.Read(item_id);
          }
          break;
        }
        case Operation::Type::COMMIT: {
          if (std::optional<AnomalyType> a = trans.Commit(act_cnt_); a.has_value()) {
            TRY_LOG(os_) << a.value() << " " << op.trans_id() << std::endl;
            ret_anomally.push_back(a.value());  // add by ym : TODO we do not check what anomaly it is, yet.
            trans.Abort();
            abort_trans_[op.trans_id()] = std::move(active_trans_[op.trans_id()]);
            active_trans_.erase(op.trans_id());
          } else {
            commit_order_.push_back(op.trans_id());
            commit_trans_[op.trans_id()] = std::move(active_trans_[op.trans_id()]);
            active_trans_.erase(op.trans_id());
          }
          break;
        }
        case Operation::Type::ABORT: {
          if (std::optional<AnomalyType> a = trans.Abort(); a.has_value()) {
            TRY_LOG(os_) << a.value() << " " << op.trans_id() << std::endl;
            ret_anomally.push_back(a.value());  // add by ym : TODO we do not check what anomaly it is, yet.
          }
          abort_trans_[op.trans_id()] = std::move(active_trans_[op.trans_id()]);
          active_trans_.erase(op.trans_id());
          break;
        }
        default:
          assert(false);
      }
      ++act_cnt_;
    }
    return ret_anomally;
    ;
  }

  virtual ItemVersionDesc<TransDesc>& ReadVersion(const uint64_t item_id, const uint64_t version,
                                                  TransDesc* const r_trans) {
    const auto& r_ver = item_vers_[item_id][version];
    uint64_t nc_version = version;
    if (r_ver->w_trans_ != nullptr && abort_trans_.count(r_ver->w_trans_->trans_id()) > 0) {
      bool find_version = true;
      while (find_version) {
        const auto& r_ver_loop = item_vers_[item_id][--nc_version];
        if (r_ver_loop->w_trans_ == nullptr) {
          r_ver_loop->r_transs_.emplace(r_trans->trans_id(), r_trans);
          return *r_ver_loop;
        } else if (abort_trans_.count(r_ver_loop->w_trans_->trans_id()) == 0) {
          find_version = false;
          r_ver_loop->r_transs_.emplace(r_trans->trans_id(), r_trans);
          return *r_ver_loop;
        }
      }
    } else {
      r_ver->r_transs_.emplace(r_trans->trans_id(), r_trans);
    }

    return *r_ver;
  }

  // actually we do not need to alter here.
  virtual uint64_t GetVisiableVersion(const uint64_t item_id, TransDesc* const r_trans) {
    return item_vers_[item_id].size() - 1;
  }

 public:
  std::map<uint64_t, std::unique_ptr<TransDesc>> active_trans_;
  std::map<uint64_t, std::unique_ptr<TransDesc>> commit_trans_;
  std::map<uint64_t, std::unique_ptr<TransDesc>> abort_trans_;
  std::vector<uint64_t> commit_order_;

 private:
  uint64_t act_cnt_;
  std::map<uint64_t, uint64_t> latest_commit_time_;
};

}  // namespace occ_algorithm
}  // namespace ttts
