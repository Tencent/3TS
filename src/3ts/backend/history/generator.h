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
#include <random>

#include "../util/generic.h"

namespace ttts {

class HistoryGenerator {
 public:
  HistoryGenerator() {}
  ~HistoryGenerator() {}
  virtual void DeliverHistories(const std::function<void(History &&)> &handle) const = 0;
};

class InputHistoryGenerator : public HistoryGenerator {
 public:
  InputHistoryGenerator(const std::string &path) : path_(path) {}
  ~InputHistoryGenerator() {}
  virtual void DeliverHistories(const std::function<void(History &&)> &handle) const override {
    std::ifstream fs(path_);
    if (!fs) {
      std::cerr << "Open Operation Sequences File Failed" << std::endl;
      return;
    }
    for (History history; fs >> history;) {
      handle(std::move(history));
    }
  }

 private:
  const std::string path_;
};

class RandomHistoryGenerator : public HistoryGenerator {
 public:
  RandomHistoryGenerator(const Options &opt, const uint64_t history_num)
      : trans_num_(opt.trans_num),
        item_num_(opt.item_num),
        dml_operation_num_(opt.max_dml),
        history_num_(history_num),
        with_abort_(opt.with_abort),
        tail_tcl_(opt.tail_tcl),
        rd_(),
        gen_(rd_()),
        rand_trans_id_(0, trans_num_ - 1),
        rand_item_id_(0, item_num_ - 1),
        rand_bool_(0.5) {}

  ~RandomHistoryGenerator() {}

  virtual void DeliverHistories(const std::function<void(History &&)> &handle) const {
    for (uint64_t history_no = 0; history_no < history_num_; ++history_no) {
      if (tail_tcl_) {
        handle(MakeDMLHistory() + MakeTCLHistory());
      } else {
        handle(ShuffleHistory(MakeDMLHistory() + MakeTCLHistory()));
      }
    }
  }

  History ShuffleHistory(History &&history) const {
    std::vector<uint64_t> last_pos(trans_num_, -1);
    for (uint64_t i = 0; i < dml_operation_num_; ++i) last_pos[history[i].trans_id()] = i;
    uint64_t left = -1;
    std::uniform_int_distribution<int> rand_pos;
    for (uint64_t i = dml_operation_num_; i < history.size(); ++i) {
      left = std::max(left, last_pos[history[i].trans_id()]) + 1;
      if (left >= i) {
        break;
      }
      rand_pos = std::uniform_int_distribution<int>(left, i);
      left = rand_pos(gen_);

      for (uint64_t j = i - 1; j >= left; --j) {
        last_pos[history[j].trans_id()] = std::max(j + 1, last_pos[history[j].trans_id()]);
        std::swap(history[j], history[j + 1]);
      }
    }
    return history;
  }

  History MakeDMLHistory() const {
    std::vector<Operation> dml_acts;
    int64_t max_trans_id = -1;
    int64_t max_item_id = -1;
    const auto gen_rand_id = [&gen = gen_](std::uniform_int_distribution<uint64_t> &rand_id_dis,
                                           int64_t &cur_max_id) {
      const uint64_t rand_id = rand_id_dis(gen);
      return ((int64_t)rand_id <= cur_max_id) ? rand_id : (++cur_max_id);
    };
    for (uint64_t dml_operation_no = 0; dml_operation_no < dml_operation_num_; ++dml_operation_no) {
      const Operation::Type dml_type =
          rand_bool_(gen_) ? Operation::Type::WRITE : Operation::Type::READ;
      const uint64_t trans_id = gen_rand_id(rand_trans_id_, max_trans_id);
      const uint64_t item_id = gen_rand_id(rand_item_id_, max_item_id);
      dml_acts.emplace_back(dml_type, trans_id, item_id);
    }
    return History(trans_num_, item_num_, dml_acts);
  }

  History MakeTCLHistory() const {
    std::vector<Operation> dtl_acts;
    uint64_t abort_trans_num;
    for (uint64_t trans_id = 0; trans_id < trans_num_; ++trans_id) {
      dtl_acts.emplace_back(
          (with_abort_ && rand_bool_(gen_)) ? Operation::Type::ABORT : Operation::Type::COMMIT,
          trans_id);
      if (dtl_acts.back().type() == Operation::Type::ABORT) {
        abort_trans_num++;
      }
    }

    std::shuffle(dtl_acts.begin(), dtl_acts.end(), gen_);
    return History(trans_num_, item_num_, dtl_acts, abort_trans_num);
  }

 private:
  const uint64_t trans_num_;
  const uint64_t item_num_;
  const uint64_t dml_operation_num_;
  const uint64_t history_num_;
  const bool with_abort_;
  const bool tail_tcl_;
  mutable std::random_device rd_;
  mutable std::mt19937 gen_;
  mutable std::uniform_int_distribution<uint64_t> rand_trans_id_;
  mutable std::uniform_int_distribution<uint64_t> rand_item_id_;
  mutable std::bernoulli_distribution rand_bool_;
};

class TraversalHistoryGenerator : public HistoryGenerator {
 public:
  TraversalHistoryGenerator(const Options &opt)
      : trans_num_(opt.trans_num),
        item_num_(opt.item_num),
        dml_operation_num_(opt.max_dml),
        subtask_num_(opt.subtask_num),
        dfs_cnt_(opt.subtask_num - opt.subtask_id),
        with_abort_(opt.with_abort),
        tail_tcl_(opt.tail_tcl),
        allow_empty_trans_(opt.allow_empty_trans),
        dynamic_history_len_(opt.dynamic_history_len) {}

  void DeliverHistories(const std::function<void(History &&)> &handle) const {
    std::vector<Operation> tmp_operations;
    RecursiveFillDMLHistory(
        [this, &handle](const History &dml_history, const uint64_t max_trans_id) {
          HandleDMLHistory(handle, dml_history, max_trans_id);
        },
        tmp_operations, 0, 0);
  }

  static uint64_t cut_down_;

 private:
  void HandleHistory(const std::function<void(History &&)> &handle, const History &history) const {
    History history_copy = history;  // copy History
    handle(std::move(history_copy));
  }

  void HandleTCLHistory(const std::function<void(History &&)> &handle, const History &dml_history,
                        const History &dtl_history) const {
    // Firstly, append every dml_history with dtl_history.
    History history_tot = dml_history + dtl_history;
    // Then move dtl_history at a suitable location, forward.
    RecursiveMoveForwardTCLOperation(
        [this, &handle](const History &history) { HandleHistory(handle, history); }, history_tot,
        dml_history.size());
  }

  void HandleDMLHistory(const std::function<void(History &&)> &handle, const History &dml_history,
                        const uint64_t max_trans_id) const {
    std::vector<Operation::Type> dtl_operation_types;
    RecursiveFillTCLHistory(
        [this, &handle, &dml_history](const History &dtl_history) {
          HandleTCLHistory(handle, dml_history, dtl_history);
        },
        dtl_operation_types, max_trans_id, 0);
  }

  bool OnlyOneTrans(const std::vector<Operation> &operations) const {
    for (uint64_t i = 1; i < operations.size(); ++i) {
      if (operations[i].trans_id() != operations[i - 1].trans_id()) {
        return false;
      }
    }
    return true;
  }

  void RecursiveFillDMLHistory(const std::function<void(const History &, const uint64_t)> &handle,
                               std::vector<Operation> &operations, uint64_t max_trans_id,
                               uint64_t max_item_id) const {
    size_t cur = operations.size();
    if (dynamic_history_len_ || cur == dml_operation_num_) {
      if (dfs_cnt_ == subtask_num_) {
        if (max_trans_id == trans_num_ || allow_empty_trans_) {
          handle(History(max_trans_id, item_num_, operations), max_trans_id);
        } else {
          cut_down_++;
        }
        dfs_cnt_ -= subtask_num_;
      }
      ++dfs_cnt_;
    }
    if (cur != dml_operation_num_) {
      // Make sure trans id is increment
      for (uint64_t trans_id = 0; trans_id < std::min(max_trans_id + 1, trans_num_); ++trans_id) {
        for (uint64_t item_id = 0; item_id < std::min(max_item_id + 1, item_num_); ++item_id) {
          for (Operation::Type dml_operation_type :
               {Operation::Type::READ, Operation::Type::WRITE}) {
            // Continuous read in same transaction is meaningless
            if (cur > 0 && dml_operation_type == Operation::Type::READ &&
                dml_operation_type == operations[cur - 1].type() &&
                trans_id == operations[cur - 1].trans_id() &&
                item_id == operations[cur - 1].item_id()) {
              continue;
            }
            operations.emplace_back(dml_operation_type, trans_id, item_id);
            RecursiveFillDMLHistory(handle, operations, std::max(trans_id + 1, max_trans_id),
                                    std::max(item_id + 1, max_item_id));
            operations.pop_back();
          }
        }
      }
    }
  }

  void RecursiveFillTCLHistory(const std::function<void(const History &)> &handle,
                               std::vector<Operation::Type> &dtl_operation_types,
                               const uint64_t max_trans_id, uint64_t abort_trans_num) const {
    if (!with_abort_) {
      dtl_operation_types.assign(max_trans_id, Operation::Type::COMMIT);
    }
    if (dtl_operation_types.size() == max_trans_id) {
      std::vector<uint64_t> trans_order;
      for (uint64_t trans_id = 0; trans_id < max_trans_id; ++trans_id) {
        trans_order.push_back(trans_id);
      }
      do {
        std::vector<Operation> dtl_operations;
        for (uint64_t trans_id : trans_order) {
          dtl_operations.emplace_back(dtl_operation_types[trans_id], trans_id);
        }
        handle(History(max_trans_id, item_num_, dtl_operations, abort_trans_num));
      } while (std::next_permutation(trans_order.begin(), trans_order.end()));
    } else {
      for (Operation::Type dtl_operation_type : {Operation::Type::COMMIT, Operation::Type::ABORT}) {
        dtl_operation_types.emplace_back(dtl_operation_type);
        if (dtl_operation_type == Operation::Type::ABORT) {
          ++abort_trans_num;
        }
        RecursiveFillTCLHistory(handle, dtl_operation_types, max_trans_id, abort_trans_num);
        dtl_operation_types.pop_back();
      }
    }
  }

  void RecursiveMoveForwardTCLOperation(const std::function<void(const History &)> &handle,
                                        History &history, const size_t pos) const {
    if (pos == history.size() || tail_tcl_) {
      handle(history);
    } else {
      RecursiveMoveForwardTCLOperation(handle, history, pos + 1);
      size_t i = pos;
      while (i > 0 && history[i - 1].trans_id() != history[i].trans_id() &&
             (history[i - 1].IsPointDML() || history[i - 1].type() == Operation::Type::SCAN_ODD)) {
        std::swap(history[i - 1], history[i]);
        RecursiveMoveForwardTCLOperation(handle, history, pos + 1);
        --i;
      }
      while (i < pos) {
        std::swap(history[i], history[i + 1]);
        ++i;
      }
    }
  }

  const uint64_t trans_num_;
  const uint64_t item_num_;
  const uint64_t dml_operation_num_;
  const uint64_t subtask_num_;
  mutable uint64_t dfs_cnt_;
  const bool with_abort_;
  const bool tail_tcl_;
  const bool allow_empty_trans_;
  const bool dynamic_history_len_;
};
uint64_t TraversalHistoryGenerator::cut_down_ = 0;
}  // namespace ttts
