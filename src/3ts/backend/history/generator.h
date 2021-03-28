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
        tcl_position_(opt.tcl_position),
        rd_(),
        gen_(rd_()),
        rand_trans_id_(0, trans_num_ - 1),
        rand_item_id_(0, item_num_ - 1),
        rand_bool_(0.5) {}

  ~RandomHistoryGenerator() {}

  virtual void DeliverHistories(const std::function<void(History &&)> &handle) const {
    for (uint64_t history_no = 0; history_no < history_num_; ++history_no) {
      if (tcl_position_ == TclPosition::TAIL) {
        handle(MakeDMLHistory() + MakeTCLHistory());
      } else if (tcl_position_ == TclPosition::ANYWHERE) {
        handle(ShuffleHistory(MakeDMLHistory() + MakeTCLHistory()));
      } else {
        assert(tcl_position_ == TclPosition::NOWHERE);
        handle(ShuffleHistory(MakeDMLHistory()));
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
      const uint64_t trans_id = gen_rand_id(rand_trans_id_, max_trans_id);
      const uint64_t item_id = gen_rand_id(rand_item_id_, max_item_id);
      rand_bool_(gen_) ? dml_acts.emplace_back(Operation::ReadTypeConstant(), trans_id, item_id) :
                         dml_acts.emplace_back(Operation::WriteTypeConstant(), trans_id, item_id);
    }
    return History(trans_num_, item_num_, dml_acts);
  }

  History MakeTCLHistory() const {
    std::vector<Operation> dtl_acts;
    uint64_t abort_trans_num;
    for (uint64_t trans_id = 0; trans_id < trans_num_; ++trans_id) {
      with_abort_ && rand_bool_(gen_) ?
        dtl_acts.emplace_back(Operation::AbortTypeConstant(), trans_id) :
        dtl_acts.emplace_back(Operation::CommitTypeConstant(), trans_id);
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
  const TclPosition tcl_position_;
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
        tcl_position_(opt.tcl_position),
        allow_empty_trans_(opt.allow_empty_trans),
        dynamic_history_len_(opt.dynamic_history_len),
        with_scan_(opt.with_scan),
        with_write_(opt.with_write) {}

  void DeliverHistories(const std::function<void(History &&)> &handle) const {
    std::vector<Operation> tmp_operations;
    RecursiveFillDMLHistory(
        [this, &handle](History&& dml_history, const uint64_t max_trans_id) {
          HandleDMLHistory(handle, std::move(dml_history), max_trans_id);
        },
        tmp_operations, 0, 0);
  }

  static uint64_t cut_down_;

 private:
  void HandleTCLHistory(const std::function<void(History &&)> &handle, History&& dml_history,
                        History&& dtl_history) const {
    History history_tot = dml_history + dtl_history;
    if (tcl_position_ == TclPosition::TAIL) {
      handle(std::move(history_tot));
    } else {
      assert(tcl_position_ == TclPosition::ANYWHERE);
      RecursiveMoveForwardTCLOperation(handle, std::move(history_tot), dml_history.size());
    }
  }

  void HandleDMLHistory(const std::function<void(History&&)> &handle, History&& dml_history,
                        const uint64_t max_trans_id) const {
    std::vector<bool> is_commits;
    if (tcl_position_ == TclPosition::NOWHERE) {
      handle(std::move(dml_history));
    } else {
      RecursiveFillTCLHistory(
          [this, &handle, &dml_history](History&& dtl_history) {
            HandleTCLHistory(handle, std::move(dml_history), std::move(dtl_history));
          },
          is_commits, dml_history.trans_num());
    }
  }

  bool OnlyOneTrans(const std::vector<Operation> &operations) const {
    for (uint64_t i = 1; i < operations.size(); ++i) {
      if (operations[i].trans_id() != operations[i - 1].trans_id()) {
        return false;
      }
    }
    return true;
  }

  void RecursiveFillDMLHistoryOver(const std::function<void(History&&, const uint64_t)> &handle,
                                   std::vector<Operation> &operations, uint64_t max_trans_id,
                                   uint64_t max_item_id) const {
    const auto check_has_operation = [this, &operations](const Operation::Type type) {
      // check if allow no scan operations
      for (const Operation operation : operations) {
        if (operation.type() == type) { return true; }
      }
      return false;
    };

    if (dfs_cnt_ == subtask_num_) {
      if ((with_scan_ != Intensity::ALL_HAVE || check_has_operation(Operation::Type::SCAN_ODD)) &&
          (with_write_ != Intensity::ALL_HAVE || check_has_operation(Operation::Type::WRITE)) && // cannot all readonly transactions
          (allow_empty_trans_ || max_trans_id == trans_num_)) {
        handle(History(max_trans_id, max_item_id, operations), max_trans_id);
      } else {
        cut_down_++;
      }
      dfs_cnt_ -= subtask_num_;
    }
    ++dfs_cnt_;
  }

  void RecursiveFillDMLHistoryContinue(const std::function<void(History&&, const uint64_t)> &handle,
                                       std::vector<Operation> &operations, uint64_t max_trans_id,
                                       uint64_t max_item_id) const {
    const size_t cur = operations.size();
    // Make sure trans id is increment
    for (uint64_t trans_id = 0; trans_id < std::min(max_trans_id + 1, trans_num_); ++trans_id) {
      for (uint64_t item_id = 0; item_id < std::min(max_item_id + 1, item_num_); ++item_id) {
        const auto fill_dml =
            [this, cur, trans_id, item_id, max_trans_id, max_item_id, &handle, &operations]
            (auto&& type_constant) {
          // Continuous same operation is meaningless
          if (!(cur > 0 && operations[cur - 1].type() == type_constant.value &&
                trans_id == operations[cur - 1].trans_id() &&
                item_id == operations[cur - 1].item_id())) {
            operations.emplace_back(type_constant, trans_id, item_id);
            RecursiveFillDMLHistory(handle, operations, std::max(trans_id + 1, max_trans_id),
                                    std::max(item_id + 1, max_item_id));
            operations.pop_back();
          }
        };
        fill_dml(Operation::ReadTypeConstant());
        if (with_write_ != Intensity::NONE_HAVE) {
          fill_dml(Operation::WriteTypeConstant());
        }
      }
      if (with_scan_ != Intensity::NONE_HAVE &&
          !(cur > 0 && operations[cur - 1].type() == Operation::Type::SCAN_ODD &&
            trans_id == operations[cur - 1].trans_id())) {
        operations.emplace_back(Operation::ScanOddTypeConstant(), trans_id);
        RecursiveFillDMLHistory(handle, operations, std::max(trans_id + 1, max_trans_id), max_item_id);
        operations.pop_back();
      }
    }
  }

  // Generate all possible DML histories and pass each history to handle.
  void RecursiveFillDMLHistory(const std::function<void(History&&, const uint64_t)> &handle,
                               std::vector<Operation> &operations, uint64_t max_trans_id,
                               uint64_t max_item_id) const {
    if (dynamic_history_len_ || operations.size() == dml_operation_num_) {
      RecursiveFillDMLHistoryOver(handle, operations, max_trans_id, max_item_id);
    }
    if (operations.size() != dml_operation_num_) {
      RecursiveFillDMLHistoryContinue(handle, operations, max_trans_id, max_item_id);
    }
  }

  void RecursiveFillTCLHistoryOver(const std::function<void(History&&)> &handle,
                                   const std::vector<bool> &is_commits) const {
    std::vector<Operation> dtl_operations;
    uint64_t abort_trans_num = 0;
    const uint64_t trans_num = is_commits.size();
    for (uint64_t trans_id = 0; trans_id < trans_num; ++trans_id) {
      if (is_commits[trans_id]) {
        dtl_operations.emplace_back(Operation::CommitTypeConstant(), trans_id);
      } else {
        ++ abort_trans_num;
        dtl_operations.emplace_back(Operation::AbortTypeConstant(), trans_id);
      }
    }
    // traverse all possible transaction commit/abort order
    do {
      handle(History(trans_num, 0 /* DTL history has no items */, dtl_operations, abort_trans_num));
    } while (std::next_permutation(dtl_operations.begin(), dtl_operations.end()));
  }

  void RecursiveFillTCLHistoryContinue(const std::function<void(History&&)> &handle,
                                       std::vector<bool> &is_commits, const uint64_t trans_num) const {
    // traverse all possible transaction commit/abort states
    for (const bool is_commit : { true, false }) {
      is_commits.emplace_back(is_commit);
      RecursiveFillTCLHistory(handle, is_commits, trans_num);
      is_commits.pop_back();
    }
  }

  // Generate all possible TCL histories and pass each history to handle.
  void RecursiveFillTCLHistory(const std::function<void(History&&)> &handle,
                               std::vector<bool> &is_commits, const uint64_t trans_num) const {
    if (!with_abort_) {
      is_commits.assign(trans_num, true);
    }
    if (is_commits.size() == trans_num) {
      RecursiveFillTCLHistoryOver(handle, is_commits);
    } else {
      RecursiveFillTCLHistoryContinue(handle, is_commits, trans_num);
    }
  }

  // Remix the DML operations and tailing TCL operations.
  template <typename HistoryRef>
  void RecursiveMoveForwardTCLOperation(const std::function<void(History&&)> &handle,
                                        HistoryRef&& history, const size_t pos) const {
    if (pos == history.size() || tcl_position_ == TclPosition::TAIL) {
      // copy the history because we will go on moving forward tcl in this history
      handle(History(history));
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
  const TclPosition tcl_position_;
  const bool allow_empty_trans_;
  const bool dynamic_history_len_;
  const Intensity with_scan_;
  const Intensity with_write_;
};
uint64_t TraversalHistoryGenerator::cut_down_ = 0;
}  // namespace ttts
