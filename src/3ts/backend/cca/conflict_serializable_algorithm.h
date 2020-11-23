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
#include "algorithm.h"

namespace ttts {

class ConflictGraphNode {
 public:
  ConflictGraphNode() : removed_(false) {}
  ~ConflictGraphNode() {}

  bool HasNoPreTrans() const { return pre_trans_set_.empty(); }
  void AddPreTrans(const uint64_t pre_trans_id) { pre_trans_set_.insert(pre_trans_id); }
  void RemovePreTrans(const uint64_t pre_trans_id) { pre_trans_set_.erase(pre_trans_id); }
  void Remove() { removed_ = true; }
  bool IsRemoved() const { return removed_; }

 private:
  std::set<uint64_t> pre_trans_set_;
  bool removed_;
};

class ConflictGraph {
 public:
  ConflictGraph(const uint64_t trans_num) : nodes_(trans_num) {}

  void Insert(const uint64_t pre_trans_id, const uint64_t trans_id) {
    if (pre_trans_id == trans_id) {
      return;
    }
    nodes_[trans_id].AddPreTrans(pre_trans_id);
  }

  void Insert(const std::set<uint64_t>& pre_trans_set, const uint64_t cur_trans_id) {
    for (const uint64_t pre_trans : pre_trans_set) {
      Insert(pre_trans, cur_trans_id);
    }
  }

  bool HasCycle() {
    RemoveNodesNotInCycle();
    for (const ConflictGraphNode& node : nodes_) {
      if (!node.HasNoPreTrans()) {
        return true;
      }
    }
    return false;
  }

 private:
  void RemoveNodesNotInCycle() {
    bool found_no_pre_trans = false;
    // A trans which did not have pretrans can not be a part of cycle.
    do {
      found_no_pre_trans = false;
      for (uint64_t trans_id = 0; trans_id < nodes_.size(); ++trans_id) {
        if (nodes_[trans_id].HasNoPreTrans() && !nodes_[trans_id].IsRemoved()) {
          found_no_pre_trans = true;
          nodes_[trans_id].Remove();
          for (ConflictGraphNode& node : nodes_) {
            node.RemovePreTrans(trans_id);
          }
        }
      }
    } while (found_no_pre_trans);
  }

 private:
  std::vector<ConflictGraphNode> nodes_;
};

class ConflictSerializableAlgorithm : public HistoryAlgorithm {
 public:
  ConflictSerializableAlgorithm() : HistoryAlgorithm("Conflict Serializable") {}
  virtual ~ConflictSerializableAlgorithm() {}

  virtual bool Check(const History& history, std::ostream* const os) const override {
    ConflictGraph graph(history.trans_num());
    std::vector<std::set<uint64_t>> read_trans_set_for_items(history.item_num());
    std::vector<std::set<uint64_t>> write_trans_set_for_items(history.item_num());
    std::vector<std::set<uint64_t>> write_item_set_for_transs(history.trans_num());
    for (const Operation& operation : history.operations()) {
      const uint64_t trans_id = operation.trans_id();
      if (operation.IsPointDML()) {
        std::set<uint64_t>& read_trans_set = read_trans_set_for_items[operation.item_id()];
        std::set<uint64_t>& write_trans_set = write_trans_set_for_items[operation.item_id()];
        if (Operation::Type::READ == operation.type()) {
          graph.Insert(write_trans_set, trans_id);
          read_trans_set.insert(trans_id);
        } else if (Operation::Type::WRITE == operation.type()) {
          graph.Insert(read_trans_set, trans_id);
          graph.Insert(write_trans_set, trans_id);
          write_trans_set.insert(trans_id);
          write_item_set_for_transs[trans_id].insert(operation.item_id());  // record for abort
        }
      } else if (Operation::Type::SCAN_ODD == operation.type()) {
        // TODO: realize scan odd
      } else if (Operation::Type::ABORT == operation.type()) {
        for (const uint64_t write_item : write_item_set_for_transs[trans_id]) {
          // restore all items has been written
          graph.Insert(read_trans_set_for_items[write_item], trans_id);
          graph.Insert(write_trans_set_for_items[write_item], trans_id);
        }
      }
    }
    return !graph.HasCycle();
  }

 private:
  struct ReadWriteSet {
    std::set<uint64_t> reads_;
    std::set<uint64_t> writes_;
  };
};

}  // namespace ttts
