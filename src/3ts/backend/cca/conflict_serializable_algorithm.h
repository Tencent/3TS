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

#ifdef ENUM_BEGIN
#ifdef ENUM_MEMBER
#ifdef ENUM_END

ENUM_BEGIN(PreceType)
ENUM_MEMBER(PreceType, RW)
ENUM_MEMBER(PreceType, WR)
ENUM_MEMBER(PreceType, WCR)
ENUM_MEMBER(PreceType, WW)
ENUM_MEMBER(PreceType, WCW)
ENUM_MEMBER(PreceType, RA)
ENUM_MEMBER(PreceType, WC)
ENUM_MEMBER(PreceType, WA)
ENUM_END(PreceType)

ENUM_BEGIN(AnomalyType)
// ======== WAT - 1 =========
ENUM_MEMBER(AnomalyType, DIRTY_WRITE)
ENUM_MEMBER(AnomalyType, INTERMEDIATE_WRITE)
ENUM_MEMBER(AnomalyType, LOST_SELF_UPDATE)
ENUM_MEMBER(AnomalyType, LOST_UPDATE_1)
// ======== WAT - 2 =========
ENUM_MEMBER(AnomalyType, DOUBLE_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, READ_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, FULL_WRITE_SKEW)
// ======== WAT - 3 =========
ENUM_MEMBER(AnomalyType, STEP_WAT)
// ======== RAT - 1 =========
ENUM_MEMBER(AnomalyType, DIRTY_READ)
ENUM_MEMBER(AnomalyType, INTERMEDIATE_READ)
ENUM_MEMBER(AnomalyType, NON_REPEATABLE_READ)
// ======== RAT - 2 =========
ENUM_MEMBER(AnomalyType, WRITE_READ_SKEW)
ENUM_MEMBER(AnomalyType, DOUBLE_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, READ_SKEW)
ENUM_MEMBER(AnomalyType, STEP_RAT)
// ======== IAT - 1 =========
ENUM_MEMBER(AnomalyType, LOST_UPDATE_2)
// ======== IAT - 2 =========
ENUM_MEMBER(AnomalyType, READ_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, WRITE_SKEW)
// ======== IAT - 3 =========
ENUM_MEMBER(AnomalyType, STEP_IAT)
// ======== Unknown =========
ENUM_MEMBER(AnomalyType, UNKNOWN_SINGLE)
ENUM_MEMBER(AnomalyType, UNKNOWN_DOUBLE)
ENUM_END(AnomalyType)

#endif
#endif
#endif

#ifndef CCA_CONFLICT_SERIALIZABLE_ALGORITHM
#define CCA_CONFLICT_SERIALIZABLE_ALGORITHM

#include <optional>
#include <functional>
#include <iterator>
#include <algorithm>
#include "algorithm.h"

namespace ttts {

#define ENUM_FILE "../cca/conflict_serializable_algorithm.h"
#include "../util/extend_enum.h"

class PreceInfo {
 public:
  PreceInfo(const uint64_t trans_id, const uint64_t pre_trans_id, const uint64_t item_id, const PreceType type, const uint32_t pre_order, const uint32_t order)
    : trans_id_(trans_id), pre_trans_id_(pre_trans_id), item_id_(item_id), type_(type), order_(order) {}
  PreceInfo(const PreceInfo&) = default;

  friend std::ostream& operator<<(std::ostream& os, const PreceInfo prece) {
    return os << 'T' << static_cast<char>('0' + prece.pre_trans_id_) << "-[" << prece.type_ << "-"
              << static_cast<char>('a' + prece.item_id_) << "]->" << 'T' << static_cast<char>('0' + prece.trans_id_);
  }
  bool operator>(const PreceInfo& p) const { return order_ > p.order_ || (order_ == p.order_ && pre_order_ > p.pre_order_); }
  bool operator<(const PreceInfo& p) const { return order_ < p.order_ || (order_ == p.order_ && pre_order_ < p.pre_order_); }

  uint64_t item_id() const { return item_id_; }
  PreceType type() const { return type_; }

 private:
  uint64_t trans_id_;
  uint64_t pre_trans_id_;
  uint64_t item_id_;
  PreceType type_;
  uint32_t pre_order_;
  uint32_t order_;
};

class ConflictGraphNode {
 public:
  ConflictGraphNode(const uint64_t trans_id) : trans_id_(trans_id), removed_(false) {}
  ~ConflictGraphNode() {}

  bool HasNoPreTrans() const { return pre_trans_set_.empty(); }
  void AddPreTrans(const uint64_t pre_trans_id, const uint64_t item_id, const PreceType type, const uint32_t pre_order, const uint32_t order) {
    // we only record the first precedence between the two specific transactions
    pre_trans_set_.emplace(pre_trans_id, PreceInfo{trans_id_, pre_trans_id, item_id, type, pre_order, order});
  }
  void RemovePreTrans(const uint64_t pre_trans_id) { pre_trans_set_.erase(pre_trans_id); }
  void Remove() { removed_ = true; }
  bool IsRemoved() const { return removed_; }
  std::optional<bool>& is_committed() { return is_committed_; }
  const std::map<uint64_t, PreceInfo>& pre_trans_set() const { return pre_trans_set_; }
  uint64_t trans_id() const { return trans_id_; }

 private:
  const uint64_t trans_id_;
  std::map<uint64_t, PreceInfo> pre_trans_set_;
  bool removed_;
  std::optional<bool> is_committed_;
};

template <typename Container, typename Compare>
static void sort(Container&& container, Compare&& comp) {
  if (!std::is_sorted(container.begin(), container.end(), comp)) {
    std::sort(container.begin(), container.end(), comp);
  }
}

class Path {
 public:
  Path() {}
  Path(std::vector<PreceInfo>&& preces) : preces_((sort(preces, std::greater<PreceInfo>()), std::move(preces))) {}
  Path(const PreceInfo& prece) : preces_{prece} {}
  Path(const Path&) = default;
  Path(Path&&) = default;
  Path& operator=(const Path&) = default;
  Path& operator=(Path&&) = default;

  bool operator<(const Path& p) const {
    // impassable has the greatest weight
    if (!passable()) {
      return false;
    }
    if (!p.passable()) {
      return true;
    }
    return std::lexicographical_compare(preces_.begin(), preces_.end(), p.preces_.begin(), p.preces_.end());
  }

  Path operator+(const Path& p) const {
    if (!passable() || !p.passable()) {
      return {};
    }
    std::vector<PreceInfo> preces;
    std::merge(preces_.begin(), preces_.end(), p.preces_.begin(), p.preces_.end(), std::back_inserter(preces), std::greater<PreceInfo>());
    return preces;
  }

  friend std::ostream& operator<<(std::ostream& os, const Path& path) {
    if (path.preces_.empty()) {
      os << "Empty path";
    } else {
      std::copy(path.preces_.begin(), path.preces_.end(), std::ostream_iterator<PreceInfo>(os, ", "));
    }
    return os;
  }

  bool passable() const { return !preces_.empty(); }

  const std::vector<PreceInfo>& preces() const { return preces_; }

 private:
  std::vector<PreceInfo> preces_;
};

class ConflictGraph {
 public:
  ConflictGraph(const uint64_t trans_num) : nodes_() {
    nodes_.reserve(trans_num);
    for (uint64_t trans_id = 0; trans_id < trans_num; ++trans_id) {
      nodes_.emplace_back(trans_id);
    }
  }

  void Insert(const std::pair<uint64_t, uint32_t>& pre_trans_id_and_order, const std::pair<uint64_t, uint32_t>& trans_id_and_order,
      const uint64_t item_id, PreceType type) {
    const uint64_t pre_trans_id = pre_trans_id_and_order.first;
    const uint64_t trans_id = trans_id_and_order.first;
    if (pre_trans_id == trans_id) {
      return;
    }
    if (type == PreceType::WW || type == PreceType::WCW) {
      if (!nodes_[pre_trans_id].is_committed().has_value()) {
        type = PreceType::WW;
      } else if (nodes_[pre_trans_id].is_committed().value()) {
        type = PreceType::WCW;
      } else {
        return; // W1A2W1 does not have T2->T1 precedence
      }
    } else if (type == PreceType::WR || type == PreceType::WCR) {
      if (!nodes_[pre_trans_id].is_committed().has_value()) {
        type = PreceType::WR;
      } else if (nodes_[pre_trans_id].is_committed().value()) {
        type = PreceType::WCR;
      } else {
        return; // W1A2R1 does not have T2->T1 precedence
      }
    } else if ((type == PreceType::WA || type == PreceType::WC) &&
        nodes_[pre_trans_id].is_committed().has_value() &&
        !nodes_[pre_trans_id].is_committed().value()) {
      return; // W1W2A2A1 and W1W2A2C1 do not have T2->T1 precedence
    }
    nodes_[trans_id].AddPreTrans(pre_trans_id, item_id, type, pre_trans_id_and_order.second, trans_id_and_order.second);
  }

  void Insert(const std::map<uint64_t, uint32_t>& pre_trans_id_set, const std::pair<uint64_t, uint32_t>& trans_id_and_order,
      const uint64_t item_id, const PreceType type) {
    for (const auto& pre_trans_id_and_order : pre_trans_id_set) {
      Insert(pre_trans_id_and_order, trans_id_and_order, item_id, type);
    }
  }

  bool HasCycle() {
    RemoveNodesNotInCycle_();
    for (const ConflictGraphNode& node : nodes_) {
      if (!node.HasNoPreTrans()) {
        return true;
      }
    }
    return false;
  }

  std::optional<bool>& is_committed(const uint64_t trans_id) { return nodes_[trans_id].is_committed(); }

  // Find the first conflict cycle in history. The latest precedence is at the head of return path.
  Path MinCycleByFloyd() const {
    const size_t trans_num = nodes_.size();
    Path matrix[trans_num][trans_num];

    // init matrix
    for (uint64_t pre_trans_id = 0; pre_trans_id < trans_num; ++pre_trans_id) {
      for (uint64_t trans_id = 0; trans_id < trans_num; ++trans_id) {
        auto& path = matrix[pre_trans_id][trans_id];
        auto& pre_trans_set = nodes_[trans_id].pre_trans_set();
        if (const auto it = pre_trans_set.find(pre_trans_id); it != nodes_[trans_id].pre_trans_set().end()) {
          path = it->second;
        }
      }
    }

    static auto update_path = [](Path& path, Path&& new_path) {
      if (new_path < path) {
        path = std::move(new_path); // do not use std::min because there is a copy cost when assign self
      }
    };

    Path min_cycle;
    for (uint64_t mid = 0; mid < trans_num; ++mid) {
      // find mini cycle when pass mid node
      for (uint64_t start = 0; start < mid; ++start) {
        update_path(min_cycle, matrix[start][mid] + matrix[mid][start]);
        for (uint64_t end = 0; end < mid; ++end) {
          if (start != end) {
            update_path(min_cycle, matrix[start][end] + matrix[end][mid] + matrix[mid][start]);
          }
        }
      }

      // update direct path
      for (uint64_t start = 0; start < trans_num; ++start) {
        for (uint64_t end = 0; end < trans_num; ++end) {
          update_path(matrix[start][end], matrix[start][mid] + matrix[mid][end]);
        }
      }
    }
    return min_cycle;
  }

 private:
  // keep remove nodes of which indegree is 0
  void RemoveNodesNotInCycle_() {
    bool removed_node = false;
    do {
      removed_node = false;
      for (ConflictGraphNode& node_to_remove : nodes_) {
        if (!node_to_remove.HasNoPreTrans() || node_to_remove.IsRemoved()) {
          continue;
        }
        removed_node = true;
        node_to_remove.Remove();
        for (ConflictGraphNode& node : nodes_) {
          node.RemovePreTrans(node_to_remove.trans_id());
        }
      }
    } while (removed_node);
  }

 private:
  std::vector<ConflictGraphNode> nodes_;
};

class ConflictSerializableAlgorithm : public HistoryAlgorithm {
 public:
  ConflictSerializableAlgorithm() : HistoryAlgorithm("Conflict Serializable"), anomaly_counts_{0}, no_anomaly_count_(0) {}
  virtual ~ConflictSerializableAlgorithm()
  {
    std::cout.setf(std::ios::right);
    std::cout.precision(4);

    static auto print_percent = [](auto&& value, auto&& total) {
      std::cout << std::setw(10) << static_cast<double>(value) / total * 100 << "% = " << std::setw(5) << value << " / " << std::setw(5) << total;
    };

    const auto anomaly_count = std::accumulate(anomaly_counts_.begin(), anomaly_counts_.end(), 0);
    std::cout << "=== Conflict Serializable ===" << std::endl;

    std::cout << std::setw(25) << "True Rollback: ";
    print_percent(anomaly_count, anomaly_count + no_anomaly_count_);
    std::cout << std::endl;

    std::vector<std::pair<AnomalyType, uint32_t>> sorted_anomaly_counts_;
    for (const auto anomaly : AllAnomalyType) {
      sorted_anomaly_counts_.emplace_back(anomaly, anomaly_counts_.at(static_cast<uint32_t>(anomaly)));
    }
    std::sort(sorted_anomaly_counts_.begin(), sorted_anomaly_counts_.end(), [](auto&& _1, auto&& _2) { return _1.second > _2.second; });
    for (const auto& [anomaly, count] : sorted_anomaly_counts_) {
      std::cout << std::setw(25) << (std::string("[") + ToString(anomaly) + "] ");
      print_percent(count, anomaly_count);
      std::cout << std::endl;
    }
  }

  virtual bool Check(const History& history, std::ostream* const os) const override {
    ConflictGraph graph(history.trans_num());
    std::vector<std::map<uint64_t, uint32_t>> read_trans_set_for_items(history.item_num());
    std::vector<std::map<uint64_t, uint32_t>> write_trans_set_for_items(history.item_num());
    std::vector<std::set<uint64_t>> write_item_set_for_transs(history.trans_num());
    for (size_t i = 0, size = history.size(); i < size; ++i) {
      const Operation& operation = history.operations()[i];
      const uint64_t trans_id = operation.trans_id();
      if (operation.IsPointDML()) {
        const uint64_t item_id = operation.item_id();
        auto& read_trans_set = read_trans_set_for_items[item_id];
        auto& write_trans_set = write_trans_set_for_items[item_id];
        if (Operation::Type::READ == operation.type()) {
          graph.Insert(write_trans_set, {trans_id, i}, item_id, PreceType::WR); // WR or WCR is both possible
          read_trans_set.emplace(trans_id, i);
        } else if (Operation::Type::WRITE == operation.type()) {
          // TODO: the two different type precedences happens at the same time
          graph.Insert(write_trans_set, {trans_id, i}, item_id, PreceType::WW); // WW or WCW is both possible
          graph.Insert(read_trans_set, {trans_id, i}, item_id, PreceType::RW);
          write_trans_set.emplace(trans_id, i);
          write_item_set_for_transs[trans_id].insert(item_id); // record for check WA or WC
        }
      } else if (Operation::Type::SCAN_ODD == operation.type()) {
        // TODO: realize scan odd
      } else if (Operation::Type::ABORT == operation.type()) {
        graph.is_committed(trans_id) = false;
        for (const uint64_t write_item : write_item_set_for_transs[trans_id]) {
          // TODO: the two different type precedences happens at the same time
          graph.Insert(read_trans_set_for_items[write_item], {trans_id, i}, write_item, PreceType::RA);
          graph.Insert(write_trans_set_for_items[write_item], {trans_id, i}, write_item, PreceType::WA);
        }
      } else if (Operation::Type::COMMIT == operation.type()) {
        graph.is_committed(trans_id) = true;
        for (const uint64_t write_item : write_item_set_for_transs[trans_id]) {
          graph.Insert(write_trans_set_for_items[write_item], {trans_id, i}, write_item, PreceType::WC);
        }
      }
    }

    const bool has_cycle = graph.HasCycle();
    if (has_cycle) {
      const auto cycle = graph.MinCycleByFloyd();
      const auto anomaly = IdentifyAnomaly_(cycle.preces());
      TRY_LOG(os) << "[" << anomaly << "] " << cycle;
      ++(anomaly_counts_.at(static_cast<uint32_t>(anomaly)));
    } else {
      ++no_anomaly_count_;
    }

    return !has_cycle;
  }

 private:
  static AnomalyType IdentifyAnomaly_(const std::vector<PreceInfo>& preces) {
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WA || prece.type() == PreceType::WC; })) {
      // WA and WC precedence han only appear
      return AnomalyType::DIRTY_WRITE;
    } else if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::RA; })) {
      return AnomalyType::DIRTY_READ;
    } else if (preces.size() > 2) {
      return IdentifyAnomalyMultiple_(preces);
    } else if (preces[0].item_id() == preces[1].item_id()) {
      // when build path, later happened precedence is sorted to front. preces1 is happens before preces0
      return IdentifyAnomalySimple_(preces[1].type(), preces[0].type());
    } else {
      return IdentifyAnomalyDouble_(preces[1].type(), preces[0].type());
    }
  }

  // require type1 precedence happens before type2 precedence
  static AnomalyType IdentifyAnomalySimple_(const PreceType early_type, const PreceType later_type) {
    if (early_type == PreceType::WW && (later_type == PreceType::WW || later_type == PreceType::WCW)) {
      return AnomalyType::INTERMEDIATE_WRITE;
    } else if (early_type == PreceType::WW && (later_type == PreceType::WR || later_type == PreceType::WCR)) {
      return AnomalyType::LOST_SELF_UPDATE;
    } else if (early_type == PreceType::RW && later_type == PreceType::WW) {
      return AnomalyType::LOST_UPDATE_1;
    } else if (early_type == PreceType::WR && later_type == PreceType::RW) {
      return AnomalyType::INTERMEDIATE_READ;
    } else if (early_type == PreceType::RW && (later_type == PreceType::WR || later_type == PreceType::WCR)) {
      return AnomalyType::NON_REPEATABLE_READ;
    } else if (early_type == PreceType::RW && later_type == PreceType::WCW) {
      return AnomalyType::LOST_UPDATE_2;
    } else {
      return AnomalyType::UNKNOWN_SINGLE;
    }
  }

  static AnomalyType IdentifyAnomalyDouble_(const PreceType early_type, const PreceType later_type) {
    const auto any_order = [early_type, later_type](const PreceType type1, const PreceType type2) {
      return ((early_type == type1 && later_type == type2) ||
              (early_type == type2 && later_type == type1));
    };
    if (any_order(PreceType::WR, PreceType::WW) || (early_type == PreceType::WW && later_type == PreceType::WCR)) {
      return AnomalyType::DOUBLE_WRITE_SKEW_1;
    } else if (any_order(PreceType::RW, PreceType::WW)) {
      return AnomalyType::READ_WRITE_SKEW_1;
    } else if (early_type == PreceType::WW && (later_type == PreceType::WW || later_type == PreceType::WCW)) {
      return AnomalyType::FULL_WRITE_SKEW;
    } else if (early_type == PreceType::WR && (later_type == PreceType::WR || later_type == PreceType::WCR)) {
      return AnomalyType::WRITE_READ_SKEW;
    } else if (early_type == PreceType::WR && later_type == PreceType::WCW) {
      return AnomalyType::DOUBLE_WRITE_SKEW_2;
    } else if (any_order(PreceType::RW, PreceType::WR) || (early_type == PreceType::RW && later_type == PreceType::WCR)) {
      return AnomalyType::READ_SKEW;
    } else if (early_type == PreceType::RW && later_type == PreceType::WCW) {
      return AnomalyType::READ_WRITE_SKEW_2;
    } else if (early_type == PreceType::RW && later_type == PreceType::RW) {
      return AnomalyType::WRITE_SKEW;
    } else {
      return AnomalyType::UNKNOWN_DOUBLE;
    }
  }

  static AnomalyType IdentifyAnomalyMultiple_(const std::vector<PreceInfo>& preces) {
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WW; })) {
      return AnomalyType::STEP_WAT;
    }
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WR || prece.type() == PreceType::WCR; })) {
      return AnomalyType::STEP_RAT;
    }
    return AnomalyType::STEP_IAT;
  }

  mutable std::array<std::atomic<uint64_t>, AnomalyTypeCount> anomaly_counts_;
  mutable uint64_t no_anomaly_count_;
};

}  // namespace ttts

#endif
