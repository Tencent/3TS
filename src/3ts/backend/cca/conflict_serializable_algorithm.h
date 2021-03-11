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
ENUM_MEMBER(AnomalyType, WAT_1_DIRTY_WRITE)
ENUM_MEMBER(AnomalyType, WAT_1_INTERMEDIATE_WRITE)
ENUM_MEMBER(AnomalyType, WAT_1_LOST_SELF_UPDATE)
ENUM_MEMBER(AnomalyType, WAT_1_LOST_UPDATE_1)
// ======== WAT - 2 =========
ENUM_MEMBER(AnomalyType, WAT_2_DOUBLE_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, WAT_2_READ_WRITE_SKEW_1)
ENUM_MEMBER(AnomalyType, WAT_2_FULL_WRITE_SKEW)
// ======== WAT - 3 =========
ENUM_MEMBER(AnomalyType, WAT_STEP)
// ======== RAT - 1 =========
ENUM_MEMBER(AnomalyType, RAT_1_DIRTY_READ)
ENUM_MEMBER(AnomalyType, RAT_1_INTERMEDIATE_READ)
ENUM_MEMBER(AnomalyType, RAT_1_NON_REPEATABLE_READ)
// ======== RAT - 2 =========
ENUM_MEMBER(AnomalyType, RAT_2_WRITE_READ_SKEW)
ENUM_MEMBER(AnomalyType, RAT_2_DOUBLE_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, RAT_2_READ_SKEW)
// ======== RAT - 3 =========
ENUM_MEMBER(AnomalyType, RAT_STEP)
// ======== IAT - 1 =========
ENUM_MEMBER(AnomalyType, IAT_1_LOST_UPDATE_2)
// ======== IAT - 2 =========
ENUM_MEMBER(AnomalyType, IAT_2_READ_WRITE_SKEW_2)
ENUM_MEMBER(AnomalyType, IAT_2_WRITE_SKEW)
// ======== IAT - 3 =========
ENUM_MEMBER(AnomalyType, IAT_STEP)
// ======== Unknown =========
ENUM_MEMBER(AnomalyType, UNKNOWN_1)
ENUM_MEMBER(AnomalyType, UNKNOWN_2)
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
  PreceInfo(const uint64_t pre_trans_id, const uint64_t trans_id, const uint64_t item_id, const PreceType type, const uint32_t order)
    : pre_trans_id_(pre_trans_id), trans_id_(trans_id), item_id_(item_id), type_(type), order_(order) {}
  PreceInfo(const PreceInfo&) = default;

  friend std::ostream& operator<<(std::ostream& os, const PreceInfo prece) {
    return os << 'T' << static_cast<char>('0' + prece.pre_trans_id_) << "-[" << prece.type_ << "-"
              << static_cast<char>('a' + prece.item_id_) << "]->" << 'T' << static_cast<char>('0' + prece.trans_id_);
  }
  bool operator>(const PreceInfo& p) const { return order_ > p.order_; }
  bool operator<(const PreceInfo& p) const { return order_ < p.order_; }

  uint64_t item_id() const { return item_id_; }
  PreceType type() const { return type_; }

 private:
  uint64_t trans_id_;
  uint64_t pre_trans_id_;
  uint64_t item_id_;
  PreceType type_;
  uint32_t order_;
};

class ConflictGraphNode {
 public:
  ConflictGraphNode(const uint64_t trans_id) : trans_id_(trans_id), removed_(false) {}
  ~ConflictGraphNode() {}

  bool HasNoPreTrans() const { return pre_trans_set_.empty(); }
  void AddPreTrans(const uint64_t pre_trans_id, const uint64_t item_id, const PreceType type, const uint32_t order) {
    // we only record the first precedence between the two specific transactions
    pre_trans_set_.try_emplace(pre_trans_id, PreceInfo{pre_trans_id, trans_id_, item_id, type, order});
  }
  void RemovePreTrans(const uint64_t pre_trans_id) { pre_trans_set_.erase(pre_trans_id); }
  void Remove() { removed_ = true; }
  bool IsRemoved() const { return removed_; }
  std::optional<bool>& is_committed() { return is_committed_; }
  const std::optional<bool>& is_committed() const { return is_committed_; }
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

  template <PreceType TYPE>
  void Insert(const uint64_t pre_trans_id, const uint64_t trans_id, const uint64_t item_id, const uint32_t order) {
    if (pre_trans_id == trans_id) {
      return;
    }
    const auto type = RealPreceType_<TYPE>(pre_trans_id);
    if (type.has_value()) {
      nodes_[trans_id].AddPreTrans(pre_trans_id, item_id, *type, order);
    }
  }

  template <PreceType TYPE>
  void Insert(const std::set<uint64_t>& pre_trans_id_set, const uint64_t trans_id, const uint64_t item_id, const uint32_t order) {
    for (const uint64_t pre_trans_id : pre_trans_id_set) {
      Insert<TYPE>(pre_trans_id, trans_id, item_id, order);
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
  std::optional<PreceType> RealPreceType_(const uint64_t pre_trans_id, const std::optional<PreceType>& active_prece_type,
      const std::optional<PreceType>& committed_prece_type, const std::optional<PreceType>& aborted_prece_type) const {
    if (!nodes_[pre_trans_id].is_committed().has_value()) {
      return active_prece_type;
    } else if (nodes_[pre_trans_id].is_committed().value()) {
      return committed_prece_type;
    } else {
      return aborted_prece_type;
    }
  }

  template <PreceType TYPE>
  std::optional<PreceType> RealPreceType_(const uint64_t pre_trans_id) const {
    if constexpr (TYPE == PreceType::WW) {
      return RealPreceType_(pre_trans_id, PreceType::WW, PreceType::WCW, {});
    } else if constexpr (TYPE == PreceType::WR) {
      return RealPreceType_(pre_trans_id, PreceType::WR, PreceType::WCR, {});
    } else {
      return RealPreceType_(pre_trans_id, TYPE, TYPE, {});
    }
  }

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

template <bool IDENTIFY_ANOMALY>
class ConflictSerializableAlgorithm : public HistoryAlgorithm {
 public:
  ConflictSerializableAlgorithm() : HistoryAlgorithm(IDENTIFY_ANOMALY ? "DLI_IDENTIFY OK" : "Conflict Serializable"), anomaly_counts_{0}, no_anomaly_count_(0) {}
  virtual ~ConflictSerializableAlgorithm()
  {
    if (!IDENTIFY_ANOMALY) { return; }
    std::cout.setf(std::ios::right);
    std::cout.precision(4);

    static auto print_percent = [](auto&& value, auto&& total) {
      std::cout << std::setw(10) << static_cast<double>(value) / total * 100 << "% = " << std::setw(5) << value << " / " << std::setw(5) << total;
    };

    const auto anomaly_count = std::accumulate(anomaly_counts_.begin(), anomaly_counts_.end(), 0);
    std::cout << "=== DLI_IDENTIFY ===" << std::endl;

    std::cout << std::setw(30) << "True Rollback: ";
    print_percent(anomaly_count, anomaly_count + no_anomaly_count_);
    std::cout << std::endl;

    std::vector<std::pair<AnomalyType, uint32_t>> sorted_anomaly_counts_;
    for (const auto anomaly : Members<AnomalyType>()) {
      sorted_anomaly_counts_.emplace_back(anomaly, anomaly_counts_.at(static_cast<uint32_t>(anomaly)));
    }
    std::sort(sorted_anomaly_counts_.begin(), sorted_anomaly_counts_.end(), [](auto&& _1, auto&& _2) { return _1.second > _2.second; });
    for (const auto& [anomaly, count] : sorted_anomaly_counts_) {
      std::cout << std::setw(30) << (std::string("[") + ToString(anomaly) + "] ");
      print_percent(count, anomaly_count);
      std::cout << std::endl;
    }
  }

  virtual bool Check(const History& history, std::ostream* const os) const override {
    ConflictGraph graph(history.trans_num());
    std::vector<std::set<uint64_t>> read_trans_set_for_items(history.item_num());
    std::vector<std::set<uint64_t>> write_trans_set_for_items(history.item_num());
    std::vector<std::set<uint64_t>> write_item_set_for_transs(history.trans_num());
    for (size_t i = 0, size = history.size(); i < size; ++i) {
      const Operation& operation = history.operations()[i];
      const uint64_t trans_id = operation.trans_id();
      if (operation.IsPointDML()) {
        const uint64_t item_id = operation.item_id();
        std::set<uint64_t>& read_trans_set = read_trans_set_for_items[item_id];
        std::set<uint64_t>& write_trans_set = write_trans_set_for_items[item_id];
        if (Operation::Type::READ == operation.type()) {
          graph.Insert<PreceType::WR>(write_trans_set, trans_id, item_id, i); // WR or WCR is both possible
          read_trans_set.insert(trans_id);
        } else if (Operation::Type::WRITE == operation.type()) {
          // WW precedence's priority is higher than RW precedence
          graph.Insert<PreceType::WW>(write_trans_set, trans_id, item_id, i); // WW or WCW is both possible
          graph.Insert<PreceType::RW>(read_trans_set, trans_id, item_id, i);
          write_trans_set.insert(trans_id);
          write_item_set_for_transs[trans_id].insert(item_id); // record for check WA or WC
        }
      } else if (Operation::Type::SCAN_ODD == operation.type()) {
        // TODO: realize scan odd
      } else if (Operation::Type::ABORT == operation.type()) {
        graph.is_committed(trans_id) = false;
        for (const uint64_t write_item : write_item_set_for_transs[trans_id]) {
          // WA precedence's priority is higher than RA precedence
          graph.Insert<PreceType::WA>(write_trans_set_for_items[write_item], trans_id, write_item, i);
          graph.Insert<PreceType::RA>(read_trans_set_for_items[write_item], trans_id, write_item, i);
        }
      } else if (Operation::Type::COMMIT == operation.type()) {
        graph.is_committed(trans_id) = true;
        for (const uint64_t write_item : write_item_set_for_transs[trans_id]) {
          graph.Insert<PreceType::WC>(write_trans_set_for_items[write_item], trans_id, write_item, i);
        }
      }
    }

    const bool has_cycle = graph.HasCycle();
    if (IDENTIFY_ANOMALY && has_cycle) {
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
  static uint64_t ItemCount_(const std::vector<PreceInfo>& preces) {
    std::set<uint64_t> item_ids;
    for (const auto prece : preces) {
      item_ids.emplace(prece.item_id());
    }
    return item_ids.size();
  }

  static AnomalyType IdentifyAnomaly_(const std::vector<PreceInfo>& preces) {
    assert(preces.size() >= 2);
    const uint64_t item_count = ItemCount_(preces);
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WA || prece.type() == PreceType::WC; })) {
      // WA and WC precedence han only appear
      return AnomalyType::WAT_1_DIRTY_WRITE;
    } else if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::RA; })) {
      return AnomalyType::RAT_1_DIRTY_READ;
    } else if (item_count == 1) {
      // when build path, later happened precedence is sorted to front
      return IdentifyAnomalySimple_(preces.back().type(), preces.front().type());
    } else if (item_count == 2) {
      return IdentifyAnomalyDouble_(preces.back().type(), preces.front().type());
    } else {
      return IdentifyAnomalyMultiple_(preces);
    }
  }

  // require type1 precedence happens before type2 precedence
  static AnomalyType IdentifyAnomalySimple_(const PreceType early_type, const PreceType later_type) {
    if ((early_type == PreceType::WW || early_type == PreceType::WR) && (later_type == PreceType::WW || later_type == PreceType::WCW)) {
      return AnomalyType::WAT_1_INTERMEDIATE_WRITE; // WW-WW | WR-WW = WWW
    } else if (early_type == PreceType::WR && early_type == PreceType::WW) {
      return AnomalyType::WAT_1_INTERMEDIATE_WRITE; // WR-WW = WWW
    } else if ((early_type == PreceType::WW || early_type == PreceType::WR) && (later_type == PreceType::WR || later_type == PreceType::WCR)) {
      return AnomalyType::WAT_1_LOST_SELF_UPDATE; // WW-WR = WWR
    } else if (early_type == PreceType::RW && later_type == PreceType::WW) {
      return AnomalyType::WAT_1_LOST_UPDATE_1; // RW-WW | RW-RW = RWW
    } else if (early_type == PreceType::WR && later_type == PreceType::RW) {
      return AnomalyType::RAT_1_INTERMEDIATE_READ; // WR-RW = WRW
    } else if (early_type == PreceType::RW && (later_type == PreceType::WR || later_type == PreceType::WCR)) {
      return AnomalyType::RAT_1_NON_REPEATABLE_READ; // RW-WR = RWR
    } else if (early_type == PreceType::RW && later_type == PreceType::WCW) {
      return AnomalyType::IAT_1_LOST_UPDATE_2; // RW-WW(WCW) = RWW
    } else {
      return AnomalyType::UNKNOWN_1;
    }
  }

  static AnomalyType IdentifyAnomalyDouble_(const PreceType early_type, const PreceType later_type) {
    const auto any_order = [early_type, later_type](const PreceType type1, const PreceType type2) {
      return ((early_type == type1 && later_type == type2) ||
              (early_type == type2 && later_type == type1));
    };
    if (any_order(PreceType::WR, PreceType::WW) || (early_type == PreceType::WW && later_type == PreceType::WCR)) {
      return AnomalyType::WAT_2_DOUBLE_WRITE_SKEW_1;
    } else if (any_order(PreceType::RW, PreceType::WW)) {
      return AnomalyType::WAT_2_READ_WRITE_SKEW_1;
    } else if (early_type == PreceType::WW && (later_type == PreceType::WW || later_type == PreceType::WCW)) {
      return AnomalyType::WAT_2_FULL_WRITE_SKEW;
    } else if (early_type == PreceType::WR && (later_type == PreceType::WR || later_type == PreceType::WCR)) {
      return AnomalyType::RAT_2_WRITE_READ_SKEW;
    } else if (early_type == PreceType::WR && later_type == PreceType::WCW) {
      return AnomalyType::RAT_2_DOUBLE_WRITE_SKEW_2;
    } else if (any_order(PreceType::RW, PreceType::WR) || (early_type == PreceType::RW && later_type == PreceType::WCR)) {
      return AnomalyType::RAT_2_READ_SKEW;
    } else if (early_type == PreceType::RW && later_type == PreceType::WCW) {
      return AnomalyType::IAT_2_READ_WRITE_SKEW_2;
    } else if (early_type == PreceType::RW && later_type == PreceType::RW) {
      return AnomalyType::IAT_2_WRITE_SKEW;
    } else {
      return AnomalyType::UNKNOWN_2;
    }
  }

  static AnomalyType IdentifyAnomalyMultiple_(const std::vector<PreceInfo>& preces) {
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WW; })) {
      return AnomalyType::WAT_STEP;
    }
    if (std::any_of(preces.begin(), preces.end(), [](const PreceInfo& prece) { return prece.type() == PreceType::WR || prece.type() == PreceType::WCR; })) {
      return AnomalyType::RAT_STEP;
    }
    return AnomalyType::IAT_STEP;
  }

  mutable std::array<std::atomic<uint64_t>, Count<AnomalyType>()> anomaly_counts_;
  mutable std::atomic<uint64_t> no_anomaly_count_;
};

}  // namespace ttts

#endif
