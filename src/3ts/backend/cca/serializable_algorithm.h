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

enum class SerializeLevel {
  ALL_SAME,     // committed transactions and aborted transactions must read same versions
  COMMIT_SAME,  // only committed transactions must read same versions
  FINAL_SAME    // reading same is not necessary
};

template <SerializeLevel L>
std::string ToString();
template <SerializeReadPolicy R>
std::string ToString();

template <>
std::string ToString<SerializeLevel::ALL_SAME>() {
  return "ALL_SAME";
}
template <>
std::string ToString<SerializeLevel::COMMIT_SAME>() {
  return "COMMIT_SAME";
}
template <>
std::string ToString<SerializeLevel::FINAL_SAME>() {
  return "FINAL_SAME";
}
template <>
std::string ToString<SerializeReadPolicy::UNCOMMITTED_READ>() {
  return "UNCOMMITTED_READ";
}
template <>
std::string ToString<SerializeReadPolicy::COMMITTED_READ>() {
  return "COMMITTED_READ";
}
template <>
std::string ToString<SerializeReadPolicy::REPEATABLE_READ>() {
  return "REPEATABLE_READ";
}
template <>
std::string ToString<SerializeReadPolicy::SI_READ>() {
  return "SI_READ";
}

// The executing result of one history. The history is serializable if the two results of original
// history and serialized history are same.
// The history result contains each transaction's read result and each item's final version.
class HistoryResult {
 public:
  HistoryResult(const uint64_t trans_num) : trans_results_(trans_num) {}
  HistoryResult(HistoryResult&&) = default;
  ~HistoryResult() {}
  HistoryResult& operator=(HistoryResult&&) = default;

  // Check if all transactions read the same versions
  bool ReadEqual(const HistoryResult& result) const { return trans_results_ == result.trans_results_; }

  // Check if all committed transaction read the same versions, ignore aborted transactions
  bool CommitReadEqual(const HistoryResult& result) const {
    if (trans_results_.size() != result.trans_results_.size()) {
      return false;
    }
    for (uint64_t trans_id = 0; trans_id < trans_results_.size(); ++trans_id) {
      if ((trans_results_[trans_id].committed_ !=
           result.trans_results_[trans_id].committed_) ||  // different transaction status
          (trans_results_[trans_id].committed_ &&
           trans_results_[trans_id].read_results_ != result.trans_results_[trans_id].read_results_)) {
        return false;
      }  // different read results
    }
    return true;
  }

  // Check if the final versions of each item are same.
  bool FinalEqual(const HistoryResult& result) const { return item_final_versions_ == result.item_final_versions_; }

  // Add version to read result to compare with other history results.
  void PushTransReadResult(const uint64_t trans_id, const uint64_t item_id, const uint64_t version) {
    assert(trans_id < trans_results_.size());
    trans_results_[trans_id].read_results_.emplace_back(std::vector<std::pair<uint64_t, uint64_t>>{{item_id, version}});
  }

  // Add versions to read result to compare with other history results.
  void PushTransReadResult(const uint64_t trans_id, std::vector<std::pair<uint64_t, uint64_t>>&& item_vers) {
    assert(trans_id < trans_results_.size());
    trans_results_[trans_id].read_results_.emplace_back(std::move(item_vers));
  }

  // Mark transaction as committed transaction.
  void SetTransCommitted(const uint64_t trans_id, const bool committed) {
    assert(trans_id < trans_results_.size());
    trans_results_[trans_id].committed_ = committed;
  }

  // Record the final version of each variables to compare with other history results.
  void SetItemFinalVersions(std::vector<uint64_t>&& versions) { item_final_versions_ = std::move(versions); }

 private:
  // Record each version the transaction has been read and whether the transaction is committed or
  // aborted.
  struct TransResult {
    bool operator==(const TransResult& result) const {
      return read_results_ == result.read_results_ && committed_ == result.committed_;
    }
    std::vector<std::vector<std::pair<uint64_t, uint64_t>>> read_results_;
    bool committed_;
  };

  std::vector<TransResult> trans_results_;     // size = trans_num
  std::vector<uint64_t> item_final_versions_;  // size = item_num
};

// Traverse all serialized histories and pass each history to handle. Once handle returns true, then
// return true.
bool Serialize(const History& history, const std::function<bool(History&&)>& handle) {
  std::vector<std::vector<Operation>> trans_operations(history.trans_num());
  for (const Operation& operation : history.operations()) {
    trans_operations[operation.trans_id()].push_back(operation);
  }
  std::vector<uint64_t> trans_order;
  for (uint64_t trans_id = 0; trans_id < history.trans_num(); ++trans_id) {
    trans_order.push_back(trans_id);
  }
  do {
    std::vector<Operation> operations;
    for (uint64_t trans_id : trans_order)
      for (const Operation& operation : trans_operations[trans_id]) {
        operations.push_back(operation);
      }
    if (handle(History(history.trans_num(), history.item_num(), std::move(operations)))) {
      return true;
    }
  } while (std::next_permutation(trans_order.begin(), trans_order.end()));
  return false;
}

template <SerializeLevel>
inline static bool ResultsEqual(const HistoryResult& _1, const HistoryResult& _2);

template <SerializeReadPolicy>
inline static HistoryResult Result(const History& history);

// Main entry of the algorithm.
template <SerializeLevel L, SerializeReadPolicy R>
class HistorySerializableAlgorithm : public HistoryAlgorithm {
 public:
  HistorySerializableAlgorithm() : HistoryAlgorithm("Serialize " + ToString<L>() + " " + ToString<R>()) {}
  virtual ~HistorySerializableAlgorithm() {}

  virtual bool Check(const History& history, std::ostream* const os) const override {
    History history_with_write_version = history;
    history_with_write_version.UpdateWriteVersions();
    const auto check_serial = [this, &history_with_write_version, &os](History&& serial_history) {
      // Check if the result of original history is same as serialized history.
      if (ResultsEqual<L>(Result<R>(history_with_write_version), Result<R>(serial_history))) {
        TRY_LOG(os) << serial_history;
        return true;  // break
      }
      return false;  // continue
    };
    return Serialize(history_with_write_version, check_serial);
  }

 private:
};

template <>
bool ResultsEqual<SerializeLevel::ALL_SAME>(const HistoryResult& _1, const HistoryResult& _2) {
  return _1.ReadEqual(_2) && _1.FinalEqual(_2);
}

template <>
bool ResultsEqual<SerializeLevel::COMMIT_SAME>(const HistoryResult& _1, const HistoryResult& _2) {
  return _1.CommitReadEqual(_2) && _1.FinalEqual(_2);
}

template <>
bool ResultsEqual<SerializeLevel::FINAL_SAME>(const HistoryResult& _1, const HistoryResult& _2) {
  return _1.FinalEqual(_2);
}

// Call read_version for each item to determine which version to read and check whether is odd.
std::vector<std::pair<uint64_t, uint64_t>> ScanOdd(const uint64_t item_num,
                                                   const std::function<uint64_t(const uint64_t)>& read_version) {
  std::vector<std::pair<uint64_t, uint64_t>> scan_item_vers;
  for (uint64_t item_id = 0; item_id < item_num; ++item_id) {
    const uint64_t version = read_version(item_id);
    if (version % 2 == 0) {
      scan_item_vers.emplace_back(item_id, version);
    }
  }
  return scan_item_vers;
}

// Get result of the history with uncomitted read strategy.
// A write version wrote by an uncomitted write transaction, may be read by other read transactions.
// When a transaction writes a version, update the version to item_version_link directly and release
// the old version if the transaction has wrote twice to a same item.
// When a transaction aborts, release all versions the transaction has wrote.
template <>
HistoryResult Result<SerializeReadPolicy::UNCOMMITTED_READ>(const History& history) {
  HistoryResult result(history.trans_num());
  std::vector<std::vector<std::optional<uint64_t>>> trans_write_item_versions(
      history.trans_num(), std::vector<std::optional<uint64_t>>(history.item_num()));
  std::vector<std::vector<std::optional<uint64_t>>> item_version_link(history.item_num(),
                                                                      {0}); /* 0 is always the first version */
  const auto latest_version = [&item_version_link](const uint64_t item_id) {
    const std::vector<std::optional<uint64_t>>& version_link = item_version_link[item_id];
    uint64_t i = version_link.size() - 1;
    for (; i >= 0 && !version_link[i].has_value(); --i)
      ;
    assert(i < version_link.size());
    return version_link[i].value();
  };
  const auto release_version = [&item_version_link](const uint64_t item_id, const uint64_t version) {
    for (std::optional<uint64_t>& cur_version : item_version_link[item_id]) {
      if (cur_version.has_value() && cur_version.value() == version) {
        cur_version = {};
        return;
      }
    }
    assert(false);  // cannot found the version
  };
  for (const Operation& operation : history.operations()) {
    if (operation.type() == Operation::Type::READ) {
      // we did not change version when resort operations, so we use latest version instead of
      // operation.version()
      result.PushTransReadResult(operation.trans_id(), operation.item_id(), latest_version(operation.item_id()));
    } else if (operation.type() == Operation::Type::SCAN_ODD) {
      result.PushTransReadResult(
          operation.trans_id(),
          ScanOdd(history.item_num(), [&latest_version](const uint64_t item_id) { return latest_version(item_id); }));
    } else if (operation.type() == Operation::Type::WRITE) {
      item_version_link[operation.item_id()].push_back(operation.version());
      std::optional<uint64_t>& my_last_write_version =
          trans_write_item_versions[operation.trans_id()][operation.item_id()];
      if (my_last_write_version.has_value()) {
        release_version(operation.item_id(), my_last_write_version.value());
      }
      my_last_write_version = operation.version();
    } else if (operation.type() == Operation::Type::ABORT) {
      for (uint64_t item_id = 0; item_id < history.item_num(); ++item_id) {
        const std::optional<uint64_t>& my_last_write_version = trans_write_item_versions[operation.trans_id()][item_id];
        if (my_last_write_version.has_value()) {
          release_version(item_id, my_last_write_version.value());
        }
      }
      result.SetTransCommitted(operation.trans_id(), false);
    } else if (operation.type() == Operation::Type::COMMIT) {
      result.SetTransCommitted(operation.trans_id(), true);
    } else {
      throw "Unexpected operation type:" + std::to_string(static_cast<char>(operation.type()));
    }
  }

  std::vector<uint64_t> final_versions(history.item_num());
  for (uint64_t item_id = 0; item_id < history.item_num(); ++item_id) {
    final_versions[item_id] = latest_version(item_id);
  }
  result.SetItemFinalVersions(std::move(final_versions));
  return result;
}

// Get result of the history with at least committed read strategy.
// Which version to read depends on read_version callback.
// When a transaction writes a version, record the version to write set only.
// When a transaction commits, update all versions in write set to latest_versions then the versions
// can be seend by other read transactions.
template <typename ReadVersion>
HistoryResult ResultAtLeastCommittedRead(const History& history, ReadVersion&& read_version) {
  HistoryResult result(history.trans_num());
  std::vector<std::vector<std::optional<uint64_t>>> trans_write_item_versions(
      history.trans_num(), std::vector<std::optional<uint64_t>>(history.item_num()));
  std::vector<uint64_t> latest_versions(history.item_num(), 0);
  for (const Operation& operation : history.operations()) {
    if (operation.type() == Operation::Type::READ) {
      result.PushTransReadResult(
          operation.trans_id(), operation.item_id(),
          read_version(operation.trans_id(), operation.item_id(), trans_write_item_versions, latest_versions));
    } else if (operation.type() == Operation::Type::SCAN_ODD) {
      result.PushTransReadResult(
          operation.trans_id(),
          ScanOdd(history.item_num(), std::bind(read_version, operation.trans_id(), std::placeholders::_1,
                                                trans_write_item_versions, latest_versions)));
    } else if (operation.type() == Operation::Type::WRITE) {
      trans_write_item_versions[operation.trans_id()][operation.item_id()] = operation.version();
    } else if (operation.type() == Operation::Type::ABORT) {
      result.SetTransCommitted(operation.trans_id(), false);
    } else if (operation.type() == Operation::Type::COMMIT) {
      for (uint64_t item_id = 0; item_id < history.item_num(); item_id++) {
        std::optional<uint64_t> write_version = trans_write_item_versions[operation.trans_id()][item_id];
        if (write_version.has_value()) {
          latest_versions[item_id] = write_version.value();
        }
      }
      result.SetTransCommitted(operation.trans_id(), true);
    } else {
      throw "Unexpected operation type:" + std::to_string(static_cast<char>(operation.type()));
    }
  }
  result.SetItemFinalVersions(std::move(latest_versions));
  return result;
}

// Get result of the history with committed read strategy.
// Always read the latest version.
template <>
HistoryResult Result<SerializeReadPolicy::COMMITTED_READ>(const History& history) {
  return ResultAtLeastCommittedRead(
      history, [](const uint64_t trans_id, const uint64_t item_id,
                  const std::vector<std::vector<std::optional<uint64_t>>>& trans_write_item_versions,
                  const std::vector<uint64_t>& latest_versions) {
        const std::optional<uint64_t>& write_version = trans_write_item_versions[trans_id][item_id];
        if (write_version.has_value()) {  // item has written
          return write_version.value();
        } else {
          return latest_versions[item_id];
        }
      });
}

// Get result of the history with repeatable read strategy.
// Read the latest version only when the item has not been read or written.
template <>
HistoryResult Result<SerializeReadPolicy::REPEATABLE_READ>(const History& history) {
  std::vector<std::vector<std::optional<uint64_t>>> trans_read_item_versions(
      history.trans_num(), std::vector<std::optional<uint64_t>>(history.item_num()));
  return ResultAtLeastCommittedRead(
      history,
      [&trans_read_item_versions](const uint64_t trans_id, const uint64_t item_id,
                                  const std::vector<std::vector<std::optional<uint64_t>>>& trans_write_item_versions,
                                  const std::vector<uint64_t>& latest_versions) {
        const std::optional<uint64_t>& write_version = trans_write_item_versions[trans_id][item_id];
        if (write_version.has_value()) {  // item has written
          return write_version.value();
        } else {
          std::optional<uint64_t>& read_version = trans_read_item_versions[trans_id][item_id];
          if (read_version.has_value()) {  // item has read
            read_version = latest_versions[item_id];
          }
          return read_version.value();
        }
      });
}

template <>
HistoryResult Result<SerializeReadPolicy::SI_READ>(const History& history) {
  HistoryResult result(history.trans_num());
  std::vector<uint64_t> latest_versions(history.item_num(), 0);
  std::vector<std::vector<uint64_t>> trans_item_versions_backup(history.trans_num());
  std::vector<std::vector<uint64_t>> trans_item_versions_snapshot(history.trans_num());
  for (const Operation& operation : history.operations()) {
    if (trans_item_versions_snapshot[operation.trans_id()].empty()) {
      trans_item_versions_snapshot[operation.trans_id()] = latest_versions;
      trans_item_versions_backup[operation.trans_id()] = latest_versions;
    }
    if (operation.type() == Operation::Type::READ) {
      uint64_t read_version = trans_item_versions_snapshot[operation.trans_id()][operation.item_id()];
      result.PushTransReadResult(operation.trans_id(), operation.item_id(), read_version);
    } else if (operation.type() == Operation::Type::SCAN_ODD) {
      result.PushTransReadResult(
          operation.trans_id(),
          ScanOdd(history.item_num(),
                  [&trans_item_versions_snapshot, trans_id = operation.trans_id() ](const uint64_t item_id) {
            return trans_item_versions_snapshot[trans_id][item_id];
          }));
    } else if (operation.type() == Operation::Type::WRITE) {
      trans_item_versions_snapshot[operation.trans_id()][operation.item_id()] = operation.version();
    } else if (operation.type() == Operation::Type::ABORT) {
      result.SetTransCommitted(operation.trans_id(), false);
    } else if (operation.type() == Operation::Type::COMMIT) {
      for (uint64_t item_id = 0; item_id < history.item_num(); ++item_id) {
        if (trans_item_versions_snapshot[operation.trans_id()][item_id] !=
            trans_item_versions_backup[operation.trans_id()][item_id]) {
          latest_versions[item_id] = trans_item_versions_snapshot[operation.trans_id()][item_id];
        }
      }
      result.SetTransCommitted(operation.trans_id(), true);
    } else {
      throw "Unexpected operation type:" + std::to_string(static_cast<char>(operation.type()));
    }
  }

  result.SetItemFinalVersions(std::move(latest_versions));
  return result;
}
}  // namespace ttts
