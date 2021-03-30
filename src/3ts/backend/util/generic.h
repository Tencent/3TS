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

ENUM_BEGIN(Intensity)
ENUM_MEMBER(Intensity, NONE_HAVE)
ENUM_MEMBER(Intensity, ALL_HAVE)
ENUM_MEMBER(Intensity, NO_LIMIT)
ENUM_END(Intensity)

ENUM_BEGIN(TclPosition)
ENUM_MEMBER(TclPosition, TAIL)
ENUM_MEMBER(TclPosition, ANYWHERE)
ENUM_MEMBER(TclPosition, NOWHERE)
ENUM_END(TclPosition)

#endif
#endif
#endif

#ifndef UTIL_GENERIC_H
#define UTIL_GENERIC_H
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace ttts {

enum Anomally {
  // I, II
  DIRTY_WRITE,
  EDGE_CROESS,
  LOST_UPDATE,
  READ_SKEW,
  READ_WRITE_SKEW,
  THREE_TRANS_WRITE_SKEW,
  MULTI_TRANS_ANOMALY,
  // WSI
  WRITE_SKEW,
  WW_CONFLICT,
  // SSI
  RW_CONFLICT,
  // BOCC FOCC UNKNOWN
  UNKNOWN
};

std::unordered_map<int, std::string> Anomally2Name = {
    // I, II
    {DIRTY_WRITE, "DIRTY_WRITE"},
    {EDGE_CROESS, "EDGE_CROESS"},
    {LOST_UPDATE, "LOST_UPDATE"},
    {READ_SKEW, "READ_SKEW"},
    {READ_WRITE_SKEW, "READ_WRITE_SKEW"},
    {THREE_TRANS_WRITE_SKEW, "THREE_TRANS_WRITE_SKEW"},
    {MULTI_TRANS_ANOMALY, "MULTI_TRANS_ANOMALY"},
    // WSI
    {WRITE_SKEW, "WRITE_SKEW"},
    {WW_CONFLICT, "WW_CONFLICT"},
    // SSI
    {RW_CONFLICT, "RW_CONFLICT"},
    // BOCC FOCC UNKNOWN
    {UNKNOWN, "UNKNOWN"}};

std::ostream& operator<<(std::ostream& os, const Anomally e) {
  switch (e) {
    case DIRTY_WRITE:
      return os << "DIRTY_WRITE";
    case EDGE_CROESS:
      return os << "EDGE_CROESS";
    case LOST_UPDATE:
      return os << "LOST_UPDATE";
    case READ_SKEW:
      return os << "READ_SKEW";
    case READ_WRITE_SKEW:
      return os << "READ_WRITE_SKEW";
    case THREE_TRANS_WRITE_SKEW:
      return os << "THREE_TRANS_WRITE_SKEW";
    case MULTI_TRANS_ANOMALY:
      return os << "MULTI_TRANS_ANOMALY";
    case WRITE_SKEW:
      return os << "WRITE_SKEW";
    case WW_CONFLICT:
      return os << "WW_CONFLICT";
    case RW_CONFLICT:
      return os << "RW_CONFLICT";
    default:
      return os << "UNKNOWN";
  }
}

enum class SerializeReadPolicy { UNCOMMITTED_READ, COMMITTED_READ, REPEATABLE_READ, SI_READ };

class Operation {
 public:
  enum class Type : char {
    UNKNOWN = '?',
    READ = 'R',
    WRITE = 'W',
    COMMIT = 'C',
    ABORT = 'A',
    SCAN_ODD = 'S'
  };
  using ReadTypeConstant = std::integral_constant<Type, Type::READ>;
  using WriteTypeConstant = std::integral_constant<Type, Type::WRITE>;
  using CommitTypeConstant = std::integral_constant<Type, Type::COMMIT>;
  using AbortTypeConstant = std::integral_constant<Type, Type::ABORT>;
  using ScanOddTypeConstant = std::integral_constant<Type, Type::SCAN_ODD>;
  Operation() : type_(Type::UNKNOWN), trans_id_(0) {}
  Operation(const std::integral_constant<Type, Type::COMMIT> dtl_type, const uint64_t trans_id)
    : type_(dtl_type.value), trans_id_(trans_id) {}
  Operation(const std::integral_constant<Type, Type::ABORT> dtl_type, const uint64_t trans_id)
    : type_(dtl_type.value), trans_id_(trans_id) {}
  Operation(const std::integral_constant<Type, Type::SCAN_ODD> dtl_type, const uint64_t trans_id)
    : type_(dtl_type.value), trans_id_(trans_id) {}
  Operation(const std::integral_constant<Type, Type::READ> dml_type, const uint64_t trans_id,
            const uint64_t item_id, const std::optional<uint64_t> version = {})
      : type_(dml_type.value), trans_id_(trans_id), item_id_(item_id), version_(version) {}
  Operation(const std::integral_constant<Type, Type::WRITE> dml_type, const uint64_t trans_id,
            const uint64_t item_id, const std::optional<uint64_t> version = {})
      : type_(dml_type.value), trans_id_(trans_id), item_id_(item_id), version_(version) {}
  Operation& operator=(const Operation& operation) = default;
  virtual ~Operation() {}

  Type type() const { return type_; }
  uint64_t trans_id() const { return trans_id_; }
  uint64_t item_id() const { return item_id_.value(); }
  uint64_t version() const { return version_.value(); }
  void SetTransId(uint64_t trans_id) { trans_id_ = trans_id; }
  void SetItemId(const uint64_t item_id) {
    if (IsTCL()) {
      throw "TCL operations update item id is meaningless";
    }
    item_id_ = item_id;
  }
  void UpdateVersion(const uint64_t version) {
    if (IsTCL()) {
      throw "DML operations update version is meaningless";
    }
    version_ = version;
  }

  friend std::ostream& operator<<(std::ostream& os, const Operation& operation) {
    os << static_cast<char>(operation.type_) << operation.trans_id_;
    if (operation.IsPointDML()) {
      if (operation.item_id_.value() >= 26) {
        throw "Not support item_id equal or larger than 26 yet";
      }
      os << static_cast<char>('a' + operation.item_id_.value());
    }

    return os;
  }

  friend std::istream& operator>>(std::istream& is, Type& type) {
    char c;
    is >> c;
    switch (c) {
      case 'W':
        type = Operation::Type::WRITE;
        break;
      case 'R':
        type = Operation::Type::READ;
        break;
      case 'C':
        type = Operation::Type::COMMIT;
        break;
      case 'A':
        type = Operation::Type::ABORT;
        break;
      case 'S':
        type = Operation::Type::SCAN_ODD;
        break;
      default:
        type = Operation::Type::UNKNOWN;
        if (c != '\0') {
          std::cerr << "Unknonw operation type character: " << c << ". Supported operations: R W C A" << std::endl;
        }
    }
    return is;
  }

  friend std::istream& operator>>(std::istream& is, Operation& operation) {
    if (!(is >> operation.type_)) {
      is.setstate(std::ios::failbit);
      return is;
    }
    if (!(is >> operation.trans_id_)) {
      is.setstate(std::ios::failbit);
      std::cerr << "Transaction ID character must be a number" << std::endl;
      return is;
    } else {
      if (operation.trans_id_ >= 10 || operation.trans_id_ < 0) {
        is.setstate(std::ios::failbit);
        std::cerr << "Transaction ID must be less than 10 and more than 0." << std::endl;
        return is;
      }
    }
    if (char item_c; operation.type_ == Type::WRITE || operation.type_ == Type::READ) {
      if (!(is >> item_c) || !std::islower(item_c)) {
        is.setstate(std::ios::failbit);
        std::cerr << "Data Item must be lowercase letter" << std::endl;
        return is;
      }
      operation.item_id_ = item_c - 'a';
    }
    return is;
  }

  bool IsPointDML() const { return IsPointDML(type_); }
  bool IsTCL() const { return IsTCL(type_); }
  static bool IsPointDML(const Type& type) { return type == Type::READ || type == Type::WRITE; }
  static bool IsTCL(const Type& type) { return type == Type::COMMIT || type == Type::ABORT; }
  // surport std::map
  bool operator<(const Operation& r) const {
    if (trans_id() == r.trans_id()) {
      uint64_t l_item_id = item_id_.has_value() ? item_id() : -1;
      uint64_t r_item_id = r.item_id_.has_value() ? r.item_id() : -1;
      if (l_item_id == r_item_id) {
        uint64_t l_version = version_.has_value() ? version() : -1;
        uint64_t r_version = r.version_.has_value() ? r.version() : -1;
        if (l_version == r_version) return type() < r.type();
        return l_version < r.version();
      }
      return l_item_id < r_item_id;
    }
    return trans_id() < r.trans_id();
  }

 private:
  Type type_;
  uint64_t trans_id_;
  std::optional<uint64_t> item_id_;
  std::optional<uint64_t> version_;  // version_ identify a unique version, but it CANNOT be
                                     // compared to judge new or old
};

class History {
 public:
  History() : History(0, 0, {}) {}
  History(const uint64_t trans_num, const uint64_t item_num,
          const std::vector<Operation>& operations)
      : trans_num_(trans_num),
        item_num_(item_num),
        operations_(operations),
        abort_trans_num_(0),
        anomaly_name_("") {}
  History(const uint64_t trans_num, const uint64_t item_num, std::vector<Operation>&& operations)
      : trans_num_(trans_num),
        item_num_(item_num),
        operations_(operations),
        abort_trans_num_(0),
        anomaly_name_("") {}
  History(const uint64_t trans_num, const uint64_t item_num,
          const std::vector<Operation>& operations, const uint64_t abort_trans_num)
      : trans_num_(trans_num),
        item_num_(item_num),
        operations_(operations),
        abort_trans_num_(abort_trans_num),
        anomaly_name_("") {}
  History(History&& history) = default;
  History(const History& history) = default;
  ~History() {}

  History& operator=(History&& history) = default;
  History operator+(const History& history) const {
    std::vector<Operation> new_operations = operations_;
    for (const auto& operation : history.operations_) {
      new_operations.push_back(operation);
    }
    return History(std::max(trans_num_, history.trans_num_), std::max(item_num_, history.item_num_),
        std::move(new_operations), abort_trans_num_ + history.abort_trans_num_);
  }
  std::vector<Operation>& operations() { return operations_; }
  const std::vector<Operation>& operations() const { return operations_; }
  uint64_t trans_num() const { return trans_num_; }
  uint64_t abort_trans_num() const { return abort_trans_num_; }
  uint64_t item_num() const { return item_num_; }
  size_t size() const { return operations_.size(); }
  void set_anomaly_name(const std::string& anomaly_name) { anomaly_name_ = anomaly_name; }
  std::string anomaly_name() const { return anomaly_name_; }
  friend std::ostream& operator<<(std::ostream& os, const History& history) {
    for (const Operation& operation : history.operations_) {
      os << operation << ' ';
    }
    return os;
  }

  friend std::istream& operator>>(std::istream& is, History& history) {
    std::string s;
    if (std::getline(is, s)) {
      std::stringstream ss(s);
      std::vector<Operation> operations;
      uint64_t trans_num = 0;
      uint64_t item_num = 0;
      std::unordered_map<uint64_t, uint64_t> trans_num_map;
      std::unordered_map<uint64_t, uint64_t> item_num_map;
      for (std::stringstream ss(s); !ss.eof() && !ss.fail();) {
        Operation operation;
        if (Operation operation; ss >> operation) {
          if (trans_num_map.count(operation.trans_id()) == 0) {
            trans_num_map[operation.trans_id()] = trans_num_map.size();
          }
          operation.SetTransId(trans_num_map[operation.trans_id()]);
          if (operation.IsPointDML()) {
            if (item_num_map.count(operation.item_id()) == 0) {
              item_num_map[operation.item_id()] = item_num_map.size();
            }
            operation.SetItemId(item_num_map[operation.item_id()]);
          }
          operations.emplace_back(operation);
        }
      }
      trans_num = trans_num_map.size();
      item_num = item_num_map.size();
      if (ss.fail()) {
        std::cout << "Invalid history: \'" << s << "\'" << std::endl;
      } else {
        history = History(trans_num, item_num, operations);
      }
    }
    return is;
  }

  Operation& operator[](const size_t index) { return operations_[index]; }

  void UpdateWriteVersions() {
    std::vector<uint64_t> item_version(item_num_, 0);
    for (Operation& operation : operations_) {
      if (operation.type() == Operation::Type::WRITE) {
        operation.UpdateVersion(++item_version[operation.item_id()]);
      }
    }
  }

  // update write version, clean up read version
  void FillWriteVersions() {
    std::vector<uint64_t> item_version(item_num_, 0);
    for (Operation& operation : operations_) {
      if (operation.type() == Operation::Type::WRITE) {
        operation.UpdateVersion(++item_version[operation.item_id()]);
      } else if (operation.type() == Operation::Type::READ) {
        operation.UpdateVersion(-1);  // clear read version as -1
      }
    }
  }

 private:
  uint64_t trans_num_;
  uint64_t abort_trans_num_;
  uint64_t item_num_;
  std::vector<Operation> operations_;
  std::string anomaly_name_;
};

#define ENUM_FILE "./generic.h"
#include "extend_enum.h"

struct Options {
  uint64_t trans_num;
  uint64_t item_num;

  uint64_t subtask_num;
  uint64_t subtask_id;
  uint64_t max_dml;

  bool with_abort;
  TclPosition tcl_position;
  bool allow_empty_trans;
  bool dynamic_history_len;

  Intensity with_scan;
  Intensity with_write;
};

}  // namespace ttts
#endif
