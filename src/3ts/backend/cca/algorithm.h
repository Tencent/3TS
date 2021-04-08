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
#include <fstream>

#include "../util/generic.h"

namespace ttts {

#define TRY_LOG(os) \
  if ((os) != nullptr) *(os)

class HistoryAlgorithm {
 public:
  HistoryAlgorithm(const std::string& name) : name_(name) {}
  virtual ~HistoryAlgorithm() {}

  virtual bool Check(const History& history, std::ostream* const os = nullptr) const = 0;
  virtual void Statistics() const {};
  std::string name() const { return name_; }

  const std::string name_;
};

class RollbackRateAlgorithm : public HistoryAlgorithm {
 public:
  RollbackRateAlgorithm(const std::string& name) : HistoryAlgorithm(name) {}
  virtual ~RollbackRateAlgorithm() {}

  virtual bool Check(const History& history, std::ostream* const os = nullptr) const {
    return RollbackNum(history, os).size() == 0;
  }
  virtual std::vector<int> RollbackNum(const History& history, std::ostream* const os = nullptr) const = 0;
};
}  // namespace ttts
