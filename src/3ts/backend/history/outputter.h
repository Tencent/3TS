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
#include "../util/generic.h"
#include "generator.h"
namespace ttts {

struct CheckResult {
  CheckResult() = default;
  CheckResult(const CheckResult&) = delete;
  CheckResult(CheckResult&&) = default;
  ~CheckResult() {}
  bool ok_;
  std::string algorithm_name_;
  std::ostringstream info_;
  std::optional<std::vector<int>> rollback_type_vec_;
  std::optional<double> time_compt_;
};

class Outputter {
 public:
  Outputter(const std::string& output_filename) : os_(output_filename) {}
  virtual ~Outputter() {}
  virtual void Output(const std::vector<std::unique_ptr<CheckResult>>& results,
                      const History& history) = 0;
  virtual void ResultToFile(const std::string&) = 0;

 protected:
  std::ofstream os_;
};

// Output rollback result
class RollbackRateOutputter : public Outputter {
 public:
  RollbackRateOutputter(const std::string& output_filename) : Outputter(output_filename) {}
  virtual ~RollbackRateOutputter() { ResultToFile("finish success"); }
  virtual void ResultToFile(const std::string& s) override {
    os_ << s << std::endl;
    for (const auto& i : tot_) {
      os_ << ">>>>>> " << i.first << std::endl;
      os_ << rollback_num_[i.first] << "/" << i.second << std::endl;
      os_ << rollback_num_[i.first] * 100.0 / i.second << "%" << std::endl;
    }
  }
  void Output(const std::vector<std::unique_ptr<CheckResult>>& results, const History& history) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const std::unique_ptr<CheckResult>& result : results) {
      if (result->rollback_type_vec_.has_value() && result->rollback_type_vec_->size()) {
        tot_[result->algorithm_name_] += history.trans_num();
        rollback_num_[result->algorithm_name_] += result->rollback_type_vec_->size();
      }
    }
  }

 private:
  std::mutex mutex_;
  std::map<std::string, uint64_t> tot_;
  std::map<std::string, uint64_t> rollback_num_;
};

// Compare result with the 0th algorithm and calculate true/false rollback rate.
class DatumOutputter : public Outputter {
 private:
  struct Info {
    Info()
        : has_rollback_rate_(false),
          time_consume_(0.0),
          ok_count_(0),
          ng_count_(0),
          missed_judgement_count_(0),
          wrong_judgement_count_(0),
          commit_trans_num_(0),
          rollback_trans_num_(0),  // active_rollback_trans_num_ + passive_rollback_trans_num_
          passive_rollback_trans_num_(0),  // true_rollback_trans_num_ + false_rollback_trans_num_
          true_rollback_trans_num_(0),
          false_rollback_trans_num_(0) {}

    bool has_rollback_rate_;  // the algorithm support collecting rollback rate statistic or not
    double time_consume_;

    uint64_t ok_count_;
    uint64_t ng_count_;
    uint64_t missed_judgement_count_;
    uint64_t wrong_judgement_count_;

    uint64_t commit_trans_num_;            // number of transactions committing successful
    uint64_t rollback_trans_num_;          // number of transactions finally rollback
    uint64_t passive_rollback_trans_num_;  // number of transactions committing failed
    uint64_t true_rollback_trans_num_;
    uint64_t false_rollback_trans_num_;

    std::unordered_map<uint64_t, uint64_t> anomally_type_num_;
  };

 public:
  DatumOutputter(const std::string& output_filename)
      : Outputter(output_filename),
        history_count_(0),
        trans_num_(0),
        active_rollback_trans_num_(0),
        try_commit_trans_num_(0) {}
  virtual ~DatumOutputter() { ResultToFile("finish success"); }
  virtual void ResultToFile(const std::string& s) override {
    os_ << s << std::endl;
    os_ << "Datum Algorithm: " << datum_algorithm_name_ << std::endl;
    os_ << "Total Histories: " << history_count_ << std::endl;
    os_ << "Total Transactions: " << trans_num_ << " (try commit: " << try_commit_trans_num_ << ")"
        << std::endl;
    os_ << std::endl;

    for (auto& [algorithm_name, info] : infos_) {
      if (info.time_consume_ > 0) {
        os_ << ">>>>>> " << algorithm_name << " (" << info.time_consume_ << "s)" << std::endl;
      } else {
        os_ << ">>>>>> " << algorithm_name << std::endl;
      }
      os_ << std::setiosflags(std::ios::fixed) << std::setprecision(3);

      os_ << "[ HISTORY LEVEL INFO ]" << std::endl;
      os_ << "Total Histories: " << history_count_ << std::endl;
      os_ << " ├ OK Histories: " << info.ok_count_ << " ("
          << 100.0 * info.ok_count_ / history_count_ << "%)" << std::endl;
      os_ << " |  └ Missed Judgement Histories: " << info.missed_judgement_count_ << " ("
          << 100.0 * info.missed_judgement_count_ / history_count_ << "%)" << std::endl;
      os_ << " └ NG Histories: " << info.ng_count_ << " ("
          << 100.0 * info.ng_count_ / history_count_ << "%)" << std::endl;
      os_ << "    └ Wrong Judgement Histories: " << info.wrong_judgement_count_ << " ("
          << 100.0 * info.wrong_judgement_count_ / history_count_ << "%)" << std::endl;

      if (info.has_rollback_rate_) {
        os_ << "[ TRANSACTION LEVEL INFO ]" << std::endl;
        os_ << "Total Transactions: " << trans_num_ << " (try commit: " << try_commit_trans_num_
            << ")" << std::endl;
        os_ << " ├ Commit Successfull Transactions: " << info.commit_trans_num_ << " ("
            << 100.0 * info.commit_trans_num_ / trans_num_ << "%)" << std::endl;
        os_ << " └ Rollback Transactions: " << info.rollback_trans_num_ << " ("
            << 100.0 * info.rollback_trans_num_ / trans_num_ << "%)" << std::endl;
        os_ << "    ├ Active Rollback Transactions: " << active_rollback_trans_num_ << " ("
            << 100.0 * active_rollback_trans_num_ / trans_num_ << "%)" << std::endl;
        os_ << "    └ Passive Rollback Transactions: " << info.passive_rollback_trans_num_ << " ("
            << 100.0 * info.passive_rollback_trans_num_ / trans_num_ << "%)" << std::endl;
        os_ << "       ├ True Rollback Transactions: " << info.true_rollback_trans_num_ << " ("
            << 100.0 * info.true_rollback_trans_num_ / trans_num_ << "%)" << std::endl;
        os_ << "       └ False Rollback Transactions: " << info.false_rollback_trans_num_ << " ("
            << 100.0 * info.false_rollback_trans_num_ / trans_num_ << "%)" << std::endl;
        os_ << "[ ANOMALY TYPE INFO ]" << std::endl;
        for (auto ano_loop : Anomally2Name) {
          os_ << ano_loop.second << ": " << info.anomally_type_num_[ano_loop.first] << std::endl;
        }
      }

      os_ << std::endl;
    }
  }

  void Output(const std::vector<std::unique_ptr<CheckResult>>& results,
              const History& history) override {
    std::lock_guard<std::mutex> lock(mutex_);

    ++history_count_;
    trans_num_ += history.trans_num();
    active_rollback_trans_num_ += history.abort_trans_num();
    try_commit_trans_num_ = trans_num_ - active_rollback_trans_num_;

    auto datum_ok = results[0]->ok_;  // datum algorithm consider history has no anomalies
    datum_algorithm_name_ = results[0]->algorithm_name_;
    for (const auto& result : results) {
      Info& info = infos_[result->algorithm_name_];
      if (result->time_compt_.has_value()) {
        info.time_consume_ += result->time_compt_.value();
      }

      // history level info
      info.missed_judgement_count_ += (!datum_ok && result->ok_);
      info.wrong_judgement_count_ += (datum_ok && !result->ok_);
      info.ok_count_ += result->ok_;
      info.ng_count_ += !result->ok_;

      // transaction level info
      if (info.has_rollback_rate_ = result->rollback_type_vec_.has_value()) {  // do assignment
        // TODO: A transaction plan to active rollback may be rollbacked by algorithm. In this case,
        // the transactions is both count in abort_trans_num and rollback_type_vec_
        info.passive_rollback_trans_num_ += result->rollback_type_vec_->size();
        // TODO: All passive rollback in anomaly history will be considered as true rollback.
        (datum_ok ? info.false_rollback_trans_num_ : info.true_rollback_trans_num_) +=
            result->rollback_type_vec_->size();
        info.commit_trans_num_ = try_commit_trans_num_ - info.passive_rollback_trans_num_;
        info.rollback_trans_num_ = active_rollback_trans_num_ + info.passive_rollback_trans_num_;
        for (const auto ano_type : result->rollback_type_vec_.value()) {
          ++info.anomally_type_num_[ano_type];
        }
      }
    }
  }

 private:
  std::string datum_algorithm_name_;
  uint64_t history_count_;
  uint64_t trans_num_;
  uint64_t active_rollback_trans_num_;
  uint64_t try_commit_trans_num_;
  std::mutex mutex_;
  std::map<std::string, Info> infos_;
};

// Output detail infomation for each algorithm
class DetailOutputter : public Outputter {
 public:
  DetailOutputter(const std::string& output_filename) : Outputter(output_filename), no_(0) {}
  virtual ~DetailOutputter() {}
  virtual void ResultToFile(const std::string&) override {}
  virtual void Output(const std::vector<std::unique_ptr<CheckResult>>& results,
                      const History& history) override {
    std::stringstream ss;
    ss << ">>>>>> {" << (++no_) << "} " << history << std::endl;
    for (const std::unique_ptr<CheckResult>& result : results) {
      ss << "[ " << result->algorithm_name_ << " ] " << (result->ok_ ? "true" : "false")
         << std::endl;
      ss << result->info_.str() << std::endl;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    os_ << ss.rdbuf() << std::endl;
  }

 private:
  std::mutex mutex_;
  std::atomic<uint64_t> no_;
};

// Compare each algorithm's result and do category
class CompareOutputter : public Outputter {
 public:
  CompareOutputter(const std::string& output_filename)
      : Outputter(output_filename), inited_(false) {}
  virtual ~CompareOutputter() { ResultToFile("finish success"); }
  virtual void ResultToFile(const std::string& s) override {
    os_ << s << std::endl;
    for (std::unique_ptr<CompareCategory>& category : categories_) {
      os_ << "[Counts:" << category->count_ << "] " << category->cate_name_ << std::endl;
      category->temp_output_file_.close();
      if (std::ifstream temp_if(category->temp_output_filename_);
          temp_if.peek() != std::ifstream::traits_type::eof()) {
        os_ << temp_if.rdbuf();
      }
      os_ << std::endl;
      category.reset();
    }
  }
  virtual void Output(const std::vector<std::unique_ptr<CheckResult>>& results,
                      const History& history) override {
    if (!inited_.load()) {
      InitCategories(results);
    }
    OutputHistoryToTempFile(results, history);
  }

 private:
  struct CompareCategory {
    CompareCategory(const std::string& cate_name, const std::string& temp_output_filename)
        : cate_name_(cate_name),
          temp_output_filename_(temp_output_filename),
          temp_output_file_(temp_output_filename_),
          count_(0) {}
    ~CompareCategory() { std::remove(temp_output_filename_.c_str()); }

    const std::string cate_name_;
    const std::string temp_output_filename_;
    std::ofstream temp_output_file_;
    uint64_t count_;
  };

  void InitCategories(const std::vector<std::unique_ptr<CheckResult>>& results) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!categories_.empty()) {
      return; /* inited */
    }
    for (uint64_t i = 0; i < (static_cast<uint64_t>(1) << results.size()); ++i) {
      const std::string cate_name = CategoryName(results, Int2Bits(results.size(), i));
      const std::string temp_filename =
          "__COMPARE_RESULT_OUTPUTTER_TEMP_FILE_" + std::to_string(i) + "__";
      categories_.emplace_back(new CompareCategory(cate_name, temp_filename));
    }
    inited_ = true;
  }

  void OutputHistoryToTempFile(const std::vector<std::unique_ptr<CheckResult>>& results,
                               const History& history) {
    CompareCategory& category = *categories_[Bits2Int(ExtractOKs(results))];
    std::lock_guard<std::mutex> lock(mutex_);
    category.temp_output_file_ << history << std::endl;
    ++category.count_;
  }

  static std::string CategoryName(const std::vector<std::unique_ptr<CheckResult>>& results,
                                  const std::vector<bool>& oks) {
    std::ostringstream ss;
    assert(results.size() == oks.size() && results.size() > 0);
    const auto cat_name_f = [&ss, &results, &oks](const uint64_t index) {
      if (!oks[index]) {
        ss << "NOT ";
      }
      ss << results[index]->algorithm_name_;
    };
    cat_name_f(0);
    for (uint64_t i = 1; i < oks.size(); ++i) {
      ss << " && ";
      cat_name_f(i);
    }
    return ss.str();
  }

  static std::vector<bool> Int2Bits(const uint64_t len, uint64_t n) {
    std::vector<bool> ret(len, false);
    for (uint64_t i = len - 1; n > 0; --i) {
      ret[i] = n - n / 2 * 2;
      n /= 2;
    }
    return ret;
  }

  static uint64_t Bits2Int(const std::vector<bool>& bits) {
    uint64_t ret = 0;
    for (const bool bit : bits) {
      ret = ret * 2 + bit;
    }
    return ret;
  }

  static std::vector<bool> ExtractOKs(const std::vector<std::unique_ptr<CheckResult>>& results) {
    std::vector<bool> ret;
    for (const std::unique_ptr<CheckResult>& result : results) {
      ret.push_back(result->ok_);
    }
    return ret;
  }

  std::mutex mutex_;
  std::atomic<bool> inited_;
  std::vector<std::unique_ptr<CompareCategory>> categories_;
};

}  // namespace ttts
