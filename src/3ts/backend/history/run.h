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
#include <signal.h>
#include <unistd.h>

#include "../cca/algorithm.h"
#include "../util/generic.h"
#include "../util/thread_pool.h"
#include "generator.h"
#include "outputter.h"
#define VEC_NUM 100

using namespace ttts;

std::vector<std::shared_ptr<Outputter>> outs;
void handler(int signum) {
  for (auto &i : outs) {
    i->ResultToFile("[WARNING] The test is uncompleted!");
  }
  exit(0);
}

// Pass each history created by generator to task and run task in a new thread.
// The function will not exit until all histories are checked.
void ThreadRunBase(const std::shared_ptr<HistoryGenerator> &generator, const std::function<void(const History &)> &task,
                   const uint32_t thread_num) {
  ThreadPool thread_pool(thread_num);
  generator->DeliverHistories([&thread_pool, &task](History &&history) {
    thread_pool.PushTask([
      history_tmp = std::move(history),
      &task
    ]() { task(history_tmp); });
  });
}

template <typename Algorithm>
void SetCheckResult(Algorithm &&algorithm, const History &history, CheckResult &check_result) {
  if constexpr (std::is_same_v<RollbackRateAlgorithm, std::decay_t<Algorithm>> ||
              std::is_base_of_v<RollbackRateAlgorithm, std::decay_t<Algorithm>>) {
      check_result.rollback_type_vec_ = algorithm.RollbackNum(history, &check_result.info_);
      check_result.ok_ = check_result.rollback_type_vec_->size() == 0;
  } else {
    check_result.ok_ = algorithm.Check(history, &check_result.info_);
  }
}

// Each algorithm check the history and determine whether output the result by each algorithm's
// filter.
void FilterRun(
    const std::shared_ptr<HistoryGenerator> &generator,
    const std::vector<std::pair<std::variant<std::shared_ptr<HistoryAlgorithm>, std::shared_ptr<RollbackRateAlgorithm>>,
                                std::optional<bool>>> &algorithms,
    const std::vector<std::shared_ptr<Outputter>> &outputters, const uint64_t thread_num) {
  // For each history, call task(history)
  const auto task = [&algorithms, &outputters](const History &history) {
    std::vector<std::unique_ptr<CheckResult>> check_results;  // results of each algorithm
    // For each algorithm, check the history
    for (const auto &algorithm_and_filter : algorithms) {
      const std::optional<bool> &filter = algorithm_and_filter.second;
      if (!std::visit([&filter, &history, &check_results ](auto && algorithm)->bool {
                        auto check_result = std::make_unique<CheckResult>();
                        const auto start_time = std::chrono::system_clock::now();
                        SetCheckResult(*algorithm, history, *check_result);
                        check_result->time_compt_ = (std::chrono::system_clock::now() - start_time).count();
                        check_result->algorithm_name_ = algorithm->name();
                        // If filter == true, output only if check passes;
                        // If filter == false, output only if check not passes;
                        // If filter not has a value, output whether check passes or not
                        if (!filter.has_value() || check_result->ok_ == filter.value()) {
                          check_results.emplace_back(std::move(check_result));
                          return true;  // result satisfies filter, go to next algorithm
                        } else {
                          return false;  // result not satisfies filter, cannot output result
                        }
                      },
                      algorithm_and_filter.first)) {
        return;  // result not satisfies filter, cannot output result
      }
    }
    // Each algorithm's result satisfies its filter, can output result
    for (const std::shared_ptr<Outputter> &outputter : outputters) {
      outputter->Output(check_results, history);
    }
  };

  signal(SIGINT, handler);
  signal(SIGTERM, handler);
  outs = outputters;
  ThreadRunBase(generator, task, thread_num);
  for (const auto & [ variant_alg, _ ] : algorithms) {
    std::visit([](auto &&alg) { alg->Statistics(); }, variant_alg);
  }
}

// Each algorithm check the histories and record the time cost.
template <typename OS>
void BenchmarkRun(const std::vector<History> &histories,
                  const std::vector<std::shared_ptr<HistoryAlgorithm>> &algorithms, OS &&os) {
  for (const std::shared_ptr<HistoryAlgorithm> &algorithm : algorithms) {
    auto start = std::chrono::system_clock::now();
    uint64_t ok_count = 0;
    for (const History &history : histories) {
      if (algorithm->Check(history)) {
        ++ok_count;
      }
    }
    std::chrono::duration<double> diff = std::chrono::system_clock::now() - start;
    os << "\'" << algorithm->name() << "\'"
       << " ok histories: " << ok_count << " duration: " << diff.count() << "s" << std::endl;
  }
}

// Each algorithm check the histories and record the time cost.
template <typename OS>
void BenchmarkRun(const std::shared_ptr<HistoryGenerator> &generator,
                  const std::vector<std::shared_ptr<HistoryAlgorithm>> &algorithms, OS &&os) {
  std::vector<History> histories;
  generator->DeliverHistories([&histories](History &&history) { histories.emplace_back(std::move(history)); });
  BenchmarkRun(histories, algorithms, std::forward<OS>(os));
}

// Each algorithm check the histories and record the time cost.
template <typename OS>
void BenchmarkRun(const std::vector<uint64_t> &trans_nums, const std::vector<uint64_t> &item_nums, const uint64_t num,
                  const std::vector<std::shared_ptr<HistoryAlgorithm>> &algorithms, OS &&os, const bool with_abort,
                  const TclPosition tcl_position) {
  Options opts;
  opts.with_abort = with_abort;
  opts.tcl_position = tcl_position;
  for (const uint64_t trans_num : trans_nums) {
    for (const uint64_t item_num : item_nums) {
      const uint64_t dml_operation_num = trans_num * item_num / 4;
      os << "====== trans_num: " << trans_num << " item_num: " << item_num
         << " dml_operation_num: " << dml_operation_num << " ======" << std::endl;
      opts.trans_num = trans_num;
      opts.item_num = item_num;
      opts.max_dml = dml_operation_num;
      BenchmarkRun(std::make_shared<RandomHistoryGenerator>(opts, num), algorithms, std::forward<OS>(os));
    }
    os << std::endl;
  }
}
