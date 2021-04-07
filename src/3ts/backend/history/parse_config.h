/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: elioyan@tencent.com
 *         williamcliu@tencent.com
 *
 */
#pragma once
#include <libconfig.h++>
#include "../cca/conflict_serializable_algorithm.h"
#include "../cca/occ_algorithm/occ_algorithm.h"
#include "../cca/occ_algorithm/trans/bocc_trans.h"
#include "../cca/occ_algorithm/trans/focc_trans.h"
#include "../cca/occ_algorithm/trans/ssi_trans.h"
#include "../cca/occ_algorithm/trans/wsi_trans.h"
#include "../cca/occ_algorithm/trans/dli_trans.h"
#include "../cca/serializable_algorithm.h"
#include "../cca/unified_history_algorithm/unified_history_algorithm.h"
#include "../util/generic.h"
#include "generator.h"
#include "outputter.h"
#include "run.h"

template <typename EnumType>
EnumType EnumParse(const std::string& s)
{
  for (const auto e : Members<EnumType>()) {
    if (s == ToString(e)) {
      return e;
    }
  }
  throw std::string("Parse Enum failed: ") + s + " type:" + typeid(EnumType).name();
}

std::shared_ptr<ttts::HistoryGenerator> GeneratorParse(const libconfig::Config &cfg,
                                                       const std::string &name) {
  try {
    const libconfig::Setting &s = cfg.lookup(name);
    std::shared_ptr<ttts::HistoryGenerator> res;
    if (name == "InputGenerator") {
      const std::string &file = s.lookup("file");
      res = std::make_shared<ttts::InputHistoryGenerator>(file);
    } else {
      ttts::Options opt;
      opt.trans_num = s.lookup("trans_num");
      opt.item_num = s.lookup("item_num");
      opt.max_dml = s.lookup("max_dml");
      opt.with_abort = s.lookup("with_abort");
      opt.tcl_position = EnumParse<TclPosition>(s.lookup("tcl_position"));
      opt.dynamic_history_len = s.lookup("dynamic_history_len");
      opt.allow_empty_trans = s.lookup("allow_empty_trans");
      opt.with_scan = EnumParse<Intensity>(s.lookup("with_scan"));
      opt.with_write = EnumParse<Intensity>(s.lookup("with_write"));
      if (name == "TraversalGenerator") {
        opt.subtask_num = s.lookup("subtask_num");
        opt.subtask_id = s.lookup("subtask_id");
        res = std::make_shared<ttts::TraversalHistoryGenerator>(opt);
      } else {
        uint64_t history_num = s.lookup("history_num");
        res = std::make_shared<ttts::RandomHistoryGenerator>(opt, history_num);
      }
    }

    return res;
  } catch (const libconfig::SettingNotFoundException &nfex) {
    throw name + "setting " + std::string(nfex.getPath()) + " no found";
  }
}

template <bool only_rollback_rate, typename AddAlgorithm>
void AlgorithmParseInternal_(const libconfig::Config &cfg, const std::string &algorithm_name,
                             AddAlgorithm &&add_algorithm) {
  if (algorithm_name == "SSI") {
    add_algorithm(std::make_shared<ttts::OCCAlgorithm<ttts::occ_algorithm::SSITransactionDesc>>());
  } else if (algorithm_name == "WSI") {
    add_algorithm(std::make_shared<ttts::OCCAlgorithm<ttts::occ_algorithm::WSITransactionDesc>>());
  } else if (algorithm_name == "BOCC") {
    add_algorithm(std::make_shared<ttts::OCCAlgorithm<occ_algorithm::BoccTransactionDesc>>());
  } else if (algorithm_name == "FOCC") {
    add_algorithm(std::make_shared<ttts::OCCAlgorithm<occ_algorithm::FoccTransactionDesc>>());
  } else if (algorithm_name == "DLI") {
    add_algorithm(std::make_shared<ttts::OCCAlgorithm<occ_algorithm::DLITransactionDesc>>());
  } else if constexpr (only_rollback_rate) {
    throw "Unknown algorithm name " + algorithm_name +
        " in algorithms supporting rollback rate statistics";
  } else if (algorithm_name == "SerializableAlgorithm_ALL_SAME_RR") {
    add_algorithm(std::make_shared<ttts::HistorySerializableAlgorithm<
                      SerializeLevel::ALL_SAME, SerializeReadPolicy::REPEATABLE_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_ALL_SAME_RC") {
    add_algorithm(std::make_shared<ttts::HistorySerializableAlgorithm<
                      SerializeLevel::ALL_SAME, SerializeReadPolicy::COMMITTED_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_ALL_SAME_RU") {
    add_algorithm(std::make_shared<ttts::HistorySerializableAlgorithm<
                      SerializeLevel::ALL_SAME, SerializeReadPolicy::UNCOMMITTED_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_ALL_SAME_SI") {
    add_algorithm(
        std::make_shared<ttts::HistorySerializableAlgorithm<SerializeLevel::ALL_SAME,
                                                            SerializeReadPolicy::SI_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_COMMIT_SAME_RR") {
    add_algorithm(std::make_shared<ttts::HistorySerializableAlgorithm<
                      SerializeLevel::COMMIT_SAME, SerializeReadPolicy::REPEATABLE_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_COMMIT_SAME_RC") {
    add_algorithm(std::make_shared<ttts::HistorySerializableAlgorithm<
                      SerializeLevel::COMMIT_SAME, SerializeReadPolicy::COMMITTED_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_COMMIT_SAME_RU") {
    add_algorithm(std::make_shared<ttts::HistorySerializableAlgorithm<
                      SerializeLevel::COMMIT_SAME, SerializeReadPolicy::UNCOMMITTED_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_COMMIT_SAME_SI") {
    add_algorithm(
        std::make_shared<ttts::HistorySerializableAlgorithm<SerializeLevel::COMMIT_SAME,
                                                            SerializeReadPolicy::SI_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_FINAL_SAME_RR") {
    add_algorithm(std::make_shared<ttts::HistorySerializableAlgorithm<
                      SerializeLevel::FINAL_SAME, SerializeReadPolicy::REPEATABLE_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_FINAL_SAME_RC") {
    add_algorithm(std::make_shared<ttts::HistorySerializableAlgorithm<
                      SerializeLevel::FINAL_SAME, SerializeReadPolicy::COMMITTED_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_FINAL_SAME_RU") {
    add_algorithm(std::make_shared<ttts::HistorySerializableAlgorithm<
                      SerializeLevel::FINAL_SAME, SerializeReadPolicy::UNCOMMITTED_READ>>());
  } else if (algorithm_name == "SerializableAlgorithm_FINAL_SAME_SI") {
    add_algorithm(
        std::make_shared<ttts::HistorySerializableAlgorithm<SerializeLevel::FINAL_SAME,
                                                            SerializeReadPolicy::SI_READ>>());
  } else if (algorithm_name == "ConflictSerializableAlgorithm") {
    add_algorithm(std::make_shared<ttts::ConflictSerializableAlgorithm<false>>());
  } else if (algorithm_name == "DLI_IDENTIFY") {
    add_algorithm(std::make_shared<ttts::ConflictSerializableAlgorithm<true>>());
  } else if (algorithm_name == "DLI_IDENTIFY_CYCLE") {
    add_algorithm(std::make_shared<ttts::UnifiedHistoryAlgorithm<ttts::UniAlgs::UNI_DLI_IDENTIFY_CYCLE, uint64_t>>());
  } else if (algorithm_name == "DLI_IDENTIFY_CHAIN") {
    add_algorithm(std::make_shared<ttts::UnifiedHistoryAlgorithm<ttts::UniAlgs::UNI_DLI_IDENTIFY_CHAIN, uint64_t>>());
  } else if (algorithm_name == "DLI_IDENTIFY_SSI") {
    add_algorithm(std::make_shared<ttts::UnifiedHistoryAlgorithm<ttts::UniAlgs::UNI_DLI_IDENTIFY_SSI, uint64_t>>());
  } else {
    throw "Unknown algorithm name " + algorithm_name;
  }
}

#define CONSTEXPR_CONDITIONAL(cond, v1, v2) \
  [&]() -> decltype(auto) {                 \
    if constexpr ((cond)) {                 \
      return (v1);                          \
    } else {                                \
      return (v2);                          \
    }                                       \
  }()
enum ParseAlgorithmType { ONLY_NORMAL_ALGS, ONLY_ROLLBACK_RATE_ALGS, MIXED_ALGS };
template <ParseAlgorithmType parse_algorithm_type>
using AutoAlgorithm = std::conditional_t<
    parse_algorithm_type == MIXED_ALGS,
    std::variant<std::shared_ptr<HistoryAlgorithm>, std::shared_ptr<RollbackRateAlgorithm>>,
    std::conditional_t<parse_algorithm_type == ONLY_ROLLBACK_RATE_ALGS,
                       std::shared_ptr<RollbackRateAlgorithm>, std::shared_ptr<HistoryAlgorithm>>>;
template <ParseAlgorithmType parse_algorithm_type, bool enable_filter>
using AlgorithmList = std::vector<std::conditional_t<
    enable_filter, std::pair<AutoAlgorithm<parse_algorithm_type>, std::optional<bool>>,
    AutoAlgorithm<parse_algorithm_type>>>;

// If parse_algorithm_type is set, algorithms will convert to RollbackRateAlgorithm if is base of.
// If enable_filter is set, a optional<bool> for each algorithm will be returned to show filter.
template <ParseAlgorithmType parse_algorithm_type, bool enable_filter>
auto MultiAlgorithmParse(const libconfig::Config &cfg, const libconfig::Setting &s) {
  AlgorithmList<parse_algorithm_type, enable_filter> algorithms;
  const int len = s.getLength();
  if (len == 0) {
    throw "algorithm list is empty";
  }
  for (int i = 0; i < len; i++) {
    const std::string &algorithm_name =
        CONSTEXPR_CONDITIONAL(enable_filter, s[i].lookup("name"), s[i]);
    AlgorithmParseInternal_<parse_algorithm_type ==
                            ONLY_ROLLBACK_RATE_ALGS /* only_rollback_rate */>(
        cfg, algorithm_name, [&s, i, &algorithms, &algorithm_name](auto &&algorithm) {
          constexpr bool is_rollback_rate_algorithm =
              std::is_base_of_v<RollbackRateAlgorithm, std::decay_t<decltype(*algorithm)>>;
          using AlgorithmType =
              std::shared_ptr<std::conditional_t<parse_algorithm_type != ONLY_NORMAL_ALGS &&
                                                     is_rollback_rate_algorithm,
                                                 RollbackRateAlgorithm, HistoryAlgorithm>>;
          if (!is_rollback_rate_algorithm && parse_algorithm_type == ONLY_ROLLBACK_RATE_ALGS) {
            throw algorithm_name + " does not support rollback rate statistic";
          }
          if constexpr (enable_filter) {
            std::optional<bool> filter;
            try {
              filter = s[i].lookup("filter");
            } catch (const libconfig::SettingNotFoundException &nfex) {
              // If <filter> cannot find, it means we need not do filter, do nothing there
            }
            algorithms.emplace_back(
                AutoAlgorithm<parse_algorithm_type>(std::forward<AlgorithmType>(algorithm)),
                filter);
          } else {
            algorithms.emplace_back(std::forward<AlgorithmType>(algorithm));
          }
        });
  }
  return algorithms;
}
#undef CONSTEXPR_CONDITIONAL

auto OneAlgorithmParse(const libconfig::Config &cfg, const std::string &algorithm_name) {
  std::shared_ptr<HistoryAlgorithm> algorithm_res;
  AlgorithmParseInternal_<false /* only_rollback_rate */>(
      cfg, algorithm_name, [&algorithm_res](auto &&algorithm) {
        algorithm_res = std::forward<std::shared_ptr<HistoryAlgorithm>>(algorithm);
      });
  return algorithm_res;
}

// if you want add outtputer, add here
std::vector<std::shared_ptr<ttts::Outputter>> OutputterParse(const libconfig::Config &cfg,
                                                             const libconfig::Setting &s) {
  std::vector<std::shared_ptr<ttts::Outputter>> res;
  try {
    int len = s.getLength();
    for (int i = 0; i < len; i++) {
      const std::string &outputter = s[i];
      const std::string &file = cfg.lookup(outputter).lookup("file");
      if (outputter == "RollbackRateOutputter") {
        res.emplace_back(std::make_shared<ttts::RollbackRateOutputter>(file));
      } else if (outputter == "DetailOutputter") {
        res.emplace_back(std::make_shared<ttts::DetailOutputter>(file));
      } else if (outputter == "CompareOutputter") {
        res.emplace_back(std::make_shared<ttts::CompareOutputter>(file));
      } else if (outputter == "DatumOutputter") {
        res.emplace_back(std::make_shared<ttts::DatumOutputter>(file));
      } else {
        throw "outputter name err";
      }
    }
  } catch (const libconfig::SettingNotFoundException &nfex) {
    throw "Outputter setting " + std::string(nfex.getPath()) + " no found";
  }
  return res;
}

void FilterRunParse(const libconfig::Config &cfg) {
  try {
    const libconfig::Setting &s = cfg.lookup("FilterRun");
    auto generator = GeneratorParse(cfg, s.lookup("generator"));
    auto algorithms =
        MultiAlgorithmParse<MIXED_ALGS, true /* enable_filter */>(cfg, s.lookup("algorithms"));
    auto outputters = OutputterParse(cfg, s.lookup("outputters"));
    const uint64_t thread_num = s.lookup("thread_num");
    FilterRun(generator, algorithms, outputters, thread_num);
  } catch (const libconfig::SettingNotFoundException &nfex) {
    throw "Func FilterRun setting " + std::string(nfex.getPath()) + "  no found";
  }
}

void BenchmarkRunParse(const libconfig::Config &cfg) {
  try {
    const libconfig::Setting &s = cfg.lookup("BenchmarkRun");
    auto algorithms = MultiAlgorithmParse<ONLY_NORMAL_ALGS, false /* enable_filter */>(
        cfg, s.lookup("algorithms"));

    std::vector<uint64_t> trans_nums, item_nums;
    const libconfig::Setting &trans_nums_ = s.lookup("trans_nums");
    const libconfig::Setting &item_nums_ = s.lookup("item_nums");
    int len = trans_nums_.getLength();
    if (len != item_nums_.getLength()) {
      throw "BenchmarkRun trans num != item num";
    }
    for (size_t i = 0; i < len; i++) {
      trans_nums.emplace_back(trans_nums_[i]);
      item_nums.emplace_back(item_nums_[i]);
    }

    const uint64_t history_num = s.lookup("history_num");
    const std::string os = s.lookup("os");
    const bool with_abort = s.lookup("with_abort");
    const TclPosition tcl_position = EnumParse<TclPosition>(s.lookup("tcl_position"));
    if (os == "cout")
      BenchmarkRun(trans_nums, item_nums, history_num, algorithms, std::cout, with_abort, tcl_position);
    else
      BenchmarkRun(trans_nums, item_nums, history_num, algorithms, std::ofstream(os), with_abort,
                   tcl_position);
  } catch (const libconfig::SettingNotFoundException &nfex) {
    throw "Func BenchmarkRun setting " + std::string(nfex.getPath()) + " no found";
  }
}

// if you want add func, add here and corresponding parser
void TargetParse(const libconfig::Config &cfg) {
  try {
    const libconfig::Setting &target = cfg.lookup("Target");
    int len = target.getLength();
    for (int i = 0; i < len; i++) {
      const std::string &str = target[i];
      if (str == "FilterRun") {
        FilterRunParse(cfg);
      } else if (str == "BenchmarkRun") {
        BenchmarkRunParse(cfg);
      } else {
        throw "func name err";
      }
    }
  } catch (const libconfig::SettingNotFoundException &nfex) {
    throw "Target setting " + std::string(nfex.getPath()) + " no found";
  }
}

void ReadAndRun(const std::string &conf_path) {
  libconfig::Config cfg;
  try {
    cfg.readFile(conf_path.c_str());
  } catch (const libconfig::FileIOException &fioex) {
    throw "I/O error while reading file.";
  } catch (const libconfig::ParseException &pex) {
    throw "Parse error";
  }
  TargetParse(cfg);
}
