//
// No False Negatives Database - A prototype database to test concurrency control that scales to many cores.
// Copyright (C) 2019 Dominik Durner <dominik.durner@tum.de>
//
// This file is part of No False Negatives Database.
//
// No False Negatives Database is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// No False Negatives Database is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with No False Negatives Database.  If not, see <http://www.gnu.org/licenses/>.
//
// SPDX-License-Identifier: GPL-3.0-or-later
//

#pragma once
#include "atomic_extent_vector.hpp"
#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>

namespace common {

struct LogInfo {
  uint64_t transaction_id;
  uint64_t log_id;
  uint64_t table_id;
  uint64_t offset;
  char rwac;
};

class GlobalLogger {
 private:
  std::string log_name;
  std::atomic<uint64_t> push_back_ctr;
  atom::AtomicExtentVector<LogInfo> log_information_;

 public:
  GlobalLogger() {
    time_t t = time(0);
    log_name = "transaction_log_" + std::to_string(t) + ".log";
  }

  ~GlobalLogger() { writeLogToFile(); }

  void writeLogToFile() {
    for (auto l : log_information_) {
      std::ofstream log_file(log_name, std::ios_base::out | std::ios_base::app);
      (log_file << l.rwac << "," << l.transaction_id << "," << l.log_id << "," << l.table_id << "," << l.offset)
          << std::endl;
    }
  }

  inline void log(const LogInfo log_info) { log_information_.push_back(log_info); }
  /*std::ofstream log_file(log_name, std::ios_base::out | std::ios_base::app);
  (log_file << log_info.rwac << "," << log_info.transaction_id << "," << log_info.log_id << "," << log_info.table_id
            << "," << log_info.offset)
      << std::endl;
}*/

  inline void log(const std::string s) {
    std::ofstream log_file(log_name, std::ios_base::out | std::ios_base::app);
    log_file << s << std::endl;
  }
};
};  // namespace common
