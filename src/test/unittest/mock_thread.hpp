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

#include <future>
#include <list>
#include <mutex>
#include <thread>

namespace testing {
class MockThread {
  std::mutex m;
  std::condition_variable v;
  std::atomic<uint64_t> w;
  std::list<std::function<void()>> work;
  std::thread this_thread_;
  std::atomic<bool> a;

 public:
  MockThread() : m(), v(), w(), work(), this_thread_(), a(false) {}
  ~MockThread() {
    a = true;
    v.notify_all();
    this_thread_.join();
  }

  void operator()() {
    while (!a) {
      std::function<void()> f;
      {
        std::unique_lock<std::mutex> l(m);
        if (work.empty()) {
          v.wait(l, [&] { return !work.empty() || a; });
          if (a)
            return;
        }
        f = std::move(work.front());
        work.pop_front();
      }
      f();
      w--;
    }
  }

  void runASync(std::function<void()> f) {
    {
      std::unique_lock<std::mutex> l(m);
      work.push_back(std::move(f));
      w++;
      v.notify_all();
    }
  }

  void runSync(std::function<void()> f) {
    {
      std::unique_lock<std::mutex> l(m);
      work.push_back(std::move(f));
      w++;
      v.notify_all();
    }
    while (w > 0) {
    };
  }

  void start() {
    this_thread_ = std::thread([this] { (*this)(); });
  }
};
};  // namespace testing
