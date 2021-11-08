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

#include <atomic>
#include <cassert>
#include <stdint.h>

namespace common {
struct SharedSpinMutex {
 private:
  std::atomic<uint8_t> lock_;

 public:
  SharedSpinMutex() : lock_(0) {}

  inline void lock() {
    auto xchg = false;
    auto l = lock_.load();
    while (!xchg) {
      if (l == 0) {
        xchg = lock_.compare_exchange_weak(l, (1u << 7));
      }
      l = lock_;
    }
  }

  inline void unlock() {
    auto l = lock_.load();
    assert(l == (1u << 7));
    auto xchg = false;
    while (!xchg) {
      xchg = lock_.compare_exchange_weak(l, 0);
    }
  }

  inline void lock_shared() {
    auto xchg = false;
    auto l = lock_.load();
    while (!xchg) {
      if (l != (1u << 7) && l < (1u << 7) - 2) {
        xchg = lock_.compare_exchange_weak(l, (l + 1));
      }
      l = lock_;
    }
  }

  inline void unlock_shared() {
    auto l = lock_.load();
    assert(l != (1u << 7));
    auto xchg = false;
    while (!xchg) {
      xchg = lock_.compare_exchange_weak(l, (l - 1));
      l = lock_;
    }
  }
};
};  // namespace common
