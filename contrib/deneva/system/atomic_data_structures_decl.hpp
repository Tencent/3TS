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

#include "shared_spin_mutex.hpp"
#include "std_allocator.hpp"
#include <atomic>
#include <memory>
#include <stdint.h>

namespace atom {

/////
// AtomicUnorderedHashtable
////

struct NUMAMutex {
  common::SharedSpinMutex* mut_;

  NUMAMutex() : mut_(new common::SharedSpinMutex{}) {}
  ~NUMAMutex() { delete mut_; }

  common::SharedSpinMutex& getMutex() const { return *mut_; }
};

template <typename Bucket, typename Key = uint64_t, typename Allocator = common::StdAllocator, bool Size = true>
class AtomicUnorderedHashtable;

/////
// AtomicUnorderedMap
////

template <typename Value, typename Key, typename Bucket, typename Allocator, bool Size>
class AtomicUnorderedMapIterator;

template <typename Value, typename Key, typename Bucket, typename Allocator, bool Size>
class UnsafeAtomicUnorderedMapIterator;

template <typename Value, typename Key>
struct AtomicUnorderedMapBucket {
  Key key;
  std::atomic<Value> val;
  std::atomic<AtomicUnorderedMapBucket*> next;

  AtomicUnorderedMapBucket(Key key, Value val) : key(key), val(val), next(nullptr) {}

  AtomicUnorderedMapBucket() : key(), val(), next(nullptr) {}
};

template <typename Value, typename Key>
struct alignas(64) AtomicUnorderedMapNUMABucket {
  Key key;
  std::atomic<Value> val;
  std::atomic<AtomicUnorderedMapNUMABucket*> next;

  AtomicUnorderedMapNUMABucket(Key key, Value val) : key(key), val(val), next(nullptr) {}

  AtomicUnorderedMapNUMABucket() : key(), val(), next(nullptr) {}
};

template <typename Value,
          typename Key = uint64_t,
          typename Bucket = AtomicUnorderedMapBucket<Value, Key>,
          typename Allocator = common::StdAllocator,
          bool Size = true>
class AtomicUnorderedMap;

/////
// AtomicUnorderedMap
////

template <typename Value, typename Key, typename Bucket, typename Allocator, bool Size>
class AtomicUnorderedMultiMapIterator;

template <typename Value,
          typename Key = uint64_t,
          typename Bucket = AtomicUnorderedMapBucket<Value, Key>,
          typename Allocator = common::StdAllocator,
          bool Size = true>
class AtomicUnorderedMultiMap;

/////
// AtomicUnorderedSet
////

template <typename Key, typename Bucket, typename Allocator, bool Size>
class AtomicUnorderedSetIterator;

template <typename Key>
struct AtomicUnorderedSetBucket {
  Key key;
  std::atomic<AtomicUnorderedSetBucket*> next;

  AtomicUnorderedSetBucket(Key key) : key(key), next(nullptr) {}

  AtomicUnorderedSetBucket() : key(), next(nullptr) {}
};

template <typename Key>
struct alignas(64) AtomicUnorderedSetNUMABucket {
  Key key;
  std::atomic<AtomicUnorderedSetNUMABucket*> next;

  AtomicUnorderedSetNUMABucket(Key key) : key(key), next(nullptr) {}

  AtomicUnorderedSetNUMABucket() : key(), next(nullptr) {}
};

template <typename Key = uint64_t,
          typename Bucket = AtomicUnorderedSetBucket<Key>,
          typename Allocator = common::StdAllocator,
          bool Size = true>
class AtomicUnorderedSet;

};  // namespace atom
