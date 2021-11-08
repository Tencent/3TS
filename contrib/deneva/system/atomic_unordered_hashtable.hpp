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

#include "chunk_allocator.hpp"
#include "epoch_manager.hpp"
#include "atomic_data_structures_decl.hpp"

namespace atom {

template <typename Bucket, typename Key, typename Allocator, bool Size>
class AtomicUnorderedHashtable {
 public:
  using EMB = EpochManagerBase<Allocator>;
  using EM = EpochManager<Allocator>;

 protected:
  uint64_t max_size_ = 0;
  std::atomic<uint64_t> size_;
  Allocator* alloc_;
  EMB* em_;

  std::deleted_unique_ptr<std::atomic<Bucket*>[]> buckets_;
  std::deleted_unique_ptr<std::atomic<uint64_t>[]> mutex_;
  std::deleted_unique_ptr<std::atomic<uint64_t>[]> check_mutex_;

 protected:
  template <class K, typename std::enable_if_t<std::is_integral<K>::value>* = nullptr>
  inline constexpr uint64_t hashKey(K key) const {
    uint64_t k = key;
    constexpr uint64_t m = 0xc6a4a7935bd1e995;
    constexpr int r = 47;
    uint64_t h = 0x8445d61a4e774912 ^ (8 * m);
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h | (1ull << 63);
  }

  template <class K, typename std::enable_if<!std::is_integral<K>::value, int>::type = 0>
  inline constexpr uint64_t hashKey(K key) const {
    uint64_t k = std::hash<K>{}(key);
    constexpr uint64_t m = 0xc6a4a7935bd1e995;
    constexpr int r = 47;
    uint64_t h = 0x8445d61a4e774912 ^ (8 * m);
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h | (1ull << 63);
  }

  inline constexpr uint64_t upper_power_of_two(uint64_t v) const {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
  }

  AtomicUnorderedHashtable(uint64_t buildSize, Allocator* alloc, EMB* em) : alloc_(alloc), em_(em) {
    max_size_ = upper_power_of_two(buildSize);  // make it a power of 2
    auto ptr = alloc->template allocate<std::atomic<Bucket*>>(max_size_, true);
    buckets_ = std::deleted_unique_ptr<std::atomic<Bucket*>[]>(new (ptr) std::atomic<Bucket*>[max_size_](),
                                                               [=](auto* ptr) { alloc_->deallocate(ptr, 0); });
    auto ptr_int = alloc->template allocate<std::atomic<uint64_t>>(max_size_, true);
    mutex_ = std::deleted_unique_ptr<std::atomic<uint64_t>[]>(new (ptr_int) std::atomic<uint64_t>[max_size_](),
                                                              [=](auto* ptr) { alloc_->deallocate(ptr, 0); });
    ptr_int = alloc->template allocate<std::atomic<uint64_t>>(max_size_, true);
    check_mutex_ = std::deleted_unique_ptr<std::atomic<uint64_t>[]>(new (ptr_int) std::atomic<uint64_t>[max_size_](),
                                                                    [=](auto* ptr) { alloc_->deallocate(ptr, 0); });
    size_.store(0);
  }

  ~AtomicUnorderedHashtable() {
    for (uint64_t i = 0; i < max_size_; i++) {
      std::atomic<Bucket*> elem;
      std::atomic<Bucket*> prv;

      if (buckets_[i] == nullptr)
        continue;

      elem.store(buckets_[i]);
      prv.store(nullptr);
      while (elem != nullptr) {
        prv.store(elem);
        elem.store(elem.load()->next);
        EpochGuard<EMB, EM> eg{em_};
        eg.add(prv.load());
      }
    }
  }

  inline uint64_t lock(uint64_t offset) {
    auto val = mutex_[offset].fetch_add(1);
    while (check_mutex_[offset] != val) {
    }
    return val + 1;
  }

  inline void unlock(uint64_t offset, uint64_t val) { check_mutex_[offset].exchange(val); }

 public:
  inline constexpr uint64_t combine_key(const uint64_t key_1, const uint64_t key_2, const uint8_t bits_of_key_1) const {
    if (!bits_of_key_1)
      return key_2;
    if (bits_of_key_1 >= 64)
      return key_1;

    uint64_t key = 0;
    key = key_1 << (64 - bits_of_key_1);
    key |= ((key_2 << bits_of_key_1) >> bits_of_key_1);

    return key;
  }

  inline constexpr uint64_t size() const { return size_; }
  inline constexpr uint64_t max_size() const { return max_size_; }

  inline bool erase(const Key key) {
    uint64_t hash = hashKey(key) % max_size_;
    std::atomic<Bucket*> prv;
    std::atomic<Bucket*> elem;
    bool check = false;
    bool keyfound = false;
    auto id = lock(hash);
    EpochGuard<EMB, EM> eg{em_};
    do {
      elem.store(buckets_[hash]);
      prv.store(nullptr);
      while (elem != nullptr && !keyfound) {
        Bucket* cur = elem;
        if (cur->key == key) {
          keyfound = true;
          Bucket* ne = cur->next;
          if (prv != nullptr) {
            // load is fine here since this only could damage other write
            // operations but since we are in the write mutex this cant happen
            check = prv.load()->next.compare_exchange_strong(cur, ne);
          } else {
            check = buckets_[hash].compare_exchange_strong(cur, ne);
          }
        }
        prv.store(cur);
        elem.store(cur->next);
      }
    } while (!check && keyfound);
    unlock(hash, id);
    if (keyfound) {
      eg.add(prv);
      if (Size)
        size_--;
    }
    return keyfound;
  }
};
};  // namespace atom
