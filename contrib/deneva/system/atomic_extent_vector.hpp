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
#include <atomic>
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>
#include <stdint.h>
#include <sys/mman.h>

namespace atom {
template <typename Value>
class AtomicExtentVectorIterator;

template <typename Value>
class AtomicExtentVector {
 public:
 private:
  static constexpr uint8_t max_power_size_ = 64;
  std::atomic<Value>** buckets_;
  std::atomic<Value>** deallocations[3];
  std::atomic<bool>** alive_;
  std::atomic<bool>** alive_deallocations[3];

  std::atomic<uint64_t> extent_;
  uint8_t reserved_ = 0;
  std::atomic<uint64_t> size_;
  std::atomic<uint64_t> lowest_alive_;
  std::atomic<uint64_t> safe_read_;

  common::SharedSpinMutex mutex_;

  friend class AtomicExtentVectorIterator<Value>;

  using iterator = AtomicExtentVectorIterator<Value>;

 public:
  AtomicExtentVector() : extent_(0), size_(0), lowest_alive_(0), safe_read_(0), mutex_(){};

  AtomicExtentVector(const AtomicExtentVector&) = default;
  ~AtomicExtentVector() {
    for (uint64_t i = 0; i < extent_; i++) {
      auto size = 1ul << (i + reserved_ - 1);
      deallocate(buckets_[i], sizeof(std::atomic<Value>) * size);
      deallocate(alive_[i], sizeof(std::atomic<bool>) * size);
    }

    if (extent_ > 0) {
      delete[] buckets_;
      delete[] alive_;
    }
    if (extent_ > 8) {
      delete[] deallocations[0];
      delete[] alive_deallocations[0];
    }
    if (extent_ > 16) {
      delete[] deallocations[1];
      delete[] alive_deallocations[1];
    }
    if (extent_ > 32) {
      delete[] deallocations[2];
      delete[] alive_deallocations[2];
    }
  }

  inline constexpr uint64_t safe_size() const { return safe_read_; }

  inline constexpr uint64_t size() const { return size_; }

  inline constexpr uint64_t max_size() const { return extent_ > 0 ? 1 << (reserved_ + extent_ - 1) : 0; }

  inline constexpr uint64_t get_segment_base(const uint64_t v) const { return v == 0 ? 0 : 1 << (v + reserved_ - 1); }

  inline constexpr uint8_t get_segment_base_offset(const uint64_t n) const {
    if (!n)
      return 0;
    uint8_t clz = __builtin_clzl(n);
    clz = (64 - reserved_) > clz ? clz + reserved_ : 64;
    return 64 - clz;
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

  inline Value operator[](const uint64_t n) const {
    uint8_t v = get_segment_base_offset(n);
    uint64_t off_n = n - get_segment_base(v);
    return buckets_[v][off_n].load();
  }

  inline Value lookup(const uint8_t segment, const uint64_t off_n) const { return buckets_[segment][off_n].load(); }

  inline Value atomic_replace(const uint64_t n, const Value& value) {
    uint8_t v = get_segment_base_offset(n);
    uint64_t off_n = n - get_segment_base(v);
    Value old = buckets_[v][off_n].exchange(value);
    return old;
  }

  inline Value atomic_plus(const uint64_t n) {
    uint8_t v = get_segment_base_offset(n);
    uint64_t off_n = n - get_segment_base(v);
    Value old = buckets_[v][off_n].fetch_add(1);
    return old;
  }

  inline Value atomic_or(const uint64_t n, Value val) {
    uint8_t v = get_segment_base_offset(n);
    uint64_t off_n = n - get_segment_base(v);
    Value old = buckets_[v][off_n].fetch_or(val);
    return old;
  }

  inline Value atomic_minus(const uint64_t n) {
    uint8_t v = get_segment_base_offset(n);
    uint64_t off_n = n - get_segment_base(v);
    Value old = buckets_[v][off_n].fetch_sub(1);
    return old;
  }

  inline bool compare_exchange(const uint64_t n, Value old, Value value) {
    uint8_t v = get_segment_base_offset(n);
    uint64_t off_n = n - get_segment_base(v);
    return buckets_[v][off_n].compare_exchange_weak(old, value);
  }

  inline void erase(const uint64_t n) {
    uint8_t v = get_segment_base_offset(n);
    uint64_t off_n = n - get_segment_base(v);
    alive_[v][off_n] = false;
    // uint64_t delCtr = deleteCtr[v].fetch_add(1) + 1;
    // if (delCtr > get_segment_base_offset(v + 1)) {
    //  delete[] buckets_[v];
    //  buckets_[v] = nullptr;
    //}
  }

  inline bool isAlive(const uint64_t n) const {
    uint8_t v = get_segment_base_offset(n);
    uint64_t off_n = n - get_segment_base(v);
    return alive_[v][off_n];
  }

  template <typename T>
  inline uint64_t push_back(T&& value) {
    uint64_t new_n = size_.fetch_add(1);
    while (new_n >= max_size()) {
      resize();
    }
    uint8_t v = get_segment_base_offset(new_n);
    uint64_t n = new_n - get_segment_base(v);
    buckets_[v][n].store(std::forward<T>(value));  // std::atomic<Value>
    safe_read_++;
    alive_[v][n].store(true);
    return new_n;
  }

  inline void reserve(std::size_t n) {
    mutex_.lock();
    if (extent_ == 0 && n > 0) {
      n = upper_power_of_two(n);
      buckets_ = new std::atomic<Value>*[8]();
      alive_ = new std::atomic<bool>*[8]();

      buckets_[extent_] = new (allocate(sizeof(std::atomic<Value>) * n)) std::atomic<Value>[n]();
      alive_[extent_] = new (allocate(sizeof(std::atomic<bool>) * n)) std::atomic<bool>[n]();

      reserved_ = 64 - __builtin_clzl(n) - 1;
      extent_++;
    }
    mutex_.unlock();
  }

  inline void resize() {
    mutex_.lock();
    if (size_ >= max_size()) {
      uint64_t n = max_size();
      if (extent_ == 0) {
        n = 1;
        buckets_ = new std::atomic<Value>*[8]();
        alive_ = new std::atomic<bool>*[8]();
        reserved_ = 64 - __builtin_clzl(n) - 1;
      }

      if (extent_ == 8 || extent_ == 16 || extent_ == 32) {
        auto buckets = new std::atomic<Value>*[2 * extent_]();
        auto deleted = new std::atomic<bool>*[2 * extent_]();

        std::memcpy(buckets, buckets_, extent_ * sizeof(std::atomic<Value>*));
        std::memcpy(deleted, alive_, extent_ * sizeof(std::atomic<bool>*));

        if (extent_ == 8) {
          deallocations[0] = buckets_;
          alive_deallocations[0] = alive_;
        } else if (extent_ == 16) {
          deallocations[1] = buckets_;
          alive_deallocations[1] = alive_;
        } else {
          deallocations[2] = buckets_;
          alive_deallocations[2] = alive_;
        }

        buckets_ = buckets;
        alive_ = deleted;
      }
      buckets_[extent_] = new (allocate(sizeof(std::atomic<Value>) * n)) std::atomic<Value>[n]();
      alive_[extent_] = new (allocate(sizeof(std::atomic<bool>) * n)) std::atomic<bool>[n]();
      extent_++;
    }
    mutex_.unlock();
  }

  inline void* allocate(uint64_t page_size) {
    auto chunk_ptr = mmap(nullptr, page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (page_size > (1ul << 12))
      madvise(chunk_ptr, page_size, MADV_HUGEPAGE);
    if (chunk_ptr == (void*)-1) {
      std::cout << "Error: " << errno << " - " << strerror(errno) << std::endl;
    }
    return chunk_ptr;
  }

  inline void deallocate(void* ptr, uint64_t page_size) { munmap(ptr, page_size); }

  inline iterator begin() { return iterator(*this, false); }

  inline iterator end() { return iterator(*this, true); }

  inline iterator at(const uint64_t pos) { return iterator(*this, pos); }
};

template <typename Value>
class AtomicExtentVectorIterator : public std::iterator<std::bidirectional_iterator_tag, Value> {
  AtomicExtentVector<Value>& vector_;
  uint64_t pos_;
  uint8_t base_offset_;
  uint64_t base_;
  bool end_;

 public:
  AtomicExtentVectorIterator() : pos_(0), base_offset_(0), base_(0) {}

  AtomicExtentVectorIterator(AtomicExtentVector<Value>& vec, uint64_t pos) : vector_(vec), pos_(pos), end_(false) {
    base_offset_ = vector_.get_segment_base_offset(pos_);
    base_ = pos_ - vector_.get_segment_base(base_offset_);
    if (vector_.size() > 0 && pos_ < vector_.size() && pos_ < vector_.max_size() && pos_ < vector_.safe_read_ &&
        !vector_.alive_[base_offset_][base_]) {
      operator++();
    }
  }

  AtomicExtentVectorIterator(AtomicExtentVector<Value>& vec, bool end) : vector_(vec), pos_(0), end_(end) {
    if (!end) {
      base_offset_ = vector_.get_segment_base_offset(pos_);
      base_ = pos_ - vector_.get_segment_base(base_offset_);
      if (vector_.size() > 0 && pos_ < vector_.size() && pos_ < vector_.max_size() &&
          !vector_.alive_[base_offset_][base_]) {
        operator++();
      }
    }
  }

  AtomicExtentVectorIterator(const AtomicExtentVectorIterator& it)
      : vector_(it.vector_), pos_(it.pos_), base_offset_(it.base_offset_), base_(it.base_), end_(it.end_) {}

  AtomicExtentVectorIterator& operator++() {
    ++pos_;
    base_offset_ = vector_.get_segment_base_offset(pos_);
    base_ = pos_ - vector_.get_segment_base(base_offset_);
    if (vector_.size() > 0 && pos_ < vector_.size() && pos_ < vector_.max_size() && pos_ < vector_.safe_read_ &&
        !vector_.alive_[base_offset_][base_]) {
      return operator++();
    }
    return *this;
  }

  AtomicExtentVectorIterator operator++(int) {
    AtomicExtentVectorIterator tmp(*this);
    operator++();
    return tmp;
  }

  AtomicExtentVectorIterator& operator--() {
    --pos_;
    base_offset_ = vector_.get_segment_base_offset(pos_);
    base_ = pos_ - vector_.get_segment_base(base_offset_);
    if (pos_ > 0 && vector_.size() > 0 && pos_ < vector_.size() && pos_ < vector_.max_size() &&
        pos_ < vector_.safe_read_ && !vector_.alive_[base_offset_][base_]) {
      return operator--();
    }
    return *this;
  }

  AtomicExtentVectorIterator operator--(int) {
    AtomicExtentVectorIterator tmp(*this);
    operator--();
    return tmp;
  }

  bool operator==(const AtomicExtentVectorIterator& rhs) const {
    if (!end_ && !rhs.end_)
      return &vector_ == &rhs.vector_ && pos_ == rhs.pos_;
    else if (!end_) {
      return pos_ >= vector_.safe_read_;
    } else {
      return rhs.pos_ >= rhs.vector_.safe_read_;
    }
  }

  bool operator!=(const AtomicExtentVectorIterator& rhs) const { return !(*this == rhs); }

  Value operator*() const { return vector_.buckets_[base_offset_][base_].load(); }
};

};  // namespace atom
