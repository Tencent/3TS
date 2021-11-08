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

#include "atomic_data_structures_decl.hpp"
#include "atomic_unordered_hashtable.hpp"

namespace atom {

template <typename Value, typename Key, typename Bucket, typename Allocator, bool Size>
class AtomicUnorderedMap : public AtomicUnorderedHashtable<Bucket, Key, Allocator, Size> {
 public:
  using iterator = AtomicUnorderedMapIterator<Value, Key, Bucket, Allocator, Size>;
  using unsafe_iterator = UnsafeAtomicUnorderedMapIterator<Value, Key, Bucket, Allocator, Size>;
  using Base = AtomicUnorderedHashtable<Bucket, Key, Allocator, Size>;
  friend class AtomicUnorderedMapIterator<Value, Key, Bucket, Allocator, Size>;
  friend class UnsafeAtomicUnorderedMapIterator<Value, Key, Bucket, Allocator, Size>;

  AtomicUnorderedMap(uint64_t buildSize, Allocator* alloc, typename Base::EMB* em)
      : AtomicUnorderedHashtable<Bucket, Key, Allocator, Size>(buildSize, alloc, em){};

  inline bool lookup(const Key key, Value& val) const {
    uint64_t hash = Base::hashKey(key) % Base::max_size_;

    EpochGuard<typename Base::EMB, typename Base::EM> eg{Base::em_};
    Bucket* elem = Base::buckets_[hash].load();
    while (elem != nullptr) {
      if (elem->key == key) {
        val = elem->val;
        return true;
      }
      Bucket* elem_next = elem->next.load();
      elem = elem_next;
    }
    return false;
  }

  template <typename T>
  inline bool insert(const Key key, T&& val) {
    uint64_t hash = Base::hashKey(key) % Base::max_size_;
    Bucket* old;

    auto id = Base::lock(hash);
    old = Base::buckets_[hash].load();
    while (old != nullptr) {
      if (old->key == key) {
        Base::unlock(hash, id);
        return false;
      }
      old = old->next.load();
    }

    void* addr = Base::alloc_->template allocate<Bucket>(1);
    Bucket* elem = new (addr) Bucket(key, std::forward<T>(val));
    do {
      old = Base::buckets_[hash].load();
      elem->next = old;
    } while (!Base::buckets_[hash].compare_exchange_weak(old, elem));
    Base::unlock(hash, id);
    if (Size)
      Base::size_++;
    return true;
  }

  inline uint64_t hashKey(const Key key) { return Base::hashKey(key) % Base::max_size_; }

  template <typename T>
  bool replace(const Key key, T&& val, const uint64_t hashKey) {
    Bucket* old;
    old = Base::buckets_[hashKey].load();
    while (old != nullptr) {
      if (old->key == key) {
        old->val = std::forward<T>(val);
        return true;
      }
      old = old->next.load();
    }
    void* addr = Base::alloc_->template allocate<Bucket>(1);
    Bucket* elem = new (addr) Bucket(key, std::forward<T>(val));
    auto id = Base::lock(hashKey);
    do {
      old = Base::buckets_[hashKey].load();
      elem->next = old;
    } while (!Base::buckets_[hashKey].compare_exchange_weak(old, elem));
    Base::unlock(hashKey, id);
    if (Size)
      Base::size_++;
    return true;
  }

  template <typename T>
  bool replace(const Key key, T&& val) {
    uint64_t hash = Base::hashKey(key) % Base::max_size_;
    return replace(key, std::forward<T>(val), hash);
  }

  // if no existing tries to insert it, but if insert fails as well no second try is started and false is returned
  template <typename T>
  inline bool compare_and_swap(const Key key, T&& expected, T&& val) {
    uint64_t hash = Base::hashKey(key) % Base::max_size_;
    Bucket* old;
    auto id = Base::lock(hash);
    old = Base::buckets_[hash].load();
    while (old != nullptr) {
      if (old->key == key) {
        if (old->val != std::forward<T>(expected)) {
          Base::unlock(hash, id);
          return false;
        }
        old->val = std::forward<T>(val);
        Base::unlock(hash, id);
        return true;
      }
      old = old->next.load();
    }
    Base::unlock(hash, id);
    return false;
  }

  inline iterator begin() { return iterator(*this, 0, Base::em_); }

  inline iterator end() { return iterator(*this, Base::max_size_, Base::em_); }

  inline unsafe_iterator unsafe_begin() { return unsafe_iterator(*this, 0); }

  inline unsafe_iterator unsafe_end() { return unsafe_iterator(*this, Base::max_size_); }
};

template <typename Value, typename Key, typename Bucket, typename Allocator, bool Size>
class AtomicUnorderedMapIterator : public std::iterator<std::forward_iterator_tag, Value> {
  AtomicUnorderedMap<Value, Key, Bucket, Allocator, Size>& map_;
  uint64_t bucket_position_;
  Bucket* cur_bucket_;
  EpochGuard<typename AtomicUnorderedMap<Value, Key, Bucket, Allocator, Size>::Base::EMB,
             typename AtomicUnorderedMap<Value, Key, Bucket, Allocator, Size>::Base::EM>
      eg_;

 public:
  AtomicUnorderedMapIterator(AtomicUnorderedMap<Value, Key, Bucket, Allocator, Size>& map,
                             uint64_t bucket_position,
                             typename AtomicUnorderedMap<Value, Key, Bucket, Allocator, Size>::Base::EMB* em)
      : map_(map), bucket_position_(bucket_position), cur_bucket_(nullptr), eg_(em) {
    if (bucket_position_ < map_.max_size_) {
      cur_bucket_ = map.buckets_[bucket_position].load();
      if (cur_bucket_ == nullptr)
        operator++();
    }
  }

  AtomicUnorderedMapIterator(const AtomicUnorderedMapIterator& it)
      : map_(it.map_), bucket_position_(it.bucket_position_), cur_bucket_(it.cur_bucket_), eg_(it.eg_) {}

  inline AtomicUnorderedMapIterator& operator++() {
    Bucket* cur_bucket_next = nullptr;
    if (cur_bucket_ != nullptr) {
      cur_bucket_next = cur_bucket_->next.load();
      cur_bucket_ = cur_bucket_next;
    }
    while (cur_bucket_ == nullptr) {
      ++bucket_position_;
      if (bucket_position_ >= map_.max_size_)
        break;
      cur_bucket_ = map_.buckets_[bucket_position_].load();
    }
    return *this;
  }

  inline AtomicUnorderedMapIterator operator++(int) {
    AtomicUnorderedMapIterator tmp(*this);
    operator++();
    return tmp;
  }

  inline bool operator==(const AtomicUnorderedMapIterator& rhs) const {
    return &map_ == &rhs.map_ && cur_bucket_ == rhs.cur_bucket_;
  }

  inline bool operator!=(const AtomicUnorderedMapIterator& rhs) const { return !(*this == rhs); }

  inline Value operator*() const { return cur_bucket_->val; }

  inline uint64_t getKey() const { return cur_bucket_->key; }
};

template <typename Value, typename Key, typename Bucket, typename Allocator, bool Size>
class UnsafeAtomicUnorderedMapIterator : public std::iterator<std::forward_iterator_tag, Value> {
  AtomicUnorderedMap<Value, Key, Bucket, Allocator, Size>& map_;
  uint64_t bucket_position_;
  Bucket* cur_bucket_;

 public:
  UnsafeAtomicUnorderedMapIterator(AtomicUnorderedMap<Value, Key, Bucket, Allocator, Size>& map,
                                   uint64_t bucket_position)
      : map_(map), bucket_position_(bucket_position), cur_bucket_(nullptr) {
    if (bucket_position_ < map_.max_size_) {
      cur_bucket_ = map.buckets_[bucket_position].load();
      if (cur_bucket_ == nullptr)
        operator++();
    }
  }

  UnsafeAtomicUnorderedMapIterator(const UnsafeAtomicUnorderedMapIterator& it)
      : map_(it.map_), bucket_position_(it.bucket_position_), cur_bucket_(it.cur_bucket_) {}

  inline UnsafeAtomicUnorderedMapIterator& operator++() {
    Bucket* cur_bucket_next = nullptr;
    if (cur_bucket_ != nullptr) {
      cur_bucket_next = cur_bucket_->next.load();
      cur_bucket_ = cur_bucket_next;
    }
    while (cur_bucket_ == nullptr) {
      ++bucket_position_;
      if (bucket_position_ >= map_.max_size_)
        break;
      cur_bucket_ = map_.buckets_[bucket_position_].load();
    }
    return *this;
  }

  inline UnsafeAtomicUnorderedMapIterator operator++(int) {
    UnsafeAtomicUnorderedMapIterator tmp(*this);
    operator++();
    return tmp;
  }

  inline bool operator==(const UnsafeAtomicUnorderedMapIterator& rhs) const {
    return &map_ == &rhs.map_ && cur_bucket_ == rhs.cur_bucket_;
  }

  inline bool operator!=(const UnsafeAtomicUnorderedMapIterator& rhs) const { return !(*this == rhs); }

  inline Value operator*() const { return cur_bucket_->val; }

  inline uint64_t getKey() const { return cur_bucket_->key; }
};
};  // namespace atom
