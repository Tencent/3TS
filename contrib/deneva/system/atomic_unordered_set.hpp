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
template <typename Key, typename Bucket, typename Allocator, bool Size>
class AtomicUnorderedSet : public AtomicUnorderedHashtable<Bucket, Key, Allocator, Size> {
 public:
  friend class AtomicUnorderedSetIterator<Key, Bucket, Allocator, Size>;
  using iterator = AtomicUnorderedSetIterator<Key, Bucket, Allocator, Size>;
  using Base = AtomicUnorderedHashtable<Bucket, Key, Allocator, Size>;

  AtomicUnorderedSet(uint64_t buildSize, Allocator* alloc, typename Base::EMB* em)
      : AtomicUnorderedHashtable<Bucket, Key, Allocator, Size>(buildSize, alloc, em){};

  inline bool find(const Key key) const {
    uint64_t hash = Base::hashKey(key) % Base::max_size_;

    EpochGuard<typename Base::EMB, typename Base::EM> eg{Base::em_};
    Bucket* elem = Base::buckets_[hash];
    while (elem != nullptr) {
      if (elem->key == key) {
        return true;
      }
      Bucket* elem_next = elem->next;
      elem = elem_next;
    }
    return false;
  }

  inline bool insert(const Key key) {
    uint64_t hash = Base::hashKey(key) % Base::max_size_;

    Bucket* old;

    auto id = Base::lock(hash);
    old = Base::buckets_[hash];
    while (old != nullptr) {
      if (old->key == key) {
        Base::unlock(hash, id);
        return false;
      }
      old = old->next;
    }

    void* addr = Base::alloc_->template allocate<Bucket>(1);
    Bucket* elem = new (addr) Bucket(key);
    do {
      old = Base::buckets_[hash];
      elem->next = old;
    } while (!Base::buckets_[hash].compare_exchange_strong(old, elem));
    Base::unlock(hash, id);
    if (Size)
      Base::size_++;
    return true;
  }

  inline iterator begin() { return iterator(*this, 0, Base::em_); }

  inline iterator end() { return iterator(*this, Base::max_size_, Base::em_); }
};

template <typename Key, typename Bucket, typename Allocator, bool Size>
class AtomicUnorderedSetIterator : public std::iterator<std::forward_iterator_tag, Key> {
  AtomicUnorderedSet<Key, Bucket, Allocator, Size>& set_;
  uint64_t bucket_position_;
  Bucket* cur_bucket_;
  EpochGuard<typename AtomicUnorderedSet<Key, Bucket, Allocator, Size>::Base::EMB,
             typename AtomicUnorderedSet<Key, Bucket, Allocator, Size>::Base::EM>
      eg_;

 public:
  AtomicUnorderedSetIterator(AtomicUnorderedSet<Key, Bucket, Allocator, Size>& set,
                             uint64_t bucket_position,
                             typename AtomicUnorderedSet<Key, Bucket, Allocator, Size>::Base::EMB* em)
      : set_(set), bucket_position_(bucket_position), cur_bucket_(nullptr), eg_(em) {
    if (bucket_position_ < set_.max_size_) {
      cur_bucket_ = set.buckets_[bucket_position].load();
      if (cur_bucket_ == nullptr)
        operator++();
    }
  }

  AtomicUnorderedSetIterator(const AtomicUnorderedSetIterator& it)
      : set_(it.set_), bucket_position_(it.bucket_position_), cur_bucket_(it.cur_bucket_), eg_(it.eg_) {}

  inline AtomicUnorderedSetIterator& operator++() {
    Bucket* cur_bucket_next = nullptr;
    if (cur_bucket_ != nullptr) {
      cur_bucket_next = cur_bucket_->next.load();
      cur_bucket_ = cur_bucket_next;
    }
    while (cur_bucket_ == nullptr) {
      ++bucket_position_;
      if (bucket_position_ >= set_.max_size_)
        break;
      cur_bucket_ = set_.buckets_[bucket_position_].load();
    }
    return *this;
  }

  inline AtomicUnorderedSetIterator operator++(int) {
    AtomicUnorderedSetIterator tmp(*this);
    operator++();
    return tmp;
  }

  inline bool operator==(const AtomicUnorderedSetIterator& rhs) const {
    return &set_ == &rhs.set_ && cur_bucket_ == rhs.cur_bucket_;
  }

  inline bool operator!=(const AtomicUnorderedSetIterator& rhs) const { return !(*this == rhs); }

  inline Key operator*() const { return cur_bucket_->key; }
};

};  // namespace atom
