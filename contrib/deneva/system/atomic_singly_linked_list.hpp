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
#include "shared_spin_mutex.hpp"
#include "atomic_unordered_map.hpp"
#include <atomic>
#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <vector>
#include <stdint.h>

namespace atom {
template <typename Value>
class AtomicSinglyLinkedListIterator;

template <typename Value>
struct AtomicSinglyLinkedListBucket {
  uint64_t id;
  std::atomic<AtomicSinglyLinkedListBucket*> next;
  Value val;
  bool marked;

  AtomicSinglyLinkedListBucket(bool marked) : id(0), next(nullptr), val(), marked(marked) {}
  AtomicSinglyLinkedListBucket(Value& val, uint64_t id) : id(id), next(nullptr), val(val), marked(false) {}
  AtomicSinglyLinkedListBucket(Value&& val, uint64_t id)
      : id(id), next(nullptr), val(std::forward<Value>(val)), marked(false) {}
  AtomicSinglyLinkedListBucket() : id(0), next(nullptr), val(), marked(false) {}
};

template <typename Value>
class AtomicSinglyLinkedList {
 public:
  friend class AtomicSinglyLinkedListIterator<Value>;
  using iterator  = AtomicSinglyLinkedListIterator<Value>;
  using Bucket    = AtomicSinglyLinkedListBucket<Value>;
  using Allocator = common::ChunkAllocator;
  using EMB       = EpochManagerBase<Allocator>;
  using EM        = EpochManager<Allocator>;

 private:
  std::atomic<uint64_t> id_;
  std::atomic<uint64_t> size_;
  std::atomic<Bucket*> head_;
  common::SharedSpinMutex lock_;
  Allocator* alloc_;
  EMB* em_;

 public:
  AtomicSinglyLinkedList(Allocator* alloc, EMB* em) : id_(0), size_(0), head_(), alloc_(alloc), em_(em) {}

  ~AtomicSinglyLinkedList() {
    EpochGuard<EMB, EM> eg{em_};
    auto run = head_.load();
    while (run != nullptr) {
      eg.add(run);
      run = run->next;
    }
  }

  inline uint64_t size() const { return size_; }

  inline bool erase_unsafe(const uint64_t id) {
    bool break_loop = false;
    Bucket *left_node = nullptr, *right_node = nullptr, *right_node_next = nullptr;
    Bucket* right_node_next_marked = alloc_->allocate<Bucket>(1);
    new (right_node_next_marked) Bucket{true};
    EpochGuard<EMB, EM> eg{em_};
    do {
      right_node = head_.load();
      while (right_node != nullptr && right_node->id != id) {
        left_node = right_node;
        // std::cout << "Lockfree?: " << std::atomic_is_lock_free(&right_node->next) << std::endl;
        right_node = right_node->next.load();
      }
      if (right_node == nullptr || right_node->marked) {
        return false;
      }

      if (left_node == nullptr) {
        return false;
      }

      right_node_next = right_node->next.load();
      if (right_node_next != nullptr && !right_node_next->marked) {
        right_node_next_marked->next = right_node_next->next.load();
        right_node_next_marked->val = right_node_next->val;
        right_node_next_marked->id = right_node_next->id;

        if (right_node->next.compare_exchange_weak(right_node_next, right_node_next_marked)) {
          break_loop = true;
          // std::cout << "New Marked Node: " << std::atomic_load(&right_node_next_marked)->id << " "  <<
          // std::atomic_load(&right_node_next_marked)->val << std::endl;
        }
      } else if (right_node_next == nullptr) {
        break_loop = true;
        // std::cout << "NO new Marked Node! " << right_node->id << " " << right_node->val << std::endl;
      }
    } while (!break_loop);

    if (left_node != nullptr) {
      while (!left_node->next.compare_exchange_weak(right_node, right_node_next)) {
      }
    } else {
      while (!head_.compare_exchange_weak(right_node, right_node_next)) {
      }
    }
    eg.add(right_node_next_marked);
    eg.add(right_node);
    size_--;
    return true;
  }

  inline bool erase(const uint64_t id) {
    Bucket *left_node = nullptr, *right_node = nullptr, *right_node_next = nullptr;
    bool break_loop = false;
    lock_.lock();
    EpochGuard<EMB, EM> eg{em_};
    do {
      right_node = head_.load();
      while (right_node != nullptr && right_node->id != id) {
        left_node = right_node;
        right_node = right_node->next.load();
      }
      if (right_node == nullptr || right_node->marked) {
        lock_.unlock();
        return false;
      }

      right_node_next = right_node->next.load();

      if (left_node != nullptr) {
        break_loop = left_node->next.compare_exchange_weak(right_node, right_node_next);
      } else {
        break_loop = head_.compare_exchange_weak(right_node, right_node_next);
      }
    } while (!break_loop);
    eg.add(right_node);
    size_--;
    lock_.unlock();
    return true;
  }

  template <typename T>
  inline uint64_t push_front(T&& value) {
    uint64_t id = id_.fetch_add(1);
    Bucket* elem = alloc_->allocate<Bucket>(1);
    new (elem) Bucket{std::forward<T>(value), id};
    Bucket* old;
    EpochGuard<EMB, EM> eg{em_};
    do {
      elem->next = head_.load();
      old = elem->next;
      if (old != nullptr && old->marked) {
        std::cout << em_->global_counter_ << " " << em_->same_epoch_ctr_[0] << " " << em_->same_epoch_ctr_[1] << " "
                  << em_->same_epoch_ctr_[2] << std::endl;
        assert(!old->marked);
      }
    } while ((old != nullptr && old->marked) || !head_.compare_exchange_weak(old, elem));
    size_++;
    return id;
  }

  template <typename T>
  inline uint64_t fake_front(T&& value) {
    return id_.fetch_add(1);
  }

  // only there for test propuses
  inline bool find(uint64_t id, Value& val) {
    EpochGuard<EMB, EM> eg{em_};
    Bucket* node = head_.load();
    while (node != nullptr && node->id != id) {
      node = node->next.load();
    }
    if (node == nullptr) {
      return false;
    }
    val = node->val;
    return true;
  }

  inline iterator begin() { return iterator(*this, true); }

  inline iterator end() { return iterator(*this, false); }
};

template <typename Value>
class AtomicSinglyLinkedListIterator : public std::iterator<std::forward_iterator_tag, Value> {
  using Bucket = AtomicSinglyLinkedListBucket<Value>;
  using Allocator = common::ChunkAllocator;
  using EMB = EpochManagerBase<Allocator>;
  using EM = EpochManager<Allocator>;

  AtomicSinglyLinkedList<Value>& list_;
  Bucket* bucket_;
  EpochGuard<EMB, EM> eg_;

 public:
  AtomicSinglyLinkedListIterator(AtomicSinglyLinkedList<Value>& list, bool begin) : list_(list), eg_(list_.em_) {
    if (begin) {
      auto h = list_.head_.load();
      while (!h && list_.size_ > 0) {
        h = list_.head_.load();
      }
      bucket_ = h;
    } else {
      bucket_ = nullptr;
    }
  }

  AtomicSinglyLinkedListIterator(const AtomicSinglyLinkedListIterator<Value>& it)
      : list_(it.list_), bucket_(it.bucket_), eg_(list_.em_) {}

  ~AtomicSinglyLinkedListIterator() {}

  AtomicSinglyLinkedListIterator& operator++() {
    Bucket* next_bucket = bucket_->next;
    /*if (next_bucket != nullptr && next_bucket->marked) {
      std::cout << list_.em_->global_counter_ << " " << list_.em_->same_epoch_ctr_[0] << " "
                << list_.em_->same_epoch_ctr_[1] << " " << list_.em_->same_epoch_ctr_[2] << std::endl;
      assert(!next_bucket->marked);
    }*/
    while (next_bucket && next_bucket->marked) {
      next_bucket = next_bucket->next;
      /*if (next_bucket != nullptr && next_bucket->marked) {
        std::cout << list_.em_->global_counter_ << " " << list_.em_->same_epoch_ctr_[0] << " "
                  << list_.em_->same_epoch_ctr_[1] << " " << list_.em_->same_epoch_ctr_[2] << std::endl;
        assert(!next_bucket->marked);
      }*/
    }
    bucket_ = next_bucket;
    return *this;
  }

  AtomicSinglyLinkedListIterator operator++(int) {
    AtomicSinglyLinkedListIterator tmp(*this);
    operator++();
    return tmp;
  }

  bool operator==(const AtomicSinglyLinkedListIterator& rhs) const {
    return &list_ == &rhs.list_ && bucket_ == rhs.bucket_;
  }

  bool operator!=(const AtomicSinglyLinkedListIterator& rhs) const { return !(*this == rhs); }

  Value operator*() const { return bucket_->val; }

  uint64_t getId() const { return bucket_->id; }
};

};  // namespace atom
