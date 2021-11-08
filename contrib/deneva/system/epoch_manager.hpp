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

#include "std_allocator.hpp"
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <functional>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <vector>
#include <tbb/spin_mutex.h>

namespace atom {
template <typename Allocator>
class EpochManager;

// this class holds all the "global" attributes of the epochmanager
template <typename Allocator>
class EpochManagerBase {
 private:
  Allocator* alloc_;
  EpochManagerBase(const EpochManagerBase& other) = delete;
  EpochManagerBase(EpochManagerBase&& other) = delete;
  EpochManagerBase& operator=(const EpochManagerBase& other) = delete;
  EpochManagerBase& operator=(EpochManagerBase&& other) = delete;

 public:
  std::atomic<uint64_t> global_counter_;
  std::atomic<uint64_t> instance_ctr_;
  std::atomic<uint64_t> inactive_ctr_;
  std::atomic<uint64_t> safe_read_;
  std::atomic<uint64_t> group_safe_read_;
  std::array<std::atomic<uint16_t>, 6> same_epoch_ctr_;
  std::array<std::vector<void*>*, 6> global_transaction_information_;  // global
  tbb::spin_mutex global_mutex_;
  thread_local static EpochManager<Allocator>* thread_em_;
  std::unordered_map<uint64_t, EpochManager<Allocator>*> thread_epoch_map_;

  inline EpochManager<Allocator>* get() {
    if (thread_em_ != nullptr)
      return thread_em_;

    thread_em_ = new EpochManager<Allocator>(alloc_, this);
    global_mutex_.lock();
    thread_epoch_map_.insert(std::make_pair(pthread_self(), thread_em_));
    global_mutex_.unlock();
    return thread_em_;
  }

  inline void remove() {
    if (thread_em_ != nullptr) {
      global_mutex_.lock();
      thread_epoch_map_.erase(pthread_self());
      global_mutex_.unlock();
      delete thread_em_;
      thread_em_ = nullptr;
    }
  }

  EpochManagerBase(Allocator* alloc)
      : alloc_(alloc),
        global_counter_(0),
        instance_ctr_(0),
        inactive_ctr_(0),
        safe_read_(0),
        same_epoch_ctr_(),
        global_transaction_information_(),
        global_mutex_(),
        thread_epoch_map_(std::thread::hardware_concurrency()) {}

  ~EpochManagerBase() {
    for (auto t : thread_epoch_map_) {
      delete t.second;
    }
  }
};

template <typename Allocator>
class EpochManager {
 public:
  using FuncVoidIntVoid = std::add_pointer_t<void(void*, uint64_t, void*)>;

 private:
  std::array<std::vector<void*>*, 6> transaction_information_;  // local
  std::array<std::vector<std::tuple<FuncVoidIntVoid, void*, uint64_t, void*>>*, 6> version_erase_information_;
  int64_t active_ctr_;
  uint64_t my_counter_;
  uint64_t min_delete_ctr_;
  Allocator* alloc_;
  EpochManagerBase<Allocator>* emb_;
  uint64_t runs_;

  EpochManager(const EpochManager& other) = delete;
  EpochManager(EpochManager&& other) = delete;
  EpochManager& operator=(const EpochManager& other) = delete;
  EpochManager& operator=(EpochManager&& other) = delete;

 public:
  EpochManager(Allocator* alloc, EpochManagerBase<Allocator>* emb)
      : transaction_information_(), active_ctr_(0), min_delete_ctr_(0), alloc_(alloc), emb_(emb), runs_(0) {
    emb_->global_mutex_.lock();
    auto before = emb_->instance_ctr_.fetch_add(1);
    if (before == 0) {
      for (uint64_t i = 0; i < emb_->global_transaction_information_.size(); i++) {
        emb_->global_transaction_information_[i] = new std::vector<void*>();
        emb_->same_epoch_ctr_[i] = 0;
      }
    }
    my_counter_ = emb_->global_counter_;
    ++emb_->same_epoch_ctr_[my_counter_ % getModulo()];
    emb_->global_mutex_.unlock();

    for (uint64_t i = 0; i < transaction_information_.size(); i++) {
      transaction_information_[i] = new std::vector<void*>();
      version_erase_information_[i] = new std::vector<std::tuple<FuncVoidIntVoid, void*, uint64_t, void*>>();
    }
  }

  /*
   * usage of local queue can lead to mem leaks if epoch manager gets destroyed before all left elements are freed
   * therefore we use a lock and add it to the global list!
   */
  ~EpochManager() {
    emb_->global_mutex_.lock();
    --emb_->same_epoch_ctr_[my_counter_ % getModulo()];
    --emb_->instance_ctr_;

    for (uint64_t i = 0; i < transaction_information_.size(); i++) {
      for (auto t : *transaction_information_[i]) {
        emb_->global_transaction_information_[i]->emplace_back(t);
      }
      transaction_information_[i]->clear();
    }

    for (auto t : transaction_information_) {
      delete t;
    }

    for (uint64_t i = 0; i < version_erase_information_.size(); i++) {
      for (auto t : *version_erase_information_[i]) {
        emb_->global_transaction_information_[i]->emplace_back(std::get<3>(t));
      }
      version_erase_information_[i]->clear();
    }

    for (auto v : version_erase_information_) {
      delete v;
    }

    if (emb_->instance_ctr_ == 0) {
      std::cout << "clean em [" << std::endl;
      for (uint64_t i = 0; i < emb_->global_transaction_information_.size(); i++) {
        for (auto t : *emb_->global_transaction_information_[i]) {
          alloc_->deallocate(t, 1);
        }
        emb_->global_transaction_information_[i]->clear();
      }

      for (auto t : emb_->global_transaction_information_) {
        delete t;
      }
      if (std::is_same<Allocator, common::NoAllocator>::value) {
        std::cout << "NoAllocator" << std::endl;
      }
      std::cout << "Versions: " << emb_->global_counter_ << std::endl;
      std::cout << "] clean em" << std::endl;
    }
    emb_->global_mutex_.unlock();
  }

  inline bool incrementCounter() {
    uint64_t old = emb_->global_counter_;
    if (emb_->same_epoch_ctr_[old % getModulo()] >= (emb_->instance_ctr_ - emb_->inactive_ctr_)) {
      return emb_->global_counter_.compare_exchange_strong(old, (old + 1));
    }
    return false;
  }

  inline void add(void* ptr) {
    transaction_information_[emb_->global_counter_ % getModulo()]->emplace_back(ptr);
    cleanup();
  }

  inline void erase(FuncVoidIntVoid func, void* chain, uint64_t offset, void* ptr) {
    version_erase_information_[emb_->global_counter_ % getModulo()]->emplace_back(
        std::move(std::make_tuple(func, chain, offset, ptr)));
    cleanup();
  }

  inline void pin(bool force = false) {
    if (active_ctr_ > 0) {
      active_ctr_++;
      return;
    }
    active_ctr_ = 1;
    runs_++;
    if (emb_ != nullptr && (runs_ >= emb_->instance_ctr_ || emb_->inactive_ctr_ > 0 || emb_->safe_read_ > 0)) {
      incrementCounter();
      runs_ = 0;
      if (my_counter_ != emb_->global_counter_) {
        --emb_->same_epoch_ctr_[my_counter_ % getModulo()];
        my_counter_ = emb_->global_counter_;
        ++emb_->same_epoch_ctr_[my_counter_ % getModulo()];
      }
    }
  }

  inline void unpin() {
    active_ctr_--;
    assert(active_ctr_ >= 0);
  }

  inline void cleanup() {
    uint8_t delete_bucket = (emb_->global_counter_ + 1) % getModulo();
    auto tid = transaction_information_[delete_bucket];
    // std::cout << "del size: " << tid.size() << std::endl;
    for (auto t : *tid) {
      // std::cout << "del" << std::endl;
      alloc_->deallocate(t, 1);
    }
    transaction_information_[delete_bucket]->clear();

    uint8_t erase_bucket = (emb_->global_counter_ + 1) % getModulo();
    auto vid = version_erase_information_[erase_bucket];
    for (auto v : *vid) {
      std::get<0>(v)(std::get<1>(v), std::get<2>(v), std::get<3>(v));
      transaction_information_[emb_->global_counter_ % getModulo()]->emplace_back(std::get<3>(v));
    }
    version_erase_information_[erase_bucket]->clear();
  }

  inline uint64_t getCurrentCounter() { return emb_->global_counter_; }

  inline uint64_t myCounter() { return my_counter_; }

  inline uint64_t waitSafeRead(uint64_t startCounter) {
    uint64_t ctr = my_counter_;
    auto inc = false;
    auto stop = false;
    emb_->safe_read_++;
    while (!stop) {
      incrementCounter();
      if (my_counter_ != emb_->global_counter_) {
        --emb_->same_epoch_ctr_[my_counter_ % getModulo()];
        my_counter_ = emb_->global_counter_;
        ++emb_->same_epoch_ctr_[my_counter_ % getModulo()];
      }
      ctr = my_counter_;

      if (!inc && ctr >= startCounter + 3) {
        inc = true;
        emb_->group_safe_read_++;
      }
      if (inc && ctr % getModulo() == 0 && emb_->safe_read_ == emb_->group_safe_read_) {
        stop = true;
      }
    }
    return ctr - 3;
  }

  inline uint64_t getModulo() const { return transaction_information_.size(); }

  inline void decSafeRead() { emb_->safe_read_--; }

  inline void decGroupSafeRead() { emb_->group_safe_read_--; }

  inline void incInactive() { emb_->inactive_ctr_++; }

  inline void decInactive() { emb_->inactive_ctr_--; }
};

template <typename EpochManagerBase, typename EpochManager>
class EpochGuard {
  EpochManager* em_;
  uint64_t safe_read_version_ = std::numeric_limits<uint64_t>::max();
  uint64_t commit_ctr_ = std::numeric_limits<uint64_t>::max();

 public:
  EpochGuard(EpochManagerBase* em) {
    em_ = em->get();
    em_->pin();
  }

  EpochGuard(const EpochGuard& eg) {
    em_ = eg.em_;
    em_->pin();
  }

  ~EpochGuard() {
    if (safe_read_version_ != std::numeric_limits<uint64_t>::max()) {
      em_->decSafeRead();
      em_->decGroupSafeRead();
    }
    em_->unpin();
  }

  inline void add(void* ptr) { em_->add(ptr); }

  inline void erase(typename EpochManager::FuncVoidIntVoid func, void* chain, uint64_t offset, void* ptr) {
    em_->erase(func, chain, offset, ptr);
  }

  inline uint64_t commit() {
    commit_ctr_ = em_->getCurrentCounter();
    return commit_ctr_;
  }

  inline void waitSafeRead() {
    safe_read_version_ = em_->waitSafeRead(em_->myCounter());
    // std::cout << std::this_thread::get_id() << ": " << safe_read_version_ << std::endl;
  }

  inline uint64_t getSafeReadVersion() { return safe_read_version_; }

  inline uint64_t getCommitCtr() { return commit_ctr_; }

  inline uint8_t getSaveCounter() { return ((em_->getCurrentCounter() + 1) % em_->getModulo()); }

  inline uint8_t getCurrentCounter() { return em_->getCurrentCounter() % em_->getModulo(); }

  inline void incInactive() { em_->incInactive(); }

  inline void decInactive() { em_->decInactive(); }
};

};  // namespace atom
