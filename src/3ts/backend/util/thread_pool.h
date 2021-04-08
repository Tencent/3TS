/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: williamcliu@tencent.com
 *
 */
#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <queue>
#include <thread>

#include "generic.h"

// std::atomic<std::shared_ptr<T>> is not supported until C++20, so we use std::atomic<T*> instead
// here

namespace ttts {

template <typename T>
class SpinLockQueue {
 public:
  void Push(T &&value) {
    while (std::atomic_flag_test_and_set_explicit(&spin_lock_, std::memory_order_acquire))
      ;
    queue_.emplace(std::move(value));
    std::atomic_flag_clear_explicit(&spin_lock_, std::memory_order_release);
  }

  std::optional<T> Pop() {
    while (std::atomic_flag_test_and_set_explicit(&spin_lock_, std::memory_order_acquire))
      ;
    std::optional<T> value;
    if (!queue_.empty()) {
      value = queue_.front();
      queue_.pop();
    }
    std::atomic_flag_clear_explicit(&spin_lock_, std::memory_order_release);
    return value;
  }

  bool IsEmpty() {
    while (std::atomic_flag_test_and_set_explicit(&spin_lock_, std::memory_order_acquire))
      ;
    const bool is_empty = queue_.empty();
    std::atomic_flag_clear_explicit(&spin_lock_, std::memory_order_release);
    return is_empty;
  }

 private:
  std::atomic_flag spin_lock_ = ATOMIC_FLAG_INIT;
  std::queue<T> queue_;
};

class Semaphore {
 private:
  int cnt_;
  std::mutex m_;
  std::condition_variable c_;

 public:
  Semaphore(int n) : cnt_(n) {}

  void Down() {
    std::unique_lock<std::mutex> lock(m_);
    c_.wait(lock, [&] { return cnt_ > 0; });
    --cnt_;
  }

  void Up() {
    std::lock_guard<std::mutex> lock(m_);
    ++cnt_;
    c_.notify_one();
  }
};

class ThreadPool {
 public:
  ThreadPool(const uint64_t size) : is_over_(false), workers_(size), produce_conf_(buffer_size_), consumer_conf_(0) {
    for (std::thread &worker : workers_) {
      worker = std::thread(std::bind(&ThreadPool::WorkerThread, this));
    }
  }

  ~ThreadPool() {
    is_over_ = true;
    for (std::thread &worker : workers_) {
      if (worker.joinable()) {
        consumer_conf_.Up();
        worker.join();
      }
    }
  }

  void PushTask(std::function<void()> &&f) {
    produce_conf_.Down();
    task_queue_.Push(std::move(f));
    consumer_conf_.Up();
  }

 private:
  void WorkerThread() {
    while (!is_over_.load() || !task_queue_.IsEmpty()) {
      consumer_conf_.Down();
      const std::optional<std::function<void()>> task = task_queue_.Pop();
      if (task.has_value()) {
        task.value()();
        produce_conf_.Up();
      } else {
        consumer_conf_.Up();
      }
    }
  }

  std::atomic<bool> is_over_;
  SpinLockQueue<std::function<void()>> task_queue_;
  std::vector<std::thread> workers_;

  static const uint64_t buffer_size_ = 1024;
  Semaphore produce_conf_;
  Semaphore consumer_conf_;
};

}  // namespace ttts
