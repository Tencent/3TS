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
#include "epoch_manager.hpp"
#include "atomic_data_structures_decl.hpp"
#include "atomic_extent_vector.hpp"
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <functional>
#include <iostream>
#include <type_traits>
#include <vector>


namespace std {
template <typename T>
using deleted_unique_ptr =
    std::unique_ptr<T, std::function<void(std::conditional_t<std::is_array_v<T>, std::decay_t<T>, T*>)>>;
};

namespace common {
class ChunkAllocator {  //: std::allocator_traits<common::ChunkAllocator<T, concurrent_threads, page_size>> {
 private:
  static constexpr uint64_t max_page_hold_ = 16;
  static constexpr uint8_t alignment_ = 8;
  static constexpr unsigned int bits_page_ = 24;
  static constexpr uint64_t page_size_ = 1 << bits_page_;
  static constexpr uint8_t bits_chunks_ = 64 - bits_page_;

  static thread_local uint64_t chunk_ptr_offset[2];
  static thread_local void* chunk_ptr[2];
  static thread_local std::pair<std::atomic<uint64_t>, std::deleted_unique_ptr<void>>* count_address_pair[2];
  static thread_local std::vector<std::unique_ptr<std::pair<std::atomic<uint64_t>, std::deleted_unique_ptr<void>>>>
      free_chunks;

  ChunkAllocator(const common::ChunkAllocator& other) = delete;
  ChunkAllocator(common::ChunkAllocator&& other) = delete;
  ChunkAllocator& operator=(const common::ChunkAllocator& other) = delete;
  ChunkAllocator& operator=(common::ChunkAllocator&& other) = delete;

  void retrievePtr(uint64_t size, bool long_living);
  void remove(void* p);
  void freeChunk(std::pair<std::atomic<uint64_t>, std::deleted_unique_ptr<void>>* cap, bool free);

 public:
  void tidyUp();
  void printDetails();

  template <typename T>
  T* allocate(std::size_t n, bool long_living = false) {
    // std::cout << alignof(T) << std::endl;
    assert(alignof(T) <= alignment_);

    // return reinterpret_cast<T*>(malloc(sizeof(T)));

    constexpr uint64_t pad = (~(sizeof(T) + alignment_) + 1) & (alignment_ - 1);
    constexpr uint64_t size = sizeof(T) + 8 + pad;
    static_assert(pad < alignment_, "Alignment wrong!");
    static_assert(size <= page_size_, "Element larger than Page!");

    const auto alloc_size = n * size;

    retrievePtr(alloc_size, long_living);

    chunk_ptr_offset[long_living] += alloc_size;

    uintptr_t addr_ptr = reinterpret_cast<uintptr_t>(chunk_ptr[long_living]);
    addr_ptr += (chunk_ptr_offset[long_living] - alloc_size + pad + 8);

    uint64_t* vptr = reinterpret_cast<uint64_t*>(reinterpret_cast<void*>(addr_ptr - 8));
    *vptr = reinterpret_cast<uintptr_t>(count_address_pair[long_living]);

    // assert(chunk_ptr_new - chunk_ptr_old - 8 - pad < page_size_);
    // size_count_[(chunk_ptr_new - 1) >> bits_page_]->_a.fetch_add(chunk_ptr_new - chunk_ptr_old);

    return reinterpret_cast<T*>(reinterpret_cast<void*>(addr_ptr));
  }

  inline void deallocate(void* p, std::size_t n) {
    // free(p);
    remove(p);
  }

  template <typename T>
  inline void deallocate(T* p, std::size_t n) {
    p->~T();
    // free(p);
    remove(p);
  }
};
};  // namespace common
