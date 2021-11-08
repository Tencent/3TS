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

#include "chunk_allocator.hpp"
#include "atomic_unordered_map.hpp"
#include <sys/mman.h>

namespace common {
thread_local std::pair<std::atomic<uint64_t>, std::deleted_unique_ptr<void>>*
    common::ChunkAllocator::count_address_pair[2] = {nullptr, nullptr};
thread_local void* common::ChunkAllocator::chunk_ptr[2] = {nullptr, nullptr};
thread_local uint64_t common::ChunkAllocator::chunk_ptr_offset[2] = {0, 0};
thread_local std::vector<std::unique_ptr<std::pair<std::atomic<uint64_t>, std::deleted_unique_ptr<void>>>>
    common::ChunkAllocator::free_chunks{};

void ChunkAllocator::freeChunk(std::pair<std::atomic<uint64_t>, std::deleted_unique_ptr<void>>* cap,
                               bool free = false) {
  if (!free && free_chunks.size() < max_page_hold_) {
    free_chunks.emplace_back(cap);
  } else {
    cap->second.reset();
    delete cap;
  }
}

void ChunkAllocator::retrievePtr(uint64_t size, bool long_living) {
  if (chunk_ptr[long_living] == nullptr || chunk_ptr_offset[long_living] + size > page_size_) {
    if (chunk_ptr[long_living] != nullptr) {
      auto res = count_address_pair[long_living]->first.fetch_or(1ul << 63);
      if (res == 0) {
        freeChunk(count_address_pair[long_living]);
      }
    }
    if (free_chunks.size() > 0 && size <= page_size_) {
      count_address_pair[long_living] = free_chunks.back().release();
      free_chunks.pop_back();
      count_address_pair[long_living]->first = 1;
      chunk_ptr[long_living] = count_address_pair[long_living]->second.get();
    } else {
      auto alloc_size = (size > page_size_) ? size : page_size_;
      chunk_ptr[long_living] = mmap(nullptr, alloc_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      if (chunk_ptr == (void*)-1) {
        std::cout << "mmap error: " << errno << " - " << strerror(errno) << std::endl;
      }
      madvise(chunk_ptr[long_living], alloc_size, MADV_HUGEPAGE);
      if (chunk_ptr[long_living] == (void*)-1) {
        std::cout << "madv error: " << errno << " - " << strerror(errno) << std::endl;
      }
      auto ptr = std::deleted_unique_ptr<void>{chunk_ptr[long_living], [=](void* p) { munmap(p, alloc_size); }};
      count_address_pair[long_living] =
          new std::pair<std::atomic<uint64_t>, std::deleted_unique_ptr<void>>{1, std::move(ptr)};
    }
    chunk_ptr_offset[long_living] = 0;
  } else {
    count_address_pair[long_living]->first++;
  }
}

void ChunkAllocator::printDetails() {}

void ChunkAllocator::remove(void* p) {
  if (p == nullptr)
    return;

  uintptr_t addr_ptr = reinterpret_cast<uintptr_t>(p);

  void* cap = nullptr;
  memcpy(&cap, reinterpret_cast<void*>(addr_ptr - 8), 8);

  auto tcap = reinterpret_cast<std::pair<std::atomic<uint64_t>, std::deleted_unique_ptr<void>>*>(cap);

  auto res = tcap->first.fetch_sub(1);

  if (res == ((1ul << 63) | 1)) {
    freeChunk(tcap);
  }
}

void ChunkAllocator::tidyUp() {}
};  // namespace common
