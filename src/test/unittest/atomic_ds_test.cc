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

#include "../../../contrib/deneva/system/chunk_allocator.hpp"
#include "../../../contrib/deneva/system/epoch_manager.hpp"
#include "../../../contrib/deneva/system/atomic_singly_linked_list.hpp"
#include "../../../contrib/deneva/system/atomic_unordered_map.hpp"
#include "mock_thread.hpp"

#include <iostream>
#include <gtest/gtest.h>
#include <tbb/tbb.h>

auto ca = new common::ChunkAllocator{};
auto emp = new atom::EpochManagerBase<common::ChunkAllocator>{ca};

/*
 * AtomicSinglyLinkedList
 */

TEST(AtomicSinglyLinkedList, Insert) {
  atom::AtomicSinglyLinkedList<uint64_t> linkedList{ca, emp};
  for (uint64_t i = 0; i < 100; i++) {
    linkedList.push_front(i);
  }
  uint64_t i = 99;
  for (auto l : linkedList) {
    ASSERT_EQ(l, i);
    i--;
  }
  emp->remove();
}

TEST(AtomicSinglyLinkedList, InsertMultithread) {
  //tbb::task_scheduler_init init(16);
  tbb::task_arena arena(16);
  atom::AtomicSinglyLinkedList<uint64_t> linkedList{ca, emp};
  parallel_for(tbb::blocked_range<std::size_t>(0, 1000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      linkedList.push_front(i);
    }
    emp->remove();
  });

  uint64_t i = 0;
  for (auto l : linkedList) {
    i += l;
  }

  ASSERT_EQ(i, (1000 * 999) / 2);
}

TEST(AtomicSinglyLinkedList, InsertDelete) {
  atom::AtomicSinglyLinkedList<uint64_t> linkedList{ca, emp};
  uint64_t t = 0;
  uint64_t c = 0;
  for (uint64_t i = 0; i < 10000; i++) {
    if (i % 2 == 0 && i > 0) {
      bool found = linkedList.erase(i - 2);
      t -= i - 2;
      if (found)
        c--;
    }
    linkedList.push_front(i);
    t += i;
    c++;
  }
  uint64_t i = 9999;
  for (auto l : linkedList) {
    if (i % 2 != 0) {
      ASSERT_EQ(l, i);
    } else if (i == (10000 - 2)) {
      ASSERT_EQ(l, i);
    }

    if (i > (10000 - 2))
      i--;
    else
      i -= 2;
  }

  i = 0;
  for (auto l : linkedList) {
    i += l;
  }
  ASSERT_EQ(i, t);
  ASSERT_EQ(linkedList.size(), c);
  emp->remove();
}

TEST(AtomicSinglyLinkedList, InsertDeleteMultithreadCount) {
  atom::AtomicSinglyLinkedList<uint64_t> linkedList{ca, emp};
  //tbb::task_scheduler_init init(16);
  tbb::task_arena arena(16);
  std::atomic<uint64_t> t(0);
  parallel_for(tbb::blocked_range<std::size_t>(0, 100000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      uint64_t s = 0;
      bool found = linkedList.find(i - 2, s);
      if (found) {
        bool works = linkedList.erase(i - 2);
        if (works) {
          t -= s;
        }
      }

      linkedList.push_front(i);
      t += i;
    }
    emp->remove();
  });

  uint64_t i = 0;
  for (auto l : linkedList) {
    i += l;
  }

  ASSERT_EQ(i, t);
}

TEST(AtomicSinglyLinkedList, InsertDeleteMultithreadSize) {
 // tbb::task_scheduler_init init(16);
 tbb::task_arena arena(16);
  atom::AtomicSinglyLinkedList<uint64_t> linkedList{ca, emp};
  std::atomic<uint64_t> t(100000);
  parallel_for(tbb::blocked_range<std::size_t>(0, 100000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      bool works = linkedList.erase(i / 2);
      if (works)
        t--;

      linkedList.push_front(i);
    }
    emp->remove();
  });

  uint64_t i = 0;
  for (auto l : linkedList) {
    if (l > 0)
      i++;
    else
      i++;
  }

  ASSERT_EQ(i, t);
  ASSERT_EQ(linkedList.size(), t);
}

TEST(AtomicSinglyLinkedList, InsertReadMultithread) {
  //tbb::task_scheduler_init init(16);
  tbb::task_arena arena(16);
  atom::AtomicSinglyLinkedList<uint64_t> linkedList{ca, emp};

  parallel_for(tbb::blocked_range<std::size_t>(0, 10000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      uint64_t t = 0;
      for (auto l : linkedList) {
        if (l > 0)
          t++;
        if (t > 100)
          break;
      }
      linkedList.push_front(i);
    }
    emp->remove();
  });

  uint64_t i = 0;
  for (auto l : linkedList) {
    i += l;
  }

  ASSERT_EQ(i, (10000 * 9999) / 2);
}

TEST(AtomicSinglyLinkedList, InsertDeleteReadMultithread) {
  //tbb::task_scheduler_init init(32);
  tbb::task_arena arena(16);
  atom::AtomicSinglyLinkedList<uint64_t> linkedList{ca, emp};

  std::atomic<uint64_t> t(0);
  std::atomic<uint64_t> counter(0);

  parallel_for(tbb::blocked_range<std::size_t>(0, 100000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      if (i % 3 == 0 && i > 0) {
        uint64_t s = 0;
        bool found = linkedList.find(i - 1, s);
        if (found) {
          bool works = linkedList.erase(i - 1);
          if (works) {
            counter--;
            t -= s;
          }
        }
      }
      linkedList.push_front(i);
      t += i;
      counter++;

      uint64_t c = 0;
      for (auto l : linkedList) {
        if (l > 0)
          c++;
        if (c > 20)
          break;
      }
    }
    emp->remove();
  });

  uint64_t i = 0;
  uint64_t c = 0;
  for (auto l : linkedList) {
    i += l;
    c++;
  }

  ASSERT_EQ(i, t);
  ASSERT_EQ(linkedList.size(), c);
  ASSERT_EQ(c, counter);
}

/*
 * AtomicUnorderedMap
 */

TEST(AtomicUnorderedMap, Insert) {
  atom::AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::ChunkAllocator>
      unordered_map(100, ca, emp);

  for (uint64_t i = 0; i < 100; i++) {
    unordered_map.insert(i, i);
  }
  uint64_t i = 100;
  for (auto l : unordered_map) {
    if (l < 100)
      i--;
  }
  emp->remove();
  ASSERT_EQ(0, i);
}

TEST(AtomicUnorderedMap, InsertMultithread) {
  //tbb::task_scheduler_init init(16);
  tbb::task_arena arena(16);
  atom::AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::ChunkAllocator>
      unordered_map(100, ca, emp);

  parallel_for(tbb::blocked_range<std::size_t>(0, 1000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      unordered_map.insert(i, i);
    }
    emp->remove();
  });

  uint64_t i = 0;
  for (auto l : unordered_map) {
    i += l;
  }

  ASSERT_EQ(i, (1000 * 999) / 2);
}

TEST(AtomicUnorderedMap, InsertDelete) {
  atom::AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::ChunkAllocator>
      unordered_map(100, ca, emp);

  uint64_t t = 0;
  uint64_t c = 0;
  for (uint64_t i = 0; i < 10000; i++) {
    if (i % 2 == 0 && i > 0) {
      bool found = unordered_map.erase(i - 2);
      t -= i - 2;
      if (found)
        c--;
    }
    unordered_map.insert(i, i);
    t += i;
    c++;
  }
  uint64_t i = 10000;
  for (auto l : unordered_map) {
    if (l >= 10000 - 2)
      i--;
    else
      i -= 2;
  }
  ASSERT_EQ(i, 0);

  i = 0;
  for (auto l : unordered_map) {
    i += l;
  }
  emp->remove();
  ASSERT_EQ(i, t);
  ASSERT_EQ(unordered_map.size(), c);
}

TEST(AtomicUnorderedMap, InsertDeleteMultithreadCount) {
  //tbb::task_scheduler_init init(16);
  tbb::task_arena arena(16);
  atom::AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::ChunkAllocator>
      unordered_map(100, ca, emp);

  std::atomic<uint64_t> t(0);
  parallel_for(tbb::blocked_range<std::size_t>(0, 10000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      uint64_t s = 0;
      bool found = unordered_map.lookup(i - 2, s);
      if (found) {
        bool works = unordered_map.erase(i - 2);
        if (works) {
          t -= s;
        }
      }

      unordered_map.insert(i, i);
      t += i;
    }
    emp->remove();
  });

  uint64_t i = 0;
  for (auto l : unordered_map) {
    i += l;
  }

  ASSERT_EQ(i, t);
}

TEST(AtomicUnorderedMap, InsertDeleteMultithreadSize) {
  //tbb::task_scheduler_init init(16);
  tbb::task_arena arena(16);
  atom::AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::ChunkAllocator>
      unordered_map(100, ca, emp);

  std::atomic<uint64_t> t(10000);
  parallel_for(tbb::blocked_range<std::size_t>(0, 10000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      if (i % 3 == 0) {
        bool works = unordered_map.erase(i / 2);
        if (works)
          t--;
      }
      unordered_map.insert(i, i);
    }
    emp->remove();
  });

  uint64_t i = 0;
  for (auto l : unordered_map) {
    if (l > 0)
      i++;
    else
      i++;
  }

  ASSERT_EQ(i, t);
  ASSERT_EQ(unordered_map.size(), t);
}

TEST(AtomicUnorderedMap, InsertReadMultithread) {
  //tbb::task_scheduler_init init(16);
  tbb::task_arena arena(16);
  atom::AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::ChunkAllocator>
      unordered_map(100, ca, emp);

  parallel_for(tbb::blocked_range<std::size_t>(0, 10000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      uint64_t t = 0;
      for (auto l : unordered_map) {
        if (l > 0)
          t++;
        if (t > 100)
          break;
      }
      unordered_map.insert(i, i);
    }
    emp->remove();
  });

  uint64_t i = 0;
  for (auto l : unordered_map) {
    i += l;
  }

  ASSERT_EQ(i, (10000 * 9999) / 2);
}

TEST(AtomicUnorderedMap, InsertDeleteReadMultithread) {
  //tbb::task_scheduler_init init(32);
  tbb::task_arena arena(32);
  atom::AtomicUnorderedMap<uint64_t, uint64_t, atom::AtomicUnorderedMapBucket<uint64_t, uint64_t>,
                           common::ChunkAllocator>
      unordered_map(100, ca, emp);

  std::atomic<uint64_t> t(0);
  std::atomic<uint64_t> counter(0);

  parallel_for(tbb::blocked_range<std::size_t>(0, 100000), [&](const tbb::blocked_range<size_t>& range) {
    for (size_t i = range.begin(); i != range.end(); ++i) {
      if (i % 2 == 0 && i > 0) {
        uint64_t s = 0;
        bool found = unordered_map.lookup(i - 1, s);
        if (found) {
          bool works = unordered_map.erase(i - 1);
          if (works) {
            counter--;
            t -= s;
          }
        }
      }
      unordered_map.insert(i, i);
      t += i;
      counter++;

      uint64_t c = 0;
      for (auto l : unordered_map) {
        if (l > 0)
          c++;
        if (c > 20)
          break;
      }
    }
    emp->remove();
  });

  uint64_t i = 0;
  uint64_t c = 0;
  for (auto l : unordered_map) {
    i += l;
    c++;
  }

  ASSERT_EQ(i, t);
  ASSERT_EQ(unordered_map.size(), c);
  ASSERT_EQ(c, counter);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}






