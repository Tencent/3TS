#include "../../../contrib/deneva/system/chunk_allocator.hpp"
#include "../../../contrib/deneva/system/epoch_manager.hpp"
#include "../../../contrib/deneva/system/atomic_singly_linked_list.hpp"
#include "../../../contrib/deneva/system/atomic_unordered_map.hpp"
#include "../../../contrib/deneva/concurrency_control/row_opt_ssi.h"
#include "../../../contrib/deneva/system/mem_alloc.h"

#include "mock_thread.hpp"

#include <iostream>
#include <gtest/gtest.h>
#include <tbb/tbb.h>
#include <unordered_set>

class Row_opt_ssi;

auto ca = new common::ChunkAllocator{};
auto emp = new atom::EpochManagerBase<common::ChunkAllocator>{ca};
Row_opt_ssi *sg =  (Row_opt_ssi *) malloc(sizeof(Row_opt_ssi));
/*
 * SGT
 */




TEST(SGraph, InsertNoCycle) {
  
  Row_opt_ssi *sg =  (Row_opt_ssi *) malloc(sizeof(Row_opt_ssi));
  sg->setEMB(emp);
  sg->setAlloc(ca);

  testing::MockThread* t1 = new testing::MockThread{};
  testing::MockThread* t2 = new testing::MockThread{};
  testing::MockThread* t3 = new testing::MockThread{};

  t1->start();
  t2->start();
  t3->start();

  uintptr_t n1, n2, n3;
  t1->runSync([&] { n1 = sg->createNode(); });
  t2->runSync([&] { n2 = sg->createNode(); });
  t3->runSync([&] { n3 = sg->createNode(); });

  t2->runSync([&] { ASSERT_EQ(sg->insert_and_check(n1, false), true); });
  t3->runSync([&] { ASSERT_EQ(sg->insert_and_check(n1, false), true); });
  t3->runSync([&] { ASSERT_EQ(sg->insert_and_check(n2, false), true); });

  std::unordered_set<uint64_t> abort_tc;
  t1->runSync([&] { sg->abort(abort_tc); });
  t2->runSync([&] { sg->abort(abort_tc); });
  t3->runSync([&] { sg->abort(abort_tc); });

  delete t1;
  delete t2;
  delete t3;

}

TEST(SerializationGraph, InsertCycle) {

  assert(emp != nullptr);
  assert(ca != nullptr);

  
  sg->setEMB(emp);
  sg->setAlloc(ca);
  if (sg == nullptr) 
     printf("XXXXXXX-wrong in memalloc for sgraph\n");
  else 
     printf(">>>>>>>-OK,in memalloc for sgraph\n");

  

  testing::MockThread* t1 = new testing::MockThread{};
  testing::MockThread* t2 = new testing::MockThread{};
  testing::MockThread* t3 = new testing::MockThread{};

  t1->start();
  t2->start();
  t3->start();

  uintptr_t n1, n2, n3;
  t1->runSync([&] { n1 = sg->createNode(); });
  t2->runSync([&] { n2 = sg->createNode(); });
  t3->runSync([&] { n3 = sg->createNode(); });

  if (n1 == 0 || n2 == 0 || n3 == 0)
     printf("wrong in mem alloc for txn\n");
  else 
     printf("OK, in memalloc for txn\n");

  t2->runSync([&] { ASSERT_EQ(sg->insert_and_check(n1, false), true); });
  t3->runSync([&] { ASSERT_EQ(sg->insert_and_check(n1, false), true); });
  t3->runSync([&] { ASSERT_EQ(sg->insert_and_check(n2, false), true); });
  t1->runSync([&] { ASSERT_EQ(sg->insert_and_check(n3, false), false); });

  std::unordered_set<uint64_t> abort_tc;
  t1->runSync([&] { sg->abort(abort_tc); });
  t2->runSync([&] { sg->abort(abort_tc); });
  t3->runSync([&] { sg->abort(abort_tc); });

  delete t1;
  delete t2;
  delete t3;

}

TEST(SerializationGraph, InsertNoCycleCommitAfter) {

  Row_opt_ssi *sg =  (Row_opt_ssi *) malloc(sizeof(Row_opt_ssi));

  testing::MockThread* t1 = new testing::MockThread{};
  testing::MockThread* t2 = new testing::MockThread{};
  testing::MockThread* t3 = new testing::MockThread{};

  sg->setEMB(emp);
  sg->setAlloc(ca);

  t1->start();
  t2->start();
  t3->start();

  uintptr_t n1, n2, n3;
  t1->runSync([&] { n1 = sg->createNode(); });
  t2->runSync([&] { n2 = sg->createNode(); });
  t3->runSync([&] { n3 = sg->createNode(); });

  t2->runSync([&] { ASSERT_EQ(sg->insert_and_check(n1, false), true); });
  t3->runSync([&] { ASSERT_EQ(sg->insert_and_check(n1, false), true); });
  t3->runSync([&] { ASSERT_EQ(sg->insert_and_check(n2, false), true); });

  std::unordered_set<uint64_t> abort_tc;
  t2->runSync([&] { ASSERT_EQ(sg->checkCommited(), false); });
  t1->runSync([&] { ASSERT_EQ(sg->checkCommited(), true); });
  t3->runSync([&] { ASSERT_EQ(sg->checkCommited(), false); });
  t2->runSync([&] { ASSERT_EQ(sg->checkCommited(), true); });
  t3->runSync([&] { ASSERT_EQ(sg->checkCommited(), true); });

  delete t1;
  delete t2;
  delete t3;

  free(sg);
}




int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}