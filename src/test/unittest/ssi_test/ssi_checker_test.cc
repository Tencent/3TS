/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: moggma@tencent.com
 *
 */
#include "../../backend/cca/ssi_checker.h"

#include "gtest/gtest.h"

TEST(SSICheckerTest, SingleTransaction) {
  std::vector<t3s::Action> acts{
      {t3s::Action::Type::READ, 0, 0},
      {t3s::Action::Type::WRITE, 0, 0},
      {t3s::Action::Type::COMMIT, 0},
  };

  t3s::ActionSequence seq(1, 1, acts);

  t3s::SSIChecker checker;
  ASSERT_TRUE(checker.Check(seq, nullptr));
}

TEST(SSICheckerTest, WWConfilct) {
  std::vector<t3s::Action> acts{{t3s::Action::Type::WRITE, 0, 0},
                                   {t3s::Action::Type::WRITE, 1, 0},
                                   {t3s::Action::Type::COMMIT, 0},
                                   {t3s::Action::Type::COMMIT, 1}};

  t3s::ActionSequence seq(2, 1, acts);

  t3s::SSIChecker checker;
  ASSERT_FALSE(checker.Check(seq, nullptr));
}

TEST(SSICheckerTest, WriteScrew) {
  std::vector<t3s::Action> acts{
      {t3s::Action::Type::READ, 0, 0},  {t3s::Action::Type::READ, 0, 1},
      {t3s::Action::Type::READ, 1, 0},  {t3s::Action::Type::READ, 1, 1},
      {t3s::Action::Type::WRITE, 0, 0}, {t3s::Action::Type::WRITE, 1, 1},
      {t3s::Action::Type::COMMIT, 0},   {t3s::Action::Type::COMMIT, 1}};

  t3s::ActionSequence seq(2, 2, acts);

  t3s::SSIChecker checker;
  ASSERT_EQ(checker.RollbackNum(seq), 1);
}

TEST(SSICheckerTest, Success) {
  std::vector<t3s::Action> acts{
      {t3s::Action::Type::READ, 0, 1},  {t3s::Action::Type::READ, 1, 0},
      {t3s::Action::Type::WRITE, 0, 1}, {t3s::Action::Type::WRITE, 1, 0},
      {t3s::Action::Type::COMMIT, 0},   {t3s::Action::Type::COMMIT, 1}};

  t3s::ActionSequence seq(2, 2, acts);

  t3s::SSIChecker checker;
  ASSERT_TRUE(checker.Check(seq, nullptr));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
