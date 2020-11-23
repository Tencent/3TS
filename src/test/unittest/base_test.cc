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
#include "../backend/history/parse_config.h"
#include "gtest/gtest.h"
using namespace std;
using namespace t3s;

shared_ptr<ActionSequenceChecker> checker = pre_checkers["SSI"];
vector<string> inputs = {
    // Dirty Write
    "W1a1 W2a2 C1 R3a2 C2 C3", "W1a1 W2a2 C1 R3a2 A2 C3", "W1a1 W2a2 A1 C2 R3a0 C3",
    "W1a1 W2a2 A1 A2 R3a1 C3", "W1a1 W2a2 C2 C1 R3a2 C3", "W1a1 W2a2 C2 A1 R3a0 C3",

    // Dirty Read
    "W1a1 R2a1 A1 C2"
    "W1a1 R2a1 C2 A1"

    // Intermediate Reads
    "W1a1 R2a1 W1a2 C1 C2",
    "W1a1 R2a1 W1a2 C2 C1",

    // Fuzzy or Non-Repeatable Read
    "R1a0 W2a1 C2 R1a1 C1", "R1a0 W2a1 R1a1 C2 C1",

    // Lost Update
    "R1a0 W2a1 C2 W1a2 C1",

    // Read Skew
    "R1a0 W2a1 W2b1 R1b1 C1 C2", "R1a0 W2a1 W2b1 R1b1 C2 C1", "R1a0 W2b1 W2a1 R1b1 C1 C2",
    "R1a0 W2b1 W2a1 R1b1 C2 C1", "R1a0 W2b1 R1b1 W2a1 C1 C2", "R1a0 W2b1 R1b1 W2a1 C2 C1",
    "R1a0 W2a1 W2b1 C2 R1b1 C1", "R1a0 W2a1 W2b1 C2 R1b1 C1", "R1a0 W2b1 W2a1 C2 R1b1 C1",
    "R1a0 W2b1 W2a1 C2 R1b1 C1",

    // Read and Write Skew
    "R1a0 W2a1 W2b1 C2 W1b1 C1", "R1a0 W2a1 W2b1 W1b1 C2 C1", "R1a0 W2a1 W2b1 W1b1 C1 C2",

    // Step Read Skew
    "R1a0 W2a1 W2b1 C2 W3b2 W3c1 C3 R1c1 C1",

    // Phantom
    //"S1a0 W2a1 W2b1 C2 S1a0 C1",

    // Write Skew
    "R1a0 R2b0 W1b1 W2a1 C1 C2",

    // Sawtooth Write Skew
    "R1a0 R2b0 R3c0 W1b1 W2c1 W3a1 C1 C2 C3",

    // Predicate Write Skew
    //"S1a0 W2a1 W2b1 C2 C1",
};

class BaseTest : public ::testing::TestWithParam<ActionSequence> {};

TEST_P(BaseTest, AllErr) {
  ActionSequence seq = GetParam();
  ASSERT_FALSE(checker->Check(seq));
}

vector<ActionSequence> Init() {
  vector<ActionSequence> seqs;
  for (const string &s : inputs) {
    ActionSequence seq;
    stringstream ss(s);
    ss >> seq;
    seqs.emplace_back(std::move(seq));
  }
  return seqs;
}

INSTANTIATE_TEST_CASE_P(AllErr, BaseTest, testing::ValuesIn(Init()));

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
