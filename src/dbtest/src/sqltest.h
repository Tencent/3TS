/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: farrisli (farrisli@tencent.com)
 *
 */
#include "sql_cntl.h"
// Executor class for managing and executing test sequences.
class JobExecutor {
public:
    bool ExecAllTestSequence(std::vector<TestSequence> test_sequence_list);
    bool ExecTestSequence(TestSequence& test_sequence, TestResultSet& test_result_set, DBConnector db_connector);
};
