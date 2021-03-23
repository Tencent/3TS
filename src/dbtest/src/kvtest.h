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
#include "kv_cntl.h"
class JobExecutor {
public:
    bool ExecAllTestSequence(std::vector<TestSequence>& test_sequence_list, std::vector<TestResultSet>& test_result_set_list,
                             MongoConnector& mongo_connector, JobExecutor& job_executor);
    bool ExecTestSequence(TestSequence& test_sequence, TestResultSet& test_result_set, MongoConnector& mongo_connector);
    bool MongoStmtExcutor(const int session_id, const int stmt_id, const std::string& stmt,
                          MongoConnector& mongo_connector, TestResultSet& test_result_set,
                          std::unordered_map<int, std::vector<std::string>>& cur_result_set, const std::string& test_process_file);
};
