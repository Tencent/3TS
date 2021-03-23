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
#include "common.h"

class TestResultSet {
private:
    std::string test_case_type_;
    std::string isolation_;
    std::string result_type_;
    std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list_;
public:
    TestResultSet() {};
    TestResultSet(const std::string& test_case_type) {test_case_type_ = test_case_type;};

    void SetResultType(const std::string& result_type) {result_type_ = result_type;};
    void SetIsolation(const std::string& isolation) {isolation_ = isolation;};

    std::string TestCaseType() {return test_case_type_;};
    std::string Isolation() {return isolation_;};
    std::string ResultType() {return result_type_;};
    std::vector<std::unordered_map<int, std::vector<std::string>>> ExpectedResultSetList() {
        return expected_result_set_list_;
    };

    void AddSqlResultSet(std::unordered_map<int, std::vector<std::string>> sql_result_set) {
        expected_result_set_list_.push_back(sql_result_set);
    };
};

// The sql with txn_id in test case
class TxnSql {
private:
    int txn_id_;
    int sql_id_;
    std::string sql_;
    std::string test_case_type_;
public:
    TxnSql(const int sql_id, const int txn_id, const std::string& sql,
           const std::string& test_case_type):txn_id_(txn_id),
           sql_id_(sql_id), sql_(sql), test_case_type_(test_case_type) {};

    int TxnId() {return txn_id_;};
    int SqlId() {return sql_id_;};
    std::string Sql() {return sql_;};
    std::string TestCaseType() {return test_case_type_;};
};

// TestSequence->exception test case, include a series of TxnSql
class TestSequence {
private:
    // such as mysql_dirty-read
    std::string test_case_type_;
    std::vector<TxnSql> txn_sql_list_;
    int param_num_;
public:
    TestSequence() {};
    TestSequence(const std::string& test_case_type):test_case_type_(test_case_type) {};

    std::string TestCaseType() {return test_case_type_;};
    std::vector<TxnSql> TxnSqlList() {return txn_sql_list_;};
    int ParamNum() {return param_num_;};

    void AddTxnSql(TxnSql txn_sql) {txn_sql_list_.push_back(txn_sql);};
    void SetParamNum(const int param_num) {param_num_ = param_num;};
};

//read and parse sql from file
class CaseReader {
private:
    std::vector<TestSequence> test_sequence_list_;
    std::vector<TestResultSet> test_result_set_list_;
public:
    bool InitTestSequenceAndTestResultSetList(const std::string& test_path, const std::string& db_type);

    void AddTestSequence(TestSequence test_sequence) {
        test_sequence_list_.push_back(test_sequence);
    };
    void AddTestResultSet(TestResultSet test_result_set) {
        test_result_set_list_.push_back(test_result_set);
    };

    std::pair<TestSequence, TestResultSet> TestSequenceAndTestResultSetFromFile(const std::string& test_file, const std::string& db_type);

    std::vector<std::string> TxnIdAndSql(const std::string& line);
    std::pair<int, std::string> SqlIdAndResult(const std::string& line);
    std::string Isolation(const std::string& line);

    std::vector<TestSequence> TestSequenceList() {return test_sequence_list_;};
    std::vector<TestResultSet> TestResultSetList() {return test_result_set_list_;};
};

class ResultHandler {
public:
    bool IsSqlExpectedResult(std::vector<std::string> cur_result, std::vector<std::string> expected_result, const int sql_id, const std::string& sql);
    bool IsTestExpectedResult(std::unordered_map<int, std::vector<std::string>>& cur_result,
                              std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list,
                              std::unordered_map<int, std::string> sql_map, const std::string& test_process_file);
};

class Outputter {
public:
    void WriteResultTotal(std::vector<TestResultSet> test_result_set_list, const std::string& ret_file);

    bool PrintAndWriteTxnSqlResult(std::vector<std::string> cur_result,
                                   std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list,
                                   const int sql_id, const std::string& sql, const int session_id, const std::string& test_process_file);
    bool WriteTestCaseTypeToFile(const std::string& test_case_type, const std::string& test_process_file);
    bool WriteResultType(const std::string& result_type, const std::string& test_process_file);
};

extern Outputter outputter;
extern ResultHandler result_handler;
