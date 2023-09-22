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
    // Type of the test cases
    std::string test_case_type_;
    // Transaction isolation level
    std::string isolation_;
    // Test result type
    std::string result_type_;
    // Expected result set list
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
    // Transaction ID
    int txn_id_;
    // ID of the SQL statement
    int sql_id_;
    // SQL statement
    std::string sql_;
    // Type of the test case
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
    // Type of the test cases, such as mysql_dirty-read
    std::string test_case_type_;
    // List of test sequences
    std::vector<TxnSql> txn_sql_list_;
    // The number of columns in the result set.
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
    // Store test sequences read from the file
    std::vector<TestSequence> test_sequence_list_;
    // Store the expected test results read from the file
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

    // Parse execution order ID, transaction ID and SQL from the given line
    std::vector<std::string> TxnIdAndSql(const std::string& line);
    // Parse SQL ID and result from the given line
    std::pair<int, std::string> SqlIdAndResult(const std::string& line);
    // Parse isolation level from the given line
    std::string Isolation(const std::string& line);

    std::vector<TestSequence> TestSequenceList() {return test_sequence_list_;};
    std::vector<TestResultSet> TestResultSetList() {return test_result_set_list_;};
};

// Process and validate the expected test results
class ResultHandler {
public:
    // Compare the current result of a single SQL query with its expected result
    bool IsSqlExpectedResult(std::vector<std::string> cur_result, std::vector<std::string> expected_result, const int sql_id, const std::string& sql);
    // Verify if the current result set of SQL queries matches any of the expected results in the result set list
    bool IsTestExpectedResult(std::unordered_map<int, std::vector<std::string>>& cur_result,
                              std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list,
                              std::unordered_map<int, std::string> sql_map, const std::string& test_process_file);
};

class Outputter {
public:
    // Write the overall test results to the specified file
    void WriteResultTotal(std::vector<TestResultSet> test_result_set_list, const std::string& ret_file);

    // Print the results of the transaction SQL
    bool PrintAndWriteTxnSqlResult(std::vector<std::string> cur_result,
                                   std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list,
                                   const int sql_id, const std::string& sql, const int session_id, const std::string& test_process_file);
    // Write the test case type to the specified file
    bool WriteTestCaseTypeToFile(const std::string& test_case_type, const std::string& test_process_file);
    // Write the result type to the specified file
    bool WriteResultType(const std::string& result_type, const std::string& test_process_file);
};

extern Outputter outputter;
extern ResultHandler result_handler;
