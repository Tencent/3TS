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
#include "gflags/gflags.h"
#include "sqltest.h"

DEFINE_string(db_type, "mysql", "data resource name, please see /etc/odbc.ini, such as mysql pg oracle ob tidb sqlserver crdb");
DEFINE_string(user, "test123", "username");
DEFINE_string(passwd, "Ly.123456", "password");
DEFINE_string(db_name, "test", "create database name");
DEFINE_int32(conn_pool_size, 4, "db_conn pool size");
DEFINE_string(isolation, "serializable", "transation isolation level: read-uncommitted read-committed repeatable-read serializable");
DEFINE_string(case_dir, "mysql", "test case dir name");
DEFINE_string(timeout, "2", "timeout");

bool JobExecutor::ExecTestSequence(TestSequence& test_sequence, TestResultSet& test_result_set, DBConnector db_connector) {
    std::string test_process_file = "./" + FLAGS_db_type + "/" + FLAGS_isolation + "/" + test_sequence.TestCaseType() + "_" + FLAGS_isolation + ".txt";
    std::cout << test_process_file << std::endl;
    std::ifstream test_process_tmp(test_process_file);
    if (test_process_tmp) {
        std::remove(test_process_file.c_str());
    }
    std::ofstream test_process(test_process_file, std::ios::app);

    if (!test_process) {
        std::cout << "create test_process_file failed" << std::endl;
    }

    test_process << "#### db_type: " + FLAGS_db_type + " ####" << std::endl;
    std::cout << "#### db_type: " + FLAGS_db_type + " ####" << std::endl;

    std::string test_case_type = test_sequence.TestCaseType();
    auto index_t = test_case_type.find_first_of("_");
    if (index_t != test_case_type.npos) {
        test_case_type = test_case_type.substr(int(index_t) + 1);
    }
    test_process << "#### test_type: " + test_case_type + " ####" << std::endl;
    test_process << "#### isolation: " + FLAGS_isolation + " ####\n" << std::endl;

    std::cout << "#### test_type: " + test_case_type + " ####" << std::endl;
    std::cout << "#### isolation: " + FLAGS_isolation + " ####\n" << std::endl;

    std::string current_info = "current_result: The query result of the current SQL statement. Each row of table is separated by a space, and the fields in each row are separated by commas";
    std::string expected_info = "expected_result: The expected result is the expected query result for each SQL that conforms to the serializability theory";
    std::cout << current_info << std::endl;
    std::cout << expected_info << std::endl;
    test_process << current_info << std::endl;
    test_process << expected_info << std::endl;

    std::cout << " " << std::endl;
    test_process << " " << std::endl;

    if (FLAGS_db_type != "oracle" && FLAGS_db_type != "ob") {
        test_process << "set TXN_ISOLATION = " + FLAGS_isolation + " for each session"<< std::endl;
    }

    std::vector<TxnSql> txn_sql_list = test_sequence.TxnSqlList();
    std::cout << dash + test_sequence.TestCaseType() + " test prepare" + dash << std::endl;
    test_process << dash + test_sequence.TestCaseType() + " test prepare" + dash << std::endl;
    //int i = 0;
    std::unordered_map<int, std::vector<std::string>> cur_result_set;
    std::unordered_map<int, std::string> sql_map;
    std::string table_name;
    int sql_id_old;
    for (auto& txn_sql : txn_sql_list) {
        int sql_id = txn_sql.SqlId();
        int txn_id = txn_sql.TxnId();
        std::string sql = txn_sql.Sql();
        sql_map[sql_id] = sql;

        std::string ret_type = test_result_set.ResultType();
        auto index_T = ret_type.find("Timeout");
        auto index_R = ret_type.find("Rollback");
        if (index_T != ret_type.npos || index_R != ret_type.npos) {
            for (int i = 0; i < FLAGS_conn_pool_size; i++) {
                if (FLAGS_db_type != "ob") {
                    if (FLAGS_db_type == "crdb") {
                        db_connector.ExecWriteSql(1024, "ROLLBACK TRANSACTION", test_result_set, i + 1);
                    } else {
                        db_connector.SQLEndTnx("rollback", i + 1, 1024, test_result_set, FLAGS_db_type);
                    }
                }
            }
            break;
        }

        auto index_read = sql.find("SELECT");
        auto index_read_1 = sql.find("select");
        auto index_begin = sql.find("BEGIN");
        auto index_begin_1 = sql.find("begin");
        auto index_commit = sql.find("COMMIT");
        auto index_commit_1 = sql.find("commit");
        auto index_rollback = sql.find("ROLLBACK");
        auto index_rollback_1 = sql.find("rollback");

        if (sql_id == 1 && sql_id_old == 0) {
            std::cout << "" << std::endl;
            test_process << "" << std::endl;
            std::cout << dash + test_sequence.TestCaseType() + " test run" + dash << std::endl;
            test_process << dash + test_sequence.TestCaseType() + " test run" + dash << std::endl;
        }
        if (index_read != sql.npos || index_read_1 != sql.npos) {
            if (!db_connector.ExecReadSql2Int(sql_id, sql, test_result_set, cur_result_set, txn_id, test_sequence.ParamNum(), test_process_file)) {
                return false;
            }
        } else if (index_begin != sql.npos || index_begin_1 != sql.npos) {
            if (FLAGS_db_type != "crdb" && FLAGS_db_type != "ob") {
                if (FLAGS_db_type == "tidb") {
                    if (!db_connector.ExecWriteSql(0, "BEGIN PESSIMISTIC;", test_result_set, txn_id)) {
                        return false;
                    }
                } else {
                    if (!db_connector.SQLStartTxn(txn_id, sql_id, test_process_file)) {
                        return false;
                    }
                    // set pg lock timeout
                    if (FLAGS_db_type == "pg") {
                        std::string sql_timeout = "SET LOCAL lock_timeout = '" + FLAGS_timeout + "s';";
                        if (!db_connector.ExecWriteSql(1024, sql_timeout, test_result_set, txn_id)) {
                            return false;
                        }
                    }
                }
            } else {
                if (FLAGS_db_type == "crdb") {
                    if (!db_connector.ExecWriteSql(0, "BEGIN TRANSACTION;", test_result_set, txn_id, test_process_file)) {
                        return false;
                    }
                } else {
                    if (!db_connector.ExecWriteSql(0, "BEGIN;", test_result_set, txn_id, test_process_file)) {
                        return false;
                    }
                }
            }
        } else if (index_commit != sql.npos || index_commit_1 != sql.npos) {
            if (FLAGS_db_type != "crdb") {
                if (!db_connector.SQLEndTnx("commit", txn_id, sql_id, test_result_set, test_process_file)) {
                    return false;
                }
            } else {
                if (!db_connector.ExecWriteSql(0, "COMMIT TRANSACTION;", test_result_set, txn_id, test_process_file)) {
                    return false;
                }
            }
        } else if (index_rollback != sql.npos || index_rollback_1 != sql.npos) {
            if (!db_connector.SQLEndTnx("rollback", txn_id, sql_id, test_result_set, test_process_file)) {
                return false;
            }
        } else {
            if (!db_connector.ExecWriteSql(sql_id, sql, test_result_set, txn_id, test_process_file)) {
                return false;
            }
        }
    sql_id_old = sql_id;
    }
    if (test_result_set.ResultType() == "") {
        if (result_handler.IsTestExpectedResult(cur_result_set, test_result_set.ExpectedResultSetList(), sql_map, test_process_file)) {
            test_result_set.SetResultType("Avoid\nReason: Data anomaly did not occur and the data is consistent");
        } else {
            test_result_set.SetResultType("Exception\nReason: Data anomaly is not recognized by the database, resulting in data inconsistencies");
        }
    }
    if(!outputter.WriteResultType(test_result_set.ResultType(), test_process_file)) {
        return false;
    }
    return true;
}

int main(int argc, char* argv[]) {
    // parse gflags args
    google::ParseCommandLineFlags(&argc, &argv, true);
    std::cout << "input param-> " << std::endl;
    std::cout << "  db_type: " + FLAGS_db_type << std::endl;
    std::cout << "  user: " + FLAGS_user << std::endl;
    std::cout << "  passwd: " + FLAGS_passwd << std::endl;
    std::cout << "  isolation: " + FLAGS_isolation << std::endl;
    // read test sequence
    CaseReader case_reader;
    std::string test_path_base = "t/";
    std::string test_path = test_path_base + FLAGS_db_type;
    if (!case_reader.InitTestSequenceAndTestResultSetList(test_path, FLAGS_db_type)) {
        std::cout << "init test sequence and test result set failed" << std::endl;
    }

    std::vector<TestSequence> test_sequence_list = case_reader.TestSequenceList();
    std::vector<TestResultSet> test_result_set_list = case_reader.TestResultSetList();

    // init db_connector
    std::cout << dash + "init db_connector start" + dash << std::endl;
    DBConnector db_connector;
    if (!db_connector.InitDBConnector(FLAGS_user, FLAGS_passwd, FLAGS_db_type, FLAGS_conn_pool_size)) {
        std::cout << "init db_connector failed" << std::endl;
    }
    // init database
    /*
    if (FLAGS_db_type != "crdb") {
        if (FLAGS_db_type != "oracle" && FLAGS_db_type != "ob") {
            std::cout << dash + "init database start" + dash << std::endl;
            TestResultSet test_rs__;
            db_connector.ExecWriteSql(0, "create database if not exists " + FLAGS_db_name, test_rs__, 1);
            db_connector.ExecWriteSql(0, "use " + FLAGS_db_name, test_rs__, 1);
        }
    }*/

    // set TXN_ISOLATION
    // crdb has only one isolation level, which is serializable by default
    if (FLAGS_db_type != "crdb" && FLAGS_db_type != "mongodb") {
        std::cout << dash + "set TXN_ISOLATION = " + FLAGS_isolation + dash << std::endl;
        //std::cout << dash + "set TIMEOUT = " + FLAGS_timeout + dash << std::endl;
        int idx = 1;
        for (auto hdbc : db_connector.DBConnPool()) {
            // set timeout
            if (!db_connector.SetTimeout(idx, FLAGS_timeout, FLAGS_db_type)) {
                return false;
            }
            if(!db_connector.SetIsolationLevel(hdbc, FLAGS_isolation, idx, FLAGS_db_type)) {
                return false;
            }
        idx++;
        }
        std::cout << "set TXN_ISOLATION = " + FLAGS_isolation + " success"<< std::endl;
    }

    // create test_process_output_file's dir
    if (access(FLAGS_db_type.c_str(), 0) == -1) {
        mkdir(FLAGS_db_type.c_str(), S_IRWXU);
    }
    // create isolation dir
    std::vector<std::string> iso_list = {"read-uncommitted", "read-committed", "repeatable-read", "serializable"};
    for (auto iso : iso_list) {
        std::string iso_dir = FLAGS_db_type + "/" + iso;
        if (access(iso_dir.c_str(), 0) == -1) {
            mkdir(iso_dir.c_str(), S_IRWXU);
        }
    }
    // send sql
    JobExecutor job_executor;
    int len = test_sequence_list.size();
    for (int i = 0; i < len; i++) {
        if (!job_executor.ExecTestSequence(test_sequence_list[i], test_result_set_list[i], db_connector)) {
            std::cout << "test sequence " + test_sequence_list[i].TestCaseType() + " execute failed" << std::endl;
        } else {
            std::string result_type = test_result_set_list[i].ResultType();
            std::cout << "Test Result: " << result_type + "\n" << std::endl;
        }
    }
    std::string ret_file = "./" + FLAGS_db_type + "/" + FLAGS_db_type + "_" + FLAGS_isolation + "_total-result.txt";
    outputter.WriteResultTotal(test_result_set_list, ret_file);
    db_connector.ReleaseConn();

    return 0;
}
