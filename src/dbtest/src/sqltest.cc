/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: axingguchen xenitchen farrisli (axingguchen,xenitchen,farrisli@tencent.com)
 *
 */
#include "gflags/gflags.h"
#include "sqltest.h"
#include <thread>
#include <unistd.h>
#include <mutex>
#include <regex>

// Implement command-line argument parsing based on Google's gflags library
DEFINE_string(db_type, "mysql", "data resource name, please see /etc/odbc.ini, such as mysql pg oracle ob tidb sqlserver crdb");
DEFINE_string(user, "test123", "username");
DEFINE_string(passwd, "Ly.123456", "password");
DEFINE_string(db_name, "test", "create database name");
DEFINE_int32(conn_pool_size, 6, "db_conn pool size");
DEFINE_string(isolation, "serializable", "transation isolation level: read-uncommitted read-committed repeatable-read serializable");
DEFINE_string(case_dir, "mysql", "test case dir name");
DEFINE_string(timeout, "20", "timeout");

// std::vector<std::mutex *> mutex_txn(5); // same as conn_pool_size
std::vector<pthread_mutex_t *> mutex_txn(FLAGS_conn_pool_size);  // same as conn_pool_size

/**
 * Tries to lock the specified mutex within the given timeout.
 * 
 * @param wait_second The timeout in seconds.
 * @param wait_nanosecond The timeout in nanoseconds.
 * @param txn_id The transaction ID.
 * @return True if the lock is acquired within the timeout, false otherwise.
 */
bool try_lock_wait(float wait_second, float wait_nanosecond, int txn_id)
{
    struct timespec timeoutTime;
    timeoutTime.tv_nsec = wait_nanosecond;
    timeoutTime.tv_sec = wait_second;
    return pthread_mutex_timedlock( mutex_txn[txn_id], &timeoutTime ) == 0; // == 0 locked else no lock
}


/**
 * Executes multiple SQL transactions in a multi-threaded environment.
 *
 * @param txn_sql_list The list of transaction SQL to execute.
 * @param test_sequence Represents the test sequence object.
 * @param test_result_set Represents the expected test result set object.
 * @param db_connector The object used for database interactions.
 * @param test_process_file The file for logging or recording the test process.
 * @param cur_result_set A map to store the current result set.
 * @param sleeptime The sleep time between transaction executions. Note: Different threads may have different sleep times to stagger their execution.
 * @return True if the transactions are executed successfully, false otherwise.
 */
bool MultiThreadExecution(std::vector<TxnSql>& txn_sql_list, TestSequence& test_sequence, TestResultSet& test_result_set, 
    DBConnector db_connector, std::string test_process_file, std::unordered_map<int, std::vector<std::string>>& cur_result_set, int sleeptime){
    
    // time between queries
    // usleep(1000000*sleeptime); // 1 second
    usleep(100000*sleeptime); // 0.1 second
    
    std::ofstream test_process(test_process_file, std::ios::app);
    int txn_id;
    // If txn_sql_list is not empty, attempt to acquire a mutex lock associated with the current transaction ID.
    if (txn_sql_list.size() && txn_sql_list[0].TxnId()){
        txn_id = txn_sql_list[0].TxnId();
        pthread_mutex_lock(mutex_txn[txn_id]);
        /*  pthread for wait die
        float wait_second = 5;
        float wait_nanosecond = 0;
        if (!try_lock_wait(wait_second, wait_nanosecond, txn_id))
        {
                std::string blank(blank_base*(txn_id - 1), ' ');
                // std::cout<< blank <<"wait too long"<<std::endl;
                return false;
        }
        */
    }
    else{
        return false;
    }

    // Iterate through each transaction SQL in txn_sql_list.
    for (auto& txn_sql : txn_sql_list) {
        
        // for test purpose
        // std::cout << " SQLID: " << txn_sql_list[0].SqlId() << " TXNID: " <<  txn_sql_list[0].TxnId() << " SQL: " << txn_sql_list[0].Sql() <<  std::endl;
        int sql_id = txn_sql.SqlId();
        txn_id = txn_sql.TxnId();
        std::string sql = txn_sql.Sql();
        
        // oracle mode
        if (FLAGS_db_type == "oracle" || FLAGS_db_type == "ob") {
        // mysql mode
        // if (FLAGS_db_type == "oracle") {
            std::string sub_str ("IF EXISTS");
            if (sql.find(sub_str) != std::string::npos) {
                sql = std::regex_replace(sql, std::regex("IF EXISTS "), "");
                sql = std::regex_replace(sql, std::regex("if exists "), "");
                std::string sql_timeout = "alter session set ddl_lock_timeout = " + FLAGS_timeout + ";";
                if (!db_connector.ExecWriteSql(1024, sql_timeout, test_result_set, txn_id, test_process_file)) {
                    goto jump;
                }
            }
         
        }

        // replace sql keywords to capital
        sql = std::regex_replace(sql, std::regex("set"), "SET"); // replace 'set' -> 'SET'
        sql = std::regex_replace(sql, std::regex("from"), "FROM"); // replace 'from' -> 'FROM'
        sql = std::regex_replace(sql, std::regex("values"), "VALUES");
        sql = std::regex_replace(sql, std::regex("where"), "WHERE");
        sql = std::regex_replace(sql, std::regex("insert"), "INSERT");
        sql = std::regex_replace(sql, std::regex("select"), "SELECT"); 
        sql = std::regex_replace(sql, std::regex("update"), "UPDATE"); 
        

        std::string ret_type = test_result_set.ResultType();
        auto index_T = ret_type.find("Timeout");
        auto index_R = ret_type.find("Rollback");
        if (index_T != ret_type.npos || index_R != ret_type.npos) {
            for (int i = 0; i < FLAGS_conn_pool_size; i++) {
                if (FLAGS_db_type != "ob") {
                    if (FLAGS_db_type == "crdb") {
                        db_connector.ExecWriteSql(1024, "ROLLBACK TRANSACTION", test_result_set, i + 1, test_process_file);
                    } else {
                        db_connector.SQLEndTnx("rollback", i + 1, 1024, test_result_set, FLAGS_db_type,test_process_file);
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

        if (sql_id == 1) {
            std::cout << "" << std::endl;
            test_process << "" << std::endl;
            std::cout << dash + test_sequence.TestCaseType() + " test run" + dash << std::endl;
            test_process << dash + test_sequence.TestCaseType() + " test run" + dash << std::endl;
        }
        if (index_read != sql.npos || index_read_1 != sql.npos) {
            if (!db_connector.ExecReadSql2Int(sql_id, sql, test_result_set, cur_result_set, txn_id, test_sequence.ParamNum(), test_process_file)) {
                goto jump;
            }
        } else if (index_begin != sql.npos || index_begin_1 != sql.npos) {
            if (FLAGS_db_type != "crdb" && FLAGS_db_type != "ob") {
                if (FLAGS_db_type == "tidb") {
                    // Pessimistic mode
                    if (!db_connector.ExecWriteSql(sql_id, "BEGIN PESSIMISTIC;", test_result_set, txn_id, test_process_file)) {
                    // Optimistic mode
                    // if (!db_connector.ExecWriteSql(sql_id, "BEGIN OPTIMISTIC;", test_result_set, txn_id, test_process_file)) {
                        goto jump;
                    }
                // MyRocks SI mode, comment it for 4 normal levels
                } else if (FLAGS_db_type == "myrocks") {
                    if (!db_connector.ExecWriteSql(sql_id, "START TRANSACTION WITH CONSISTENT SNAPSHOT;", test_result_set, txn_id, test_process_file)) {
                        goto jump;
                    }
                // YugabyteDB https://docs.yugabyte.com/preview/explore/transactions/isolation-levels/
                } else if (FLAGS_db_type == "yugabyte") {
                    if (!db_connector.SQLStartTxn(txn_id, sql_id, test_process_file)) {
                        goto jump;
                    }
                    std::string begin_sql = "BEGIN TRANSACTION";
                    // beta version: You must set the YB-Tserver flag yb_enable_read_committed_isolation to true when start database service for read-committed level!!!
                    // if (FLAGS_isolation == "read-committed") {
                    //    begin_sql += " ISOLATION LEVEL READ COMMITTED;";
                    // }
                    if (FLAGS_isolation == "snapshot") {
                        begin_sql += ";";
                    } else {
                        begin_sql += " ISOLATION LEVEL SERIALIZABLE;";
                    }
                    if (!db_connector.ExecWriteSql(sql_id, begin_sql, test_result_set, txn_id, test_process_file)) {
                        goto jump;
                    }
                } else {
                    if (!db_connector.SQLStartTxn(txn_id, sql_id, test_process_file)) {
                        goto jump;
                    }
                    // set pg lock timeout
                    if (FLAGS_db_type == "pg") {
                        std::string sql_timeout = "SET LOCAL lock_timeout = '" + FLAGS_timeout + "s';";
                        if (!db_connector.ExecWriteSql(1024, sql_timeout, test_result_set, txn_id, test_process_file)) {
                            goto jump;
                        }
                    }
                    // // oracle this will kill the session.
                    // if (FLAGS_db_type == "oracle") {
                    //     std::string sql_timeout = "alter session set ddl_lock_timeout = " + FLAGS_timeout + ";";
                    //     std::cout << " sql: " << sql_timeout << std::endl;
                    //     if (!db_connector.ExecWriteSql(1024, sql_timeout, test_result_set, txn_id, test_process_file)) {
                    //         goto jump;
                    //     }
                    // }
                    // // alter session set ddl_lock_timeout = 5

                }
            } else {
                if (FLAGS_db_type == "crdb") {
                    if (!db_connector.ExecWriteSql(sql_id, "BEGIN TRANSACTION;", test_result_set, txn_id, test_process_file)) {
                        goto jump;
                    }
                } else {
                    if (!db_connector.ExecWriteSql(sql_id, "BEGIN;", test_result_set, txn_id, test_process_file)) {
                        goto jump;
                    }
                }
            }
        } else if (index_commit != sql.npos || index_commit_1 != sql.npos) {
            if (FLAGS_db_type != "crdb") {
                if (!db_connector.SQLEndTnx("commit", txn_id, sql_id, test_result_set, FLAGS_db_type, test_process_file)) {
                    goto jump;
                }
            } else {
                if (!db_connector.ExecWriteSql(sql_id, "COMMIT TRANSACTION;", test_result_set, txn_id, test_process_file)) {
                    goto jump;
                }
            }
        } else if (index_rollback != sql.npos || index_rollback_1 != sql.npos) {
            if (FLAGS_db_type != "crdb") {
                if (!db_connector.SQLEndTnx("rollback", txn_id, sql_id, test_result_set, FLAGS_db_type, test_process_file)) {
                    goto jump;
                }
            } else {
                if (!db_connector.SQLEndTnx("rollback", txn_id, sql_id, test_result_set, FLAGS_db_type, test_process_file)) {
                    goto jump;
                }
            }
        } else {
            if (!db_connector.ExecWriteSql(sql_id, sql, test_result_set, txn_id, test_process_file)) {
                goto jump;
            }
        }
        
        // mutex_txn[txn_id]->unlock(); 
    }
    pthread_mutex_unlock(mutex_txn[txn_id]);
    return true;
jump:
    pthread_mutex_unlock(mutex_txn[txn_id]);
    // mutex_txn[txn_id]->unlock(); 
    return false;
    

};

/**
 * Executes a series of database test transactions and writes the results to a file.
 *
 * @param test_sequence A TestSequence object containing a series of database transactions to execute.
 * @param test_result_set A TestResultSet object used to store test results.
 * @param db_connector A DBConnector object providing database connectivity.
 * @return True if the transactions are executed successfully, false otherwise.
 */
bool JobExecutor::ExecTestSequence(TestSequence& test_sequence, TestResultSet& test_result_set, DBConnector db_connector) {
    // Define the path and name of the test result file.
    std::string test_process_file = "./" + FLAGS_db_type + "/" + FLAGS_isolation + "/" + test_sequence.TestCaseType()  + ".txt";
    // std::string test_process_file = "./" + FLAGS_db_type + "/" + FLAGS_isolation + "/" + test_sequence.TestCaseType() + "_" + FLAGS_isolation + ".txt";
    std::cout << test_process_file << std::endl;
    // Open an output file stream named "test_process" for writing test process information.
    std::ifstream test_process_tmp(test_process_file);
    if (test_process_tmp) {
        std::remove(test_process_file.c_str());
    }
    std::ofstream test_process(test_process_file, std::ios::app);

    if (!test_process) {
        std::cout << "create test_process_file failed" << std::endl;
    }

    // Print the database type and isolation level to both the file and the console.
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

    // Get the list of transaction SQLs from test_sequence.
    std::vector<TxnSql> txn_sql_list = test_sequence.TxnSqlList();
    std::cout << dash + test_sequence.TestCaseType() + " test prepare" + dash << std::endl;
    test_process << dash + test_sequence.TestCaseType() + " test prepare" + dash << std::endl;
    //int i = 0;
    std::unordered_map<int, std::vector<std::string>> cur_result_set;
    std::unordered_map<int, std::string> sql_map;
    std::string table_name;
    

    std::vector<std::vector<TxnSql>> init_group;
    std::vector<std::vector<TxnSql>> split_groups;
    std::vector<TxnSql> group;

    // test purpose
    // for(int i = 0; i != txn_sql_list.size(); i++) {
    //     std::cout << txn_sql_list[i].SqlId() <<  std::endl;
    // }

    // std::cout << " SQLID: " << txn_sql_list[0].SqlId() << " TXNID: " <<  txn_sql_list[0].TxnId() << " SQL: " << txn_sql_list[0].Sql() <<  std::endl;
    int txn_id_old;
    int thread_cnt = 0;
    // Group transactions based on SQL ID and transaction ID.
    for (auto& txn_sql : txn_sql_list) {   
        // split into anther group
        if (txn_sql.SqlId() == 1 || txn_sql.TxnId() != txn_id_old){
            // put into init group
            if (txn_sql.SqlId() == 1) {
                init_group.push_back(group);
                group.clear();
            }
            // put into parallel groups
            else{
                split_groups.push_back(group);
                group.clear();
                thread_cnt = thread_cnt + 1;
            }
        }
        sql_map[txn_sql.SqlId()] = txn_sql.Sql();
        // add into the same group
        TxnSql txn_sql1(txn_sql.SqlId(), txn_sql.TxnId(), txn_sql.Sql(), txn_sql.TestCaseType());
        group.push_back(txn_sql1);
        txn_id_old = txn_sql.TxnId();
    }
    split_groups.push_back(group);
    thread_cnt = thread_cnt + 1;
    
   // Special handling for "crdb" database type.
    if (FLAGS_db_type == "crdb"){
        // remove non transaction commit
        // remove the first commit at the prepraration
        init_group[0].pop_back();
        // remove the last commit at verification selct
        split_groups[thread_cnt-1].pop_back();
    }

    // prepration phase 
    for (auto& group : init_group) {
        // for (auto& txn_sql : group) {   
        //     std::cout << " SQLID: " << txn_sql.SqlId() << " TXNID: " <<  txn_sql.TxnId() << " SQL: " << txn_sql.Sql() <<  std::endl;
        // }
        // std::cout << std::endl;
        if (! MultiThreadExecution(group, test_sequence, test_result_set, db_connector, test_process_file, cur_result_set, 0)) {return false;}

    }

    std::vector<std::thread> threads;

    // exlcude last group for parallel execution
    for (int i = 0; i < thread_cnt-1; i++) {
        threads.push_back(std::thread(MultiThreadExecution, std::ref(split_groups[i]), std::ref(test_sequence), std::ref(test_result_set), std::ref(db_connector), test_process_file, std::ref(cur_result_set), i+1));
    }

    for (auto &th : threads) {
        th.join();
        // th.detach();
    }

    // execute last group if correct (no error)
    if (test_result_set.ResultType() == "") {
        if (! MultiThreadExecution(split_groups[thread_cnt-1], test_sequence, test_result_set, db_connector, test_process_file, cur_result_set, 0)) {return false;}
    }

    if (test_result_set.ResultType() == "") {
        if (result_handler.IsTestExpectedResult(cur_result_set, test_result_set.ExpectedResultSetList(), sql_map, test_process_file)) {
            test_result_set.SetResultType("Avoid\nReason: Data anomaly did not occur and the data is consistent");
        } else {
            test_result_set.SetResultType("Anomaly\nReason: Data anomaly is not recognized by the database, resulting in data inconsistencies");
        }
    }
    if(!outputter.WriteResultType(test_result_set.ResultType(), test_process_file)) {
        return false;
    }
    return true;
}

int main(int argc, char* argv[]) {
    // parse gflags args
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    std::cout << "input param-> " << std::endl;
    std::cout << "  db_type: " + FLAGS_db_type << std::endl;
    std::cout << "  user: " + FLAGS_user << std::endl;
    std::cout << "  passwd: " + FLAGS_passwd << std::endl;
    std::cout << "  isolation: " + FLAGS_isolation << std::endl;

    // mutex for txn
    for(int i=0;i<FLAGS_conn_pool_size;++i) {
        mutex_txn[i] = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(mutex_txn[i], NULL);
    }

    // read test sequence
    CaseReader case_reader;
    std::string test_path_base = "t/";
    // std::string test_path = test_path_base + FLAGS_db_type;
    std::string test_path = test_path_base + "pg";
    if (!case_reader.InitTestSequenceAndTestResultSetList(test_path, FLAGS_db_type)) {
        std::cout << "init test sequence and test result set failed" << std::endl;
    }

    std::vector<TestSequence> test_sequence_list = case_reader.TestSequenceList();
    std::vector<TestResultSet> test_result_set_list = case_reader.TestResultSetList();


    // // one session for all
    // // init db_connector
    // std::cout << dash + "init db_connector start" + dash << std::endl;
    // DBConnector db_connector;
    // if (!db_connector.InitDBConnector(FLAGS_user, FLAGS_passwd, FLAGS_db_type, FLAGS_conn_pool_size)) {
    //     std::cout << "init db_connector failed" << std::endl;
    // }
    // // set TXN_ISOLATION
    // // crdb has only one isolation level, which is serializable by default
    // if (FLAGS_db_type != "crdb" && FLAGS_db_type != "mongodb") {
    //     std::cout << dash + "set TXN_ISOLATION = " + FLAGS_isolation + dash << std::endl;
    //     //std::cout << dash + "set TIMEOUT = " + FLAGS_timeout + dash << std::endl;
    //     int idx = 1;
    //     for (auto hdbc : db_connector.DBConnPool()) {
    //         // set timeout
    //         if (!db_connector.SetTimeout(idx, FLAGS_timeout, FLAGS_db_type)) {
    //             return false;
    //         }
    //         if(!db_connector.SetIsolationLevel(hdbc, FLAGS_isolation, idx, FLAGS_db_type)) {
    //             return false;
    //         }
    //     idx++;
    //     }
    //     std::cout << "set TXN_ISOLATION = " + FLAGS_isolation + " success"<< std::endl;
    // }

    // // create test_process_output_file's dir
    // if (access(FLAGS_db_type.c_str(), 0) == -1) {
    //     mkdir(FLAGS_db_type.c_str(), S_IRWXU);
    // }
    // // create isolation dir
    // // std::vector<std::string> iso_list = {"read-uncommitted", "read-committed", "repeatable-read", "serializable", "result_summary"};
    // std::vector<std::string> iso_list = {"read-committed", "serializable", "result_summary"};
    // for (auto iso : iso_list) {
    //     std::string iso_dir = FLAGS_db_type + "/" + iso;
    //     if (access(iso_dir.c_str(), 0) == -1) {
    //         mkdir(iso_dir.c_str(), S_IRWXU);
    //     }
    // }

    // // send sql
    // JobExecutor job_executor;
    // int len = test_sequence_list.size();
    // for (int i = 0; i < len; i++) {

    //     if (!job_executor.ExecTestSequence(test_sequence_list[i], test_result_set_list[i], db_connector)) {
    //         std::cout << "test sequence " + test_sequence_list[i].TestCaseType() + " execute failed" << std::endl;
    //     } else {
    //         std::string result_type = test_result_set_list[i].ResultType();
    //         std::cout << "Test Result: " << result_type + "\n" << std::endl;
    //     }
    // }
    // // output result and release connection 
    // std::string ret_file = "./" + FLAGS_db_type + "/result_summary" + "/" +  FLAGS_isolation + "_total-result.txt";
    // outputter.WriteResultTotal(test_result_set_list, ret_file);
    // db_connector.ReleaseConn();

    
    // one case one intialization
    // create test_process_output_file's dir
    if (access(FLAGS_db_type.c_str(), 0) == -1) {
        mkdir(FLAGS_db_type.c_str(), S_IRWXU);
    }
    // create isolation dir
    std::vector<std::string> iso_list;
    if (FLAGS_db_type == "cassandra") {
        iso_list = std::vector<std::string>({"one"});
    } else if (FLAGS_db_type == "yugabyte") {
        iso_list = std::vector<std::string>({"serializable", "snapshot"});
    } else {
        iso_list = std::vector<std::string>({"read-uncommitted", "read-committed", "repeatable-read", "serializable", "result_summary"});
    }
    //std::vector<std::string> iso_list = {"read-uncommitted", "read-committed", "repeatable-read", "serializable", "result_summary"};
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

        // init db_connector
        std::cout << dash + "init db_connector start" + dash << std::endl;
        DBConnector db_connector;
        if (!db_connector.InitDBConnector(FLAGS_user, FLAGS_passwd, FLAGS_db_type, FLAGS_conn_pool_size)) {
            std::cout << "init db_connector failed" << std::endl;
        }
        // set TXN_ISOLATION
        // crdb has only one isolation level, which is serializable by default
        if (FLAGS_db_type != "crdb" && FLAGS_db_type != "mongodb" && FLAGS_db_type != "yugabyte") {
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

        // exec
        if (!job_executor.ExecTestSequence(test_sequence_list[i], test_result_set_list[i], db_connector)) {
            std::cout << "test sequence " + test_sequence_list[i].TestCaseType() + " execute failed" << std::endl;
        } else {
            std::string result_type = test_result_set_list[i].ResultType();
            std::cout << "Test Result: " << result_type + "\n" << std::endl;
        }  

        // db release
        db_connector.ReleaseConn();
    }
    std::string ret_file = "./" + FLAGS_db_type + "/result_summary" + "/" +  FLAGS_isolation + "_total-result.txt";
    outputter.WriteResultTotal(test_result_set_list, ret_file);


    // remove mutex for txn
    for(int i=0;i<FLAGS_conn_pool_size;++i) {
        free(mutex_txn[i] );
    } 
    return 0;
}
