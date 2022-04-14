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
#include "kvtest.h"
#include "gflags/gflags.h"

DEFINE_string(db_type, "mongodb", "data resource name");
DEFINE_string(uri, "", "conn uri");
DEFINE_string(db_name, "test", "database name");
DEFINE_int32(conn_pool_size, 4, "session pool size");
DEFINE_string(isolation, "snapshot", "transation isolation level: snapshot");
DEFINE_string(timeout, "2", "timeout");
int is_first_use = 1;

bool JobExecutor::MongoStmtExcutor(const int session_id, const int stmt_id, const std::string& stmt,
                                   MongoConnector& mongo_connector, TestResultSet& test_result_set, 
                                   std::unordered_map<int, std::vector<std::string>>& cur_result_set, const std::string& test_process_file) {
    // get stmt's option
    std::string opt = kv_parser.MongoOpt(stmt);
    // excute opt
    if ("find" == opt || "findpred" == opt) {
        const auto& [stmt_data_k, stmt_data_v] = kv_parser.StmtKVData(stmt);
        mongocxx::collection coll = kv_parser.GetColl(stmt, mongo_connector, is_first_use);
        if (!coll) {
            std::cout << "[ERROR]" << "get coll failed" << std::endl;
            return false;
        }
        bsoncxx::document::value doc_value = [&] {
            if ("findpred" == opt) {
                return kv_parser.MongoFindPred(stmt_data_k);
            } else {
                return kv_parser.MongoFind(stmt_data_k);
            }
        }();
        
        if (!mongo_connector.ExecFindKV(session_id, stmt_id, stmt, coll, doc_value, test_result_set, cur_result_set, 2, test_process_file)) {
            return false;
        }
    } else if ("update" == opt || "updatepred" == opt) {
        const auto& [stmt_data_k, stmt_data_v] = kv_parser.StmtKVData(stmt);
        mongocxx::collection coll = kv_parser.GetColl(stmt, mongo_connector, is_first_use);
        if (!coll) {
            std::cout << "[ERROR]" << "get coll failed" << std::endl;
            return false;
        }

        std::vector<bsoncxx::document::value> doc = [&] {
            if ("updatepred" == opt) {
                return kv_parser.MongoUpdatePred(stmt_data_k, stmt_data_v);
            } else {
                return kv_parser.MongoUpdate(stmt_data_k, stmt_data_v);
            }
        }();

        bsoncxx::document::value doc_value = doc[0];
        bsoncxx::document::value doc_value_update = doc[1];

        if (!mongo_connector.ExecUpdatekV(session_id, stmt_id, stmt, coll, doc_value, doc_value_update, test_result_set, test_process_file)) {
            return false;
        }
    } else if ("vinc" == opt) {
    	const auto& [stmt_data_k, stmt_data_v] = kv_parser.StmtKVData(stmt);
        mongocxx::collection coll = kv_parser.GetColl(stmt, mongo_connector, is_first_use);
	    if (!coll) {
	        std::cout << "[ERROR]" << "get coll failed" << std::endl;
            return false;
	    }

        if (!mongo_connector.ExecUpdatekVWithVInc(session_id, stmt_id, stmt, coll, test_result_set, test_process_file)) {
            return false;
        }
        
    } else if ("insert" == opt) {
        const auto& [stmt_data_k, stmt_data_v] = kv_parser.StmtKVData(stmt);
        mongocxx::collection coll = kv_parser.GetColl(stmt, mongo_connector, is_first_use);
        if (!coll) {
            std::cout << "[ERROR]" << "get coll failed" << std::endl;
            return false;
        }
        bsoncxx::document::value doc_value = kv_parser.MongoInsert(stmt_data_k, stmt_data_v);

        if (!mongo_connector.ExecInsertkV(session_id, stmt_id, stmt, coll, doc_value, test_result_set, test_process_file)) {
            return false;
        }
    } else if ("begin" == opt) {
	    if (!mongo_connector.KVStartTxn(session_id, stmt_id, test_result_set, test_process_file)) {
            return false;
        }
    } else if ("commit" == opt) {
        if (!mongo_connector.KVEndTxn(session_id, stmt_id, "COMMIT", test_result_set, test_process_file)) {
            return false;
        }
    } else if ("rollback" == opt) {
	    if (!mongo_connector.KVEndTxn(session_id, stmt_id, "ROLLBACK", test_result_set)) {
            return false;
        }
    } else {
        std::cout << "[ERROR]" << "stmt: " << stmt << " unknown option: " << opt << " The current program supports the following commands: begin t1.get(k) t1.put(k,v) t1.getpred(v) t1.putpred(v,v) t1.vinc(k,+100) commit rollback" << std::endl;
    }
    return true;
}

bool JobExecutor::ExecTestSequence(TestSequence& test_sequence, TestResultSet& test_result_set, MongoConnector& mongo_connector) {
    // std::string test_process_file = "./" + FLAGS_db_type + "/" + FLAGS_isolation + "/" + test_sequence.TestCaseType() + "_" + FLAGS_isolation + ".txt";
    std::string test_process_file = "./" + FLAGS_db_type + "/" + FLAGS_isolation + "/" + test_sequence.TestCaseType() + ".txt";
    std::cout << "test_process_file : " << test_process_file << std::endl;
    // remove old test_process_file
    std::ifstream test_process_tmp(test_process_file);
    if (test_process_tmp) {
        std::remove(test_process_file.c_str());
    }
    // create new test_process_file
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

    // test start
    std::cout << dash << "test start" << dash << std::endl;
    std::vector<TxnSql> txn_sql_list = test_sequence.TxnSqlList();
    std::unordered_map<int, std::vector<std::string>> cur_result_set;
    std::unordered_map<int, std::string> stmt_map;
    std::string table_name;
    for (auto& txn_sql : txn_sql_list) {
        int stmt_id = txn_sql.SqlId();
        int session_id = txn_sql.TxnId();
        std::string stmt = txn_sql.Sql();
        stmt_map[stmt_id] = stmt;
        // If the current case timeout, all transactions are rollback
        std::string ret_type = test_result_set.ResultType();
        auto index_T = ret_type.find("Timeout");
        auto index_R = ret_type.find("Rollback");
        if (index_T != ret_type.npos || index_R != ret_type.npos) {
            for (int i = 0; i < FLAGS_conn_pool_size; i++) {
	            mongo_connector.KVEndTxn(session_id, stmt_id, "ROLLBACK", test_result_set);
            }
            break;
        }
        if (!MongoStmtExcutor(session_id, stmt_id, stmt, mongo_connector, test_result_set, cur_result_set, test_process_file)) {
            break;
        }
    }
    if (test_result_set.ResultType() == "") {
        if (result_handler.IsTestExpectedResult(cur_result_set, test_result_set.ExpectedResultSetList(), stmt_map, test_process_file)) {
            test_result_set.SetResultType("Avoid\nReason: Data anomaly did not occur and the data is consistent");
        } else {
            test_result_set.SetResultType("Anomaly\nReason: Data anomaly is not recognized by the database, resulting in data inconsistencies");
        }
    }
    if(!outputter.WriteResultType(test_result_set.ResultType(), test_process_file)) {
        return false;
    }
    // release resource
    kv_parser.ClearKCache();
    kv_parser.ClearCollCache();
    is_first_use = 1;
    return true;
}

bool JobExecutor::ExecAllTestSequence(std::vector<TestSequence>& test_sequence_list, std::vector<TestResultSet>& test_result_set_list,
                                      MongoConnector& mongo_connector, JobExecutor& job_executor) {
    int len = test_sequence_list.size();
    for (int i = 0; i < len; i++) {
        if (!job_executor.ExecTestSequence(test_sequence_list[i], test_result_set_list[i], mongo_connector)) {
            std::cout << "test sequence " + test_sequence_list[i].TestCaseType() + " execute failed" << std::endl;
        } else {
            std::string result_type = test_result_set_list[i].ResultType();
            std::cout << "\nTest Result: " << result_type + "\n" << std::endl;
        }
    }
    return true;
}

int main(int argc, char* argv[]) {
    // parser gflags args
    google::ParseCommandLineFlags(&argc, &argv, true);
    std::cout << "input param-> " << std::endl;
    std::cout << "  db_type: " << FLAGS_db_type << std::endl;
    std::cout << "  uri: " <<  FLAGS_uri << std::endl;
    std::cout << "  isolation: " <<  FLAGS_isolation << std::endl;
    // init mongo_connector 
    // provide IP, port, and db
    MongoConnector mongo_connector("mongodb://9.134.39.34:27037/admin", "testdb");
    //MongoConnector mongo_connector("mongodb://9.134.218.253:27037/admin", "testdb");
    mongo_connector.InitMongoConnector(4);
    // init excute obj
    CaseReader case_reader;
    JobExecutor job_executor;
    // create test_process_output_file's dir
    if (access(FLAGS_db_type.c_str(), 0) == -1) {
        mkdir(FLAGS_db_type.c_str(), S_IRWXU);
    }
    // create isolation dir
    std::string iso_dir = FLAGS_db_type + "/snapshot";
    if (access(iso_dir.c_str(), 0) == -1) {
        mkdir(iso_dir.c_str(), S_IRWXU);
    }
    // read kv case test file 
    std::string test_path_base = "t/";
    std::string test_path = test_path_base + "mongodb";
    if (!case_reader.InitTestSequenceAndTestResultSetList(test_path, "mongodb")) {
        std::cout << "init test sequence and test result set failed" << std::endl;
    }
    std::vector<TestSequence> test_sequence_list = case_reader.TestSequenceList();
    std::vector<TestResultSet> test_result_set_list = case_reader.TestResultSetList();
    // execute all case
    job_executor.ExecAllTestSequence(test_sequence_list, test_result_set_list, mongo_connector, job_executor);
    // write test result total file
    std::string ret_file = "./" + FLAGS_db_type + "/" + FLAGS_db_type + "_" + FLAGS_isolation + "_total-result.txt";
    outputter.WriteResultTotal(test_result_set_list, ret_file);
    return 0;
}
