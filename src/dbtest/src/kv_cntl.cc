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
KVParser kv_parser;

// execute
bool MongoConnector::ExecInsertkV(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                                  bsoncxx::document::value& doc_value, TestResultSet& test_result_set, const std::string test_process_file) {
    const std::string blank(blank_base*(session_id - 1), ' ');
    try {
        mongocxx::client_session& session = session_pool_[session_id - 1];
        std::cout << blank << "T" << session_id << " execute stmt: '" << stmt << "'" << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "T" << session_id << " execute stmt: '" << stmt << "'" << std::endl;
        }

        //bsoncxx::document::value doc = document{} << "k" << "0" << "v" << "2" << finalize;
        coll.insert_one(session, doc_value.view());

    } catch (mongocxx::v_noabi::exception& e) {
        std::string e_info = e.what();

        std::cout << blank << "[ERROR] in MongoConnector::ExecInsertkV ->" << e_info << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "[ERROR] in MongoConnector::ExecInsertkV ->" << e_info << std::endl;
        }

        auto index_conflict = e_info.find("WriteConflict");
        if (index_conflict != e_info.npos) {
            test_result_set.SetResultType("Rollback\nReason: " + e_info);

            return false;
        }
    }

    return true;
}

bool MongoConnector::ExecDeleteKV(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                                  bsoncxx::document::value& doc_value, TestResultSet& test_result_set, const std::string test_process_file) {
    const std::string blank(blank_base*(session_id - 1), ' ');
    try {
        mongocxx::client_session& session = session_pool_[session_id - 1];

        std::cout << blank << "T" << session_id << " execute stmt: '" << stmt << "'" << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "T" << session_id << " execute stmt: '" << stmt << "'" << std::endl;
        }

        coll.delete_one(session, doc_value.view());

    } catch (mongocxx::v_noabi::exception& e) {
        std::string e_info = e.what();

        std::cout << blank << "[ERROR] in MongoConnector::ExecDeleteKV ->" << e_info << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "[ERROR] in MongoConnector::ExecDeleteKV ->" << e_info << std::endl;
        }
    }

    return true;
}

bool MongoConnector::ExecUpdatekV(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                                  bsoncxx::document::value& doc_value_filter, bsoncxx::document::value& doc_value_update,
                                  TestResultSet& test_result_set, const std::string test_process_file) {
    const std::string blank(blank_base*(session_id - 1), ' ');
    try {
        mongocxx::client_session& session = session_pool_[session_id - 1];

        std::cout << blank << "T" << session_id << " execute stmt: '" << stmt << "'" << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "T" << session_id << " execute stmt: '" << stmt << "'" << std::endl;
        }

        coll.update_many(session, doc_value_filter.view(), doc_value_update.view());

    } catch (mongocxx::v_noabi::exception& e) {
        std::string e_info = e.what();

        std::cout << blank << "[ERROR] in MongoConnector::ExecUpdatekV ->" << e_info << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "[ERROR] in MongoConnector::ExecUpdatekV ->" << e_info << std::endl;
        }

        auto index_conflict = e_info.find("WriteConflict");

        for (int i = 1;i <= 4;i++) {
            KVEndTxn(i, 1024, "ROLLBACK", test_result_set);
        }

        if (index_conflict != e_info.npos) {
            test_result_set.SetResultType("Rollback\nReason: " + e_info);
            return false;
        }
    }
    return true;
}


bool MongoConnector::ExecUpdatekVWithVInc(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                                          TestResultSet& test_result_set, const std::string test_process_file) {
    const std::string blank(blank_base*(session_id - 1), ' ');
    try {
        const auto& [stmt_data_k, stmt_data_v] = kv_parser.StmtKVData(stmt);

        bsoncxx::document::value doc_value_find = kv_parser.MongoFind(stmt_data_k);

        std::string ret_v = ExecGetV(session_id, coll, doc_value_find);
        if (ret_v.empty()) {
            std::cout << "[ERROR]" << " get value failed, stmt: " << stmt << std::endl;
            return false;
        }
        auto index_p = stmt_data_v.find("+");
        auto index_s = stmt_data_v.find("-");
        std::string target_v = stmt_data_v.substr(1);
        int update_v = 0;
        if (index_p != ret_v.npos) {
            update_v = std::stoi(ret_v) + std::stoi(target_v);
        } else if (index_s != ret_v.npos){
            update_v = std::stoi(ret_v) - std::stoi(target_v);
        } else {
            std::cout << "[ERROR] " << "unkonw stmt: " << stmt << " t1.vinc(k, v_opt)-> v_opt:+100 or -100" << std::endl;
        }

        std::vector<bsoncxx::document::value> doc = kv_parser.MongoUpdate(stmt_data_k, std::to_string(update_v));
        bsoncxx::document::value doc_value = doc[0];
        bsoncxx::document::value doc_value_update = doc[1];
        if (!ExecUpdatekV(session_id, stmt_id, stmt, coll, doc_value, doc_value_update, test_result_set, test_process_file)) {
            return false;
        }
    } catch(mongocxx::v_noabi::exception& e) {
        std::string e_info = e.what();

        std::cout << blank << "[ERROR] in MongoConnector::ExecUpdatekV ->" << e_info << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "[ERROR] in MongoConnector::ExecUpdatekV ->" << e_info << std::endl;
        }

        auto index_conflict = e_info.find("WriteConflict");

        for (int i = 1;i <= 4;i++) {
            KVEndTxn(i, 1024, "ROLLBACK", test_result_set);
        }

        if (index_conflict != e_info.npos) {
            test_result_set.SetResultType("Rollback\nReason: " + e_info);
            return false;
        }
    }
    return true;
}

std::string MongoConnector::ExecGetV(const int session_id, mongocxx::collection& coll, bsoncxx::document::value& doc_value) {
    const std::string blank(blank_base*(session_id - 1), ' ');
    try {
        mongocxx::client_session& session = session_pool_[session_id - 1];
        mongocxx::cursor cursor = coll.find(session, doc_value.view());
        std::pair<std::string, std::string> data;
        for (auto&& doc : cursor) {
            std::string doc_json = bsoncxx::to_json(doc);
            data = kv_parser.DocKVData(doc_json);
        }
        return data.second;
    } catch(mongocxx::v_noabi::exception& e) {
        std::string e_info = e.what();
        std::cout << blank << "[ERROR] in MongoConnector::ExecUpdatekV ->" << e_info << std::endl;
        return "";
    }
}

bool MongoConnector::ExecFindKV(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                                bsoncxx::document::value& doc_value, TestResultSet& test_result_set,
                                std::unordered_map<int, std::vector<std::string>>& cur_result_set,
                                const int param_num, const std::string test_process_file) {
    const std::string blank(blank_base*(session_id - 1), ' ');
    try {
        mongocxx::client_session& session = session_pool_[session_id - 1];

        auto index_a = stmt.find("*");
        
        std::cout << blank << "T" << session_id << " execute stmt: '" << stmt << "'" << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "T" << session_id << " execute stmt: '" << stmt << "'" << std::endl;
        }

        mongocxx::cursor cursor = [&] {
            if (index_a != stmt.npos) {
                return coll.find(session, {});
            } else {
                return coll.find(session, doc_value.view());
            }
        }();

        int exist = 0;
        for (auto&& doc : cursor) {
            std::string row = "";
            std::string doc_json = bsoncxx::to_json(doc);
            const auto& [k_data, v_data] = kv_parser.DocKVData(doc_json);
            row = "(" + k_data + "," + v_data + ")";
            if (cur_result_set.find(stmt_id) != cur_result_set.end()) {
                cur_result_set[stmt_id].push_back(row);
            } else {
                std::vector<std::string> sql_result;
                sql_result.push_back(row);
                cur_result_set[stmt_id] = sql_result;
            }
            exist = 1;
        }

        if (exist == 0) {
            std::vector<std::string> sql_result;
            sql_result.push_back("null");
            cur_result_set[stmt_id] = sql_result;
        }
        std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list = test_result_set.ExpectedResultSetList();
        outputter.PrintAndWriteTxnSqlResult(cur_result_set[stmt_id], expected_result_set_list, stmt_id, stmt, session_id, test_process_file);
    } catch (mongocxx::v_noabi::exception& e) {
        std::string e_info = e.what();

        std::cout << blank << "[ERROR] in MongoConnector::ExecFindkV ->" << e_info << std::endl;

        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "[ERROR] in MongoConnector::ExecFindkV ->" << e_info << std::endl;
        }

        for (int i = 1;i <= 4;i++) {
            KVEndTxn(i, 1024, "ROLLBACK", test_result_set);
        }

        auto index_no_read = e_info.find("Unable to read from a snapshot due to pending collection catalog changes");
        if (index_no_read != e_info.npos) {
            test_result_set.SetResultType("Rollback\nReason: " + e_info);
            return false;
        }
    }

    return true;
}

// transaction options
bool MongoConnector::KVStartTxn(const int session_id, const int stmt_id, TestResultSet& test_result_set, const std::string test_process_file) {
    const std::string blank(blank_base*(session_id - 1), ' ');
    try {
        mongocxx::client_session& session = session_pool_[session_id - 1];

        std::cout << blank << "T" << session_id << " start transaction success" << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "T" << session_id << " start transaction success" << std::endl;
        }

        session.start_transaction(opts_);

    } catch (mongocxx::v_noabi::exception& e) {
        std::string e_info = e.what();

        std::cout << blank << "[ERROR] in MongoConnector::KVStartTxn ->" << e_info << std::endl;

        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "[ERROR] in MongoConnector::KVStartTxn ->" << e_info << std::endl;
        }
    }
    return true;
}

bool MongoConnector::KVEndTxn(int session_id, int stmt_id, const std::string& opt,
                              TestResultSet& test_result_set, const std::string test_process_file) {
    const std::string blank(blank_base*(session_id - 1), ' ');
    try {
        mongocxx::client_session& session = session_pool_[session_id - 1];
        if (stmt_id != 1024) {
            std::cout << blank << "T" << session_id << " " << opt << " transaction success" << std::endl;
        }
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "T" << session_id << " " << opt << " transaction success" << std::endl;
        }

        if (opt == "COMMIT") {
            session.commit_transaction();
        } else if (opt == "ROLLBACK") {
            session.abort_transaction(); 
        } else {
            std::cout << "[ERROR] unknow opt in MongoConnector::KVEndTxn" << std::endl;
        }

    } catch (mongocxx::v_noabi::exception& e) {
        std::string e_info = e.what();

        if (stmt_id != 1024) {
            std::cout << blank << "[ERROR] in MongoConnector::KVEndTxn ->" << e_info << std::endl;
        }

        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << blank << "[ERROR] in MongoConnector::KVEndTxn ->" << e_info << std::endl;
        }

        auto index_conflict = e_info.find("WriteConflict");
        if (index_conflict != e_info.npos) {
            test_result_set.SetResultType("Rollback\nReason: " + e_info);
            return false;
        }
    }

    return true;
}

std::string KVParser::MongoOpt(const std::string& stmt) {
    auto index_get = stmt.find("get");
    auto index_get_p = stmt.find("getpred");
    auto index_put = stmt.find("put");
    auto index_put_p = stmt.find("putpred");
    auto index_vinc = stmt.find("vinc");
    auto index_begin = stmt.find("begin");
    auto index_commit = stmt.find("commit");
    auto index_rollback = stmt.find("rollback");
    std::string opt = "";
    if (index_get != stmt.npos) {
        if (index_get_p != stmt.npos) {
            opt = "findpred";
        } else {
            opt = "find";
        }
    } else if (index_put != stmt.npos) {
        const auto& [stmt_data_k, stmt_data_v] = StmtKVData(stmt);
        if (index_put_p == stmt.npos) {
            if (FindFromKCache(stmt_data_k)) {
                opt = "update";
            } else {
                opt = "insert";
                PutIntoKCache(stmt_data_k);
            }
        } else {
            opt = "updatepred";
        }
    } else if (index_vinc != stmt.npos) {
        opt = "vinc";
    } else if (index_begin != stmt.npos) {
        opt = "begin";
    } else if (index_commit != stmt.npos) {
        opt = "commit";
    } else if (index_rollback != stmt.npos) {
        opt = "rollback";
    }

    return opt;
}

std::pair<std::string, std::string> KVParser::DocKVData(const std::string& doc_json) {
    std::pair<std::string, std::string> doc_data;
    auto index_k = doc_json.find("k");
    auto index_right = doc_json.find_last_of("}");
    if (index_k != doc_json.npos && index_right != doc_json.npos) {
        std::string data = doc_json.substr(index_k, index_right - index_k - 1);
        auto index_v = data.find("v");
        auto index_dot = data.find(",");
        auto index_m_f = data.find_first_of(":");
        auto index_m_l = data.find_last_of(":");
        if (index_v != data.npos && index_dot != data.npos && index_m_f != data.npos && index_m_l != data.npos) {
            std::string k_data = data.substr(index_m_f + 1, index_dot - index_m_f - 1);
            std::string v_data = data.substr(index_m_l + 1);
            TrimQuo(k_data);
            TrimQuo(v_data);
            TrimSpace(k_data);
            TrimSpace(v_data);
            doc_data.first = k_data;
            doc_data.second = v_data;
        }
    }
    return doc_data;
}

std::pair<std::string, std::string> KVParser::StmtKVData(const std::string& stmt) {
    auto index_left = stmt.find("(");
    auto index_right = stmt.find(")");
    auto index_dot = stmt.find(",");

    std::pair<std::string, std::string> stmt_data;

    if (index_left != stmt.npos && index_right != stmt.npos && index_dot != stmt.npos) {
        std::string stmt_data_k = stmt.substr(index_left + 1, index_dot - index_left - 1);
        std::string stmt_data_v = stmt.substr(index_dot + 1, index_right - index_dot - 1);
        if (!stmt_data_k.empty() && !stmt_data_v.empty()) {
            TrimSpace(stmt_data_k);
            TrimSpace(stmt_data_v);
            stmt_data.first = stmt_data_k;
            stmt_data.second = stmt_data_v;
        }
    } else if (index_dot == stmt.npos) {
        std::string stmt_data_k = stmt.substr(index_left + 1, index_right - index_left - 1);
        if (!stmt_data_k.empty()) {
            TrimSpace(stmt_data_k);
            stmt_data.first = stmt_data_k;
            stmt_data.second = "";
        }
    } else {
        stmt_data.first = "";
        stmt_data.second = "";
    }

    return stmt_data;
}

std::string KVParser::StmtCollName(const std::string& stmt) {
    auto index_dot = stmt.find(".");
    std::string coll_name = "";
    if (index_dot != stmt.npos) {
        coll_name = stmt.substr(0, index_dot);
    }
    if (coll_name.empty()) {
        std::cout << "[ERROR] " << "please check stmt: " << stmt << " there is no coll_name" << std::endl;
    }

    return coll_name;
}

mongocxx::collection KVParser::GetColl(const std::string& stmt, MongoConnector& mongo_connector, int& is_first_use) {
    std::string coll_name = StmtCollName(stmt);
    if (!coll_name.empty()) {
        if (is_first_use == 1) {
            mongo_connector.InitColl(coll_name);
            is_first_use = 0;
        }
    }
    mongocxx::collection coll;
    if (!coll_name.empty()) {
        coll = GetFromCollCache(coll_name);
        if (!coll) {
            coll = mongo_connector.Coll(coll_name);
            PutIntoCollCache(coll_name, coll);
        }
    }

    return coll;
}

bsoncxx::document::value KVParser::MongoFind(const std::string& stmt_data_k) {
    bsoncxx::document::value doc_value = document{} << "k" << stmt_data_k << finalize;

    return doc_value;
}

bsoncxx::document::value KVParser::MongoFindPred(const std::string& stmt_data_k) {
    bsoncxx::document::value doc_value = document{} << "v" << stmt_data_k << finalize;

    return doc_value;
}

bsoncxx::document::value KVParser::MongoInsert(const std::string& stmt_data_k, const std::string& stmt_data_v) {
    auto builder = bsoncxx::builder::stream::document{};
    bsoncxx::document::value doc_value = builder << "k" << stmt_data_k << "v" << stmt_data_v << finalize;
    
    return doc_value;
}

std::vector<bsoncxx::document::value> KVParser::MongoUpdate(const std::string& stmt_data_k, const std::string& stmt_data_v) {
    auto builder_filter = bsoncxx::builder::stream::document{};
    auto builder_update = bsoncxx::builder::stream::document{};

    std::vector<bsoncxx::document::value> doc_update;
    bsoncxx::document::value doc_value = builder_filter << "k" << stmt_data_k << finalize;
    bsoncxx::document::value doc_value_update = builder_update << "$set" << open_document << "v" << stmt_data_v << close_document << finalize;
    doc_update.emplace_back(doc_value);
    doc_update.emplace_back(doc_value_update);

    return doc_update;
}

std::vector<bsoncxx::document::value> KVParser::MongoUpdatePred(const std::string& stmt_data_k, const std::string& stmt_data_v) {
    auto builder_filter = bsoncxx::builder::stream::document{};
    auto builder_update = bsoncxx::builder::stream::document{};

    std::vector<bsoncxx::document::value> doc_update;
    bsoncxx::document::value doc_value = builder_filter << "v" << stmt_data_k << finalize;
    bsoncxx::document::value doc_value_update = builder_update << "$set" << open_document << "v" << stmt_data_v << close_document << finalize;
    doc_update.emplace_back(doc_value);
    doc_update.emplace_back(doc_value_update);

    return doc_update;
}
