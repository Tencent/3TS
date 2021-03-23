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
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/exception/exception.hpp>

#include "case_cntl.h"

using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;

class KVParser;
extern KVParser kv_parser;

class MongoConnector {
private:
    mongocxx::instance inst_;
    mongocxx::client client_;
    const std::string test_db_;
    std::vector<mongocxx::client_session> session_pool_;
    mongocxx::write_concern wc_majority_;
    mongocxx::read_concern rc_snapshot_;
    mongocxx::read_preference rp_primary_;
    mongocxx::options::transaction opts_;
public:
    // init
    MongoConnector(const std::string& conn_uri, const std::string test_db):client_(mongocxx::client{mongocxx::uri{conn_uri}}), test_db_(test_db) {};
    void InitMongoConnector(const int session_pool_size) {
        // init read/write concern
        wc_majority_.acknowledge_level(mongocxx::write_concern::level::k_majority);
        rc_snapshot_.acknowledge_level(mongocxx::read_concern::level::k_snapshot);
        rp_primary_.mode(mongocxx::read_preference::read_mode::k_primary);
        // init transaction opts
        InitTxnOpts();
        // init session pool
        auto db = client_[test_db_];
        for (int i = 0; i < session_pool_size; i++) {
            mongocxx::client_session session = client_.start_session();
            session_pool_.emplace_back(client_.start_session());
        };
        std::cout << "init mongo_connector success" << std::endl;
    };
    // init transaction opts
    void InitTxnOpts() {
        opts_.write_concern(wc_majority_);
        opts_.read_concern(rc_snapshot_);
        opts_.read_preference(rp_primary_);
    };
    // init transaction timeout
    void InitTxnTimeout(std::chrono::milliseconds timeout) {
        opts_.max_commit_time_ms(timeout);
    };
    // init coll
    bool InitColl(const std::string& coll_name) {
        client_[test_db_][coll_name].drop(wc_majority_);
        return true;
    };
    // get obj
    //std::vector<client_session> MongoSessionPool() {return session_pool_;};
    mongocxx::collection Coll(const std::string coll_name) {return client_[test_db_][coll_name];};
    // execute
    bool ExecInsertkV(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                      bsoncxx::document::value& doc_value, TestResultSet& test_result_set, const std::string test_process_file="");

    bool ExecDeleteKV(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                      bsoncxx::document::value& doc_value, TestResultSet& test_result_set, const std::string test_process_file="");

    bool ExecUpdatekV(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                      bsoncxx::document::value& doc_value_filter, bsoncxx::document::value& doc_value_update,
                      TestResultSet& test_result_set, const std::string test_process_file="");

    bool ExecUpdatekVWithVInc(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                                              TestResultSet& test_result_set, const std::string test_process_file);

    bool ExecFindKV(const int session_id, const int stmt_id, const std::string& stmt, mongocxx::collection& coll,
                    bsoncxx::document::value& doc_value, TestResultSet& test_result_set,
                    std::unordered_map<int, std::vector<std::string>>& cur_result_set,
                    const int param_num, const std::string test_process_file="");
    std::string ExecGetV(const int session_id, mongocxx::collection& coll, bsoncxx::document::value& doc_value);

    // transaction options
    bool KVStartTxn(const int session_id, const int stmt_id, TestResultSet& test_result_set,
                    const std::string test_process_file="");
    bool KVEndTxn(int session_id, int stmt_id, const std::string& opt,
                  TestResultSet& test_result_set, const std::string test_process_file="");
    // release resource
    void Release();
};

class KVParser {
private:
    std::unordered_map<std::string, int> k_cache_;
    std::unordered_map<std::string, mongocxx::collection> coll_cache_;
public:
    std::string MongoOpt(const std::string& stmt);
    std::pair<std::string, std::string> StmtKVData(const std::string& stmt);
    std::pair<std::string, std::string> DocKVData(const std::string& doc_json);
    std::string StmtCollName(const std::string& stmt);
    mongocxx::collection GetColl(const std::string& stmt, MongoConnector& mongo_connector, int& is_first_use);

    bsoncxx::document::value MongoInsert(const std::string& stmt_data_k, const std::string& stmt_data_v);
    std::vector<bsoncxx::document::value> MongoUpdate(const std::string& stmt_data_k, const std::string& stmt_data_v);
    std::vector<bsoncxx::document::value> MongoUpdatePred(const std::string& stmt_data_k, const std::string& stmt_data_v);
    bsoncxx::document::value MongoFind(const std::string& stmt_data_k);
    bsoncxx::document::value MongoFindPred(const std::string& stmt_data_k);
    // cache options
    void PutIntoKCache(const std::string& k) {k_cache_.emplace(k, 0);};
    bool FindFromKCache(const std::string& k) {
       if (k_cache_.count(k) == 1) {
           return true;
       } else {
           return false;
       }
    };
    void ClearKCache() {k_cache_.clear();};

    void PutIntoCollCache(const std::string& coll_name, mongocxx::collection coll) {
        coll_cache_[coll_name] = coll;
    };
    mongocxx::collection GetFromCollCache(const std::string& coll_name) {
        mongocxx::collection coll;
        if (coll_cache_.find(coll_name) != coll_cache_.end()) {
            coll = coll_cache_[coll_name];
        }
        return coll;
    };
    void ClearCollCache() {coll_cache_.clear();};
};
