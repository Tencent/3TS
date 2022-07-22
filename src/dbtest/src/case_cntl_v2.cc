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
#include "case_cntl.h"
#include <regex>
Outputter outputter;
ResultHandler result_handler;

std::vector<std::string> CaseReader::TxnIdAndSql(const std::string& line) {
    std::vector<std::string> txn_id_and_sql;
    const auto index_first = line.find("-");
    const auto index_second = line.rfind("-");
    if (!line.empty()) {
        if (index_first != line.npos && index_second != line.npos) {
            txn_id_and_sql.push_back(line.substr(0, index_first));
            txn_id_and_sql.push_back(line.substr(index_first + 1, index_second - index_first - 1));
            txn_id_and_sql.push_back(line.substr(index_second + 1));
        } else {
            std::cerr << "read txn_sql failed, please check format of test file. line:" + line << std::endl;
        }
    }
    return txn_id_and_sql;
}

std::pair<int, std::string> CaseReader::SqlIdAndResult(const std::string& line) {
    std::pair<int, std::string> sql_id_and_result;
    const auto index = line.find("-");
    if (index != line.npos) {
        std::string sql_id = line.substr(0, index);
        std::string result = line.substr(index + 1);
        std::vector<std::string> ret_list;
        std::string result_enhance = "";
        split(result, ret_list, " ");
        int i = 0;
        for (auto row : ret_list) {
            if (row == "null") {
                result_enhance = row;
            } else {
                std::string r = "(" + row + ")";
                if (i != int(ret_list.size()) - 1) {
                    result_enhance += r + " ";
                } else {
                    result_enhance += r;
                }
            }
            i++;
        }
        if (!sql_id.empty()) {
            sql_id_and_result.first = std::stoi(sql_id);
        } else {
            std::cout << "line: " << line << " please check case file, there is no sql_id" << std::endl;
        }
        sql_id_and_result.second = result_enhance;
    }
    return sql_id_and_result;
}

std::string CaseReader::Isolation(const std::string& line) {
    const auto index = line.find("{");
    return line.substr(0, index - 1);
}

std::pair<TestSequence, TestResultSet> CaseReader::TestSequenceAndTestResultSetFromFile(const std::string& test_file, const std::string& db_type) {
    std::pair<TestSequence, TestResultSet> test_sequence_and_result_set;
    std::ifstream test_stream(test_file);
    const auto index_second = test_file.rfind("/");
    const auto end = test_file.find(".");
    std::string test_case = test_file.substr(index_second + 1, end - index_second - 1);
    // std::string test_case_type = db_type + "_" + test_case;
    std::string test_case_type = test_case;
    std::string line;
    TestSequence test_sequence(test_case_type);
    TestResultSet test_result_set(test_case_type);
    std::unordered_map<int, std::vector<std::string>> sql_result_set;
    if (test_stream) {
        while (getline(test_stream, line)) {
            if (!line.empty()) {
                auto index_table_type = line.find("Parameters");
                auto index_anno = line.find("#");
                if (index_table_type != line.npos) {
                    // extract first number
                    std::string output = std::regex_replace(
                    line,
                    std::regex("[^0-9]*([0-9]+).*"),
                    std::string("$1")
                    );
                    int param_num = 0;
                    if (!output.empty()) {
                        param_num = std::stoi(output);
                    }
                    test_sequence.SetParamNum(param_num);
                }
                else if (index_anno != line.npos) {
                    continue;
                }
                else {
                    std::vector<std::string> txn_id_and_sql = CaseReader::TxnIdAndSql(line);
                    int sql_id = 0;
                    int txn_id = 0;
                    std::string sql = "";
                    if (!txn_id_and_sql[0].empty()) {
                        sql_id = std::stoi(txn_id_and_sql[0]);
                    }
                    if (!txn_id_and_sql[1].empty()) {
                        txn_id = std::stoi(txn_id_and_sql[1]);
                    }
                    if (!txn_id_and_sql[2].empty()) {
                        sql = txn_id_and_sql[2]; 
                    }
                    TxnSql txn_sql(sql_id, txn_id, sql, test_case_type);
                    test_sequence.AddTxnSql(txn_sql);
                }
            }
        }
        test_sequence_and_result_set.first = test_sequence;
        test_sequence_and_result_set.second = test_result_set;
    } else {
        //throw "test file not found";
        std::cerr << "test file: " + test_file  + " not found" << std::endl;
    }
    return test_sequence_and_result_set;
}

bool CaseReader::InitTestSequenceAndTestResultSetList(const std::string& test_path, const std::string& db_type) {
    std::cout << dash + "read test sequence and test result set start" + dash << std::endl;
    std::ifstream do_test_stream("./do_test_list.txt");
    std::string test_case;
    if (do_test_stream) {
        while (getline(do_test_stream, test_case)) {
            auto index = test_case.find("#");
            if (index != test_case.npos) {
                continue;
            }
            if (!do_test_stream) {
                continue;
            }
            std::string test_file_name = test_case + ".txt";
            std::string test_file = test_path + "/" + test_file_name;
            std::pair<TestSequence, TestResultSet> test_sequence_and_result_set = CaseReader::TestSequenceAndTestResultSetFromFile(test_file, db_type);
            TestSequence test_sequence = test_sequence_and_result_set.first;
            TestResultSet test_result_set = test_sequence_and_result_set.second;
            CaseReader::AddTestResultSet(test_result_set);
            CaseReader::AddTestSequence(test_sequence);
            std::cout << test_file + " read success" << std::endl;
        }
        return true;
    } else {
        //throw "do_test_list.txt not found";
        std::cerr << "do_test_list.txt not found" << std::endl;
        return false;
    }
}

bool ResultHandler::IsSqlExpectedResult(std::vector<std::string> cur_result, std::vector<std::string> expected_result, const int sql_id, const std::string& sql) {
    if (cur_result.size() != expected_result.size()) {
        //std::cout << "stmt_id:" << sql_id << " The number of data is different, cur_result:" << cur_result.size() << " expected_result: " << expected_result.size() << std::endl;
        return false;
    }
    int len = cur_result.size();
    for (int i = 0; i < len; i++) {
        std::string cur = cur_result[i];
        std::string expected = expected_result[i];
        if (cur != expected) {
            return false;
        }
    }
    return true;
}

bool ResultHandler::IsTestExpectedResult(std::unordered_map<int, std::vector<std::string>>& cur_result,
                                         std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list,
                                         std::unordered_map<int, std::string> sql_map, const std::string& test_process_file) {
    int index = 1;
    for (auto expected_result_set : expected_result_set_list) {
        bool is_all_expected = true;
        for (auto& result_map : expected_result_set) {
            int sql_id = result_map.first;
            std::vector<std::string> sql_expected_result = result_map.second;
            std::vector<std::string> sql_cur_result = cur_result[sql_id];
            if (!IsSqlExpectedResult(sql_cur_result, sql_expected_result, sql_id, sql_map[sql_id])) {
                is_all_expected = false;
                break;
            }
        }
        if (is_all_expected) {
            std::ofstream test_process(test_process_file, std::ios::app);
            if (test_process) {
                std::string info = "\nThe current result is consistent with the [(" + std::to_string(index) + ") expected_result] of serial scheduling";
                std::cout << info << std::endl;
                test_process << info << std::endl;
            }
            return true;
        }
    index += 1;
    }
    return false;
}

void Outputter::WriteResultTotal(std::vector<TestResultSet> test_result_set_list, const std::string& ret_file) {
    std::ofstream out(ret_file);
    for (auto& test_result_set : test_result_set_list) {
        // return result without reason
        auto result = test_result_set.ResultType();
        result = result.substr(0, result.find("\n"));
        out << test_result_set.TestCaseType() + ": " << result << std::endl << std::endl;
    }
    out.close();
}

bool Outputter::PrintAndWriteTxnSqlResult(std::vector<std::string> cur_result,
                                          std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list,
                                          const int sql_id, const std::string& sql, const int session_id, const std::string& test_process_file) {
    std::string blank(blank_base*(session_id - 1), ' ');
    std::ofstream test_process(test_process_file, std::ios::app);
    if (test_process) {
        std::string read_output = blank + "Q" + std::to_string(sql_id) + "-T" + std::to_string(session_id) + " return result: ";
        for(const auto& result: cur_result) {
            read_output = read_output + result + " ";
        }
        read_output += "\n";
        std::cout << read_output;
        test_process << read_output;
        // int idx = 1;
        // for (auto& expected_result_set : expected_result_set_list) {
        //     std::vector<std::string> sql_expected_result = expected_result_set[sql_id];
        //     int len = sql_expected_result.size();
        //     if (len == 0) {
        //        std::cout << "[ERROR]" << "stmt_expected_result is empty, please check expected result. stmt_id: " << sql_id << " stmt: " << sql << std::endl;
        //     }
        //     if (result_handler.IsSqlExpectedResult(cur_result, sql_expected_result, sql_id, sql)) {
        //         std::cout << blank + "  *(" + std::to_string(idx) + ") " + "expected_result: " << std::endl;
        //         std::cout << blank + "     ";
        //         std::copy(sql_expected_result.begin(), sql_expected_result.end(), std::ostream_iterator<std::string> (std::cout, " "));
        //         std::cout << "" << std::endl;
        //         test_process << blank + "  *(" + std::to_string(idx) + ") " +  "expected_result: " << std::endl;
        //         test_process << blank + "     ";
        //         std::copy(sql_expected_result.begin(), sql_expected_result.end(), std::ostream_iterator<std::string> (test_process, " "));
        //         test_process << "" << std::endl;
        //     } else {
        //         std::cout << blank + "   (" + std::to_string(idx) + ") " + "expected_result: " << std::endl;
        //         std::cout << blank + "     ";
        //         std::copy(sql_expected_result.begin(), sql_expected_result.end(), std::ostream_iterator<std::string> (std::cout, " "));
        //         std::cout << "" << std::endl;
        //         test_process << blank + "   (" + std::to_string(idx) + ") " +  "expected_result: " << std::endl;
        //         test_process << blank + "     ";
        //         std::copy(sql_expected_result.begin(), sql_expected_result.end(), std::ostream_iterator<std::string> (test_process, " "));
        //         test_process << "" << std::endl;
        //     }
        //     idx++;
        // }
        return true;
    } else {
        std::cerr << test_process_file + "has not found" << std::endl;
        return false;
    }
}

bool Outputter::WriteTestCaseTypeToFile(const std::string& test_case_type, const std::string& test_process_file) {
    std::ofstream test_process(test_process_file, std::ios::app);
    if (test_process) {
        test_process << dash + test_case_type + dash << std::endl;
        return true;
    } else {
        std::cerr << test_process_file + "has not found" << std::endl;
        return false;
    }
}

bool Outputter::WriteResultType(const std::string& result_type, const std::string& test_process_file) {
    std::ofstream test_process(test_process_file, std::ios::app);
    if (test_process) {
        test_process << "\nTest Result: " + result_type << std::endl;
        test_process << "" << std::endl;
        return true;
    } else {
        std::cerr << test_process_file + "has not found" << std::endl;
        return false;
    }
}
