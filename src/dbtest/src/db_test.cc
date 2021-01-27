#include "db_test.h"
#include "gflags/gflags.h"
#include <iomanip>
#include <cstdlib>
#include <fstream>
#include <cassert>


DEFINE_string(db_type, "mysql", "data resource name, please see /etc/odbc.ini");
DEFINE_string(user, "test123", "username");
DEFINE_string(passwd, "Ly.123456", "password");
DEFINE_string(db_name, "test", "create database name");

std::pair<std::string, std::string> SQLReader::get_txn_id_and_sql(std::string line) {
    std::pair<std::string, std::string> txn_id_and_sql;
    const auto index = line.find_first_of(" ");
    if (index > 0) {
        txn_id_and_sql.first = line.substr(0, index);
        txn_id_and_sql.second = line.substr(index + 1);
    } else {
        throw "read txn_sql failed, please check format of test file. line:" + line;
    }
    return txn_id_and_sql;
}

TestSequence SQLReader::get_one_test_sequence_from_file(std::string& test_file) {
    std::ifstream test_stream(test_file);
    const auto start = test_file.find_last_of("/");
    const auto end = test_file.find_last_of('.');
    std::string test_case_type = test_file.substr(start + 1, end);
    std::string line;
    TestSequence test_sequence(test_case_type);
    if (test_stream) {
        while (getline(test_stream, line)) {
            std::pair<std::string, std::string> txn_id_and_sql = SQLReader::get_txn_id_and_sql(line);
            TxnSQL txn_sql(txn_id_and_sql.second, txn_id_and_sql.first, test_case_type);
            test_sequence.add_txn_sql(txn_sql);
        }
    } else {
        //throw "test file not found";
        std::cerr << "test file not found" << std::endl;
    }
    std::vector<TxnSQL> txn_sql_list = test_sequence.get_txn_sql_list();
    return test_sequence;
}

bool SQLReader::init_test_sequence_list(std::string& test_path) {
    std::cout << "start read test sequence" << std::endl;
    std::ifstream do_test_stream("./do_test_list.txt");
    std::string test_case;
    if (do_test_stream) {
        while (getline(do_test_stream, test_case)) {
            std::string test_file_name = test_case + ".txt";
            std::string test_file = test_path + "/" + test_file_name;
            TestSequence test_sequence = SQLReader::get_one_test_sequence_from_file(test_file);
            SQLReader::add_test_sequence(test_sequence);
            std::cout << test_file + " read success" << std::endl;
        }
        return true;
    } else {
        //throw "do_test_list.txt not found";
        std::cerr << "do_test_list.txt not found" << std::endl;
        return false;
    }
}

bool DBConnector::execute_sql(const std::string& sql, TestResultSet& test_rs, int conn_id) {
    SQLRETURN ret;
    SQLLEN length;
    SQLHSTMT m_hStatement = DBConnector::stmt_pool_[conn_id - 1];
    // execute sql
    std::cout <<  "execute sql:" + sql + start" << std::endl;
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cerr << "execute sql failed" << std::endl;
        return false;
    }
    // get result
    std::cout << "get and parse result start" << std::endl;
    ret = SQLFetch(m_hStatement);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cerr << "get result failed" << std::endl;
        return false;
    }
    // parse result
    SQLINTEGER ID;
    SQLGetData(m_hStatement, 1, SQL_C_LONG, &ID, 0, &length);
    std::cout << ID << std::endl;
    return true;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    // read test sequence
    SQLReader sql_reader;
    std::string test_path_base = "t/";
    std::string test_path = test_path_base + "mysql";
    if (!sql_reader.init_test_sequence_list(test_path)) {
        std::cerr << "init test sequence failed" << std::endl;
    }
    std::vector<TestSequence> test_sequence_list = sql_reader.SQLReader::get_test_sequence_list();
    // init db_connector
    DBConnector db_connector;
    if (!db_connector.InitDBConnector(FLAGS_user, FLAGS_passwd, FLAGS_db_type, 1)) {
        std::cerr << "init db_connector failed" << std::endl;
    }
    TestResultSet test_rs;
    db_connector.execute_sql("select * from sbtest1", test_rs, 1); 
    // send sql
    for (auto &test_sequence : test_sequence_list) {
        std::vector<TxnSQL> txn_sql_list = test_sequence.get_txn_sql_list();
        for (auto &txn_sql : txn_sql_list) {
            std::cout << txn_sql.get_sql() << std::endl;
        }
    }
    
    return 0;
}
