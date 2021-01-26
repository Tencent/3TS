#include "db_test.h"
#include "gflags/gflags.h"
#include <iomanip>
#include <cstdlib>
#include <fstream>
#include <cassert>


DEFINE_string(db_type, "mysql", "database type, include mysql, oracle, sql server");
DEFINE_string(host, "", "database host ip, such as x.x.x.x");
DEFINE_string(port, "", "database port, such as 3306");
DEFINE_string(user, "", "username");
DEFINE_string(passwd, "", "password");
DEFINE_string(db_name, "test", "create database name");

//std::unique_ptr<DBConnectorBase> get_db_connector(const std::string& host, 
//                                                  const std::string& port, 
//                                                  const std::string& user, 
//                                                  const std::string& passwd, 
//                                                  const DBType& db_type) {
//    return NULL;
//}

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

TestCaseType SQLReader::get_test_case_type_by_test_file(std::string& test_file) {
    if (test_file.find("dirty-read")) {
        return TestCaseType::DirtyRead;
    } else {
        //throw "unsupported test type";
        std::cerr << "unsupported test type" << std::endl;
        return TestCaseType::Unsupported;
    }
}

TestSequence SQLReader::get_one_test_sequence_from_file(std::string& test_file) {
    std::ifstream test_stream(test_file);
    TestCaseType test_case_type = SQLReader::get_test_case_type_by_test_file(test_file);
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
    TxnSQL txn_sql = txn_sql_list[0];
    std::cout << txn_sql.get_txn_id() << std::endl;
    std::cout << txn_sql.get_sql() << std::endl;
    return test_sequence;
}

void SQLReader::init_test_sequence_list(std::string& test_path) {
    std::ifstream do_test_stream("./do_test_list.txt");
    std::string test_case;
    if (do_test_stream) {
        while (getline(do_test_stream, test_case)) {
            std::string test_file_name = test_case + ".txt";
            std::string test_file = test_path + "/" + test_file_name;
            TestSequence test_sequence = SQLReader::get_one_test_sequence_from_file(test_file);
            SQLReader::add_test_sequence(test_sequence);
        }
    } else {
        //throw "do_test_list.txt not found";
        std::cerr << "do_test_list.txt not found" << std::endl; 
    }
}

//MYSQLConnector::execute_read_sql(const std::string& sql, TestResultSet& test_rs, int conn_id) {
    
//}
void MYSQLConnector::set_auto_commit() {
}

void MYSQLConnector::begin() {
}

void MYSQLConnector::rollback() {
}

void MYSQLConnector::commit() {
}

void MYSQLConnector::execute_sql(const std::string& sql, TestResultSet& test_rs, int conn_id) {
    sql::Statement* stmt = MYSQLConnector::stmt_pool_[conn_id];
    sql::ResultSet *res;
    res = stmt->executeQuery(sql);
    uint32_t index = 0;
    while (res->next()) {
        std::cout << res->getString(index) << std::endl;
        index++;
    }
}

void MYSQLConnector::close_stmt() {
}

void MYSQLConnector::close_conn() {
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    SQLReader sql_reader;
    std::string test_path_base = "t/";
    std::string test_path = test_path_base + "mysql";
    sql_reader.init_test_sequence_list(test_path);
    std::vector<TestSequence> test_sequence_list = sql_reader.SQLReader::get_test_sequence_list();
    for (auto &test_sequence : test_sequence_list) {
        std::vector<TxnSQL> txn_sql_list = test_sequence.get_txn_sql_list();
        for (auto &txn_sql : txn_sql_list) {
            std::cout << txn_sql.get_sql() << std::endl;
        }
    }
    
    MYSQLConnector mysql_connector(FLAGS_host, FLAGS_port, FLAGS_user, FLAGS_passwd, FLAGS_db_name, 1);
    TestResultSet test_rs;
    mysql_connector.execute_sql("select * from sbtest1", test_rs, 0); 
    
    return 0;
}
