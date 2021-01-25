#include "db_test.h"
#include "gflags/gflags.h"
#include <iomanip>
#include <cstdlib>
#include <fstream>
#include <cassert>


DEFINE_string(db, "mysql", "database type, include mysql, oracle, sql server");
DEFINE_string(host, "", "database host ip, such as x.x.x.x");
DEFINE_string(port, "", "database port, such as 3306");
DEFINE_string(user, "", "username");
DEFINE_string(passwd, "", "password");


std::pair<std::string, std::string> SQLReader::get_txn_id_and_sql(std::string line) {
    std::pair<std::string, std::string> txn_id_and_sql;
    int index = line.find_first_of(" ");
    if (index > 0) {
        txn_id_and_sql.first = line.substr(0, index);
        txn_id_and_sql.second = line.substr(index+1);
    } else {
        std::cout << "read txn_sql failed, please check format of test file. line:" + line << std::endl;
    }
    return txn_id_and_sql;
}

TestCaseType SQLReader::get_test_case_type_by_test_file(std::string& test_file) {
    if (test_file.find("dirty-read")) {
        return TestCaseType::DirtyRead;
    } else {
        std::cout << "unsupported test type" << std::endl;
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
        std::cout << "test file not found" << std::endl;
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
        std::cout << "do_test_list.txt not found" << std::endl;
    }
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    SQLReader sql_reader;
    std::string test_path_base = "../t/";
    std::string test_path = test_path_base + "mysql";
    sql_reader.init_test_sequence_list(test_path);
    std::vector<TestSequence> test_sequence_list = sql_reader.SQLReader::get_test_sequence_list();
    for (auto test_sequence : test_sequence_list) {
        std::vector<TxnSQL> txn_sql_list = test_sequence.get_txn_sql_list();
        for (auto txn_sql : txn_sql_list) {
            std::cout << txn_sql.get_sql() << std::endl;
        }
    }
    return 0;
}
