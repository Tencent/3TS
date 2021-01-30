#include "db_test.h"
#include "gflags/gflags.h"
#include <iomanip>
#include <cstdlib>
#include <fstream>
#include <cassert>
#include <string>


DEFINE_string(db_type, "mysql", "data resource name, please see /etc/odbc.ini");
DEFINE_string(user, "test123", "username");
DEFINE_string(passwd, "Ly.123456", "password");
DEFINE_string(db_name, "test", "create database name");
DEFINE_int32(conn_pool_size, 3, "db_conn pool size");
DEFINE_string(isolation, "serializable", "transation isolation level");
std::string dash(10, '-');

std::vector<std::string> SQLReader::TxnIdAndSql(std::string line) {
    std::vector<std::string> txn_id_and_sql;
    const auto index_first = line.find("-");
    const auto index_second = line.rfind("-");
    if (line != "") {
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

std::pair<int, std::string> SQLReader::SqlIdAndResult(std::string line) {
    std::pair<int, std::string> sql_id_and_result;
    const auto index = line.find("-");
    if (index != line.npos) {
        std::string sql_id = line.substr(0, index);
        std::string result = line.substr(index + 1);
        sql_id_and_result.first = std::stoi(sql_id);
        sql_id_and_result.second = result;
    }
    return sql_id_and_result;
}

std::string SQLReader::Isolation(std::string line) {
    const auto index = line.find("{");
    return line.substr(0, index - 1); 
}

void split(const std::string& str, 
           std::vector<std::string>& tokens, 
           const std::string delim = " ") {
    tokens.clear();
    auto start = str.find_first_not_of(delim, 0);
    auto position = str.find_first_of(delim, start);
    while (position != std::string::npos || start != std::string::npos) {
        tokens.emplace_back(std::move(str.substr(start, position - start)));
        start = str.find_first_not_of(delim, position);
        position = str.find_first_of(delim, start);
    }
}

std::pair<TestSequence, TestResultSet> SQLReader::TestSequenceAndTestResultSetFromFile(std::string& test_file) {
    std::pair<TestSequence, TestResultSet> test_sequence_and_result_set;
    std::ifstream test_stream(test_file);
    const auto index_first = test_file.find("/");
    const auto index_second = test_file.rfind("/");
    const auto end = test_file.find(".");
    std::string db_name = test_file.substr(index_first + 1, index_second - index_first - 1);
    std::string test_case = test_file.substr(index_second + 1, end - index_second - 1);
    std::string test_case_type = db_name + "_" + test_case;
    std::string line;
    TestSequence test_sequence(test_case_type);
    TestResultSet test_result_set(test_case_type);
    bool is_result_row = false;
    if (test_stream) {
        while (getline(test_stream, line)) {
            if (line != "") {
                auto index_table_type = line.find("TableType");
                auto index_left = line.find("{");
                auto index_right = line.find("}");
                if (index_table_type != line.npos) {
                    auto start = line.find(":");
                    std::string table_type_str = line.substr(start + 1);
                    int table_type = std::stoi(table_type_str);
                    test_sequence.SetTableType(table_type);
                } else if (index_left != line.npos) {
                    std::string isolation = SQLReader::Isolation(line);
                    test_result_set.SetIsolation(isolation);
                    is_result_row = true;
                } else if (index_right != line.npos) {
                    is_result_row = false;
                } else if (is_result_row) {
                    std::pair<int, std::string> sql_id_and_result = SQLReader::SqlIdAndResult(line);
                    std::vector<std::string> ret_list;
                    std::string result_str = sql_id_and_result.second;
                    split(result_str, ret_list, ",");
                    test_result_set.AddSqlResultSet(sql_id_and_result.first, ret_list);
                } else if (!is_result_row){ 
                    std::vector<std::string> txn_id_and_sql = SQLReader::TxnIdAndSql(line);
                    TxnSql txn_sql(std::stoi(txn_id_and_sql[0]), std::stoi(txn_id_and_sql[1]), txn_id_and_sql[2], test_case_type);
                    test_sequence.AddTxnSql(txn_sql);
                } 
            }
        }
        test_sequence_and_result_set.first = test_sequence;
        test_sequence_and_result_set.second = test_result_set;
    } else {
        //throw "test file not found";
        std::cerr << "test file not found" << std::endl;
    }
    return test_sequence_and_result_set;
}



bool SQLReader::InitTestSequenceAndTestResultSetList(std::string& test_path) {
    std::cout << dash + "read test sequence and test result set start" + dash << std::endl;
    std::ifstream do_test_stream("./do_test_list.txt");
    std::string test_case;
    if (do_test_stream) {
        while (getline(do_test_stream, test_case)) {
            std::string test_file_name = test_case + ".txt";
            std::string test_file = test_path + "/" + test_file_name;
            std::pair<TestSequence, TestResultSet> test_sequence_and_result_set = SQLReader::TestSequenceAndTestResultSetFromFile(test_file);
            TestSequence test_sequence = test_sequence_and_result_set.first;
            TestResultSet test_result_set = test_sequence_and_result_set.second;
            SQLReader::AddTestResultSet(test_result_set);
            SQLReader::AddTestSequence(test_sequence);
            std::cout << test_file + " read success" << std::endl;
        }
        return true;
    } else {
        //throw "do_test_list.txt not found";
        std::cerr << "do_test_list.txt not found" << std::endl;
        return false;
    }
}

// handle_type: stmt and dbc
void DBConnector::ErrInfoWithStmt(std::string handle_type, SQLHANDLE& handle, SQLCHAR ErrInfo[]) { 
    SQLCHAR SQLState[256]; 
    SQLINTEGER NativeErrorPtr = 0;
    SQLSMALLINT TextLengthPtr = 0;
    if ("stmt" == handle_type) {
        SQLGetDiagRec(SQL_HANDLE_STMT, handle, 1, SQLState, &NativeErrorPtr, ErrInfo, 256, &TextLengthPtr);
    }
    if ("dbc" == handle_type) {
        SQLGetDiagRec(SQL_HANDLE_DBC, handle, 1, SQLState, &NativeErrorPtr, ErrInfo, 256, &TextLengthPtr);
    }
}

bool DBConnector::IsSqlExecuteErr(const std::string& sql, std::string handle_type, SQLHANDLE& handle, SQLRETURN ret) {
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return false;
    } else if (ret == SQL_ERROR) {
        SQLCHAR ErrInfo[256];
        DBConnector::ErrInfoWithStmt(handle_type, handle, ErrInfo);
        std::cerr << "execute sql: '" + sql + "' failed, reason: " << ErrInfo << std::endl;
        return true;
    } else if (ret == SQL_NEED_DATA) {
        std::cout << "SQL_NEED_DATA" << std::endl;
        return true;
    } else if (ret == SQL_STILL_EXECUTING) {
        std::cout << "SQL_NEED_DATA" << std::endl;
        return true;
    } else if (ret == SQL_NO_DATA) {
        std::cout << "SQL_NEED_DATA" << std::endl;
        return true;
    } else if (ret == SQL_INVALID_HANDLE) {
        std::cout << "SQL_NEED_DATA" << std::endl;
        return true;
    } else if (ret == SQL_PARAM_DATA_AVAILABLE) {
        std::cout << "SQL_NEED_DATA" << std::endl;
        return true;
    } else {
        std::cerr << "execute sql: '" + sql + "' failed, unknow error" << std::endl;
        return true;
    }
}

bool DBConnector::ExecWriteSql(int sql_id, const std::string& sql, TestResultSet& test_result_set, int conn_id) {
    SQLRETURN ret;
    SQLHSTMT m_hStatement;
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[conn_id - 1];

    ret = SQLAllocHandle(SQL_HANDLE_STMT, m_hDatabaseConnection, &m_hStatement);
    if (DBConnector::IsSqlExecuteErr(sql, "stmt", m_hStatement, ret)) {
        return false;
    }
    // execute sql
    std::cout << "txn_id: " + std::to_string(conn_id) +  " execute sql: '" + sql + "'"<< std::endl;
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    if (DBConnector::IsSqlExecuteErr(sql, "stmt", m_hStatement, ret)) {
        return false;
    }   
    // get error info
    return true;
}

bool IsExpectedResult(std::vector<std::string> cur_result,std::vector<std::string> expected_result) {
    if (cur_result.size() != expected_result.size()) {
        std::cerr << "number of cur_result_row != number of expected_result_row" << std::endl;
        std::cout << "cur_result: " << std::endl;
        std::copy(cur_result.begin(), cur_result.end(), std::ostream_iterator<std::string> (std::cout, " "));
        std::cout << "expected_result: " << std::endl;
        std::copy(expected_result.begin(), expected_result.end(), std::ostream_iterator<std::string> (std::cout, " "));
        std::cout << "\n" << std::endl;
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

std::string SQLCHARToStr(SQLCHAR* ch) {
    char* ch_char = (char*)ch;
    std::string ch_str = ch_char;
    return ch_str;
}

bool DBConnector::ExecReadSql2Int(int sql_id, const std::string& sql, TestResultSet& test_result_set, int conn_id) {
    SQLRETURN ret;
    SQLHSTMT m_hStatement;
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[conn_id - 1];
    std::unordered_map<int, std::vector<std::vector<std::string>>> sql_result_set_map = test_result_set.SqlResultSetMap();
    std::vector<std::vector<std::string>> expected_sql_result_set_multi = sql_result_set_map[sql_id];
    std::vector<std::string> cur_sql_result_set;

    ret = SQLAllocHandle(SQL_HANDLE_STMT, m_hDatabaseConnection, &m_hStatement);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cerr << "get stmt failed" << std::endl;
        return false;
    }
    SQLLEN length;
    SQLCHAR k[20]={0}, v[20]={0};
    // execute sql
    std::cout << "txn_id: " + std::to_string(conn_id) + " execute sql: '" + sql + "'" << std::endl;
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    // parse result
    if(!DBConnector::IsSqlExecuteErr(sql, "stmt", m_hStatement, ret)) {
        // bind column data
        SQLBindCol(m_hStatement, 1, SQL_C_CHAR, (void*)k, sizeof(k), &length);
        SQLBindCol(m_hStatement, 2, SQL_C_CHAR, (void*)v, sizeof(v), &length);
        // get next row, and fill cur_sql_result_set
        while(SQL_NO_DATA != SQLFetch(m_hStatement)) {
            std::string k_str = SQLCHARToStr(k);
            std::string v_str = SQLCHARToStr(v);
            std::string row = k_str + " " + v_str;
            std::cout << "row: " + row << std::endl;
            cur_sql_result_set.push_back(row);
        }
        if (SQL_NO_DATA == SQLFetch(m_hStatement)) {
            cur_sql_result_set.push_back("null");
        }
        // check result
        for (auto expected_sql_result_set : expected_sql_result_set_multi) {
            if (IsExpectedResult(cur_sql_result_set, expected_sql_result_set)) {
                test_result_set.SetResultType("avoid");
                return true;
            }
        }
        test_result_set.SetResultType("exception");
        return true;
    } else {
        return false;
    }
}

std::string DBConnector::IsTxnRollback(SQLHDBC m_hDatabaseConnection, SQLRETURN ret) {
    if (ret == SQL_ERROR) {
        SQLCHAR ErrInfo[256];
        SQLCHAR SQLState[256];
        SQLINTEGER NativeErrorPtr = 0;
        SQLSMALLINT TextLengthPtr = 0;
        SQLGetDiagRec(SQL_HANDLE_DBC, m_hDatabaseConnection, 1, SQLState, &NativeErrorPtr, ErrInfo, 256, &TextLengthPtr);
        std::string state = SQLCHARToStr(SQLState);
        if ("25S03" == state) {
            return "rollback";
        }
        if ("40001" == state) {
            return "deadlock";
        }
    }
    return "";
}

bool DBConnector::SQLEndTnx(std::string opt, int conn_id, TestResultSet& test_result_set) {
    SQLRETURN ret;
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[conn_id - 1];
    std::cout << "txn_id: " + std::to_string(conn_id) + " execute opt: '" + opt + "'" << std::endl;
    if ("commit" == opt) {
        ret = SQLEndTran(SQL_HANDLE_DBC, m_hDatabaseConnection, SQL_COMMIT);
    } else if ("rollback" == opt) {
        ret = SQLEndTran(SQL_HANDLE_DBC, m_hDatabaseConnection, SQL_ROLLBACK);
    } else {
        std::cerr << "unknow txn opt" << std::endl;
    }
    std::string result_type = DBConnector::IsTxnRollback(m_hDatabaseConnection, ret);
    if (result_type != "") { 
        test_result_set.SetResultType(result_type);
    }
    return true;
}

bool DBConnector::SetAutoCommit(SQLHDBC m_hDatabaseConnection, int opt) {
    SQLRETURN ret;
    SQLUINTEGER autoCommit;
    if (opt == 0) {
        autoCommit = 0;
    } else {
        autoCommit = 1; 
    }
    ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)(long)autoCommit, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cerr << "set AutoCommit = 0 failed" << std::endl;
        return false;
    }
    return true;
}

bool DBConnector::SetIsolationLevel(SQLHDBC m_hDatabaseConnection, std::string opt) {
    SQLRETURN ret;
    if (opt == "read-uncommitted") {
        ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_READ_UNCOMMITTED, 0);
    } else if (opt == "read-committed") {
        ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_READ_COMMITTED, 0);
    } else if (opt == "repeatable-read") {
        ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_REPEATABLE_READ, 0);
    } else if (opt == "serializable") {
        ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_SERIALIZABLE, 0);
    } else {
        std::cerr << "unknow isolation level" << std::endl;
        return false;
    }
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cerr << "set TXN_ISOLATION = " + opt + " failed" << std::endl;
        return false;
    }
    return true;
}

bool JobExecutor::ExecTestSequence(TestSequence& test_sequence, TestResultSet& test_result_set, DBConnector db_connector) {
    std::vector<TxnSql> txn_sql_list = test_sequence.TxnSqlList();
    // set TXN_ISOLATION
    std::cout << dash + "set TXN_ISOLATION = " + FLAGS_isolation + dash << std::endl;
    for (auto hdbc : db_connector.DBConnPool()) {
        if(!db_connector.SetIsolationLevel(hdbc, FLAGS_isolation)) {
            return false;
        }
    }
    std::cout << "set TXN_ISOLATION = " + FLAGS_isolation + " success"<< std::endl;

    // set AutoCommit
    std::cout << dash + "set AutoCommit = 0" + dash << std::endl;
    for (auto hdbc : db_connector.DBConnPool()) {
        if(!db_connector.SetAutoCommit(hdbc, 0)) {
            return false;
        }
    }
    std::cout << "set AutoCommit = 0 success" << std::endl;

    std::cout << dash + test_sequence.TestCaseType() + " test start" + dash << std::endl;
    for (auto txn_sql : txn_sql_list) {
        int sql_id = txn_sql.SqlId();
        int txn_id = txn_sql.TxnId();
        std::string sql = txn_sql.Sql();
        auto index_read = sql.find("SELECT");
        auto index_commit = sql.find("COMMIT");
        auto index_rollback = sql.find("ROLLBACK");
        if (index_read != sql.npos) {
            if (test_sequence.TableType() == 1) {
                if (!db_connector.ExecReadSql2Int(sql_id, sql, test_result_set, txn_id)) {
                    return false;
                }
            } else {
                std::cerr << "unsupported table type" << std::endl;
                return false;
            }
        } else if (index_commit != sql.npos) {
            if (!db_connector.SQLEndTnx("commit", txn_id, test_result_set)) {
                return false;
            }
        } else if (index_rollback != sql.npos) {
            if (!db_connector.SQLEndTnx("rollback", txn_id, test_result_set)) {
                return false;
            }
        } else {
            if (!db_connector.ExecWriteSql(sql_id, sql, test_result_set, txn_id)) {
                return false;
            }
        }
    }
    return true;
}

int main(int argc, char* argv[]) {
    // parse gflags args
    google::ParseCommandLineFlags(&argc, &argv, true);
    // read test sequence
    SQLReader sql_reader;
    std::string test_path_base = "t/";
    std::string test_path = test_path_base + "mysql";
    if (!sql_reader.InitTestSequenceAndTestResultSetList(test_path)) {
        std::cerr << "init test sequence and test result set failed" << std::endl;
    }

    std::vector<TestSequence> test_sequence_list = sql_reader.TestSequenceList();
    std::vector<TestResultSet> test_result_set_list = sql_reader.TestResultSetList();
/*
    for (auto test_sequence : test_sequence_list) {
        std::vector<TxnSql> txn_sql_list = test_sequence.TxnSqlList();
        for (auto txn_sql : txn_sql_list) {
            std::cout << txn_sql.SqlId() << txn_sql.TxnId() << txn_sql.Sql() << std::endl;
        }
    }
    for (auto test_result_set : test_result_set_list) {
        std::unordered_map<int, std::vector<std::vector<std::string>>> sql_result_set_map = test_result_set.SqlResultSetMap();
        for (auto map : sql_result_set_map) {
            int sql_id = map.first;
            std::vector<std::vector<std::string>> result = map.second;
            for (auto ret : result) {
                for (auto row : ret) {
                    std::cout << test_result_set.TestCaseType() << test_result_set.Isolation() << sql_id << row << std::endl;
                }
            }
        }
    }
*/
    // init db_connector
    std::cout << dash + "init db_connector start" + dash << std::endl;
    DBConnector db_connector;
    if (!db_connector.InitDBConnector(FLAGS_user, FLAGS_passwd, FLAGS_db_type, FLAGS_conn_pool_size)) {
        std::cerr << "init db_connector failed" << std::endl;
    }
    // init database
    std::cout << dash + "init database start" + dash << std::endl;
    TestResultSet test_rs__;
    db_connector.ExecWriteSql(0, "create database if not exists " + FLAGS_db_name, test_rs__, 1);
    db_connector.ExecWriteSql(0, "use " + FLAGS_db_name, test_rs__, 1);

    //TestResultSet test_rs;
    //db_connector.ExecReadSql2Int("select * from sbtest1", test_rs, 1); 

    // send sql
    JobExecutor job_executor;
    int len = test_sequence_list.size();
    for (int i = 0; i < len; i++) {
        if (!job_executor.ExecTestSequence(test_sequence_list[i], test_result_set_list[i], db_connector)) {
            std::cerr << "test sequence" + test_sequence_list[i].TestCaseType() + "execute failed" << std::endl;
            return 0;
        } else {
            std::string result_type = test_result_set_list[i].ResultType();
            std::cout << "result_type:" << result_type + "\n" << std::endl;
        }
    }
    
    return 0;
}
