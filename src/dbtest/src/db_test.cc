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
DEFINE_string(isolation, "read-uncommitted", "transation isolation level");
std::string dash(10, '-');

std::pair<int, std::string> SQLReader::TxnIdAndSql(std::string line) {
    std::pair<int, std::string> txn_id_and_sql;
    const auto index = line.find(" ");
    if (index > 0) {
        txn_id_and_sql.first = std::stoi(line.substr(0, index));
        txn_id_and_sql.second = line.substr(index + 1);
    } else {
        throw "read txn_sql failed, please check format of test file. line:" + line;
    }
    return txn_id_and_sql;
}

TestSequence SQLReader::TestSequenceFromFile(std::string& test_file) {
    std::ifstream test_stream(test_file);
    const auto start = test_file.rfind("/");
    const auto end = test_file.rfind('.');
    std::string test_case_type = test_file.substr(start + 1, end);
    std::string line;
    TestSequence test_sequence(test_case_type);
    bool is_first_row = true;
    if (test_stream) {
        while (getline(test_stream, line)) {
            if (is_first_row) {
                auto start = line.find(":");
                std::string table_type_str = line.substr(start + 1);
                int table_type = std::stoi(table_type_str);
                test_sequence.SetTableType(table_type);
                is_first_row = false;
            } else {
                std::pair<int, std::string> txn_id_and_sql = SQLReader::TxnIdAndSql(line);
                TxnSQL txn_sql(txn_id_and_sql.first, txn_id_and_sql.second, test_case_type);
                test_sequence.AddTxnSql(txn_sql);
            } 
        }
    } else {
        //throw "test file not found";
        std::cerr << "test file not found" << std::endl;
    }
    return test_sequence;
}

bool SQLReader::InitTestSequenceList(std::string& test_path) {
    std::cout << dash + "read test sequence start" + dash << std::endl;
    std::ifstream do_test_stream("./do_test_list.txt");
    std::string test_case;
    if (do_test_stream) {
        while (getline(do_test_stream, test_case)) {
            std::string test_file_name = test_case + ".txt";
            std::string test_file = test_path + "/" + test_file_name;
            TestSequence test_sequence = SQLReader::TestSequenceFromFile(test_file);
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

void DBConnector::ErrInfoWithStmt(SQLHANDLE& m_hStatement, SQLCHAR ErrInfo[]) { 
    SQLCHAR SQLState[256]; 
    SQLINTEGER NativeErrorPtr = 0;
    SQLSMALLINT TextLengthPtr = 0; 
    SQLGetDiagRec(SQL_HANDLE_STMT, m_hStatement, 1, SQLState, &NativeErrorPtr, ErrInfo, 256, &TextLengthPtr);
}

bool DBConnector::IsSqlExecuteErr(const std::string& sql, SQLHSTMT& m_hStatement, SQLRETURN ret) {
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return false;
    } else if (ret == SQL_ERROR) {
        SQLCHAR ErrInfo[256];
        DBConnector::ErrInfoWithStmt(m_hStatement, ErrInfo);
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

bool DBConnector::ExecWriteSql(const std::string& sql, TestResultSet& test_rs, int conn_id) {
    SQLRETURN ret;
    SQLHSTMT m_hStatement;
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[conn_id - 1];

    ret = SQLAllocHandle(SQL_HANDLE_STMT, m_hDatabaseConnection, &m_hStatement);
    if (DBConnector::IsSqlExecuteErr(sql, m_hStatement, ret)) {
        return false;
    }
    // execute sql
    std::cout << "txn_id: " + std::to_string(conn_id) +  " execute sql: '" + sql + "'"<< std::endl;
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    if (DBConnector::IsSqlExecuteErr(sql, m_hStatement, ret)) {
        return false;
    }
    // get error info
    return true;
}

bool DBConnector::ExecReadSql2Int(const std::string& sql, TestResultSet& test_rs, int conn_id) {
    SQLRETURN ret;
    SQLHSTMT m_hStatement;
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[conn_id - 1];

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
    if(!DBConnector::IsSqlExecuteErr(sql, m_hStatement, ret)) {
        // bind column data
        SQLBindCol(m_hStatement, 1, SQL_C_CHAR, (void*)k, sizeof(k), &length);
        SQLBindCol(m_hStatement, 2, SQL_C_CHAR, (void*)v, sizeof(v), &length);
        // get next row
        while(SQL_NO_DATA != SQLFetch(m_hStatement)) {
            std::cout << "k:" << k << "v:" << v << std::endl;
        }
        return true;
    } else {
        return false;
    }
}

bool DBConnector::SQLEndTnx(std::string opt, int conn_id) {
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
    if (DBConnector::IsSqlExecuteErr(opt, m_hDatabaseConnection, ret)) {
        return false;
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
    }
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cerr << "set TXN_ISOLATION = " + opt + "failed" << std::endl;
        return false;
    }
    return true;
}

bool JobExecutor::ExecTestSequence(TestSequence& test_sequence, DBConnector db_connector) {
    std::vector<TxnSQL> txn_sql_list = test_sequence.TxnSqlList();
    TestResultSet test_rs;
    // set TXN_ISOLATION
    std::cout << dash + "set TXN_ISOLATION = " + FLAGS_isolation + dash << std::endl;
    for (auto hdbc : db_connector.DBConnPool()) {
        if(!db_connector.SetIsolationLevel(hdbc, FLAGS_isolation)) {
            return false;
        }
    }
    std::cout << "set TXN_ISOLATION = " + FLAGS_isolation + "success"<< std::endl;

    // set AutoCommit
    std::cout << dash + "set AutoCommit = 0" + dash << std::endl;
    for (auto hdbc : db_connector.DBConnPool()) {
        if(!db_connector.SetAutoCommit(hdbc, 0)) {
            return false;
        }
    }
    std::cout << "set AutoCommit = 0 success" << std::endl;

    std::cout << dash + "test start" + dash << std::endl;
    for (auto txn_sql : txn_sql_list) {
        int txn_id = txn_sql.TxnId();
        std::string sql = txn_sql.Sql();
        auto index_read = sql.find("SELECT");
        auto index_commit = sql.find("COMMIT");
        auto index_rollback = sql.find("ROLLBACK");
        if (index_read != sql.npos) {
            if (test_sequence.TableType() == 1) {
                if (!db_connector.ExecReadSql2Int(sql, test_rs, txn_id)) {
                    return false;
                }
            } else {
                std::cerr << "unsupported table type" << std::endl;
                return false;
            }
        } else if (index_commit != sql.npos) {
            if (!db_connector.SQLEndTnx("commit", txn_id)) {
                return false;
            }
        } else if (index_rollback != sql.npos) {
            if (!db_connector.SQLEndTnx("rollback", txn_id)) {
                return false;
            }
        } else {
            if (!db_connector.ExecWriteSql(sql, test_rs, txn_id)) {
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
    if (!sql_reader.InitTestSequenceList(test_path)) {
        std::cerr << "init test sequence failed" << std::endl;
    }
    std::vector<TestSequence> test_sequence_list = sql_reader.SQLReader::TestSequenceList();
    // init db_connector
    std::cout << dash + "init db_connector start" + dash << std::endl;
    DBConnector db_connector;
    if (!db_connector.InitDBConnector(FLAGS_user, FLAGS_passwd, FLAGS_db_type, FLAGS_conn_pool_size)) {
        std::cerr << "init db_connector failed" << std::endl;
    }
    // init database
    std::cout << dash + "init database start" + dash << std::endl;
    TestResultSet test_rs__;
    db_connector.ExecWriteSql("create database if not exists " + FLAGS_db_name, test_rs__, 1);
    db_connector.ExecWriteSql("use " + FLAGS_db_name, test_rs__, 1);

    //TestResultSet test_rs;
    //db_connector.ExecReadSql2Int("select * from sbtest1", test_rs, 1); 

    // send sql
    JobExecutor job_executor;
    if (!job_executor.ExecTestSequence(test_sequence_list[0], db_connector)) {
        return 0;
    }
/*
    for (auto &test_sequence : test_sequence_list) {
        std::vector<TxnSQL> txn_sql_list = test_sequence.get_txn_sql_list();
        for (auto &txn_sql : txn_sql_list) {
            std::cout << txn_sql.get_sql() << std::endl;
        }
    }
*/    
    return 0;
}
