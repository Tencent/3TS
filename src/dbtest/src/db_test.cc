#include "db_test.h"
#include "gflags/gflags.h"
#include <iomanip>
#include <cstdlib>
#include <fstream>
#include <cassert>
#include <string>


DEFINE_string(db_type, "mysql", "data resource name, please see /etc/odbc.ini, such as mysql pg oracle ob tidb sqlserver crdb");
DEFINE_string(user, "test123", "username");
DEFINE_string(passwd, "Ly.123456", "password");
DEFINE_string(db_name, "test", "create database name");
DEFINE_int32(conn_pool_size, 4, "db_conn pool size");
DEFINE_string(isolation, "serializable", "transation isolation level");
DEFINE_string(case_dir, "mysql", "test case dir name");
DEFINE_string(timeout, "5", "timeout");
std::string dash(10, '-');

std::string SQLCHARToStr(SQLCHAR* ch) {
    char* ch_char = (char*)ch;
    std::string ch_str = ch_char;
    return ch_str;
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

bool PrintAndWriteTxnSqlResult(std::vector<std::string> cur_result,
		               std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list,
			       int sql_id, std::string sql, int txn_id) {
    std::string sql_result_file = "./" + FLAGS_db_type  + "-" + FLAGS_isolation + "-read-sql-result"  + ".txt";
    std::ofstream out(sql_result_file, std::ios::app);
    if (out) {
        std::cout << "  cur_result: " << std::endl;
	std::cout << "    ";	
        std::copy(cur_result.begin(), cur_result.end(), std::ostream_iterator<std::string> (std::cout, " "));
        std::cout << "" << std::endl;

        out << "sql_id: " + std::to_string(sql_id) + " txn_id: " + std::to_string(txn_id) + " sql: " + sql << std::endl;
        out << "  cur_result: " << std::endl;
	out << "    ";
        std::copy(cur_result.begin(), cur_result.end(), std::ostream_iterator<std::string> (out, " "));
        out << "" << std::endl;
        int idx = 1;
        for (auto& expected_result_set : expected_result_set_list) {
            std::vector<std::string> sql_expected_result = expected_result_set[sql_id];
            std::cout << "  (" + std::to_string(idx) + ") " + "expected_result: " << std::endl;
	    std::cout << "    ";
            std::copy(sql_expected_result.begin(), sql_expected_result.end(), std::ostream_iterator<std::string> (std::cout, " "));
            std::cout << "" << std::endl;
            out << "  (" + std::to_string(idx) + ") " +  "expected_result: " << std::endl;
	    out << "    ";
            std::copy(sql_expected_result.begin(), sql_expected_result.end(), std::ostream_iterator<std::string> (out, " "));
            out << "" << std::endl;
	    idx++;
        }
	out << "" << std::endl;
        return true;
    } else {
        std::cerr << sql_result_file + "has not found" << std::endl;
        return false;
    }
}

bool WriteTestCaseTypeToFile(std::string test_case_type) {
    std::string sql_result_file = "./" + FLAGS_db_type  + "-" + FLAGS_isolation + "-read-sql-result"  + ".txt";
    std::ofstream out(sql_result_file, std::ios::app);
    if (out) {
        out << dash + test_case_type + dash << std::endl;
        return true;
    } else {
        std::cerr << sql_result_file + "has not found" << std::endl;
        return false;
    }
}

bool WriteResultType(std::string result_type) {
    std::string sql_result_file = "./" + FLAGS_db_type  + "-" + FLAGS_isolation + "-read-sql-result"  + ".txt";
    std::ofstream out(sql_result_file, std::ios::app);
    if (out) {
        out << "result_type: " + result_type << std::endl;
        out << "" << std::endl;
        return true;
    } else {
        std::cerr << sql_result_file + "has not found" << std::endl;
        return false;
    }

}

bool IsSqlExpectedResult(std::vector<std::string> cur_result, std::vector<std::string> expected_result, int sql_id, std::string sql) {
    if (cur_result.size() != expected_result.size()) {
        std::cerr << "number of cur_result_row != number of expected_result_row" << std::endl;
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

bool IsTestExpectedResult(std::unordered_map<int, std::vector<std::string>>& cur_result,
                          std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list,
                          std::unordered_map<int, std::string> sql_map) {
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
            return true;
        }
    }
    return false;
}

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
    int first_sql_id = -1;
    std::unordered_map<int, std::vector<std::string>> sql_result_set;
    if (test_stream) {
        while (getline(test_stream, line)) {
            if (line != "") {
                auto index_table_type = line.find("ParamNum");
                auto index_left = line.find("{");
                auto index_right = line.find("}");
                if (index_table_type != line.npos) {
                    auto start = line.find(":");
                    std::string param_num_str = line.substr(start + 1);
                    int param_num = std::stoi(param_num_str);
                    test_sequence.SetParamNum(param_num);
                } else if (index_left != line.npos) {
                    std::string isolation = SQLReader::Isolation(line);
                    test_result_set.SetIsolation(isolation);
                    is_result_row = true;
                } else if (index_right != line.npos) {
                    std::pair<int, std::string> sql_id_and_result = SQLReader::SqlIdAndResult(line);
                    std::vector<std::string> ret_list;
                    std::string result_str = sql_id_and_result.second;
                    split(result_str, ret_list, " ");
                    sql_result_set[sql_id_and_result.first] = ret_list;
                    test_result_set.AddSqlResultSet(sql_result_set);
                    is_result_row = false;
                } else if (is_result_row) {
                    std::pair<int, std::string> sql_id_and_result = SQLReader::SqlIdAndResult(line);
                    if (first_sql_id == -1) {
                        first_sql_id = sql_id_and_result.first; 
                    } else if (first_sql_id == sql_id_and_result.first) {
                        test_result_set.AddSqlResultSet(sql_result_set);
                        sql_result_set.clear();
                    }
                    std::vector<std::string> ret_list;
                    std::string result_str = sql_id_and_result.second;
                    split(result_str, ret_list, " ");
                    sql_result_set[sql_id_and_result.first] = ret_list;
                } else if (!is_result_row) { 
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
        std::cerr << "test file: " + test_file  + " not found" << std::endl;
    }
    return test_sequence_and_result_set;
}



bool SQLReader::InitTestSequenceAndTestResultSetList(std::string& test_path) {
    std::cout << dash + "read test sequence and test result set start" + dash << std::endl;
    std::ifstream do_test_stream("./do_test_list.txt");
    std::string test_case;
    if (do_test_stream) {
        while (getline(do_test_stream, test_case)) {
            auto index = test_case.find("#");
            if (index != test_case.npos) {
                continue;
            }
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
void DBConnector::ErrInfoWithStmt(std::string handle_type, SQLHANDLE& handle, SQLCHAR ErrInfo[], SQLCHAR SQLState[]) { 
    SQLINTEGER NativeErrorPtr = 0;
    SQLSMALLINT TextLengthPtr = 0;
    if ("stmt" == handle_type) {
        SQLGetDiagRec(SQL_HANDLE_STMT, handle, 1, SQLState, &NativeErrorPtr, ErrInfo, 256, &TextLengthPtr);
    }
    if ("dbc" == handle_type) {
        SQLGetDiagRec(SQL_HANDLE_DBC, handle, 1, SQLState, &NativeErrorPtr, ErrInfo, 256, &TextLengthPtr);
    }
}

std::string DBConnector::SqlExecuteErr(const std::string& sql, std::string handle_type, SQLHANDLE& handle, SQLRETURN ret) {
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return "";
    } else if (ret == SQL_ERROR) {
        SQLCHAR ErrInfo[256];
        SQLCHAR SQLState[256];
        DBConnector::ErrInfoWithStmt(handle_type, handle, ErrInfo, SQLState);
        std::cerr << "execute sql: '" + sql + "' failed, reason: " << ErrInfo << " errcode: " <<  SQLState << std::endl;
        std::string err_info = SQLCHARToStr(ErrInfo);
        return err_info;
    } else if (ret == SQL_NEED_DATA) {
        std::cout << "SQL_NEED_DATA" << std::endl;
        return "SQL_NEED_DATA";
    } else if (ret == SQL_STILL_EXECUTING) {
        std::cout << "SQL_STILL_EXECUTING" << std::endl;
        return "SQL_STILL_EXECUTING";
    } else if (ret == SQL_NO_DATA) {
        std::cout << "SQL_NO_DATA" << std::endl;
        return "SQL_NO_DATA";
    } else if (ret == SQL_INVALID_HANDLE) {
        std::cout << "SQL_INVALID_HANDLE" << std::endl;
        return "SQL_INVALID_HANDLE";
    } else if (ret == SQL_PARAM_DATA_AVAILABLE) {
        std::cout << "SQL_PARAM_DATA_AVAILABLE" << std::endl;
        return "SQL_PARAM_DATA_AVAILABLE";
    } else {
        std::cerr << "execute sql: '" + sql + "' failed, unknow error" << std::endl;
        return "unknow error";
    }
}

bool DBConnector::ExecWriteSql(int sql_id, const std::string& sql, TestResultSet& test_result_set, int conn_id) {
    SQLRETURN ret;
    SQLHSTMT m_hStatement;
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[conn_id - 1];

    ret = SQLAllocHandle(SQL_HANDLE_STMT, m_hDatabaseConnection, &m_hStatement);
    std::string err_info_stmt = DBConnector::SqlExecuteErr(sql, "stmt", m_hStatement, ret);
    if (err_info_stmt != "") {
        return false;
    }
    // execute sql
    std::cout << "sql_id: " + std::to_string(sql_id) + " txn_id: " + std::to_string(conn_id) +  " execute sql: '" + sql + "'"<< std::endl;
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    std::string err_info_sql = DBConnector::SqlExecuteErr(sql, "stmt", m_hStatement, ret);
    if (err_info_sql != "") {
	/*
        auto index_lock_wait = err_info_sql.find("Lock wait timeout exceeded");
	auto index_lock_wait_1 = err_info_sql.find("");
        if (index_lock_wait != err_info_sql.npos) {
            test_result_set.SetResultType("timeout");
            return true;
        }
	auto index_serialize = err_info_sql.find("can't serialize access");
        if (index_serialize != err_info_sql.npos) {
            test_result_set.SetResultType("can't serialize access");
            return true;
        }*/
	test_result_set.SetResultType(err_info_sql);
        //return false;
    }
    // get error info
    return true;
}


bool DBConnector::ExecReadSql2Int(int sql_id, const std::string& sql, TestResultSet& test_result_set,
                                  std::unordered_map<int, std::vector<std::string>>& cur_result_set,
                                  int conn_id, int param_num) {
    SQLRETURN ret;
    SQLHSTMT m_hStatement;
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[conn_id - 1];
    std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list = test_result_set.ExpectedResultSetList();

    ret = SQLAllocHandle(SQL_HANDLE_STMT, m_hDatabaseConnection, &m_hStatement);
    std::string err_info_stmt = DBConnector::SqlExecuteErr(sql, "stmt", m_hStatement, ret);
    if (err_info_stmt != "") {
        return false;
    }
    SQLLEN length;
    SQLCHAR value[param_num][20] = {{0}};
    // execute sql
    std::cout << "sql_id: " + std::to_string(sql_id) + " txn_id: " + std::to_string(conn_id) + " execute sql: '" + sql + "'" << std::endl;
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    // parse result
    std::string err_info_sql = DBConnector::SqlExecuteErr(sql, "stmt", m_hStatement, ret);
    if(err_info_sql == "") {
        // bind column data
        for (int i = 0; i < param_num; i++) {
            SQLBindCol(m_hStatement, i + 1, SQL_C_CHAR, (void*)value[i], sizeof(value[i]), &length);
        }
        // get next row, and fill cur_sql_result_set
        bool is_no_data = true;
        while(SQL_NO_DATA != SQLFetch(m_hStatement)) {
            std::string row = "";
            for (int i = 0; i < param_num; i++) {
                std::string v_str = SQLCHARToStr(value[i]);
                if (i == (param_num - 1)) {
                    row += v_str;
                } else {
                    row += v_str + ",";
                }
            }
            if (cur_result_set.find(sql_id) != cur_result_set.end()) {
                cur_result_set[sql_id].push_back(row);    
            } else {
                std::vector<std::string> sql_result;
                sql_result.push_back(row);
                cur_result_set[sql_id] = sql_result; 
            }
            is_no_data = false;
        }
        if (is_no_data) {
            cur_result_set[sql_id].push_back("null");
        }
        PrintAndWriteTxnSqlResult(cur_result_set[sql_id], expected_result_set_list, sql_id, sql, conn_id);
        return true;
    } else {
        auto index_lock_wait = err_info_sql.find("Lock wait timeout exceeded");
        if (index_lock_wait != err_info_sql.npos) {
            test_result_set.SetResultType("timeout");
            return true;
        }
	test_result_set.SetResultType(err_info_sql);
        //return false;
	return true;
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
    if (FLAGS_db_type != "oracle") {
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
    } else {
        TestResultSet test_result_set;
        if (!DBConnector::ExecWriteSql(0, opt, test_result_set, conn_id)) {
            return false;
        }	
    }
    return true;
}

bool DBConnector::SQLStartTxn(int conn_id) {
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[conn_id - 1];
    if(!DBConnector::SetAutoCommit(m_hDatabaseConnection, 0)) {
        std::cerr << "txn_id: " + std::to_string(conn_id)  + " start txn failed" << std::endl;
        return false;
    } else {
        std::cout << "txn_id: " + std::to_string(conn_id)  + " start txn success" << std::endl;
        return true;
    }
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

bool DBConnector::SetTimeout(SQLHDBC m_hDatabaseConnection, int timeout) {
    SQLRETURN ret;
    ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);
    std::string err_info_stmt = DBConnector::SqlExecuteErr("set timeout", "dbc", m_hDatabaseConnection, ret);
    if (err_info_stmt != "") {
        return false;
    }
	return true;
}

bool DBConnector::SetIsolationLevel(SQLHDBC m_hDatabaseConnection, std::string opt, int conn_id) {
    if (FLAGS_db_type != "oracle" && FLAGS_db_type != "ob") {
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
        std::string err_info_stmt = DBConnector::SqlExecuteErr("set isolation", "dbc", m_hDatabaseConnection, ret);
        if (err_info_stmt != "") {
            return false;
        }
    } else {
        std::string iso;
        if (opt == "read-committed") {
            iso = "read committed";
        } else if (opt == "serializable") {
            iso = "serializable";
        } else {
            std::cerr << "unknow isolation level" << std::endl;
            return false;
        }
	TestResultSet test_result_set;
	std::string sql;
	if (FLAGS_db_type == "oracle") {
	    sql = "alter session set isolation_level =" + iso;
	} else if (FLAGS_db_type == "ob") {
	    sql = "set session transaction isolation level " + iso + ";";
	}
    	if (!DBConnector::ExecWriteSql(0, sql, test_result_set, conn_id)) {
	    return false;
	}
    }
    //if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
    //    std::cerr << "set TXN_ISOLATION = " + opt + " failed" << std::endl;
    //    return false;
    //}
    return true;
}

bool JobExecutor::ExecTestSequence(TestSequence& test_sequence, TestResultSet& test_result_set, DBConnector db_connector) {
    std::vector<TxnSql> txn_sql_list = test_sequence.TxnSqlList();
    std::cout << dash + test_sequence.TestCaseType() + " test start" + dash << std::endl;
    if (!WriteTestCaseTypeToFile(test_sequence.TestCaseType())) {
        return false;
    }
    //int i = 0;
    std::unordered_map<int, std::vector<std::string>> cur_result_set;
    std::unordered_map<int, std::string> sql_map;
    std::string table_name;
    for (auto& txn_sql : txn_sql_list) {
        if (test_result_set.ResultType() == "timeout") {
	        if (test_result_set.ResultType() == "timeout") {
                for (int i = 0; i < FLAGS_conn_pool_size; i++) {
                    db_connector.SQLEndTnx("rollback", i + 1, test_result_set);
                }
            }
            WriteResultType(test_result_set.ResultType());
            break;
        }
        int sql_id = txn_sql.SqlId();
        int txn_id = txn_sql.TxnId();
        std::string sql = txn_sql.Sql();
        sql_map[sql_id] = sql;
        auto index_read = sql.find("SELECT");
        auto index_read_1 = sql.find("select");
        auto index_begin = sql.find("BEGIN");
        auto index_begin_1 = sql.find("begin");
        auto index_commit = sql.find("COMMIT");
        auto index_commit_1 = sql.find("commit");
        auto index_rollback = sql.find("ROLLBACK");
        auto index_rollback_1 = sql.find("rollback");
        if (index_read != sql.npos || index_read_1 != sql.npos) {
            if (!db_connector.ExecReadSql2Int(sql_id, sql, test_result_set, cur_result_set, txn_id, test_sequence.ParamNum())) {
                return false;
            }
        } else if (index_begin != sql.npos || index_begin_1 != sql.npos) {
	    if (FLAGS_db_type != "crdb" && FLAGS_db_type != "ob") {
                if (!db_connector.SQLStartTxn(txn_id)) {
                    return false;
                }
		/*
		if (FLAGS_db_type == "crdb") {
		    if (!db_connector.ExecWriteSql(0, "set transaction isolation level serializable;", test_result_set, txn_id)) {
		        return false;
		    }
		}*/
	    } else {
	        if (!db_connector.ExecWriteSql(0, "BEGIN;", test_result_set, txn_id)) {
                    return false;
	        }
	    }
        } else if (index_commit != sql.npos || index_commit_1 != sql.npos) {
	    if (FLAGS_db_type != "crdb") {
                if (!db_connector.SQLEndTnx("commit", txn_id, test_result_set)) {
                    return false;
                }
	    } else {
	        if (!db_connector.ExecWriteSql(0, "COMMIT;", test_result_set, txn_id)) {
                    return false;
		}
	    }
        } else if (index_rollback != sql.npos || index_rollback_1 != sql.npos) {
            if (!db_connector.SQLEndTnx("rollback", txn_id, test_result_set)) {
                return false;
            }
        } else {
            if (!db_connector.ExecWriteSql(sql_id, sql, test_result_set, txn_id)) {
                return false;
            }
        }
        //i++;
        //if (i == (int)txn_sql_list.size()) {
        //    if(!WriteResultType(test_result_set.ResultType())) {
        //        return false;
        //    }
        //}
    }
    if (test_result_set.ResultType() == "") {
        if (IsTestExpectedResult(cur_result_set, test_result_set.ExpectedResultSetList(), sql_map)) {
            test_result_set.SetResultType("avoid");
        } else {
            test_result_set.SetResultType("exception");
        }
    }
    if(!WriteResultType(test_result_set.ResultType())) {
        return false; 
    }
    return true;
}

void Outputter::WriteResultSet(std::vector<TestResultSet> test_result_set_list) {
    std::string ret_file = "./" + FLAGS_db_type + "-" + FLAGS_isolation + ".txt";
    std::ofstream out(ret_file);
    for (auto& test_result_set : test_result_set_list) {
        out << test_result_set.TestCaseType() + ": " << test_result_set.ResultType() << std::endl; 
    }
    out.close();
}

int main(int argc, char* argv[]) {
    // parse gflags args
    google::ParseCommandLineFlags(&argc, &argv, true);
    std::cout << "input param-> " << std::endl;
    std::cout << "  db_type: " + FLAGS_db_type << std::endl;
    std::cout << "  user: " + FLAGS_user << std::endl;
    std::cout << "  passwd: " + FLAGS_passwd << std::endl;
    std::cout << "  isolation: " + FLAGS_isolation << std::endl;
    // read test sequence
    SQLReader sql_reader;
    std::string test_path_base = "t/";
    std::string test_path = test_path_base + FLAGS_case_dir;
    if (!sql_reader.InitTestSequenceAndTestResultSetList(test_path)) {
        std::cerr << "init test sequence and test result set failed" << std::endl;
    }

    std::vector<TestSequence> test_sequence_list = sql_reader.TestSequenceList();
    std::vector<TestResultSet> test_result_set_list = sql_reader.TestResultSetList();
    /*
    std::cout << "begin" << std::endl;
    for (auto& test_result_set : test_result_set_list) {
        std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list = test_result_set.ExpectedResultSetList();
        for (auto& expected_result_set : expected_result_set_list) {
            for (auto& k : expected_result_set) {
                int sql_id = k.first;
                std::vector<std::string> cur_result = k.second;
                std::cout << sql_id << std::endl;
                std::copy(cur_result.begin(), cur_result.end(), std::ostream_iterator<std::string> (std::cout, " "));
                std::cout << "" << std::endl;

            }
        }
    }*/

    // init db_connector
    std::cout << dash + "init db_connector start" + dash << std::endl;
    DBConnector db_connector;
    if (!db_connector.InitDBConnector(FLAGS_user, FLAGS_passwd, FLAGS_db_type, FLAGS_conn_pool_size)) {
        std::cerr << "init db_connector failed" << std::endl;
    }
    if (FLAGS_db_type != "crdb") {
    // init database
        if (FLAGS_db_type != "oracle") {
            std::cout << dash + "init database start" + dash << std::endl;
            TestResultSet test_rs__;
            db_connector.ExecWriteSql(0, "create database if not exists " + FLAGS_db_name, test_rs__, 1);
            db_connector.ExecWriteSql(0, "use " + FLAGS_db_name, test_rs__, 1);
        }
    }
    if (FLAGS_db_type != "crdb") {
        std::cout << dash + "set TXN_ISOLATION = " + FLAGS_isolation + dash << std::endl;
        std::cout << dash + "set TIMEOUT = " + FLAGS_timeout + dash << std::endl;
        int idx = 1;
        for (auto hdbc : db_connector.DBConnPool()) {
            /*// set timeout
            if (!db_connector.SetTimeout(hdbc, std::stoi(FLAGS_timeout))) {
                return false;
            }*/
            // set TXN_ISOLATION
            if(!db_connector.SetIsolationLevel(hdbc, FLAGS_isolation, idx)) {
                return false;
            }
	    idx++;
        }
        std::cout << "set TXN_ISOLATION = " + FLAGS_isolation + " success"<< std::endl;
    }
    
    // clear result file
    std::string sql_result_file = "./" + FLAGS_db_type  + "-" + FLAGS_isolation + "-read-sql-result"  + ".txt";
    std::ifstream sql_result(sql_result_file);
    if (sql_result) {
        std::remove(sql_result_file.c_str());
	std::cout << "delete" << std::endl;
    } 
    // send sql
    JobExecutor job_executor;
    Outputter outputter;
    int len = test_sequence_list.size();
    for (int i = 0; i < len; i++) {
        if (!job_executor.ExecTestSequence(test_sequence_list[i], test_result_set_list[i], db_connector)) {
            std::cerr << "test sequence " + test_sequence_list[i].TestCaseType() + " execute failed" << std::endl;
            //return 0;
        } else {
            std::string result_type = test_result_set_list[i].ResultType();
            std::cout << "result_type:" << result_type + "\n" << std::endl;
        }
    }
    outputter.WriteResultSet(test_result_set_list);
    db_connector.ReleaseConn();
    
    return 0;
}
