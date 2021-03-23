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
#include "sql_cntl.h"

std::string SQLCHARToStr(SQLCHAR* ch) {
    char* ch_char = (char*)ch;
    std::string ch_str = ch_char;
    return ch_str;
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

std::string DBConnector::SqlExecuteErr(int session_id, const std::string& sql, std::string handle_type, SQLHANDLE& handle, SQLRETURN ret, std::string test_process_file) {
    std::ofstream test_process(test_process_file, std::ios::app);
    std::string blank(blank_base*(session_id - 1), ' ');
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return "";
    } else if (ret == SQL_ERROR) {
        SQLCHAR ErrInfo[256];
        SQLCHAR SQLState[256];
        DBConnector::ErrInfoWithStmt(handle_type, handle, ErrInfo, SQLState);
        std::string err_info = SQLCHARToStr(ErrInfo);

        auto index_not_exist = err_info.find("not exist");
        auto index_crdb_rollback = sql.find("ROLLBACK TRANSACTION");
        if (index_not_exist == err_info.npos && index_crdb_rollback == sql.npos) {
            std::cout << blank + "execute sql: '" + sql + "' failed, reason: " << ErrInfo << " errcode: " << SQLState << std::endl;
            if (!test_process) {
                test_process << blank + "execute sql: '" + sql + "' failed, reason: " << ErrInfo << " errcode: " << SQLState << std::endl;
            }
        } else {
            return "";
        }

        return err_info;
    } else if (ret == SQL_NEED_DATA) {
        std::cout << blank + "SQL_NEED_DATA" << std::endl;
        test_process << blank + "SQL_NEED_DATA" << std::endl;
        return "SQL_NEED_DATA";
    } else if (ret == SQL_STILL_EXECUTING) {
        std::cout << blank + "SQL_STILL_EXECUTING" << std::endl;
        test_process << blank + "SQL_STILL_EXECUTING" << std::endl;
        return "SQL_STILL_EXECUTING";
    } else if (ret == SQL_NO_DATA) {
        std::cout << blank + "SQL_NO_DATA" << std::endl;
        test_process << blank + "SQL_NO_DATA" << std::endl;
        return "SQL_NO_DATA";
    } else if (ret == SQL_INVALID_HANDLE) {
        std::cout << blank + "SQL_INVALID_HANDLE" << std::endl;
        test_process << blank + "SQL_INVALID_HANDLE" << std::endl;
        return "SQL_INVALID_HANDLE";
    } else if (ret == SQL_PARAM_DATA_AVAILABLE) {
        std::cout << blank + "SQL_PARAM_DATA_AVAILABLE" << std::endl;
        test_process << blank + "SQL_PARAM_DATA_AVAILABLE" << std::endl;
        return "SQL_PARAM_DATA_AVAILABLE";
    } else {
        std::cout << blank + "execute sql: '" + sql + "' failed, unknow error" << std::endl;
        test_process << blank + "execute sql: '" + sql + "' failed, unknow error" << std::endl;
        return "unknow error";
    }
}

bool DBConnector::ExecWriteSql(int sql_id, const std::string& sql, TestResultSet& test_result_set, int session_id, std::string test_process_file) {
    SQLRETURN ret;
    SQLHSTMT m_hStatement;
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[session_id - 1];

    ret = SQLAllocHandle(SQL_HANDLE_STMT, m_hDatabaseConnection, &m_hStatement);
    std::string err_info_stmt = DBConnector::SqlExecuteErr(session_id, sql, "stmt", m_hStatement, ret);
    if (!err_info_stmt.empty()) {
        std::cout << "get stmt failed in DBConnector::ExecWriteSql" << std::endl;
        return false;
    }
    // execute sql
    if (sql_id != 1024) {
        std::string blank(blank_base*(session_id - 1), ' ');
	    std::string output_info = blank + "T" + std::to_string(session_id) + " execute sql: '" + sql + "'";
        std::cout << output_info << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
	        test_process << output_info << std::endl;
        }
    }
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    std::string err_info_sql = DBConnector::SqlExecuteErr(session_id, sql, "stmt", m_hStatement, ret, test_process_file);
    if (!err_info_sql.empty()) {
        auto index_timeout1 = err_info_sql.find("timeout");
        auto index_timeout2 = err_info_sql.find("Timeout");
	    auto index_timeout3 = err_info_sql.find("time out");
        if (index_timeout1 != err_info_sql.npos || index_timeout2 != err_info_sql.npos || index_timeout3 != err_info_sql.npos) {
            test_result_set.SetResultType("Timeout\nReason: Transaction execution timeout");
            return true;
        } else {
	    if (test_result_set.ResultType().empty()) {
            std::string info = "Rollback\nReason: " + err_info_sql;
	        test_result_set.SetResultType(info);
            }
        }
        //return false;
    }
    // get error info
    return true;
}


bool DBConnector::ExecReadSql2Int(int sql_id, const std::string& sql, TestResultSet& test_result_set,
                                  std::unordered_map<int, std::vector<std::string>>& cur_result_set,
                                  int session_id, int param_num, std::string test_process_file) {
    SQLRETURN ret;
    SQLHSTMT m_hStatement;
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[session_id - 1];
    std::vector<std::unordered_map<int, std::vector<std::string>>> expected_result_set_list = test_result_set.ExpectedResultSetList();

    ret = SQLAllocHandle(SQL_HANDLE_STMT, m_hDatabaseConnection, &m_hStatement);
    std::string err_info_stmt = DBConnector::SqlExecuteErr(session_id, sql, "stmt", m_hStatement, ret);
    if (!err_info_stmt.empty()) {
        std::cout << "get stmt failed in DBConnector::ExecReadSql2Int" << std::endl;
        return false;
    }
    SQLLEN length;
    SQLCHAR value[param_num][20] = {{0}};
    // execute sql
    std::string blank(blank_base*(session_id - 1), ' ');
    std::string output_info = blank + "T" + std::to_string(session_id) + " execute sql: '" + sql + "'";
    std::cout << output_info << std::endl;
    if (!test_process_file.empty()) {
	    std::ofstream test_process(test_process_file, std::ios::app);
        test_process << output_info << std::endl;
    }
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    // parse result
    std::string err_info_sql = DBConnector::SqlExecuteErr(session_id, sql, "stmt", m_hStatement, ret, test_process_file);
    if(err_info_sql.empty()) {
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
            row = "(" + row + ")";
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
        outputter.PrintAndWriteTxnSqlResult(cur_result_set[sql_id], expected_result_set_list, sql_id, sql, session_id, test_process_file);
        return true;
    } else {
        auto index_timeout1 = err_info_sql.find("timeout");
	auto index_timeout2 = err_info_sql.find("Timeout");
	auto index_timeout3 = err_info_sql.find("time out");
        if (index_timeout1 != err_info_sql.npos || index_timeout2 != err_info_sql.npos || index_timeout3 != err_info_sql.npos) {
            test_result_set.SetResultType("Timeout\nReason: Transaction execution timeout");
            return true;
        } else {
            if (test_result_set.ResultType().empty()) { 
                std::string info = "Rollback\nReason: " + err_info_sql;
	        test_result_set.SetResultType(info);
            }
        }
        //return false;
	return true;
    }
}

bool DBConnector::SQLEndTnx(std::string opt, int session_id, int sql_id, TestResultSet& test_result_set, const std::string& db_type, std::string test_process_file) {
    if (sql_id != 1024) {
        std::string blank(blank_base*(session_id - 1), ' ');
        std::string output_info = blank + "T" + std::to_string(session_id) + " execute opt: '" + opt + "'";
        std::cout << output_info << std::endl;
        if (!test_process_file.empty()) {
	        std::ofstream test_process(test_process_file, std::ios::app);
            test_process << output_info << std::endl;
        }
    }
    if (db_type != "oracle") {
        SQLRETURN ret;
        SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[session_id - 1];
        if ("commit" == opt) {
            ret = SQLEndTran(SQL_HANDLE_DBC, m_hDatabaseConnection, SQL_COMMIT);
        } else if ("rollback" == opt) {
            ret = SQLEndTran(SQL_HANDLE_DBC, m_hDatabaseConnection, SQL_ROLLBACK);
        } else {
            std::cout << "unknow txn opt" << std::endl;
        }

	std::string err_info_sql = DBConnector::SqlExecuteErr(session_id, opt, "dbc", m_hDatabaseConnection, ret, test_process_file);
        if (!err_info_sql.empty()) {
            if (test_result_set.ResultType().empty()) {
                std::string info = "Rollback\n" + err_info_sql;
                test_result_set.SetResultType(info);
            }
        }
    } else {
        TestResultSet test_result_set;
        if (!DBConnector::ExecWriteSql(1024, opt, test_result_set, session_id, test_process_file)) {
            return false;
        }	
    }
    return true;
}

bool DBConnector::SQLStartTxn(int session_id, int sql_id, std::string test_process_file) {
    SQLHDBC m_hDatabaseConnection = DBConnector::conn_pool_[session_id - 1];
    std::ofstream test_process(test_process_file, std::ios::app);
    if(!DBConnector::SetAutoCommit(m_hDatabaseConnection, 0)) {
        std::string blank(blank_base*(session_id - 1), ' ');
	    std::string output_info = blank + "T" + std::to_string(session_id) + " start txn failed";
        std::cout << output_info << std::endl;
        if (!test_process) {
            test_process << output_info << std::endl;
        }
        return false;
    } else {
        std::string blank(blank_base*(session_id - 1), ' ');
	    std::string output_info = blank + "T" + std::to_string(session_id) + " start txn success";
        std::cout << output_info << std::endl;
	if (!test_process) {
	    test_process << output_info << std::endl;
	}
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
        std::cout << "set AutoCommit = 0 failed" << std::endl;
        return false;
    }
    return true;
}

bool DBConnector::SetTimeout(int conn_id, std::string timeout, const std::string& db_type) {
    std::string sql = "" ;
    if (db_type == "sqlserver") {
        sql = "SET LOCK_TIMEOUT " + timeout + "000" + ";";
    } else if (db_type == "ob") {
        sql = "set session ob_trx_timeout=" + timeout + "000000;"; 
    }
    if (!sql.empty()) {
        TestResultSet test_result_set;
        if (!DBConnector::ExecWriteSql(1024, sql, test_result_set, conn_id)) {
            return false;
        }
    }

    return true;
}

bool DBConnector::SetIsolationLevel(SQLHDBC m_hDatabaseConnection, std::string opt, int session_id, const std::string& db_type, std::string test_process_file) {
    if (db_type != "oracle" && db_type != "ob") {
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
            std::cout << "unknow isolation level" << std::endl;
            return false;
        }
        std::string err_info_stmt = DBConnector::SqlExecuteErr(session_id, "set isolation", "dbc", m_hDatabaseConnection, ret, test_process_file);
        if (!err_info_stmt.empty()) {
            return false;
        }
    } else {
        std::string iso;
        if (opt == "read-committed") {
            iso = "read committed";
        } else if (opt == "repeatable-read") {
	    iso = "repeatable read";
        } else if (opt == "serializable") {
            iso = "serializable";
        } else {
            std::cout << "unknow isolation level" << std::endl;
            return false;
        }
	TestResultSet test_result_set;
	std::string sql;
	if (db_type == "oracle") {
	    sql = "alter session set isolation_level =" + iso;
	} else if (db_type == "ob") {
	    sql = "set session transaction isolation level " + iso + ";";
	}
    	if (!DBConnector::ExecWriteSql(1024, sql, test_result_set, session_id, test_process_file)) {
	    return false;
	}
    }
    return true;
}
