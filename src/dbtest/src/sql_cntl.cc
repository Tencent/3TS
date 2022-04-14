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
#include <time.h>
#include <chrono>
#include <string>
#include <regex>

std::string get_current_time(){

    // date
    time_t d = time(0);
    tm* d_now = std::localtime(&d);

    // time
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();

    typedef std::chrono::duration<int, std::ratio_multiply<std::chrono::hours::period, std::ratio<8>
    >::type> Days; /* UTC: +8:00 */

    Days days = std::chrono::duration_cast<Days>(duration);
        duration -= days;
    auto hours = std::chrono::duration_cast<std::chrono::hours>(duration);
        duration -= hours;
    auto minutes = std::chrono::duration_cast<std::chrono::minutes>(duration);
        duration -= minutes;
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
        duration -= seconds;
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        duration -= milliseconds;
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration);
        duration -= microseconds;
    auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);

    // return it by year, month, day, and time
    return  std::to_string(d_now->tm_year + 1900) + "-" + std::to_string(d_now->tm_mon + 1) + "-" + std::to_string(d_now->tm_mday) 
            + " " +
            std::to_string(d_now->tm_hour) +':'+ std::to_string(minutes.count()) +":"+ std::to_string(seconds.count())+":"
            + std::to_string(milliseconds.count()) +':'+ std::to_string(microseconds.count());

}

bool replace(std::string& str, const std::string& from, const std::string& to) {
    size_t start_pos = str.find(from);
    if(start_pos == std::string::npos)
        return false;
    str.replace(start_pos, from.length(), to);
    return true;
}

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

std::string DBConnector::SqlExecuteErr(int session_id, int sql_id, const std::string& sql, std::string handle_type, SQLHANDLE& handle, SQLRETURN ret, std::string test_process_file) {
    
    std::ofstream test_process(test_process_file, std::ios::app);
    std::string blank(blank_base*(session_id - 1), ' ');
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return "";
    } else if (ret == SQL_ERROR) {
        SQLCHAR ErrInfo[256];
        SQLCHAR SQLState[256];
        DBConnector::ErrInfoWithStmt(handle_type, handle, ErrInfo, SQLState);
        std::string err_info = SQLCHARToStr(ErrInfo);

        // get error information of all line, comment it to get only first line
        // replace "\n" to " "    
        err_info = err_info.substr(0, err_info.find("\n"));

        auto index_not_exist = err_info.find("not exist");
        auto index_crdb_rollback = sql.find("ROLLBACK TRANSACTION");
        if (sql_id != 1024 && index_not_exist == err_info.npos && index_crdb_rollback == sql.npos) {
            // some time between rollback queries
            usleep(100000*sql_id^3);
            std::cout << blank + "Q" + std::to_string(sql_id) +  " failed reason: " << err_info << " errcode: " << SQLState << std::endl;
            test_process << blank + "Q" + std::to_string(sql_id) +  " failed reason: " << err_info << " errcode: " << SQLState << std::endl;
            std::string output_time_info = blank + "Q" + std::to_string(sql_id) + " failed at: " + get_current_time() ;
            std::cout << output_time_info << std::endl;
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << output_time_info << std::endl;
            // if (!test_process) {
            //     test_process << output_time_info << std::endl;
            // }
            return err_info;
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
    } else if (ret == SQL_PARAM_DIAG_UNAVAILABLE) {
        std::cout << blank + "SQL_PARAM_DIAG_UNAVAILABLE" << std::endl;
        test_process << blank + "SQL_PARAM_DIAG_UNAVAILABLE" << std::endl;
        return "SQL_PARAM_DIAG_UNAVAILABLE";
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
    std::string err_info_stmt = DBConnector::SqlExecuteErr(session_id, sql_id, sql, "stmt", m_hStatement, ret, test_process_file);
    if (!err_info_stmt.empty()) {
        std::cout << "get stmt failed in DBConnector::ExecWriteSql" << std::endl;
        std::cout << __TIMESTAMP__ << std::endl;
        return false;
    }
    // execute sql
    if (sql_id != 1024) {
        std::string sql1 = std::regex_replace(sql, std::regex("rollback"), "ROLLBACK"); 
        std::string blank(blank_base*(session_id - 1), ' ');
	    std::string output_info = blank + "Q" + std::to_string(sql_id) + "-T" + std::to_string(session_id) + " execute sql: '" + sql1 + "'";
        std::cout << output_info << std::endl;
        if (!test_process_file.empty()) {
            std::ofstream test_process(test_process_file, std::ios::app);
	        test_process << output_info << std::endl;
        }    
    }
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    std::string err_info_sql = DBConnector::SqlExecuteErr(session_id, sql_id, sql, "stmt", m_hStatement, ret, test_process_file);

    if (!err_info_sql.empty()) {
        auto index_timeout1 = err_info_sql.find("timeout");
        auto index_timeout2 = err_info_sql.find("Timeout");
	    auto index_timeout3 = err_info_sql.find("time out");
        if (index_timeout1 != err_info_sql.npos || index_timeout2 != err_info_sql.npos || index_timeout3 != err_info_sql.npos) {
            if (test_result_set.ResultType() == ""){
                test_result_set.SetResultType("Timeout\nReason: Transaction execution timeout");
            }
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
    else{
        if (sql_id != 1024  && sql_id !=0) {
            std::string blank(blank_base*(session_id - 1), ' ');
            std::string output_time_info = blank + "Q" + std::to_string(sql_id) + " finished at: " + get_current_time() ;
            std::cout << output_time_info << std::endl;
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << output_time_info << std::endl;
        }
    }
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
    std::string err_info_stmt = DBConnector::SqlExecuteErr(session_id, sql_id, sql, "stmt", m_hStatement, ret, test_process_file);
    if (!err_info_stmt.empty()) {
        std::cout << "get stmt failed in DBConnector::ExecReadSql2Int" << std::endl;
        return false;
    }
    SQLLEN length;
    SQLCHAR value[param_num][20] = {{0}};
    // execute sql
    std::string blank(blank_base*(session_id - 1), ' ');
    std::string output_info = blank + "Q" + std::to_string(sql_id) + "-T" + std::to_string(session_id) + " execute sql: '" + sql + "'";
    std::cout << output_info << std::endl;
    if (!test_process_file.empty()) {
	    std::ofstream test_process(test_process_file, std::ios::app);
        test_process << output_info << std::endl;
    }
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)sql.c_str(), SQL_NTS);
    // parse result
    std::string err_info_sql = DBConnector::SqlExecuteErr(session_id, sql_id, sql, "stmt", m_hStatement, ret, test_process_file);
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
        if (sql_id != 1024 && sql_id !=0) {
            std::string output_time_info = blank + "Q" + std::to_string(sql_id) + " finished at: " + get_current_time() ;
            std::cout << output_time_info << std::endl;
            std::ofstream test_process(test_process_file, std::ios::app);
            test_process << output_time_info << std::endl;
        }
        return true;
    } else {
        auto index_timeout1 = err_info_sql.find("timeout");
        auto index_timeout2 = err_info_sql.find("Timeout");
        auto index_timeout3 = err_info_sql.find("time out");
        if (index_timeout1 != err_info_sql.npos || index_timeout2 != err_info_sql.npos || index_timeout3 != err_info_sql.npos) {
            if (test_result_set.ResultType() == ""){
                test_result_set.SetResultType("Timeout\nReason: Transaction execution timeout");
            }
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
        std::string output_info;
        if ("commit" == opt) {
            output_info = blank + "Q" + std::to_string(sql_id) + "-T" + std::to_string(session_id) + " execute opt: '"+ "COMMIT" +"';";
        }
        else if ("rollback" == opt) {
            output_info = blank + "Q" + std::to_string(sql_id) + "-T" + std::to_string(session_id) + " execute opt: '"+ "ROLLBACK" +"';";
        }
        else {
            output_info = blank + "Q" + std::to_string(sql_id) + "-T" + std::to_string(session_id) + " execute opt: '"+ opt +"';";
        }
        
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
            if (db_type != "crdb"){
                ret = SQLEndTran(SQL_HANDLE_DBC, m_hDatabaseConnection, SQL_ROLLBACK);
            } else {
                std::string sql = "ROLLBACK TRANSCATION;"; 
                if (!DBConnector::ExecWriteSql(1024, sql, test_result_set, session_id, test_process_file)) {
                return false;
                }
            }
            
        } else {
            std::cout << "unknow txn opt" << std::endl;
        }

        std::string err_info_sql = DBConnector::SqlExecuteErr(session_id, sql_id, opt, "dbc", m_hDatabaseConnection, ret, test_process_file);
        if (!err_info_sql.empty()) {
            if (test_result_set.ResultType().empty()) {
                std::string info = "Rollback\n" + err_info_sql;
                test_result_set.SetResultType(info);
            }
        }
        else{
            if (sql_id != 1024 && sql_id !=0) {
                std::string blank(blank_base*(session_id - 1), ' ');
                std::string output_time_info = blank + "Q" + std::to_string(sql_id) + " finished at: " + get_current_time() ;
                std::cout << output_time_info << std::endl;
                std::ofstream test_process(test_process_file, std::ios::app);
                test_process << output_time_info << std::endl;
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
	    std::string output_info = blank + "Q" + std::to_string(sql_id) + "-T" + std::to_string(session_id) + " begin failed";
        std::cout << output_info << std::endl;
        if (!test_process) {
            test_process << output_info << std::endl;
        }
        return false;
    } else {
        std::string blank(blank_base*(session_id - 1), ' ');
	    std::string output_info = blank + "Q" + std::to_string(sql_id) + "-T" + std::to_string(session_id) + " execute opt: '"+"BEGIN;'";
        std::cout << output_info << std::endl;
        test_process << output_info << std::endl;
    // for test purpose
	// if (!test_process) {
	//     test_process << output_info << std::endl;
	// }
    if (sql_id != 1024 && sql_id !=0) {
        std::string output_time_info = blank + "Q" + std::to_string(sql_id) + " finished at: " + get_current_time() ;
        std::cout << output_time_info << std::endl;
        test_process << output_time_info << std::endl;
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
    // for ob oracle mode
    if (db_type != "oracle" && db_type != "ob") {
    // for ob mysql mode
    // if (db_type != "oracle") {
        SQLRETURN ret;
        if (opt == "read-uncommitted") {
            ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_READ_UNCOMMITTED, 0);
        } else if (opt == "read-committed") {
            ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_READ_COMMITTED, 0);    
        } else if (opt == "repeatable-read") {
            ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_REPEATABLE_READ, 0);  
        } else if (opt == "serializable") {
            ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_SERIALIZABLE, 0);
        } else if (opt == "snapshot") {
            TestResultSet test_result_set;
            std::string sql;
            sql = "ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;"; // SI
            std::cout << sql << std::endl;
            if (!DBConnector::ExecWriteSql(1024, sql, test_result_set, session_id, test_process_file)) {
                return false;
            }
            sql = "SET TRANSACTION ISOLATION LEVEL SNAPSHOT;"; // SI
            std::cout << sql << std::endl;
            if (!DBConnector::ExecWriteSql(1024, sql, test_result_set, session_id, test_process_file)) {
                return false;
            }
         } else if (opt == "rcsnapshot") {
            ret = SQLSetConnectAttr(m_hDatabaseConnection, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_READ_COMMITTED, 0);
            TestResultSet test_result_set;
            std::string sql;
            sql = "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH NO_WAIT;"; // RCSI
            std::cout << sql << std::endl;
            if (!DBConnector::ExecWriteSql(1024, sql, test_result_set, session_id, test_process_file)) {
                return false;
            }
        } else {
            std::cout << "unknow isolation level" << std::endl;
            return false;
        }
        std::string err_info_stmt = DBConnector::SqlExecuteErr(session_id, 1024, "set isolation", "dbc", m_hDatabaseConnection, ret, test_process_file);
        if (!err_info_stmt.empty()) {
            return false;
        }
    } 
    else {
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

    // Snapshot mode for myrocks 
    // comment for normal mode for 4 levels
    // if (db_type == "myrocks") {
    //     TestResultSet test_result_set;
    //     std::string sql;
    //     // sql = "START TRANSACTION WITH CONSISTENT ROCKSDB SNAPSHOT"; 
    //     sql = "START TRANSACTION WITH CONSISTENT SNAPSHOT"; 
    //     if (!DBConnector::ExecWriteSql(1024, sql, test_result_set, session_id, test_process_file)) {
    //         return false;
    //     }
    //     std::cout << sql << std::endl;
    // }

    return true;
}
