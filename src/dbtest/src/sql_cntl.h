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
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include "case_cntl.h"

//db connector
class DBConnector { 
private:
    std::vector<SQLHDBC> conn_pool_;
public:
    bool InitDBConnector(std::string& user, std::string& passwd, std::string& db_type, int conn_pool_size) {
        for (int i = 0; i < (int)conn_pool_size; i++) {
            SQLHENV m_hEnviroment;
            SQLHDBC m_hDatabaseConnection;
            SQLRETURN ret;
            // get env    
            ret = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &m_hEnviroment);
            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
               std::cerr << "get env failed" << std::endl;
	       return false;
            }

            SQLSetEnvAttr(m_hEnviroment, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER);
            // get conn
            ret = SQLAllocHandle(SQL_HANDLE_DBC, m_hEnviroment, &m_hDatabaseConnection);
            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                std::cerr << "get conn failed" << std::endl;
		return false;
            }
            // connect
            ret = SQLConnect(m_hDatabaseConnection,
                             (SQLCHAR*)db_type.c_str(), SQL_NTS, 
                             (SQLCHAR*)user.c_str(), SQL_NTS, 
                             (SQLCHAR*)passwd.c_str(), SQL_NTS);

	        std::string err_info_stmt = DBConnector::SqlExecuteErr(1, 1024, "connnected", "dbc", m_hDatabaseConnection, ret);
	        if (err_info_stmt != "") {
                return false;
            }
            /*
            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                std::cerr << "connected failed" << std::endl;
		        return false;
            }*/
            conn_pool_.push_back(m_hDatabaseConnection);
        }
        std::cout << "init db_connector success" << std::endl;
	return true;
    };

    bool SetAutoCommit(SQLHDBC m_hDatabaseConnection, int opt);
    bool SetTimeout(int conn_id, std::string timeout, const std::string& db_type);

    bool ExecReadSql2Int(int sql_id, const std::string& sql, TestResultSet& test_result_set, std::unordered_map<int, std::vector<std::string>>& cur_result_set, int conn_id, int param_num, std::string test_process_file="");
    bool ExecWriteSql(int sql_id, const std::string& sql, TestResultSet& test_result_set, int conn_id, std::string test_process_file="");

    void ErrInfoWithStmt(std::string handle_type, SQLHANDLE& handle, SQLCHAR ErrInfo[], SQLCHAR SQLState[]);
    std::vector<SQLHDBC> DBConnPool() {return conn_pool_;};
    std::string SqlExecuteErr(int session_id, int sql_id, const std::string& sql, std::string handle_type, SQLHANDLE& handle, SQLRETURN ret, std::string test_process_file="");

    bool SQLEndTnx(std::string opt, int session_id, int sql_id, TestResultSet& test_result_set, const std::string& db_type, std::string test_process_file="");
    bool SQLStartTxn(int session_id, int sql_id, std::string test_process_file="");
    bool SetIsolationLevel(SQLHDBC m_hDatabaseConnection, std::string opt, int conn_id, const std::string& db_type, std::string test_process_file="");

    void ReleaseConn() {
        for (int i = 0; i < (int)conn_pool_.size(); i++) {
            SQLFreeHandle(SQL_HANDLE_DBC, conn_pool_[i]);
        }
    };
    void ReleaseEnv();
    void Release() {
        ReleaseConn();        
    };
};
