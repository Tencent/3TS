#include <iostream>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
void exec_sql() {
    SQLHENV m_hEnviroment;
    SQLHDBC m_hDatabaseConnection;
    SQLHSTMT m_hStatement;
    SQLRETURN ret;
    SQLLEN length;
    // get env    
    ret = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &m_hEnviroment);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
       std::cout << "get env failed" << std::endl;
    }

    SQLSetEnvAttr(m_hEnviroment, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER);
    // get conn
    ret = SQLAllocHandle(SQL_HANDLE_DBC, m_hEnviroment, &m_hDatabaseConnection);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cout << "get conn failed" << std::endl;
    }
    // connect
    //SQLCHAR* sqlwcaDsnName = "MySQL-test";
    //SQLCHAR* sqlwcaUserName = "test123";
    //SQLCHAR* sqlwcaPassWord = "Ly.123456";

    ret = SQLConnect( m_hDatabaseConnection
                            ,(SQLCHAR*)"MySQL-test", SQL_NTS
                            ,(SQLCHAR*)"test123", SQL_NTS
                            ,(SQLCHAR*)"Ly.123456", SQL_NTS);

    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cout << "connected failed" << std::endl;
    }
    // get stmt
    ret = SQLAllocHandle(SQL_HANDLE_STMT, m_hDatabaseConnection, &m_hStatement);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cout << "get stmt failed" << std::endl;
    }
    // execute sql
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)"SELECT * FROM sbtest1", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cout << "get stmt failed" << std::endl;
    }
    // get row
    ret = SQLFetch(m_hStatement);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::cout << "get row failed" << std::endl;
    }
 
    SQLINTEGER sqlnID;
    // parse result
    SQLGetData(m_hStatement, 1, SQL_C_LONG, &sqlnID, 0, &length);

    std::cout << sqlnID << std::endl;
    SQLFreeHandle(SQL_HANDLE_STMT, m_hStatement);
    SQLFreeHandle(SQL_HANDLE_DBC, m_hDatabaseConnection);
    SQLFreeHandle(SQL_HANDLE_ENV, m_hEnviroment);
    
}

int main() {
    exec_sql();
    return 0;
}
