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
    ret = SQLConnect( m_hDatabaseConnection
                            ,(SQLCHAR*)"mysql", SQL_NTS
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
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)"INSERT INTO t1 VALUES (1, 1);", SQL_NTS);
    ret = SQLExecDirect(m_hStatement, (SQLCHAR*)"SELECT * from t1 WHERE k = 1;", SQL_NTS);
    SQLCHAR k[20]={0}, v[20]={0};
    SQLBindCol(m_hStatement, 1, SQL_C_CHAR, (void*)k, sizeof(k), &length);
    SQLBindCol(m_hStatement, 2, SQL_C_CHAR, (void*)v, sizeof(v), &length);
    // get next row
    while(SQL_NO_DATA != SQLFetch(m_hStatement)) {
        std::cout << "k:" << k << "v:" << v << std::endl;
    }
    // release
    SQLFreeHandle(SQL_HANDLE_STMT, m_hStatement);
    SQLFreeHandle(SQL_HANDLE_DBC, m_hDatabaseConnection);
    SQLFreeHandle(SQL_HANDLE_ENV, m_hEnviroment);
}

int main() {
    exec_sql();
    return 0;
}
