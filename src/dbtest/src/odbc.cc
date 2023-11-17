#include <iostream>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

/**
 * Retrieves error information associated with a SQL handle.
 * 
 * @param handle_type The type of SQL handle ('stmt' for statement handle or 'dbc' for database connection handle).
 * @param handle The SQL handle to retrieve error information from.
 * @param ErrInfo A buffer to store the error message.
 * @param SQLState A buffer to store the SQL state code.
 */
void ErrInfoWithStmt(std::string handle_type, SQLHANDLE& handle, SQLCHAR ErrInfo[], SQLCHAR SQLState[]) {
    SQLINTEGER NativeErrorPtr = 0;
    SQLSMALLINT TextLengthPtr = 0;
    if ("stmt" == handle_type) {
        SQLGetDiagRec(SQL_HANDLE_STMT, handle, 1, SQLState, &NativeErrorPtr, ErrInfo, 256, &TextLengthPtr);
    }
    if ("dbc" == handle_type) {
        SQLGetDiagRec(SQL_HANDLE_DBC, handle, 1, SQLState, &NativeErrorPtr, ErrInfo, 256, &TextLengthPtr);
    }
}

/**
 * Executes an SQL query and retrieves results.
 *
 * This function initializes the ODBC environment, establishes a database connection,
 * allocates a statement handle, and executes an SQL query. It can be used to interact
 * with a database and retrieve data.
 *
 * @note To execute specific SQL queries, you can uncomment the relevant lines
 *       within the function.
 */
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
                            ,(SQLCHAR*)"mongodb", SQL_NTS
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
    //ret = SQLExecDirect(m_hStatement, (SQLCHAR*)"BEGIN TRANSACTION;", SQL_NTS);
    //std::cout << "BEGIN TRANSACTION;" << std::endl;
    //ret = SQLExecDirect(m_hStatement, (SQLCHAR*)"update t1 set v=1 where k=1;", SQL_NTS);
    //std::cout << "update t1 set v=1 where k=1;" << std::endl;
    //ret = SQLExecDirect(m_hStatement, (SQLCHAR*)"COMMIT TRANSACTION;", SQL_NTS);
    //std::cout << "COMMIT TRANSACTION;" << std::endl;
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        std::cout << "success" << std::endl;
    } else if (ret == SQL_ERROR) {
        SQLCHAR ErrInfo[256];
        SQLCHAR SQLState[256];
        ErrInfoWithStmt("stmt", m_hStatement, ErrInfo, SQLState);
        std::cerr << "execute sql: 'SELECT * from t1 WHERE k = 1;' failed, reason: " << ErrInfo << " errcode: " << SQLState << std::endl;
    }
    SQLCHAR k[20]={0}, v[20]={0};
    SQLBindCol(m_hStatement, 1, SQL_C_CHAR, (void*)k, sizeof(k), &length);
    SQLBindCol(m_hStatement, 2, SQL_C_CHAR, (void*)v, sizeof(v), &length);
    // get next row
    //while(SQL_NO_DATA != SQLFetch(m_hStatement)) {
    //    std::cout << "k:" << k << "v:" << v << std::endl;
    //}
    // release
    SQLFreeHandle(SQL_HANDLE_STMT, m_hStatement);
    SQLFreeHandle(SQL_HANDLE_DBC, m_hDatabaseConnection);
    SQLFreeHandle(SQL_HANDLE_ENV, m_hEnviroment);
}

int main() {
    exec_sql();
    return 0;
}
