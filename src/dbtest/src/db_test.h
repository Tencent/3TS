#include <iostream>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <vector>
#include <unordered_map>
#include <iterator>
/*
enum class ResultType {
    RollBack,
    Avaid,
    Exception
};*/

// result set
class SqlResultSet {  
private:
    std::vector<std::string> sql_rs_;
public:
    SqlResultSet(std::vector<std::string> sql_rs) {sql_rs_ = sql_rs;};
    std::vector<std::string> AllSqlResultSet() {return sql_rs_;};
};

class TxnResultSet {
private:
    bool is_commit_;
    bool is_timeout_;
public:
    bool IsCommit() {return is_commit_;};
    bool IsTimeout() {return is_timeout_;};
    void UpdateCommitStatus(bool commit) {is_commit_ = commit;};
    void UpdateCimeoutStatus(bool timeout) {is_timeout_ = timeout;};
};

class TestResultSet {
private:
    std::string test_case_type_;
    std::vector<TxnResultSet> txn_result_set_list_;
    // key:1 value:[["0 0", "1 1", "2 2"], []]
    std::unordered_map<int, std::vector<std::vector<std::string>>> sql_result_set_map_;
    std::string result_type_;
    std::string isolation_;
public:
    TestResultSet() {};
    TestResultSet(std::string test_case_type) {test_case_type_ = test_case_type;};
    std::string TestCaseType() {return test_case_type_;};
    std::vector<TxnResultSet> TxnResultSetList() {return txn_result_set_list_;};
    std::unordered_map<int, std::vector<std::vector<std::string>>> SqlResultSetMap() {return sql_result_set_map_;};
    bool IsExpectedResult();
    std::string ResultType() {return result_type_;};
    void SetResultType(std::string result_type) {result_type_ = result_type;};
    void SetIsolation(std::string isolation) {isolation_ = isolation;};
    std::string Isolation() {return isolation_;};
    void AddSqlResultSet(int sql_id, std::vector<std::string> sql_result_set) {
        std::vector<std::vector<std::string>> multi_result_set;
        if (sql_result_set_map_.count(sql_id) == 1) {
            multi_result_set = sql_result_set_map_[sql_id];
            multi_result_set.push_back(sql_result_set);
        } else {
            multi_result_set.push_back(sql_result_set);
            sql_result_set_map_[sql_id] = multi_result_set;
        }
    };
};

// The sql with txn_id in test case
class TxnSql {
private:
    int sql_id_;
    std::string sql_;
    int txn_id_;
    std::string test_case_type_;
public:
    TxnSql(int sql_id, int txn_id, std::string sql, std::string test_case_type) {
        sql_id_ = sql_id;
        sql_ = sql;
        txn_id_ = txn_id;
        test_case_type_ = test_case_type;
    };
    int SqlId() {return sql_id_;};
    std::string Sql() {return sql_;};
    int TxnId() {return txn_id_;};
    std::string TestCaseType() {return test_case_type_;};
};
// TestSequence->exception test case, include a series of TxnSql
class TestSequence {
private:
    // such as mysql_dirty-read
    std::string test_case_type_;
    std::vector<TxnSql> txn_sql_list_;
    int table_type_;
public:
    TestSequence() {};
    TestSequence(std::string test_case_type) {
        test_case_type_ = test_case_type;
    };
    void AddTxnSql(TxnSql txn_sql) {txn_sql_list_.push_back(txn_sql);};
    std::vector<TxnSql> TxnSqlList() {return txn_sql_list_;};
    std::string TestCaseType() {return test_case_type_;};
    void SetTableType(int table_type) {table_type_ = table_type;};
    int TableType() {return table_type_;};
};

//read and parse sql from file
class SQLReader { 
private:
    std::vector<TestSequence> test_sequence_list_;
    std::vector<TestResultSet> test_result_set_list_;
public:
    bool InitTestSequenceAndTestResultSetList(std::string& test_path);

    void AddTestSequence(TestSequence test_sequence) {
        test_sequence_list_.push_back(test_sequence);
    };
    void AddTestResultSet(TestResultSet test_result_set) {
        test_result_set_list_.push_back(test_result_set);
    };

    std::pair<TestSequence, TestResultSet> TestSequenceAndTestResultSetFromFile(std::string& test_file);

    std::vector<std::string> TxnIdAndSql(std::string line);
    std::pair<int, std::string> SqlIdAndResult(std::string line);
    std::string Isolation(std::string line);

    std::vector<TestSequence> TestSequenceList() {return test_sequence_list_;};
    std::vector<TestResultSet> TestResultSetList() {return test_result_set_list_;};
};

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

            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                std::cerr << "connected failed" << std::endl;
		return false;
            }
            conn_pool_.push_back(m_hDatabaseConnection);
        }
        std::cerr << "init db_connector success" << std::endl;
	return true;
    };
    bool SetAutoCommit(SQLHDBC m_hDatabaseConnection, int opt);
    void Begin();
    void Rollback();
    void Commit();
    bool ExecReadSql2Int(int sql_id, const std::string& sql, TestResultSet& test_result_set, int conn_id);
    bool ExecWriteSql(int sql_id, const std::string& sql, TestResultSet& test_result_set, int conn_id);
    void ErrInfoWithStmt(std::string handle_type, SQLHANDLE& handle, SQLCHAR ErrInfo[]);
    std::vector<SQLHDBC> DBConnPool() {return conn_pool_;};
    bool IsSqlExecuteErr(const std::string& sql, std::string handle_type, SQLHANDLE& handle, SQLRETURN ret);
    std::string IsTxnRollback(SQLHDBC m_hDatabaseConnection, SQLRETURN ret);
    void ReleaseConn() {
        for (int i = 0; i < (int)conn_pool_.size(); i++) {
            SQLFreeHandle(SQL_HANDLE_DBC, conn_pool_[i]);
        }
    };
    bool SQLEndTnx(std::string opt, int conn_id, TestResultSet& test_result_set);
    bool SetIsolationLevel(SQLHDBC m_hDatabaseConnection, std::string opt);
    void ReleaseEnv();
    void Release() {
        ReleaseConn();        
    };
};

class JobExecutor {
public:
    bool ExecAllTestSequence(std::vector<TestSequence> test_sequence_list);
    bool ExecTestSequence(TestSequence& test_sequence, TestResultSet& test_result_set, DBConnector db_connector);
    void handle_result(TestResultSet& rs);
};

class Outputter {
public:
    void WriteResultSet();
};
