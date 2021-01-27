#include <iostream>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <vector>
#include <unordered_map>

enum class ResultType {
    RollBack,
    Avaid,
    Exception
};

// result set
class ReadResultSet {  
private:
    std::vector<std::vector<std::pair<std::string, std::string>>> read_rs_;
public:
    std::vector<std::vector<std::pair<std::string, std::string>>> get_rs() {return read_rs_;};
};

class TxnResultSet {
private:
    bool is_commit_;
    bool is_timeout_;
public:
    bool is_commit() {return is_commit_;};
    bool is_timeout() {return is_timeout_;};
    void update_commit_status(bool commit) {is_commit_ = commit;};
    void update_timeout_status(bool timeout) {is_timeout_ = timeout;};
};

class TestResultSet {
private:
    std::string test_case_type_;
    std::vector<TxnResultSet> txn_result_set_list_;
    // key:sql_id value:[[read set1], [read set2], ...]
    std::unordered_map<int, std::vector<ReadResultSet>> read_sql_result_set_;
    ResultType result_type_;
public:
    std::string get_test_case_type() {return test_case_type_;};
    std::vector<TxnResultSet> get_txn_result_set_list() {return txn_result_set_list_;};
    std::unordered_map<int, std::vector<ReadResultSet>> get_read_sql_result_set() {return read_sql_result_set_;};
    bool is_expected_result();
    ResultType get_result_type() {return result_type_;};
    void set_result_type(ResultType result_type) {result_type_ = result_type;};
};

// The sql with txn_id in test case
class TxnSQL {
private:
    std::string sql_;
    std::string txn_id_;
    std::string test_case_type_;
public:
    TxnSQL(std::string sql, std::string txn_id, std::string test_case_id) {
        sql_ = sql;
        txn_id_ = txn_id;
        test_case_type_ = test_case_id;
    };
    std::string get_sql() {return sql_;};
    std::string get_txn_id() {return txn_id_;};
    std::string get_test_case_type() {return test_case_type_;};
};
// TestSequence->exception test case, include a series of TxnSQL
class TestSequence {
private:
    std::string test_case_type_;
    std::vector<TxnSQL> txn_sql_list_;
public:
    TestSequence(std::string test_case_type) {
        test_case_type_ = test_case_type;
    };
    void add_txn_sql(TxnSQL txn_sql) {txn_sql_list_.push_back(txn_sql);};
    std::vector<TxnSQL> get_txn_sql_list() {return txn_sql_list_;};
    std::string get_test_case_type() {return test_case_type_;};
};

//read and parse sql from file
class SQLReader { 
private:
    std::vector<TestSequence> test_sequence_list_;
    std::vector<TestResultSet> test_result_list_;
public:
    bool init_test_sequence_list(std::string& test_path);
    void init_test_result_list(std::string& result_path);

    void add_test_sequence(TestSequence test_sequence) {
        test_sequence_list_.push_back(test_sequence);
    };
    void add_test_result(TestResultSet test_case_expected_result) {
        test_result_list_.push_back(test_case_expected_result);
    };

    TestSequence get_one_test_sequence_from_file(std::string& test_file);
    TestResultSet get_one_test_result_from_file(std::string& result_path);

    std::pair<std::string, std::string> get_txn_id_and_sql(std::string line);

    std::vector<TestSequence> get_test_sequence_list() {return test_sequence_list_;};
    std::vector<TestResultSet> get_test_result_list() {return test_result_list_;};
};

//db connector
class DBConnector { 
private:
    std::vector<SQLHDBC> conn_pool_;
    std::vector<SQLHSTMT> stmt_pool_;
public:
    bool InitDBConnector(std::string& user, std::string& passwd, std::string& db_type, int conn_pool_size) {
        for (int i = 0; i < (int)conn_pool_size; i++) {
            SQLHENV m_hEnviroment;
            SQLHDBC m_hDatabaseConnection;
            SQLHSTMT m_hStatement;
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
            // get stmt
            ret = SQLAllocHandle(SQL_HANDLE_STMT, m_hDatabaseConnection, &m_hStatement);
            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                std::cerr << "get stmt failed" << std::endl;
		return false;
            }
            stmt_pool_.push_back(m_hStatement);
        }
        std::cerr << "init db_connector success" << std::endl;
	return true;
    };
    void set_auto_commit();
    void begin();
    void rollback();
    void commit();
    bool execute_sql(const std::string& sql, TestResultSet& test_rs, int conn_id);
    void release_stmt() {
        for (int i = 0; i < (int)stmt_pool_.size(); i++) {
            SQLFreeHandle(SQL_HANDLE_STMT, stmt_pool_[i]);
        }    
    };
    void release_conn() {
        for (int i = 0; i < (int)conn_pool_.size(); i++) {
            SQLFreeHandle(SQL_HANDLE_DBC, conn_pool_[i]);
        }
    };
    void release_env();
    void release() {
        release_stmt();
        release_conn();        
    };
};
