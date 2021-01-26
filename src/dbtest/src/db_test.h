#include <vector>
#include <libconfig.h++>
#include <iostream>
#include <jdbc/mysql_connection.h>
#include <jdbc/mysql_driver.h>
#include <jdbc/cppconn/driver.h>
#include <jdbc/cppconn/statement.h>
#include <jdbc/cppconn/prepared_statement.h>
#include <unordered_map>

enum class ResultType {
    RollBack,
    Avaid,
    Exception
};

enum class TestCaseType {
    Unsupported,
    DirtyRead,
    UnrepeatableRead,
    PhantomRead,
    DirtyWrite,
    LostUpdate,
    MiddleRead,
    BlackWhite,
    IntersectingData,
    OverdraftProtection,
    PrimaryColors,
    ReadWritePartialOrder,
    ReadPartialOrder,
    Serrasoid,
    Step,
    DoubleWriteSkew,
    PredicateSawtooth,
    ReadOnlyTransactionAnomaly,
    CausalityViolationAnomaly,
    CrossPhenomenon,
    CrossPhantom, 
    UnnamedAnomaly,
    HalfPredicatesReadPartialOrder,
    TotalPredicatesReadPartialOrderSame,
    TotalPredicatesReadPartialOrderDifferent,
    HalfPredicatesWritePartialOrder,
    TotalPredicatesWritePartialOrderSame,
    TotalPredicatesWritePartialOrderDifferent
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
    TestCaseType test_case_type_;
    std::vector<TxnResultSet> txn_result_set_list_;
    // key:sql_id value:[[read set1], [read set2], ...]
    std::unordered_map<int, std::vector<ReadResultSet>> read_sql_result_set_;
    ResultType result_type_;
public:
    TestCaseType get_test_case_type() {return test_case_type_;};
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
    TestCaseType test_case_type_;
public:
    TxnSQL(std::string sql, std::string txn_id, TestCaseType test_case_id) {
        sql_ = sql;
        txn_id_ = txn_id;
        test_case_type_ = test_case_id;
    };
    std::string get_sql() {return sql_;};
    std::string get_txn_id() {return txn_id_;};
    TestCaseType get_test_case_type() {return test_case_type_;};
};
// TestSequence->exception test case, include a series of TxnSQL
class TestSequence {
private:
    TestCaseType test_case_type_;
    std::vector<TxnSQL> txn_sql_list_;
public:
    TestSequence(TestCaseType test_case_type) {
        test_case_type_ = test_case_type;
    };
    void add_txn_sql(TxnSQL txn_sql) {txn_sql_list_.push_back(txn_sql);};
    std::vector<TxnSQL> get_txn_sql_list() {return txn_sql_list_;};
    TestCaseType get_test_case_type() {return test_case_type_;};
};

//read and parse sql from file
class SQLReader { 
private:
    std::vector<TestSequence> test_sequence_list_;
    std::vector<TestResultSet> test_result_list_;
public:
    void init_test_sequence_list(std::string& test_path);
    void init_test_result_list(std::string& result_path);

    void add_test_sequence(TestSequence test_sequence) {
        test_sequence_list_.push_back(test_sequence);
    };
    void add_test_result(TestResultSet test_case_expected_result) {
        test_result_list_.push_back(test_case_expected_result);
    };

    TestSequence get_one_test_sequence_from_file(std::string& test_file);
    TestResultSet get_one_test_result_from_file(std::string& result_path);

    TestCaseType get_test_case_type_by_test_file(std::string& test_file);

    std::pair<std::string, std::string> get_txn_id_and_sql(std::string line);

    std::vector<TestSequence> get_test_sequence_list() {return test_sequence_list_;};
    std::vector<TestResultSet> get_test_result_list() {return test_result_list_;};
};

//db connector
class DBConnectorBase {
public:
    virtual void set_auto_commit() = 0;
    virtual void begin() = 0;
    virtual void rollback() = 0;
    virtual void commit() = 0;
    virtual void execute_sql(const std::string& sql, TestResultSet& test_rs, int conn_id) = 0;
    virtual void close_stmt() = 0;
    virtual void close_conn() = 0;
    virtual ~DBConnectorBase() {};
};

enum class DBType {
    MYSQL,
    ORACLE
};

class MYSQLConnector : public DBConnectorBase { 
private:
    //std::vector<std::unique_str<sql::Connection>> conn_pool_;
    std::vector<sql::Connection*> conn_pool_;
    std::vector<sql::Statement*> stmt_pool_;
public:
    MYSQLConnector(const std::string& host, const std::string port, const std::string& user, const std::string& passwd, const std::string& db_name, int conn_pool_size) {
        sql::mysql::MySQL_Driver *driver = NULL;
        driver = sql::mysql::get_mysql_driver_instance();
        for (int i = 0; i < conn_pool_size; i++) {
            //std::unique_ptr<sql::Connection> conn = NULL;
            sql::Connection* conn = NULL;
            sql::Statement* stmt = NULL;
            //std::unique_ptr<sql::Statement*> stmt = NULL;
            if (driver == NULL) {
                std::cout << "driver is null" << std::endl;
            }
            std::string url_base = "tcp://";
            std::string url = url_base + host + ":" + port;
            conn = driver->connect(url, user, passwd);
            if (conn == NULL) {
                std::cout << "conn is null" << std::endl;
            }
            stmt = conn->createStatement();
            conn_pool_.push_back(conn);
            stmt_pool_.push_back(stmt);
            std::cout << "connect suceess" << std::endl;
        }
        stmt_pool_[0]->executeQuery("create database if not exists" + db_name);
    };
    virtual void set_auto_commit() override;
    virtual void begin() override;
    virtual void rollback() override;
    virtual void commit() override;
    virtual void execute_sql(const std::string& sql, TestResultSet& test_rs, int conn_id) override;
    virtual void close_stmt() override;
    virtual void close_conn() override;
    virtual ~MYSQLConnector() {
        std::cout << "deconstruct" << std::endl;
    };
};
