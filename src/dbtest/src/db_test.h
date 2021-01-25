#include <vector>
#include <libconfig.h++>
#include <iostream>

// result set
class ReadResultSet {  
private:
    std::vector<std::pair<std::string, std::string>> rs_;
public:
    std::vector<std::pair<std::string, std::string>> get_rs() {return rs_;};
};

class WriteResultSet {
private:
    uint row_count_;
public:
    uint get_row_count() {return row_count_;};
};

class TxnResultSet {
private:
    bool is_commit_;
    bool is_timeout_;
    ReadResultSet& read_rows_;
    ReadResultSet& final_rows_;
public:
    bool is_commit() {return is_commit_;};
    bool is_timeout() {return is_timeout_;};
    ReadResultSet& get_read_rows();
    ReadResultSet& get_final_rows();
};

enum class ResultType {
    RollBack,
    Avaid,
    Exception
};

class TestResultSet {
private:
    ResultType& result_type_;
public:
    ResultType& get_result() {return result_type_;};
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

// The sql with txn_id in test case
class TxnSQL {
private:
    std::string sql_;
    std::string txn_id_;
    TestCaseType test_case_type_;
    bool is_read_sql_;
    ReadResultSet* read_rs_;
public:
    TxnSQL(std::string sql, std::string txn_id, TestCaseType test_case_id) {
        sql_ = sql;
        txn_id_ = txn_id;
        test_case_type_ = test_case_id;
    };
    std::string get_sql() {return sql_;};
    std::string get_txn_id() {return txn_id_;};
    TestCaseType get_test_case_type() {return test_case_type_;};
    bool is_read_sql() {return is_read_sql_;};
    ReadResultSet* get_read_result_set() {return read_rs_;};
};
// TestSequence->exception test case, include a series of TxnSQL
class TestSequence {
private:
    std::vector<TxnSQL> txn_sql_list_;
    TestCaseType test_case_type_;
    TestResultSet* test_result_set_;
public:
    TestSequence(TestCaseType test_case_type) {
        test_case_type_ = test_case_type;
    };
    void add_txn_sql(TxnSQL txn_sql) {txn_sql_list_.push_back(txn_sql);};
    std::vector<TxnSQL> get_txn_sql_list() {return txn_sql_list_;};
    TestCaseType get_test_case_type() {return test_case_type_;};
    TestResultSet* get_test_result_set() {return test_result_set_;};
};

class TestCaseExpectedResult {
private:
    TestCaseType test_case_type_;
    // [[sql1_r1, sql1_r2, ...], [sql2_r1, sql2_r2, ...], ...]
    std::vector<std::vector<std::string>> sql_expected_result_list_;
public:
    TestCaseType get_test_case_type() {return test_case_type_;};
    std::vector<std::vector<std::string>> get_sql_expected_result_list() {
        return sql_expected_result_list_;
    };
};

//read and parse sql from file
class SQLReader { 
private:
    std::vector<TestSequence> test_sequence_list_;
    std::vector<TestCaseExpectedResult> test_result_list_;
public:
    void init_test_sequence_list(std::string& test_path);
    void init_test_result_list(std::string& result_path);

    void add_test_sequence(TestSequence test_sequence) {
        test_sequence_list_.push_back(test_sequence);
    };
    void add_test_result(TestCaseExpectedResult test_case_expected_result) {
        test_result_list_.push_back(test_case_expected_result);
    };

    TestSequence get_one_test_sequence_from_file(std::string& test_file);
    TestCaseExpectedResult get_one_test_result_from_file(std::string& result_path);

    TestCaseType get_test_case_type_by_test_file(std::string& test_file);

    std::pair<std::string, std::string> get_txn_id_and_sql(std::string line);

    std::vector<TestSequence> get_test_sequence_list() {return test_sequence_list_;};
    std::vector<TestCaseExpectedResult> get_test_result_list() {return test_result_list_;};
};

//class ConfigBase {
//private:
//    std::string cfg_file_;
//public:
//
//    ConfigBase(std::string cfg_file) {
//        cfg_file_ = cfg_file;
//    }
//
//    const libconfig::Setting& get_db_info(const std::string& db_type) {
//        libconfig::Config cfg;
//        try {
//            cfg.readFile(cfg_file_.c_str());
//        } catch (libconfig::FileIOException& fioex) {
//            std::cerr << "I/O error while reading file." << std::endl;
//        } catch (libconfig::ParseException& pex) {
//            std::cerr << "Parse error at " << pex.getFile() << ":" << pex.getLine()
//                          << " - " << pex.getError() << std::endl;
//        }
//        try {
//            const libconfig::Setting& root = cfg.getRoot();
//            return root[db_type.c_str()];
//        } catch (libconfig::SettingNotFoundException& nfex) {
//            std::cerr << "get db info failed, please config file" << std::endl;
//        }
//    };
//};
