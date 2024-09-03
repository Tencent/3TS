### Introduction

This document is for the description of three files, including code functionality, overall structure, relationships with other files, and their roles in the project.



## case_cntl.cc 

This file is for parsing and handling SQL statements and transaction IDs in test files, assisting in reading and parsing commands and data from input files, and outputting results.

### Overall Structure:

First, header files are included, followed by the definition of global objects: `outputter` and `result_handler`, which are used for outputting and result handling, respectively. Then, function declarations and definitions are provided, including functions in the `CaseReader` class for handling SQL statement execution and error information retrieval, functions in the `ResultHandler` class for comparing SQL query results, and functions in the `Outputter` class for writing outputs related to test results to specified files.

#### Function Declarations and Definitions

- **`CaseReader`** Class
  - `TxnIdAndSql`: Used to get the transaction ID and SQL statement of a specific line, parse the input line, and extract the execution order ID, transaction ID, and SQL statement.
  - `SqlIdAndResult`: Parses the input line and extracts the SQL ID and its expected result.
  - `Isolation`: Extracts the isolation level from a string line.
  - `InitTestSequenceAndTestResultSetList`: Initializes the test sequence and result set (casecntl) based on the provided test path and database type.
  - `TestSequenceAndTestResultSetFromFile`: Parses the provided test file to extract the test sequence and their corresponding expected results.

- **`ResultHandler`** Class
  - `IsSqlExpectedResult`: Used to compare the current SQL result with the expected SQL result.
  - `IsTestExpectedResult`: Used to compare the current test result with a set of expected test results.

- **`Outputter`** Class
  - `WriteResultTotal`: Writes the summary information of the test result set to a specified file.
  - `PrintAndWriteTxnSqlResult`: Compares the current SQL result with a set of expected results and outputs the comparison result to the console and file.
  - `WriteTestCaseTypeToFile`: Appends the given result type to a specified file, with the prefix "Test Result:" for explanation.

### Role in the Project

The overall structure and content of this file revolve around the parsing and execution of SQL statements, providing a series of utility functions and class methods to handle SQL-related operations and compare test results.



## sqltest.cc 

This file is for executing multi-threaded SQL transaction tests, primarily through executing multiple sets of SQL transactions in a multi-threaded environment and verifying their results.

### Overall Structure

First, header files and libraries are included, followed by the definition of a series of command-line parameters using the gflags library, such as database type, username, password, database name, connection pool size, transaction isolation level, test case directory, timeout, etc. A global mutex vector `mutex_txn` is defined to manage locks for different transactions. Then, functions are defined.

#### Function Declarations and Definitions

- **`try_lock_wait`**: Attempts to acquire the specified mutex within a given timeout, used for multi-threaded lock mechanisms.
- **`MultiThreadExecution`**: Executes a set of SQL transactions in a multi-threaded environment. This function uses the `DBConnector` class to execute SQL statements, and performs lock operations and error handling during transaction execution.
- **`JobExecutor::ExecTestSequence`**: Executes a series of database test transactions and writes the results to a specified file. This function implements multi-threaded transaction execution by calling the `MultiThreadExecution` function.

### Role in the Project

**Support for Multiple Database Types**: Supports transaction testing for multiple database types (such as MySQL, PostgreSQL, Oracle, etc.).

**Multi-threaded Transaction Execution**: Executes transactions through a multi-threaded mechanism, simulating transaction processing in a high-concurrency environment.

**Transaction Isolation Level Testing**: Supports different transaction isolation levels, verifying the database's behavior under different isolation levels.

**Result Verification and Output**: After executing transactions, writes the test results to a specified output file for subsequent analysis and verification.



## sql_cntl.cc 

This file is for executing SQL statements and handling database connections.

### Overall Structure

First, header files and libraries are defined, followed by the definition of utility functions for time retrieval, string replacement, type conversion, etc. Then, function declarations and definitions are provided, including functions in the `DBConnector` class for handling SQL statement execution, time retrieval, string replacement, and type conversion.

#### Function Declarations and Definitions

- **`DBConnector`** Class
  - `ErrInfoWithStmt`: Used to get error information for a specific handle.
  - `SqlExecuteErr`: Handles error information for SQL execution.
  - `ExecWriteSql`: Executes write-type SQL statements.
  - `ExecReadSql2Int`: Executes read-type SQL statements and handles results.

- **`get_current_time`**: Used to get the current time.

- **`replace`**: Performs string replacement.

- **`SQLCHARToStr`**: Converts `SQLCHAR` type to `std::string` type.

### Role in the Project

This file is for interacting with the database, executing SQL statements, and handling errors and logging during execution.



## Relationship Between Files

1. `sql_cntl.cc` and `sqltest.cc`:
   - `sqltest.cc` relies on the tools and helper functions in `sql_cntl.cc` to execute specific SQL operations. `sqltest.cc` uses the result handling and output functions defined in `sql_cntl.cc` to verify correctness and report results. `sql_cntl.cc` is responsible for the specific SQL execution logic, providing support for `sqltest.cc`.

2. `sql_cntl.cc` and `case_cntl.cc`:
   - The functions and classes in `case_cntl.cc` read test cases, parse expected results, and compare actual results with these expectations. `sqltest.cc` handles the logic of executing SQL statements and calls these comparison functions.

3. `sql_test.cc` and `case_cntl.cc`:
   - `case_cntl.cc` is responsible for reading and parsing test case files and expected results. This parsed data is passed to `sqltest.cc`, which then performs multi-threaded transaction execution tests.
