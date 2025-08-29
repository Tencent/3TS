## Static Testing Module

### Overview

The static testing module of `3ts_dbtest` consists of three core source files, organized in a layered architecture:

- `case_cntl.cc` handles the parsing of input test files, result comparison, and output generation.

- `sql_cntl.cc` builds on `case_cntl.cc`, providing SQL execution functionality through the DBConnector class, which interacts with databases via ODBC.

- `sqltest.cc` sits on top of both, using `case_cntl.cc` for test data and `sql_cntl.cc` for executing transactions. It serves as the test driver, coordinating multi-threaded execution of transactional SQL sequences.

The final executable binary is named `3ts_dbtest`

---

### `case_cntl.cc`

#### Module Role

- Parsing structured test case files
- Storing test metadata and expected results
- Comparing actual SQL output with expected results
- Logging outcomes to console and file

This module provides the foundational data parsing and result validation logic used by all static tests.

#### Major Classes and Functions

##### `CaseReader` Class

- Converts test files into structured `TestSequence` and `TestResultSet` objects.
- Supports metadata parsing: execution order ID, transaction ID, SQL statement, isolation level, SQL statements, and expected result sets.
- **Core Methods**:
  - `InitTestSequenceAndTestResultSetList`: Initializes the lists of TestSequence and TestResultSet List based on a provided test path and database type.
  - `TestSequenceAndTestResultSetFromFile`: Parses the provided test file to extract both test sequences and their corresponding expected results.

##### `ResultHandler` Class

- Compares actual SQL outputs to one or more expected result sets.
- **Core Methods**:
  - `IsSqlExpectedResult`: Compares the current SQL result with the expected SQL result
  - `IsTestExpectedResult`: Compares the current test result with a set of expected test results.

##### `Outputter` Class

- Handles formatted logging of:
  - Test metadata (type, isolation level, result)
  - SQL-level execution logs
  - Success/failure status
- Supports both console output and file-based logging.
- **Core Methods**:
  - `WriteResultTotal`: Writes the summarized test results to a specified file.
  - `PrintAndWriteTxnSqlResult`: Compares the current SQL result to a set of expected results, and outputs the comparison to both the console and a file.
  - `WriteTestCaseTypeToFile`: Appends the given test case type to a specified file.
  - `WriteResultType`: Appends the given result type to a specified file. 

---

### `sql_cntl.cc`

#### Module Role
- Connection pooling
- SQL command execution (read/write)
- Transaction management
- Error handling
- Isolation-level configuration

It provides the execution backend for test sequences parsed by `case_cntl.cc`.

#### Major Classes and Functions
##### `DBConnector` Class
- Manages low-level ODBC-based database connections and SQL command execution.
- Maintains a connection pool (conn_pool_) for concurrent SQL execution.
- Supports both read and write SQL operations, along with transaction control and error handling.
- **Core Methods**:
  - `InitDBConnector`: Initializes the connection pool by establishing multiple ODBC connections.
  - `ExecReadSql2Int`: Executes an SQL statement for reading and stores the result as an integer.
  - `ExecWriteSql`: Executes an SQL statement for writing.
  - `SQLStartTxn`: Begins a transaction.
  - `SQLEndTnx`: Ends the transaction.
  - `SetIsolationLevel`: Sets the isolation level of the database connection.
  - `ReleaseConn`: Releasees all database connections in the connection pool.
  - `SqlExecuteErr`: Gets error information using the SQL statement handle.

---

### `sqltest.cc`

#### Module Role
- Command-line argument parsing via `gflags`
- Concurrency via POSIX threads (`pthread`)
- Scheduling and synchronization of transaction execution
- Final result aggregation

#### Major Classes and Functions

##### `JobExecutor` Class
- Manages the entire lifecycle of static testing, including:
  - Initialization → Parallel SQL Execution → Result Comparison → Logging
- Handles multi-threaded transaction execution
- **Core Methods**:
  - `ExecTestSequence`: Handles the full execution and logging of a database test sequence.
---

### Workflow Summary
1. Parse test files using `CaseReader`
2. Construct `TestSequence` and `TestResultSet` objects
3. Initialize ODBC connections via `DBConnector`
4. Execute initialization SQLs sequentially
5. Launch concurrent transactional SQL execution via `JobExecutor`
6. Validate actual vs expected results with `ResultHandler`
7. Write logs and summary using `Outputter`


