## Introduction
In this module, we provide a framework for dynamically testing different databases. You can use Python scripts (random_do_list.py, mda_detect.py, mda_generate.py) to generate input test files yourself. Dynamic testing is specifically implemented in (3TS/src/dbtest/src) under case_cntl_v2.cc, sqltest_v2.cc, sql_cntl_v2.cc.

## Test File Generation
Run random_do_list.py
```shell
python random_do_list.py data_num target_num
```
The script will generate a do_test_list.txt file with contents like
```
PW0-PW1
```
indicating the type of operation.
- Write (W)
- Read (R)
- Predicate (P)
- Insert (I)
- Delete (D)
- Abort (A)
- Commit (C)
Next, run mda_generate.py
```shell
python mda_generate.py db_type test_type
```
db_type includes tdsql, crdb, etc., and test_type includes single, dist.

Run mad_detect.py for error detection
```shell
python mad_detect.py
```

## Test File Format
Static testing first declares ParamNum, which means how many columns the result set has.

Then input testSequence in the format: execution order ID-transaction ID-SQL statement

Next, input the isolation level, such as serializable{...}

Inside the brackets are pairs like "9-0,1 1,1", where 9 is the SQL ID and "(0,1) (1,1)" is the result.

Comments can be added after # in the file.

Dynamic testing declares Parameters and testSequence but does not have an expected result sequence.

Parameters format is similar to the following:
```
#Parameters: #column=2 #txn=2 #operations=2 #variable=2
```

## case_cntl_v2.cc 
Implements the test case control function, including reading database configuration, SQL statements, and expected results from the test file, verifying the difference between actual results and expected results, and outputting comparison results to the specified folder.

The code first reads the specified test file using the TestSequenceAndTestResultSetFromFile function. This function parses the content of the test file and extracts the test sequence and result set information. During the process of reading the test file, each line is parsed. Depending on the content of the line, different operations are performed:

If the line contains "Parameters", the number of parameters is extracted and set to the test sequence object.

If the line starts with "#", it indicates a comment and is skipped.

In other cases, it is assumed that the line contains transaction ID and SQL statement. The TxnIdAndSql function is used to parse this line, extract the transaction ID, SQL ID, and SQL statement, then create a TxnSql object and add it to the test sequence.
Initialize the test sequence and result set list: The InitTestSequenceAndTestResultSetList function reads the "do_test_list.txt" file, which contains the list of test cases to be executed. For each test case in the list, the function reads the corresponding test file and adds the parsed test sequence and result set to the internal storage structure.

Finally, the IsSqlExpectedResult function is called to compare the current result of the SQL query with the expected result. This comparison is done item by item. If the current result and the expected result differ in size or any item does not match, the function returns false, indicating that the result does not meet expectations. Otherwise, it returns true, indicating that the test passed.

Data structures:

Outputter: Provides functions to write test cases and result data to a file

ResultHandler: Handles and verifies expected test results

Functions:

Same as v1 static testing:

CaseReader::TxnIdAndSql: Parses a given line to extract the execution order ID, transaction ID, and SQL statement.

CaseReader::SqlIdAndResult: Parses a given line to extract the SQL ID and its expected result.

CaseReader::Isolation: Parses the isolation level.

CaseReader::InitTestSequenceAndTestResultSetList: Same as v1, initializes the TestSequence and TestResultSet lists based on the provided test path and database type.

ResultHandler::IsSqlExpectedResult, ResultHandler::IsTestExpectedResult: Compares the test result with the expected result.

Outputter::WriteResultTotal, Outputter::WriteTestCaseTypeToFile, Outputter::WriteResultType: Writes test results to a file.

Different:

CaseReader::TestSequenceAndTestResultSetFromFile: Reads the file and outputs the testSequence. Unlike v1, it does not parse the isolation level and expected results.

Outputter::PrintAndWriteTxnSqlResult: Outputs transaction results. Unlike v1, it only outputs the current result without comparing it with the expected result set.

## sql_cntl_v2.cc
Code for SQL control or operations. Implements interface functions with ODBC databases, including setting database isolation levels, starting transactions, performing CRUD operations, handling SQL return values and retrieving error messages, ending transactions or rolling back transactions.

The code first uses DBConnector to get the database connection for the corresponding session ID from the connection pool, then allocates a new statement handle. If the handle allocation fails, the function calls DBConnector::SqlExecuteErr to get error information and outputs an error message. If sql_id is 1024, it skips the print output function. Then, it releases the statement handle and returns false, indicating that the SQL execution failed. Finally, it returns a boolean value indicating whether the SQL statement execution was successful.

Data structures:

DBConnector: Manages all database connection-related functions and holds the ODBC database connection handle (Database Connection Handle).

Same as v1 static testing:

DBConnector::ErrInfoWithStmt: Extracts error information from the ODBC handle.

DBConnector::ExecReadSql2Int: Executes SQL read statements and handles errors.

DBConnector::SQLStartTxn: Starts a transaction and handles errors.

DBConnector::SetAutoCommit: Sets the database connection to auto-commit mode.

DBConnector::SetTimeout: Sets the connection timeout.

DBConnector::SetIsolationLevel: Sets the isolation level.

Different:

DBConnector::SqlExecuteErr: Handles and records execution errors. Unlike v1, it outputs the session id and returns empty if it fails for some unknown reason without providing information.

DBConnector::ExecWriteSql: Executes SQL write statements and handles errors. Unlike v1, it uses the SQLRowCount function to get the number of affected rows. If no rows are affected, it outputs an error.

DBConnector::SQLEndTnx: Ends the transaction by committing or rolling back. Unlike v1, it outputs the execution time information after the SQL statement is successfully executed.

## sqltest_v2.cc
Code for SQL testing, containing the main function of the project.

First, the gflags library is used to implement command-line argument parsing.

Initialize the test environment:
Set configuration parameters such as database connection pool size and transaction timeout.

Initialize the database connector (DBConnector) to connect to the specified database.

Prepare SQL statements and transaction sequences for testing, which may include read (SELECT), write (INSERT, UPDATE, DELETE), transaction start (BEGIN), and end (COMMIT, ROLLBACK) operations. Use multi-threaded programming, where each thread is responsible for executing a group of database statements, and the program code is executed in a specified order by setting different sleep times for the sub-threads.

During execution, record the test process and results in the specified log file for subsequent analysis and audit. After completing the execution of all SQL statements, release related resources, such as closing database connections and releasing mutex locks. Summarize test results, including the number of successfully executed statements, failed statements, and their reasons.

Unlike v1, it does not support Cassandra and Yugabyte, nor their isolation level definitions.

Functions:

MultiThreadExecution: Executes SQL queries in multiple threads and returns results. Unlike v1, it does not support Yugabyte and MyRocks.

