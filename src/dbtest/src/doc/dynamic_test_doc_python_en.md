Sure! Here's the translation of your document into English:

---

## `random_do_list.py`

### File Overview

`random_do_list.py` randomly generates a series of database operations based on a predefined set of operations while ensuring that the generated operation list contains no illegal patterns. This script generates the `do_test_list.txt` file, which contains different test patterns.

`op_set = ["P", "R", "W", "I"]` # operations involved in the case

The `op_set` defines four types of database transaction operators:

* **P (Predicate)**: Predicate operation used to describe conditions in queries.
* **R (Read)**: Read operation used to retrieve data from the database.
* **W (Write)**: Write operation used to insert or update data in the database.
* **I (Insert)**: Insert operation used to insert new data into the database.

In `mda_generate.py`, there is a function called `execute_sql` that defines the SQL statements it supports, including seven types of SQL operations, as detailed in the function comment.

```python
"""
The supported SQL operations include:
- Write (W)
- Read (R)
- Predicate (P)
- Insert (I)
- Delete (D)
- Abort (A)
- Commit (C)
"""
```

### Variable Definitions

- `op_set`: Defines the set of operation types used in the script, including read ("R"), write ("W"), insert ("I"), and predicate ("P") operations.
- `illegal_op_set`: Defines the set of disallowed operation patterns because these patterns are either impossible or meaningless in actual database transactions.

### Function Definitions

- **dfs(data_count, total_num, target_num)**: Uses depth-first search to determine which variables need to be operated on twice to meet the total number of operations required in the test sequence.
- **dfs1(data_count, res, total_num)**: Recursive function that generates test cases based on the current `data_count`. It tries different operation combinations to create test sequences.

---

## `mda_generate.py`

### File Overview

`mda_generate.py` reads the required operation patterns (e.g., `RW-RR`, etc.) from the `do_test_list.txt` file and generates a test case file for each pattern.

Each test case is formatted as `sql_id-txn_id-sql`, where `sql_id` is the incrementing identifier for the statement, `txn_id` identifies the transaction executing the statement, and `sql` is the specific SQL statement.

The process of generating test case files in `mda_generate.py` can be divided into three stages:

* **Initialization Stage**: Generates the header description of the test case file, initializes tables, and inserts test data.
* **Execution Stage**: Executes transactions and validation SQL statements. The main goal is to automatically generate database test cases for different operation patterns.
* **Commit Stage**: Generates commit statements for each transaction and writes them to the corresponding test case files.

#### Initialization Stage

First, it calls `write_description` to write the description of the test cases. Then, it calls `init_table` to initialize the table structure. If it is a distributed test and the database is either `tdsql` or `crdb`, it creates a table for each transaction.

Then it calls `insert_data` to insert the data used for subsequent transactions. `insert_data` generates SQL insert statements based on provided parameters and writes these statements to a specified file. This function uses an array named `exist` to manage the presence of data elements to avoid duplicate inserts, while updating the SQL operation count.

#### Execution Stage

The main method used in this stage is `execute_txn`. This function generates SQL statements for executing operations based on the operation patterns in `do_test_list.txt`. When encountering specific operation patterns, this function reorders the operation statements to resolve conflicts as much as possible.

In `execute_txn`, there are two critical structures: `data_op_list` for storing data operation lists and `wait_op_list` for storing operations that need to be delayed. When an operation needs to be deferred, it is placed in `wait_op_list`.

#### Commit Stage

The commit stage generates commit statements for each transaction and writes them to the corresponding test case files.

### Class Definitions

- **OptionException**: Custom exception class.
- **Txn**: Transaction class, containing start and end timestamps.
- **Operation**: Operation class representing a database operation, including operation type and transaction number.
- **Wait_Operation**: Wait operation class extending the Operation class, adding an operation number.

### Function Definitions

- **init_table(file_name, sql_count, txn_count, table_num, db_type, test_type)**: Initializes database tables based on database type and test type, creating table structures and writing SQL statements to a file.
- **check_concurrency(txn_num1, txn_num2, txn)**: Checks if two transactions are concurrent.
- **check_exist_op(txn, data_op_list, op, op_num, txn_count)**: Checks if a specific type of operation exists in the transaction and if it is concurrent.
- **execute_sql(IsPredicate, file_name, sql_count, txn_count, op_num, op, data_num, txn, data_value, data_op_list)**: Executes SQL operations such as Write (W), Read (R), Insert (I), Delete (D), Abort (A), and Commit (C).
- **insert_data(...)**: Inserts data into the table and handles possible exceptions, such as data already existing.
- **delete_data(...)**: Deletes data from the table and records the delete operation.
- **write_data(...)**: Writes data to the table, typically incrementing data values by 1 and recording the write operation.
- **read_data(...)**: Reads data from the table and records the read operation.
- **read_data_predicate(...)**: Reads data from the table using a range condition.
- **abort_txn(...)**: Aborts a transaction and writes rollback SQL statements.
- **commit_txn(...)**: Commits a transaction and writes commit SQL statements.
- **execute_check(file_name, sql_count, txn_count, data_num, table_num)**: Performs consistency checks for transactions to validate data.
- **get_last_op(ops, pos)**: Retrieves the last operation before a given position in the operation list.
- **execute_first(...)** and **execute_second(...)**: Handle the first and second operations of Partial Order Pairs (POP), respectively.
- **execute_txn(...)**: Executes transactions, considering the operation order to resolve conflicts.
- **write_description(file_name, txn_num, op_num, data_num)**: Writes the description of test cases to a file.

---

## `mda_detect.py`

### File Overview

`mda_detect.py` is primarily responsible for analyzing the results generated by the testing framework (`sqltest_v2`), detecting cyclic dependencies in database transaction concurrency control, and determining whether the concurrent transactions are serializable. The theoretical principle is that a scheduling sequence is conflict-serializable if and only if its precedence graph is acyclic.

> *For mathematical proofs on this, interested readers can refer to: [Proof of correctness of the precedence graph test](http://www.cs.emory.edu/~cheung/Courses/554/Syllabus/7-serializability/graph-conflict2.html)*

#### Main Logic

`sqltest_v2` generates a schedule result file for each test case file. `mda_detect.py` reads each file, recording transaction operations, edges, in-degrees and access statuses.

* If an error is detected during reading, it outputs the error result and skips further steps.
* If no error message is found, it removes unfinished operations, builds the dependency graph, and calculates in-degrees.

  * If not marked as `go_end`, it checks for cycles in the graph.
  * If a cycle exists, it outputs the cycle result and calls the `dfs` method for depth-first search to record the path.
  * If no exsited cycle, it outputs the result  and prints the path.

### Class Definitions

- **Edge**: Represents an edge, containing type and output.
- **Operation**: Represents an operation, including operation type, transaction number, operation time, and value.
- **Txn**: Represents a transaction, including start and end timestamps.

### Function Definitions

- **get_total(lines)**: Calculates the maximum number of variables in a query.
- **find_data(query, target)**: Extracts data from the query.
- **set_finish_time(op_time, data_op_list, query, txn, version_list)**: Sets the end time of an operation and updates the transaction and version lists.
- **check_concurrency(data1, data2, txn)**: Checks if two transactions are concurrent.
- **get_edge_type(data1, data2, txn)**: Determines the edge type between two operations.
- **build_graph(data_op_list, indegree, edge, txn)**: Builds a directed graph representing the concurrency relationships of operations.
- **insert_edge(data1, data2, indegree, edge, txn)**: Inserts an edge into the directed graph.
- **init_record(query, version_list)**: Initializes records in the version list based on the query.
- **readVersion_record(query, op_time, data_op_list, version_list)**: Reads versioned records and updates operations.
- **read_record(op_time, txn_num, total_num, txn, data_op_list)**: Reads records and updates data operations.
- **write_record(op_time, txn_num, txn, data_op_list)**: Writes records and updates data operations.
- **delete_record(op_time, txn_num, txn, data_op_list)**: Deletes records and updates data operations.
- **insert_record(op_time, txn_num, txn, data_op_list)**: Inserts records
