# Core Python Components of the Dynamic Testing System

- [Core Python Components of the Dynamic Testing System](#core-python-components-of-the-dynamic-testing-system)
  - [Introduction](#introduction)
  - [Detailed File Overview](#detailed-file-overview)
    - [`random_do_list.py` - Test Pattern Generator](#random_do_listpy---test-pattern-generator)
      - [Core Functionality](#core-functionality)
      - [Overall Structure](#overall-structure)
      - [Operation Type Definitions](#operation-type-definitions)
      - [Output Format](#output-format)
    - [`mda_generate.py` - SQL Test Case Generator](#mda_generatepy---sql-test-case-generator)
      - [Core Functionality](#core-functionality-1)
      - [Three-Phase Generation Architecture](#three-phase-generation-architecture)
      - [Core Class Definitions](#core-class-definitions)
      - [SQL Operation Mapping](#sql-operation-mapping)
      - [Database Compatibility](#database-compatibility)
    - [`mda_detect.py` - Concurrency Anomaly Detector](#mda_detectpy---concurrency-anomaly-detector)
      - [Core Functionality](#core-functionality-2)
      - [Analysis Flow Architecture](#analysis-flow-architecture)
      - [Core Data Structures](#core-data-structures)
      - [Dependency Type Classification](#dependency-type-classification)
  - [Relationships with Other Files](#relationships-with-other-files)
  - [Role in the Project](#role-in-the-project)
    - [Integration with the 3TS Framework](#integration-with-the-3ts-framework)
    - [Collaboration with Other Components](#collaboration-with-other-components)



## Introduction

This document provides a detailed explanation of the three core Python components in the 3TS project's dynamic testing system: `random_do_list.py`, `mda_generate.py`, and `mda_detect.py`. These three scripts form a complete pipeline for concurrent database transaction testing—from generating random test patterns, to creating SQL test cases, to detecting concurrency anomalies and performing serializability analysis.

## Detailed File Overview

### `random_do_list.py` - Test Pattern Generator

#### Core Functionality

This file serves as the starting point of the dynamic testing workflow. It generates random combinations of database operation patterns. Based on a predefined set of operations, it uses a depth-first search (DFS) algorithm to produce various possible operation sequences while ensuring that no illegal patterns are generated. See lines 20–21 in `random_do_list.py`.

#### Overall Structure

The core logic revolves around two main DFS functions:

1. `dfs(data_count, total_num, target_num)`
   - Determines which variables need to be operated on twice to meet the total number of operations.
   - Uses string concatenation to avoid revisiting the same state.
   - Once the target number of operations is reached, calls `dfs1` to generate the actual operation sequences.
2. `dfs1(data_count, res, total_num)`
   - Recursively generates specific test cases.
   - Iterates over all possible combinations of operations (P, R, W, I in pairs).
   - Filters out illegal operation patterns (e.g., RR, RP, PR, PP).
   - Supports compound operations with commits (e.g., RCW, WCR).

#### Operation Type Definitions

The system supports four basic types of database operations:

- **P (Predicate)**: Predicate operations used for range queries.
- **R (Read)**: Read operations.
- **W (Write)**: Write operations.
- **I (Insert)**: Insert operations.

#### Output Format

Generated operation patterns are written to the file `do_test_list.txt` in formats like `PW0-RW1`, indicating that transaction 0 performs a predicate-write, while transaction 1 performs a read-write.

------

### `mda_generate.py` - SQL Test Case Generator

#### Core Functionality

This file reads the operation patterns from `do_test_list.txt` and transforms these abstract patterns into concrete SQL test case files. Each test case is organized in the format: `sql_id-txn_id-sql`.

#### Three-Phase Generation Architecture

**1. Initialization Phase**:

- `write_description()`: Generates metadata description for the test case file.
- `init_table()`: Creates table schemas based on database type (supports single-table and distributed multi-table setups).
- `insert_data()`: Inserts initial data required for the test, using an `exist` array to avoid duplicates.

**2. Execution Phase**:

- `execute_txn()`: Core function that generates SQL statements for transactions based on the operation pattern.
- Maintains two key data structures:
  - `data_op_list`: Stores all operation history for each data item.
  - `wait_op_list`: Stores operations to be executed later to resolve conflicts.
- Implements intelligent operation reordering to minimize conflicts.

**3. Commit Phase**:

- Generates `COMMIT` statements for all uncommitted transactions.
- Optional consistency checks can be performed.

#### Core Class Definitions

- **`Txn`**: Transaction class that tracks transaction start and end timestamps.
- **`Operation`**: Operation class that records the type of operation and its associated transaction.
- **`Wait_Operation`**: Extension of the `Operation` class, adding an operation ID for delayed execution.

#### SQL Operation Mapping

The `execute_sql()` function supports seven types of SQL operations:

- W → `UPDATE` statement
- R → `SELECT` statement
- P → Range-based `SELECT` statement
- I → `INSERT` statement
- D → `DELETE` statement
- A → `ROLLBACK` statement
- C → `COMMIT` statement

#### Database Compatibility

Supports multiple database types for table schema generation, including:

- Standalone databases (default simple schema)
- TDSQL (partitioned table schema)
- CockroachDB (distributed table schema)
- OceanBase in Oracle mode

------

### `mda_detect.py` - Concurrency Anomaly Detector

#### Core Functionality

This file analyzes execution results from the dynamic testing framework (`sqltest_v2`) to detect cyclic dependencies in database transaction concurrency control. Based on conflict-serializability theory: a schedule is conflict-serializable if and only if its dependency graph is acyclic.

#### Analysis Flow Architecture

**1. Input Processing Phase**:

- Reads `do_test_list.txt` to get the list of test files.
- Parses the SQL execution logs of each test result file.
- Extracts transaction operations, timestamps, and data values.

**2. Operation Recording Phase**:

- `operation_record()`: Parses SQL queries to identify operation types.
- Handles read, write, insert, delete, and commit operations separately.
- Builds an operation history list for each data item.

**3. Graph Construction Phase**:

- `build_graph()`: Constructs the transaction dependency graph.
- `get_edge_type()`: Determines dependency types between operations (RW, WR, WW, etc.).
- `check_concurrency()`: Checks if two transactions are concurrent.

**4. Cycle Detection Phase**:

- `check_cycle()`: Uses topological sorting to detect cycles in the graph.
- `dfs()`: Performs depth-first search to record cycle paths.
- Outputs analysis results: Cyclic (cycle exists), Avoid (no cycle), or Error (execution failure).

#### Core Data Structures

- **`Edge`**: Represents a dependency edge between transactions, including type and target transaction.
- **`Operation`**: Represents a single database operation, including type, transaction ID, timestamp, and value.
- **`Txn`**: Represents a transaction, recording its start and end time.

#### Dependency Type Classification

The system identifies various types of dependencies:

- **RW**: Read-after-write dependency (anti-dependency)
- **WR**: Write-after-read dependency (flow dependency)
- **WW**: Write-after-write dependency (output dependency)
- **CR/CW**: Variants of dependencies involving commits

------

## Relationships with Other Files

The entire testing flow is executed in the following sequence:

1. Run `random_do_list.py` to generate operation patterns.
2. Run `mda_generate.py` to generate SQL test cases.
3. Compile and run the C++ execution engine.
4. Run `mda_detect.py` to analyze results.

The C++ execution engine reads database configuration files to connect to different database systems. The database type parameter in `mda_generate.py` (e.g., `tdsql`, `crdb`) corresponds to these configuration files.

Execution results are stored in directories like `pg/serializable`, from which `mda_detect.py` reads for analysis.

Analysis results are stored in the `check_result/` directory and organized by timestamp:

- `check_result{timestamp}.txt`: Main result file
- Includes serializability judgment results for each test case

------

## Role in the Project

### Integration with the 3TS Framework

These three files form the core components of the dynamic testing subsystem in the 3TS project:

1. **Automated Test Generation**: Eliminates the need for manual test case writing by generating large-scale test scenarios algorithmically.
2. **Multi-Database Support**: Compatible with PostgreSQL, MySQL, TiDB, CockroachDB, and other systems.
3. **Concurrency Control Validation**: Specifically designed to verify the correctness of database concurrency control mechanisms.
4. **Serializability Detection**: Implements strict serializability detection based on graph algorithms.

### Collaboration with Other Components

- **With the Static Testing Framework**: Provides dynamic testing capabilities as a complement to static testing.
- **With the C++ Execution Engine**: Python generates test cases, and the C++ engine executes them.
- **With the Result Analysis System**: Produces standardized analysis reports for further processing.
