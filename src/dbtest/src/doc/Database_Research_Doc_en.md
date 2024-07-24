### DBMS Overview

| DBMS Name            | DBMS Type        | ODBC Support | Transaction Support | 3TS-Coo Verified | Supported Isolation Levels                                   | Concurrency Control Algorithms and Transaction Details |
| -------------------- | ---------------- | ------------ | ------------------- | ---------------- | ------------------------------------------------------------ | ------------------------------------------------------ |
| MySQL                | Relational DBMS  | Yes          | Yes                 | Yes              | Read Uncommitted, Read Committed, Repeatable Read, Serializable | MVCC, Optimistic and Pessimistic Control               |
| PostgreSQL           | Relational DBMS  | Yes          | Yes                 | Yes              | Read Uncommitted, Read Committed, Repeatable Read, Serializable | MVCC                                                   |
| MariaDB              | Relational DBMS  | Yes          | Yes                 | Yes              | Read Uncommitted, Read Committed, Repeatable Read, Serializable | MVCC                                                   |
| Microsoft SQL Server | Relational DBMS  | Yes          | Yes                 | Yes              | Read Uncommitted, Read Committed, Repeatable Read, Serializable, Snapshot | 2PL, MVCC, Optimistic and Pessimistic Control          |
| Oracle Database      | Relational DBMS  | Yes          | Yes                 | Yes              | Read Committed, Serializable                                 | 2PL (No-wait, Wait-for-Grant), MVCC                    |
| MyRocks              | Relational DBMS  | Yes          | Yes                 | Yes              | Read Committed, Repeatable Read                              | MVCC                                                   |
| MongoDB              | NoSQL DBMS       | Yes          | Supported since 3.0 | Yes              | Read Uncommitted, Snapshot                                   | Optimistic Concurrency Control (OCC)                   |
| Redis                | NoSQL DBMS       | No           | No                  | No               | Not applicable                                               | Optimistic Lock, MULTI/EXEC                            |
| Cassandra            | NoSQL DBMS       | Yes          | Yes                 | Yes              | Not applicable (controlled by consistency levels)            | LWT, Timestamp Ordering Protocol (T/O)                 |
| Neo4j                | Graph Database   | Yes          | Yes                 | No               | Read Committed                                               | MVCC                                                   |
| InfluxDB             | Time Series DBMS | Yes          | Yes                 | No               | Not applicable                                               | Optimistic Concurrency Control (OCC)                   |
| TimescaleDB          | Time Series DBMS | Yes          | Yes                 | No               | Read Uncommitted, Read Committed, Repeatable Read, Serializable | MVCC                                                   |
| CockroachDB          | Distributed DBMS | Yes          | Yes                 | Yes              | Read Committed, Serializable                                 | MVCC, Calvin                                           |
| TiDB                 | Distributed DBMS | Yes          | Yes                 | Yes              | Read Committed, Repeatable Read                              | MVCC, Percolator                                       |
| Oceanbase            | Distributed DBMS | Yes          | Yes                 | Yes              | Read Committed, Repeatable Read, Serializable                | MVCC, 2PC (Two-Phase Commit)                           |


### DBMS Detailed Overview

#### MySQL

MySQL is a widely used open-source relational database management system suitable for applications of various scales, including web and enterprise solutions. It supports transaction processing and replication, offering good performance and stability. Transactions in MySQL are implemented via the InnoDB storage engine, which supports four isolation levels (Read Uncommitted, Read Committed, Repeatable Read, Serializable) and manages concurrent transactions using Multi-Version Concurrency Control (MVCC) and row-level locks.

#### PostgreSQL

PostgreSQL is a powerful open-source relational database system supporting complex queries and transactions, along with advanced features like JSON support, full-text search, and geospatial queries. It is widely used in enterprise and research applications. PostgreSQL handles transactions via MVCC, supports four isolation levels, and manages concurrency with shared and exclusive locks.

#### MariaDB

MariaDB is a community-driven open-source database management system created by the founders of MySQL. It is compatible with MySQL and includes enhanced features and performance optimizations. It is suited for applications requiring high performance and open-source access. Transaction management in MariaDB is similar to MySQL, primarily implemented via the InnoDB engine.

#### Microsoft SQL Server

SQL Server, developed by Microsoft, is a relational database management system designed for large-scale enterprise applications and data-driven solutions. It offers high availability, security, and enterprise-level support, with broad development tool and platform support. SQL Server supports five isolation levels and manages concurrency using locks and lock escalation. It also supports Snapshot Isolation, utilizing optimistic concurrency control mechanisms.

#### Oracle Database

Oracle Database is a powerful commercial-grade relational database management system used for enterprise data management and applications. It features high scalability, security, and data integrity. Oracle manages transactions through read consistency and locking mechanisms, supporting two main isolation levels (Read Committed, Serializable). It uses Multi-Version Concurrency Control (MVCC) for consistent reads and combines it with Two-Phase Locking (2PL).

#### MyRocks

MyRocks is a high-performance persistent storage engine developed by Facebook, designed to handle large-scale OLTP workloads. Based on RocksDB, it focuses on high throughput and low latency. Transactions in MyRocks are implemented using MVCC and key-based locks, supporting two isolation levels (Read Committed, Repeatable Read).

#### MongoDB

MongoDB is a document-oriented NoSQL database suitable for handling large amounts of unstructured data and applications requiring a flexible data model, such as web apps, analytics, and real-time data processing. MongoDB supports transactions starting from version 3.0, with isolation levels mainly being Read Uncommitted and Read Committed. It uses multi-document transactions and distributed locks for concurrency management and employs optimistic concurrency control (OCC).

#### Redis

Redis is an in-memory data structure store often used as a cache and message broker. It supports various data structures (e.g., strings, hashes, lists) and offers fast access and high performance. It is suited for real-time data processing and session storage. Redis does not support traditional transaction isolation levels due to its single-threaded model, relying on optimistic locks and transaction commands (MULTI/EXEC) for simple transaction support.

#### Cassandra

Cassandra is a distributed column-family NoSQL database known for its high availability and scalability. It is suitable for applications requiring large-scale data processing and distributed data storage, such as analytics and log management. Cassandra does not support traditional transaction isolation levels but manages data consistency through consistency levels (e.g., One, Quorum, All). It uses optimistic concurrency control and lightweight transactions (LWT) and employs a Timestamp Ordering Protocol (T/O).

#### Neo4j

Neo4j is a graph database management system focusing on storing and processing graph data (nodes and edges). It is suitable for applications requiring complex relationship and network analysis, such as social networks, recommendation systems, and network security analysis. Neo4j manages transactions using ACID properties, with the main isolation level being Read Committed, and employs locks and MVCC for concurrency control.

#### InfluxDB

InfluxDB is an open-source database designed specifically for time-series data, used for handling large volumes of timestamped data, such as monitoring, IoT sensor data, and real-time analytics. It offers high performance and query efficiency. InfluxDB does not support traditional transaction isolation levels as it focuses on time-series data's write and query performance, using the uniqueness of time-series data to prevent conflicts. InfluxDB employs optimistic concurrency control (OCC).

#### TimescaleDB

TimescaleDB is an open-source time-series database built on PostgreSQL, combining SQL and NoSQL features for high-performance analysis and querying of time-series data. It is suitable for time-series data storage and complex queries. TimescaleDB inherits PostgreSQL's transaction model, supports four isolation levels, and uses MVCC for concurrency management.

#### CockroachDB

CockroachDB is a distributed relational database management system with transaction support and high availability, designed for large-scale distributed applications requiring strong consistency and horizontal scalability, such as online services and global deployments. CockroachDB handles transactions via distributed transaction protocols (e.g., Percolator), supports two isolation levels (Read Committed, Serializable), and uses optimistic concurrency control and deterministic concurrency control protocols (Calvin).

#### TiDB

TiDB is an open-source distributed relational database management system compatible with MySQL protocol, supporting transaction processing and horizontal scalability. It is suitable for mixed workloads involving Online Transaction Processing (OLTP) and Online Analytical Processing (OLAP). TiDB's transactions are managed using Percolator, supporting two isolation levels (Read Committed, Repeatable Read).