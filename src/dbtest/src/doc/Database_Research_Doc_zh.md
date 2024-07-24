### DBMS 大致介绍

| DBMS 名称            | DBMS 类型              | 是否提供 ODBC | 是否支持事务 | 是否被 3TS-Coo 验证 | 支持的事务隔离级别                                           | 并发控制算法及事务描述       |
| -------------------- | ---------------------- | ------------- | ------------ | ------------------- | ------------------------------------------------------------ | ---------------------------- |
| MySQL                | 关系型数据库管理系统   | 是            | 是           | 是                  | Read Uncommitted, Read Committed, Repeatable Read, Serializable | MVCC，乐观和悲观控制         |
| PostgreSQL           | 关系型数据库管理系统   | 是            | 是           | 是                  | Read Uncommitted, Read Committed, Repeatable Read, Serializable | MVCC                         |
| MariaDB              | 关系型数据库管理系统   | 是            | 是           | 是                  | Read Uncommitted, Read Committed, Repeatable Read, Serializable | MVCC                         |
| Microsoft SQL Server | 关系型数据库管理系统   | 是            | 是           | 是                  | Read Uncommitted, Read Committed, Repeatable Read, Serializable, Snapshot | 2PL，MVCC，乐观和悲观控制    |
| Oracle Database      | 关系型数据库管理系统   | 是            | 是           | 是                  | Read Committed, Serializable                                 | 2PL (无等待、等待死锁)，MVCC |
| MyRocks              | 关系型数据库管理系统   | 是            | 是           | 是                  | Read Committed, Repeatable Read                              | MVCC                         |
| MongoDB              | NoSQL 数据库管理系统   | 是            | 3.0 后支持   | 是                  | Read Uncommitted, Snapshot                                   | 乐观控制（OCC）              |
| Redis                | NoSQL 数据库管理系统   | 否            | 否           | 否                  | 不适用                                                       | 乐观锁，MULTI/EXEC           |
| Cassandra            | NoSQL 数据库管理系统   | 是            | 是           | 是                  | 不适用（通过一致性级别控制）                                 | LWT，时间戳排序协议（T/O）   |
| Neo4j                | 图数据库               | 是            | 是           | 否                  | Read Committed                                               | MVCC                         |
| InfluxDB             | 时间序列数据库管理系统 | 是            | 是           | 否                  | 不适用                                                       | 乐观并发控制（OCC）          |
| TimescaleDB          | 时间序列数据库管理系统 | 是            | 是           | 否                  | Read Uncommitted, Read Committed, Repeatable Read, Serializable | MVCC                         |
| CockroachDB          | 分布式数据库管理系统   | 是            | 是           | 是                  | Read Committed, Serializable                                 | MVCC，Calvin                 |
| TiDB                 | 分布式数据库管理系统   | 是            | 是           | 是                  | Read Committed, Repeatable Read                              | MVCC，Percolator             |
| Oceanbase            | 分布式数据库管理系统   | 是            | 是           | 是                  | Read Committed, Repeatable Read，Serializable                | MVCC，2PC（两阶段提交）      |



### DBMS 详细介绍

#### MySQL
MySQL 是一个广泛使用的开源关系型数据库管理系统，适合用于各种规模的应用，包括 Web 应用和企业级解决方案。支持事务处理和复制，具有良好的性能和稳定性。MySQL 的事务通过 InnoDB 存储引擎实现，支持四种隔离级别（Read Uncommitted, Read Committed, Repeatable Read, Serializable），并使用多版本并发控制（MVCC）和行级锁来管理并发事务。

#### PostgreSQL
PostgreSQL 是一个强大的开源关系型数据库系统，支持复杂的查询和事务处理，以及高级功能如 JSON 支持、全文搜索和地理空间查询。广泛用于各种企业和科研领域的应用。PostgreSQL 的事务通过 MVCC 实现，支持四种隔离级别，并使用共享锁和排他锁来管理并发事务。

#### MariaDB
MariaDB 是一个社区驱动的开源数据库管理系统，由 MySQL 的创始人创建，兼容 MySQL，并包含增强的功能和性能优化。适合需要高性能和开放源代码的应用。MariaDB 的事务管理与 MySQL 类似，也主要通过 InnoDB 引擎实现。

#### Microsoft SQL Server
SQL Server 是由 Microsoft 开发的关系型数据库管理系统，适用于大型企业应用和数据驱动的解决方案。提供高可用性、安全性和企业级支持，支持广泛的开发工具和平台。SQL Server 支持五种事务隔离级别，并使用锁和锁升级来管理并发事务。SQL Server 还支持快照隔离（Snapshot Isolation），使用乐观并发控制机制。

#### Oracle Database
Oracle Database 是一种功能强大的商业级关系数据库管理系统，用于企业级数据管理和应用程序。具有高度可扩展性、安全性和数据完整性。Oracle 的事务通过读一致性和锁来管理，并支持两种主要的隔离级别（Read Committed, Serializable）。Oracle 使用多版本并发控制（MVCC）来实现一致性读，并结合两阶段锁定协议（2PL）。

#### MyRocks
MyRocks 是 Facebook 开发的高性能持久性存储引擎，设计用于处理大规模的 OLTP 工作负载。基于 RocksDB，专注于高吞吐和低延迟的需求。MyRocks 的事务通过 MVCC 和基于键的锁实现，并支持两种隔离级别（Read Committed, Repeatable Read）。

#### MongoDB
MongoDB 是一个面向文档的 NoSQL 数据库，适合处理大量非结构化数据和需要灵活数据模型的应用，如 Web 应用、分析和实时数据处理。MongoDB 从 3.0 版本开始支持事务，隔离级别主要为 Read Uncommitted 和 Read Committed。MongoDB 使用多文档事务和分布式锁来管理并发事务，采用乐观并发控制（OCC）。

#### Redis
Redis 是一个内存中的数据结构存储，通常用作缓存和消息代理。支持多种数据结构（如字符串、哈希表、列表等），提供快速访问和高性能。适用于实时数据处理和会话存储等场景。Redis 不支持事务隔离级别，因为它是一个单线程处理模型，使用乐观锁和事务命令（MULTI/EXEC）来实现简单的事务支持。

#### Cassandra
Cassandra 是一个分布式的面向列的 NoSQL 数据库，具有高可用性和可伸缩性。适用于需要处理大量数据和分布式数据存储的应用，如分析和日志管理。Cassandra 不支持传统的事务隔离级别，而是通过一致性级别（如 One, Quorum, All）来控制数据一致性。Cassandra 使用乐观并发控制和轻量级事务（LWT）来管理并发事务，同时使用时间戳排序协议（T/O）。

#### Neo4j
Neo4j 是一个图数据库管理系统，专注于存储和处理图形结构数据（节点和边）。适用于需要复杂关系和网络分析的应用，如社交网络、推荐系统和网络安全分析。Neo4j 的事务管理使用 ACID 特性，主要隔离级别为 Read Committed，并使用锁机制和 MVCC 来管理并发事务。

#### InfluxDB
InfluxDB 是一个专为时间序列数据设计的开源数据库，用于处理大规模的时间戳数据，如监控、IoT 传感器数据和实时分析。具有高性能和查询效率。InfluxDB 不支持传统的事务隔离级别，因为它专注于时间序列数据的写入和查询性能，通过时间序列的唯一性来避免数据冲突。InfluxDB 使用乐观并发控制（OCC）。

#### TimescaleDB
TimescaleDB 是建立在 PostgreSQL 之上的开源时间序列数据库，结合了 SQL 和 NoSQL 的特性，用于处理时间序列数据的高性能分析和查询。适合于时间序列数据存储和复杂查询。TimescaleDB 继承了 PostgreSQL 的事务模型，支持四种隔离级别，并使用 MVCC 来管理并发事务。

#### CockroachDB
CockroachDB 是一个分布式的关系数据库管理系统，具有事务支持和高可用性，适用于需要强一致性和水平扩展的大规模分布式应用，如在线服务和全球部署的应用。CockroachDB 的事务通过分布式事务协议（如 Percolator）实现，支持两种隔离级别（Read Committed, Serializable），并使用乐观并发控制和确定性并发控制协议（Calvin）。

#### TiDB
TiDB 是一个开源的分布式关系数据库管理系统，兼容 MySQL 协议，支持事务处理和横向扩展。适用于在线事务处理（OLTP）和在线分析处理（OLAP）的混合工作负载。TiDB 的事务通过 Percolator 实现，支持两种隔离级别（Read Committed, Repeat)。
