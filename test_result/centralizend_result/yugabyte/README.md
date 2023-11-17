# `YugabyteDB`测试说明

## 测试环境

`Ubuntu22.04` | `yugabyte 2.19.2` | `unixODBC 2.3.9`

## 数据库简介

`YugabyteDB` 是一个高性能的开源分布式 `SQL` 数据库，旨在支持全球部署的业务关键型应用程序。它结合了传统的 `RDBMS`（关系数据库管理系统）的 ACID （原子性、一致性、隔离性、持久性）事务特性和 `Internet` 级别的水平可扩展性、高可用性和地理分布特性。

`YugabyteDB` 使用 Raft 分布式共识算法确保强一致性，并且可以容忍网络分区和节点故障，从而实现高可用性。支持多种数据模型，包括文档存储（通过 `YCQL`，兼容 Apache Cassandra 查询语言）和关系存储（通过 `YSQL`，兼容 `PostgreSQL`）。`YugabyteDB` 支持全局 `ACID` 事务，这意味着即使在分布式环境中，也能保证数据的完整性和一致性。`YugabyteDB` 的 `SQL` 接口（`YSQL`）与 `PostgreSQL` 高度兼容，这使得许多现有的应用程序和工具能够无缝迁移到 `YugabyteDB`。`YugabyteDB` 是为云而生的数据库，支持多云、混合云和 `Kubernetes` 部署。

## 隔离级别

`YugabyteDB`在事务层支持三种隔离级别：可串行化（`Serializable`）、快照（`Snapshot`）和已提交读（`Read Committed`）。`YSQL API`的默认隔离级别本质上是快照隔离（即与`PostgreSQL`的可重复读（`REPEATABLE READ`）相同）

已提交读（`Read committed`）：目前处于`Beta`阶段。只有在`YB-TServer`标志`yb_enable_read_committed_isolation`设置为`true`时，才支持已提交读隔离。

快照（`Snapshot`）：默认情况下，`yb_enable_read_committed_isolation`标志为`false`，在这种情况下，`YugabyteDB`事务层的已提交读隔离级别将回退到更严格的快照（`Snapshot`）隔离（此时，`YSQL`的`READ COMMITTED`也会使用快照隔离）。

可串行化（`Serializable`），它要求一组可串行化事务的任何并发执行都保证产生与按某种串行顺序（一次一个事务）运行它们相同的效果。

## 实际测试说明

### 关于驱动程序`ODBC`

`YugabyteDB` 是 `PostgreSQL` 兼容的，因此它通常可以使用 `PostgreSQL` 的 `ODBC` 驱动程序。

安装 `PostgreSQL ODBC` 驱动程序:

```shell
sudo apt-get install odbc-postgresql
```

### 关于隔离级别设置

注意：`YugabyteDB`的事务隔离级别要在每个事务开始时设置，而不是在线程连接时设置

由于已提交读（`Read committed`）特性仅在`beta`版本存在，因此并未测试。

快照（`Snapshot`）级别的设置命令为：`BEGIN TRANSACTION;`(默认隔离级别)

可串行化（`Serializable`）级别的设置命令为：` BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;`





