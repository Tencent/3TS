# Cassandra数据库测试说明

## 测试环境

`Ubuntu22.04` | `Cassandra 4.0.11` | `CQL spec 3.4.5` | `unixODBC 2.3.9`

## 数据库简介

`Apache Cassandra`是一个开源的、分布式的、`NoSQL`数据库系统，它最初是由`Facebook`开发的，用于处理大量的分布式数据存储。`Cassandra`是基于`Amazon`的`Dynamo`数据库和`Google`的`Bigtable`数据库的设计理念构建的，因此它结合了两者的优势，提供了高度可扩展性和高性能的数据管理功能。

## 隔离级别

`Cassandra` 数据库采用一种称为最终一致性（`Eventual Consistency`）的一致性模型，它与传统的关系型数据库中的 `ACID` 事务模型有所不同。因此，`Cassandra` 并不使用传统的隔离级别，如 `Read Committed`、`Repeatable Read` 等。而是提供一系列的一致性级别（`Consistency Levels`）来控制读写操作的一致性，一致性级别决定了在进行读取或写入操作时，必须成功应答的副本（`replica`）数量。主要的一致性级别包括：

1. **ANY**：对于写操作，只要至少有一个副本应答，操作就被视为成功。
2. **ONE**：至少有一个副本成功应答。
3. **TWO / THREE**：至少有两个/三个副本成功应答。
4. **QUORUM**：成功应答的副本数达到副本总数的大多数。
5. **ALL**：所有副本都必须成功应答。
6. **LOCAL_ONE / LOCAL_QUORUM**：对于多数据中心部署，至少有一个/大多数副本在本地数据中心应答。
7. **EACH_QUORUM**：在每个数据中心，成功应答的副本数都达到该数据中心副本总数的大多数。
8. **SERIAL / LOCAL_SERIAL**：用于轻量事务，确保线性一致性。

根据不同的一致性级别，`Cassandra` 的读写操作可能会表现出不同的隔离性，但这与传统的关系型数据库中定义的隔离级别是不同的。

### 轻量级事务

`Cassandra`支持轻量级事务（`Lightweight Transactions`），这可以为某些操作提供一种类似于传统`ACID`事务的一致性模型，但它们通常有一定的性能开销，并且不适用于所有用例。即在`CQL`语句中使用`IF NOT EXISTS`，`IF EXISTS`，或`IF`子句来实现一些形式的条件更新和删除。这种情况下，Cassandra内部会使用Paxos协议来确保操作的线性一致性，但这会带来更高的延迟和开销。

例如，可以使用`IF`条件来保证操作的原子性：

```CQL
UPDATE mykeyspace.mytable SET value = 123 WHERE key = 'some_key' IF EXISTS;
```

这个查询只有在`some_key`确实存在时才会更新，否则不会进行任何操作。但是，这并不等同于传统的`ROLLBACK`命令，因为一旦满足条件并执行了更新，这个操作是不可逆的。

### 批处理

`Cassandra`提供了一定程度的事务支持，即通过批处理（`Batch`）来实现原子性。在`Cassandra`中，可以将多个写操作（如`INSERT`、`UPDATE`、`DELETE`）组合到一个批处理中，这些操作要么全部成功，要么全部失败。但要注意，`Cassandra`中的批处理操作不同于关系型数据库中的事务，它不提供隔离性和持久性保证。

```CQL
BEGIN BATCH
INSERT INTO table1 (column1, column2) VALUES ('value1', 'value2');
UPDATE table2 SET column1 = 'value1' WHERE column2 = 'value2';
DELETE FROM table3 WHERE column1 = 'value1';
APPLY BATCH;
```

## CQL语言

### `COMMIT`和`ROLLBACK`指令

`Cassandra`不支持传统的`SQL`事务，因此也不支持`COMMIT`和`ROLLBACK`指令。因为`Cassandra`不是一个支持`ACID`属性的关系型数据库系统。`Cassandra`是一个最终一致性（`eventual consistency`）的分布式数据库，它主要关注的是可扩展性和高可用性。

### `INSERT`指令

`INSERT INTO mytab VALUES (1, 20);`语句在`CQL`语言中不被允许，需要完整形式`INSERT INTO mytab (k,v) VALUES (1, 20);`

## 实际测试说明

实际测试中编写了`sql2cql.py`脚本文件将原始测试文件中的`INSERT`语句转化为符合`CQL`语法的`INSERT`语句，其他测试语句保持原状，在单机环境对`one`一致性级别进行了测试，由于不支持传统的`SQL`事务，因此`33`个测试样例中大部分异常，小部分通过，仅有一个测试样例存在死锁情况。

针对除`one`以外的一致性级别，由于无法通过`ODBC`接口函数直接设置一致性隔离级别，因此暂时没有测试：

即`CONSISTENCY ***;`是CQL中的一个命令，用于设置后续查询的一致性级别，但如果直接通过`SQLExecDirect`函数执行语句，会报错`Malformed SQL Statement: Unrecognized keyword: consistency`，原因为ODBC是通用SQL接口，无法直接发送非标准的SQL命令，如`Cassandra`的`CONSISTENCY ***;`，对于Cassandra，可以使用专有的驱动器，如DataStax的C++驱动器，在DataStax C++驱动器中，可以这样设置一致性级别：

```c++
CassStatement* statement = cass_statement_new("SELECT * FROM table_name", 0);
cass_statement_set_consistency(statement, CASS_CONSISTENCY_QUORUM);
```







