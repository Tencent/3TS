## 静态测试分析

### 静态测试概述

静态测试的源代码包含 `case_cntl.cc`、`sql_cntl.cc`、`sqltest.cc` 以及对应的头文件，生成的可执行文件名为 `3ts_dbtest`。

- `case_cntl.cc`封装了测试用例控制所有到的类，主要包括三个方面的功能类： 1.解析测试文件，2.处理和比较结果，3.输出结果。

- `sql_cntl.cc`  涉及` SQL` 操作的代码。主要是定义实现了一个 `DBConnector`类。 `DBConnector` 向下对接 `ODBC`，向上提供了封装的 `SQL` 执行接口。包含设置数据库隔离级别、开始事务、执行增删改查等功能、处理 `SQL` 返回值并获取错误信息、结束事务或回滚事务等功能。

- `sqltest.cc` 静态测试的的入口，具体定义了静态测试的运行逻辑。该文件利用 `gflags` 库来实现命令行参数解析中，声明定义了 `JobExecutor` 负责多线程执行数据库语句。

三个源代码以及他们的头文件从上到下是层层依赖关系。`sql_cntl.cc` 使用了`case_cntl.cc` 中封装的类用来解析语句和返回结果。`sqltest.cc` 需要 `sql_cntl.cc` 中定义的 `DBConnector` 来执行具体的事务 `SQL`。

### `case_cntl.cc ` 解析

#### 主要功能

**解析测试文件**

- 从文件中逐行读取内容，识别不同的标记和格式（如注释、参数数目、事务SQL、预期结果等）。
- 将解析后的数据结构化为测试序列和结果集。

**处理和比较结果**

- 比较当前SQL执行结果与预期结果，检查是否匹配。
- 对于每个测试用例，检查当前结果是否与任意一个预期结果集匹配。
- 匹配结果会记录到指定文件中，供后续分析使用。

**输出结果**

- 输出整体测试结果，总结各测试用例的执行情况。
- 将详细的匹配结果输出到控制台和文件，便于调试和验证。

#### 主要封装类

**TestResultSet 类**:

- 存储测试用例类型 (`test_case_type_`)、事务隔离级别 (`isolation_`)、测试结果类型 (`result_type_`) 以及预期结果集列表 (`expected_result_set_list_`)。
- 提供设置和获取这些属性的方法，以及添加 SQL 结果集的方法。

**TxnSql 类**:

- 存储事务 ID (`txn_id_`)、SQL 语句 ID (`sql_id_`)、SQL 语句 (`sql_`) 和测试用例类型 (`test_case_type_`)。
- 提供获取这些属性的方法。

**TestSequence 类**:

- 存储测试用例类型 (`test_case_type_`)、测试序列列表 (`txn_sql_list_`) 以及结果集的列数 (`param_num_`)。
- 提供获取这些属性的方法、添加事务 SQL 的方法和设置参数数量的方法。

**CaseReader 类**:

- 用于从文件读取并解析测试序列和预期结果集。
- 存储从文件中读取的测试序列列表 (`test_sequence_list_`) 和测试结果集列表 (`test_result_set_list_`)。
- 提供初始化测试序列和结果集的方法、添加测试序列和结果集的方法，以及从文件中解析事务 ID、SQL 语句、SQL 结果和隔离级别的方法。

**ResultHandler 类**:

- 用于处理和验证 SQL 查询的预期结果。
- 提供方法比较单个 SQL 查询的当前结果与预期结果，以及验证 SQL 查询的当前结果集是否匹配预期结果集列表中的任何一个。

**Outputter 类**:

- 用于输出测试结果。
- 提供将总体测试结果写入指定文件的方法、打印和写入事务 SQL 结果的方法，以及将测试用例类型和结果类型写入指定文件的方法。

**全局变量**:

- `outputter` 和 `result_handler` 是 Outputter 和 ResultHandler 类的全局实例，用于在整个程序中处理输出和结果验证。



### `sql_cntl.cc` 解析

#### 主要功能

`sql_cntl.cc`  涉及` SQL` 操作的代码。主要是定义实现了一个 `DBConnector`类。 `DBConnector` 向下对接 `ODBC`，向上提供了封装的 `SQL` 执行接口。包含设置数据库隔离级别、开始事务、执行增删改查等功能、处理 `SQL` 返回值并获取错误信息、结束事务或回滚事务等功能。

#### DBConnector 类

* **conn_pool_ **：存储数据库连接句柄的向量，用于连接池管理。

* **InitDBConnector**: 初始化数据库连接器，创建指定数量的数据库连接，并将它们添加到连接池中。使用ODBC函数 `SQLAllocHandle` 分配环境和连接句柄，使用 `SQLConnect` 连接到数据库。

* **ExecReadSql2Int / ExecWriteSql**: 分别用于执行读取和写入操作的SQL语句。处理结果集或错误，并记录执行过程。
* **SqlExecuteErr**: 处理SQL执行返回值的错误信息。根据返回值判断执行状态，并返回适当的错误消息或空字符串。
* **SQLEndTnx / SQLStartTxn**: 用于事务管理的函数，包括开始、提交或回滚事务，并记录执行过程和错误信息。
* **SetAutoCommit / SetTimeout / SetIsolationLevel**: 分别设置自动提交模式、连接超时和隔离级别，通过ODBC函数调整数据库连接的行为。
* **ErrInfoWithStmt**: 通过SQL语句句柄获取错误信息，将错误信息和SQL状态填充到指定的数组中。
* **ReleaseConn / ReleaseEnv / Release**: 释放数据库连接、环境资源或整体资源的函数，确保在使用完毕后进行清理和释放。



### `sqltest.cc ` 解析

#### 主要功能

静态测试的的入口，具体定义了静态测试的运行逻辑。该文件利用 `gflags` 库来实现命令行参数解析，声明定义了 `JobExecutor` 负责多线程执行数据库语句。使用了`pthread_mutex_t`来实现事务的并发控制。



#### 主要函数

**MultiThreadExecution**

* 在多线程环境下执行多个数据库事务的函数，支持不同的数据库操作类型和事务隔离级别。
* 该函数接受一系列事务SQL，执行并将结果记录到文件中。

**ExecTestSequence**：

* `JobExecutor`的成员函数，执行一系列数据库测试事务并将结果写入文件。
* 函数首先初始化测试结果文件，然后根据事务类型和隔离级别打印相关信息，执行事务SQL，并验证结果的一致性。
* 具体调用 `MultiThreadExecution` 来在多线程环境中执行多个SQL事务。
