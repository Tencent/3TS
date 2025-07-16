## 静态测试模块

### 概览

`3ts_dbtest` 的静态测试模块由三个核心源文件组成，采用如下的分层架构设计：

- `case_cntl.cc` 处理输入测试文件的解析、结果比较和输出生成。

- `sql_cntl.cc` 基于 `case_cntl.cc`，通过 `DBConnector` 类提供 SQL 执行功能，该类通过 ODBC 与数据库进行交互。

- `sqltest.cc` 在以上两个文件的基础之上，使用 `case_cntl.cc` 来存储测试数据并使用 `sql_cntl.cc` 来执行事务。该文件用于测试驱动程序，协调事务性 SQL 序列的多线程执行。

- 最终生成名为 `3ts_dbtest`的可执行文件。

---

### `case_cntl.cc`

#### 模块职责

- 解析测试用例文件
- 存储测试的元信息与期望结果
- 比较实际 SQL 输出与期望结果
- 将测试结果输出至控制台与文件

此模块提供所有静态测试所依赖的数据解析与结果验证逻辑。

#### 主要类与函数

##### `CaseReader` 类

- 将测试文件转换为结构化的 `TestSequence` 与 `TestResultSet` 对象
- 支持元信息解析：执行顺序编号、事务编号、SQL 语句、隔离级别、SQL 序列、期望结果集
- **核心方法**：
  - `InitTestSequenceAndTestResultSetList`：根据提供的测试路径与数据库类型初始化 `TestSequence` 与 `TestResultSet` 的列表
  - `TestSequenceAndTestResultSetFromFile`：解析测试文件，提取测试序列与其对应的期望结果

##### `ResultHandler` 类

- 用于将实际 SQL 输出与一个或多个期望结果集进行比对
- **核心方法**：
  - `IsSqlExpectedResult`：比较当前 SQL 的输出与期望结果
  - `IsTestExpectedResult`：比较当前测试结果与整个测试期望结果集

##### `Outputter` 类

- 负责格式化输出以下信息：
  - 测试元数据（测试类型、隔离级别、测试结果）
  - SQL 执行日志
  - 成功/失败状态
- 支持输出至控制台与文件
- **核心方法**：
  - `WriteResultTotal`：将汇总测试结果写入指定文件
  - `PrintAndWriteTxnSqlResult`：比较当前 SQL 输出与期望结果，并将比较信息输出到控制台与文件
  - `WriteTestCaseTypeToFile`：将测试类型追加写入指定文件
  - `WriteResultType`：将测试结果类型追加写入指定文件

---

### `sql_cntl.cc`

#### 模块职责

- 数据库连接池管理
- SQL 命令执行（读/写）
- 事务控制
- 错误处理
- 隔离级别配置

该模块为 `case_cntl.cc` 解析的测试序列提供 SQL 执行后端支持。

#### 主要类与函数

##### `DBConnector` 类

- 管理基于 ODBC 的底层数据库连接与 SQL 执行
- 使用连接池（`conn_pool_`）支持并发 SQL 执行
- 支持读写 SQL 操作、事务控制与错误处理
- **核心方法**：：
  - `InitDBConnector`：通过建立多个 ODBC 连接初始化连接池
  - `ExecReadSql2Int`：执行读取类 SQL 并将结果以整数形式存储
  - `ExecWriteSql`：执行写入类 SQL
  - `SQLStartTxn`：开始一个事务
  - `SQLEndTnx`：提交或回滚事务
  - `SetIsolationLevel`：设置数据库连接的隔离级别
  - `ReleaseConn`：释放连接池中的所有连接
  - `SqlExecuteErr`：通过 SQL 语句句柄获取错误信息

---

### `sqltest.cc`

#### 模块职责

- 使用 `gflags` 解析命令行参数
- 使用 POSIX 线程（`pthread`）实现并发
- 负责调度与同步事务执行流程
- 聚合并输出最终测试结果

#### 主要类与函数

##### `JobExecutor` 类

- 管理静态测试的完整生命周期，包括：
  - 初始化 → 并发执行 SQL → 结果比较 → 日志输出
- 支持多线程事务执行
- **核心方法**：
  - `ExecTestSequence`：执行完整的数据库测试序列并输出结果日志

---

### 工作流程总结

1. 使用 `CaseReader` 解析测试文件
2. 构造 `TestSequence` 与 `TestResultSet` 对象
3. 使用 `DBConnector` 初始化 ODBC 连接
4. 顺序执行初始化 SQL
5. 通过 `JobExecutor` 并发执行事务型 SQL
6. 使用 `ResultHandler` 验证实际结果与期望结果
7. 使用 `Outputter` 输出日志与测试汇总
