### 简介

该文档负责三个文件的说明，包括了代码功能、整体结构、与其他文件的关系和在项目中的作用等。



## case_cntl.cc 

该文件负责解析和处理测试文件中的SQL语句和事务ID，帮助读取和解析输入文件中的命令和数据，并将结果输出。

### 整体结构：

首先引入头文件，接着定义全局对象：outputter`和`result_handler，分别用于输出和结果处理。再进行函数声明及定义，包括`CaseReader`类里处理SQL语句的执行和错误信息的获取函数，`ResulHandler`类里比较SQL查询结果多函数，`Outputter`类将与测试结果相关的输出写入指定的文件的函数。

#### 函数声明和定义

- `CaseReader`类
  - `TxnIdAndSql`：用于获取特定行的事务ID和SQL语句，解析输入行，提取执行顺序ID、交易事务ID和SQL语句。
   - `SqlIdAndResult`：解析输入行，提取 SQL ID 及其预期结果
  - `Isolation`：提取字符串行中的隔离级别。
  - `InitTestSequenceAndTestResultSetList`：根据提供的测试路径和数据库类型，初始化测试序列和结果集(casecntl)。
  - `TestSequenceAndTestResultSetFromFile`：解析提供的测试文件以提取测试序列及其相应的预期结果。
  
- `ResulHandler`类
- `IsSqlExpectedResult`：用于将当前 SQL 结果与预期 SQL 结果进行比较。
  - `IsTestExpectedResult`：用于将当前测试结果与一组预期测试结果进行比较。

- `Outputter`类
- `WriteResultTotal`：将测试结果集的摘要信息写入指定的文件。
  - `PrintAndWriteTxnSqlResult`：将当前 SQL 结果与一组预期结果进行比较，并将比较结果输出到控制台和文件。
- `WriteTestCaseTypeToFile`：将给定的结果类型附加写入到指定的文件中，并在结果类型前添加 "Test Result:" 前缀进行说明。

### 项目中的作用

该文件的整体结构和内容围绕SQL语句的解析和执行，提供了一系列工具函数和类方法来处理SQL相关的操作，比较测试结果。

#### 	

## sqltest.cc 

该文件用于执行多线程SQL事务测试的，主要功能是通过多线程环境下执行多组SQL事务，并验证其结果。

### 整体结构

首先进行头文件和库引入，接着使用gflags库定义了一系列命令行参数，如数据库类型、用户名、密码、数据库名、连接池大小、事务隔离级别、测试用例目录、超时时间等。定义了一个全局的互斥锁向量`mutex_txn`，用于管理不同事务的锁。再定义函数。

#### 函数声明和定义

- **`try_lock_wait`**：尝试在给定超时时间内获取指定的互斥锁，用于多线程锁机制。
- **`MultiThreadExecution`**：在多线程环境下执行一组SQL事务。该函数使用 `DBConnector` 类来执行SQL语句，并在事务执行过程中进行锁操作和错误处理。
- **`JobExecutor::ExecTestSequence`**：执行一系列数据库测试事务，并将结果写入指定文件。该函数通过调用`MultiThreadExecution`函数来实现多线程事务执行。

### 项目中的作用

**数据库类型的支持**：支持多种数据库类型（如MySQL、PostgreSQL、Oracle等）的事务测试。

**多线程事务执行**：通过多线程机制执行事务，模拟高并发环境下的事务处理情况。

**事务隔离级别测试**：支持不同的事务隔离级别，验证数据库在不同隔离级别下的行为。

**结果验证与输出**：执行事务后，将测试结果写入指定的输出文件中，以供后续分析验证。



## sql_cntl.cc 

该文件用于执行SQL语句并处理数据库连接。

### 整体结构

首先定义头文件和库引入，接着定义工具函数，用于时间获取、字符串替换和类型转换等。再进行函数声明及定义，包括`DBConnector`类处理SQL语句的执行的函数，获取时间，替换字符串等功能的函数。

#### 函数声明和定义

- `DBConnector`类
  - `ErrInfoWithStmt`：用于获取特定句柄的错误信息。
  - `SqlExecuteErr`：处理SQL执行的错误信息。
  - `ExecWriteSql`：执行写入类型的SQL语句。
  - `ExecReadSql2Int`：执行读取类型的SQL语句并处理结果。

- **`get_current_time`**：用于获取当前时间。

- **`replace`**：进行字符串替换。

- **`SQLCHARToStr`**：用于将`SQLCHAR`类型转换为`std::string`类型。

  

### 项目中的作用

该文件在整个项目中主要负责与数据库的交互，执行SQL语句，并在执行过程中进行错误处理和日志记录。



## 文件之间的关系

1. `sql_cntl.cc` 和 `sqltest.cc` ：
   - `sqltest.cc` 依赖于 `sql_cntl.cc` 中的工具和辅助函数来执行具体的SQL操作，`sqltest.cc`利用 `sql_cntl.cc` 定义的结果处理和输出函数来验证正确性，并报告结果。`sqlcntl.txt`负责具体的SQL执行逻辑，为`sqltest.txt`提供支持。
2. `sql_cntl.cc` 和 `case_cntl.cc` ：
   - `case_cntl.cc` 的函数和类读取测试用例、解析预期结果以及将实际结果与这些预期进行比较。 `sqltest.cc` 负责执行SQL语句的逻辑，并调用这些比较函数。
3. `sql_test.cc` 和 `case_cntl.cc` ：
   - `case_cntl.cc` 负责读取和解析测试用例文件，和预期结果预期结果。这些解析后的数据会被传递给 `sqltest.cc`，然后由 `sqltest.cc` 进行多线程的事务执行测试。
