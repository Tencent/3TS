### 简介

该文档负责三个文件的说明，包括了代码功能、整体结构、与其他文件的关系和在项目中的作用等。



## case_cntl.cc 

该文件负责解析和处理测试文件中的SQL语句和事务ID，帮助读取和解析输入文件中的命令和数据，并将结果输出。

### 整体结构：

头文件引入，名为`case_cntl.h`

```c++
#include "case_cntl.h"
```



全局对象：outputter`和`result_handler，分别用于输出和结果处理

```c++
Outputter outputter;
ResultHandler result_handler;
```



### 函数声明和定义

- `CaseReader`类

   包含多个成员函数，用于处理SQL语句的执行和错误信息的获取。

  - `TxnIdAndSql`：用于获取特定行的事务ID和SQL语句，解析输入行，提取执行顺序ID、交易事务ID和SQL语句。
  - `SqlIdAndResult`：解析输入行，提取 SQL ID 及其预期结果
  - `Isolation`：提取字符串行中的隔离级别。
  - `InitTestSequenceAndTestResultSetList`：根据提供的测试路径和数据库类型，初始化测试序列和结果集(casecntl)。
  - `TestSequenceAndTestResultSetFromFile`：解析提供的测试文件以提取测试序列及其相应的预期结果。

- `ResulHandler`类

  主要负责比较SQL查询结果，并确定它们是否符合预期结果。以下是该类中关键函数的声明和定义：

  - `IsSqlExpectedResult`：用于将当前 SQL 结果与预期 SQL 结果进行比较。
  - `IsTestExpectedResult`：用于将当前测试结果与一组预期测试结果进行比较。

- `Outputter`类

  主要负责将各种类型的输出写入文件，特别是与测试结果相关的输出。

  - `WriteResultTotal`：该函数功能是将测试结果集的摘要信息写入指定的文件。它从每个 `TestResultSet` 对象中提取简化的测试结果，并将每个测试用例类型及其对应的结果写入到提供的输出文件中。
  - `PrintAndWriteTxnSqlResult`：该函数功能是将当前 SQL 结果与一组预期结果进行比较，并将比较结果输出到控制台和文件。
  - `WriteTestCaseTypeToFile`：该函数功能是将给定的结果类型附加写入到指定的文件中，并在结果类型前添加 "Test Result:" 前缀进行说明。

### 项目中的作用

该文件的整体结构和内容围绕SQL语句的解析和执行，提供了一系列工具函数和类方法来处理SQL相关的操作和测试结果的比较。通过详细的函数定义和类方法，实现了对SQL语句执行过程中的各种操作和错误处理。

#### 	

## sqltest.cc 

该文件用于执行多线程SQL事务测试的，主要功能是通过多线程环境下执行多组SQL事务，并验证其结果。

### 整体结构

头文件和库引入

```c++
#include "gflags/gflags.h"
#include "sqltest.h"
#include <thread>
#include <unistd.h>
#include <mutex>
#include <regex>
```

包括必要的头文件，`gflags/gflags.h`、`sqltest.h`、`thread`、`unistd.h`、`mutex`、`regex`。

命令行参数定义，使用gflags库定义了一系列命令行参数，如数据库类型、用户名、密码、数据库名、连接池大小、事务隔离级别、测试用例目录、超时时间等。

全局变量

定义了一个全局的互斥锁向量`mutex_txn`，用于管理不同事务的锁。

### 函数声明和定义

- **`try_lock_wait`**：尝试在给定超时时间内获取指定的互斥锁，用于多线程锁机制。
- **`MultiThreadExecution`**：在多线程环境下执行一组SQL事务。该函数使用 `DBConnector` 类来执行SQL语句，并在事务执行过程中进行锁操作和错误处理。
- **`JobExecutor::ExecTestSequence`**：执行一系列数据库测试事务，并将结果写入指定文件。该函数通过调用`MultiThreadExecution`函数来实现多线程事务执行。

### 项目中的作用

**数据库类型的支持**：支持多种数据库类型（如MySQL、PostgreSQL、Oracle等）的事务测试。

**多线程事务执行**：通过多线程机制执行事务，模拟高并发环境下的事务处理情况。

**事务隔离级别测试**：支持不同的事务隔离级别，验证数据库在不同隔离级别下的行为。

**结果验证与输出**：执行事务后，将测试结果写入指定的输出文件中，以供后续分析和验证。



## sql_cntl.cc 

该文件用于执行SQL语句并处理数据库连接。文件中定义了一些工具函数和一个主要的数据库连接器类`DBConnector`。

### 整体结构

头文件和库引入

```c++
#include "sql_cntl.h"
#include <time.h>
#include <chrono>
#include <string>
#include <regex>
```

工具函数的定义：用于时间获取、字符串替换和类型转换等。

DBConnector类的定义：处理SQL语句执行、错误信息获取和结果集处理等。



### 对象声明和定义

- `DBConnector`类

  包含多个成员函数，用于处理SQL语句的执行和错误信息的获取。

  - `ErrInfoWithStmt`：用于获取特定句柄的错误信息。
  - `SqlExecuteErr`：处理SQL执行的错误信息。
  - `ExecWriteSql`：执行写入类型的SQL语句。
  - `ExecReadSql2Int`：执行读取类型的SQL语句并处理结果。



### 函数声明和定义

- **`get_current_time`**：该函数用于获取当前时间。
- **`replace`**：该函数用于进行字符串替换。
- **`SQLCHARToStr`**：该函数用于将`SQLCHAR`类型转换为`std::string`类型。
- **`DBConnector::ErrInfoWithStmt`**：该函数用于获取错误信息。
- **`DBConnector::SqlExecuteErr`**：该函数用于处理SQL执行返回值。
- **`DBConnector::ExecWriteSql`**：该函数用于执行写入SQL语句。
- **`DBConnector::ExecReadSql2Int`**：该函数用于执行读取SQL语句并处理结果集。



### 项目中的作用

该文件在整个项目中主要负责与数据库的交互，执行SQL语句，并在执行过程中进行错误处理和日志记录。通过提供对数据库操作的封装和抽象，`DBConnector`类使得上层应用程序可以更简便地进行数据库操作，同时提供详细的错误信息以便于调试和维护。



## 文件之间的关系

1. `sql_cntl.cc` 和 `sqltest.cc` ：
   - `sqltest.cc` 依赖于 `sql_cntl.cc` 中的 `DBConnector` 类来执行具体的SQL操作。`sqltest.txt` 使用 `DBConnector` 类的方法来执行SQL语句，并在多线程环境下进行事务测试。
2. `sql_cntl.cc` 和 `case_cntl.cc` ：
   - `case_cntl.cc` 中的解析结果（SQL语句和事务ID）会通过某种方式传递给 `sqltest.cc` 中的测试执行函数。而这些SQL操作实际是由 `sql_cntl.cc` 中的 `DBConnector` 类完成的。
3. `sql_test.cc` 和 `case_cntl.cc` ：
   - `case_cntl.cc` 负责读取和解析测试用例文件，提取出SQL语句和事务ID。
   - 这些解析后的数据会被传递给 `sqltest.cc`，然后由 `sqltest.cc` 进行多线程的事务执行测试
