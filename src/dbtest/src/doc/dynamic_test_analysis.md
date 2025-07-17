# 3TS-COO 动态测试代码阅读与分析文档
1

## 1. 文件列表与目录结构
- case_cntl_v2.cc  
- sqltest_v2.cc  
- sql_cntl_v2.cc  
位于 `src/dbtest/src/` 目录下。
1.文件列表与目录结构
|文件名|路径|说明|
case_cntl_v2.cc|src/dbtest/src/case_cntl_v2.cc|测试用例控制逻辑|
sqltest_v2.cc  |src/dbtest/src/sqltest_v2.cc  |动态SQL测试主流程|
sql_cntl_v2.cc |src/dbtest/src/sql_cntl_v2.cc |数据库链接与执行控制|
其他辅助文件：
- case_cntl.cc / sql_cntl.cc /sqltest.cc:旧版实现（兼容保留）
- common.*:公共工具与数据结构
- *.py:MongoDB与MDA相关脚本


cat >> doc/dynamic_test_analysis.md <<'EOF'

## 2. sqltest_v2.cc 功能概述
- 入口函数：`int main(int argc, char **argv)` 位于第 368 行  
- 线程模型：  
  1. 解析命令行参数（测试路径、数据库类型、并发线程数等）  
  2. 初始化日志 & 全局数据库连接池  
  3. 为每个测试用例启动一个 `std::thread`（`ThreadEntry`）  
- 核心测试循环（`RunTestLoop`）：  
  1. 通过 `CaseReader` 读取 `*.txt` 测试用例  
  2. 调用 `sql_cntl_v2` 接口执行 SQL  
  3. 收集结果 → 与预期比对 → 写日志 → 统计异常  
- 异常处理：连接失败、SQL 语法错误、超时均记录到 `logs/` 目录

**调用链流程图**
sqltest_v2.cc::main()
├─ ParseCLI()
├─ InitLogger()
├─ InitDBConnection()
└─ StartThreads()
└─ ThreadEntry()
└─ RunTestLoop()
├─ ReadTestCase()
├─ ExecuteSQL()
├─ CollectResult()
└─ RecordLog()
EOF

## 2. case_cntl_v2.cc 功能概述
- 无 `main()`，纯工具类实现。  
- 核心类：
  - `CaseReader`：  
    - `TxnIdAndSql()`        —— 从单行文本提取 “执行序号-事务ID-SQL”。  
    - `SqlIdAndResult()`     —— 提取 “SQL-ID-预期结果”。  
    - `Isolation()`          —— 提取隔离级别。  
    - `TestSequenceAndTestResultSetFromFile()` —— 读取整个测试文件，生成 `TestSequence` 与 `TestResultSet`。  
    - `InitTestSequenceAndTestResultSetList()` —— 批量读取 `do_test_list.txt` 中列出的所有测试用例。  
  - `ResultHandler`：  
    - `IsSqlExpectedResult()` —— 单条 SQL 结果比对。  
    - `IsTestExpectedResult()`—— 整个测试集结果比对。  
  - `Outputter`：  
    - `WriteResultTotal()`、`PrintAndWriteTxnSqlResult()` 等 —— 结果输出与日志落盘。

- 与旧版差异：  
  - 新版不再读取预期结果（动态测试用例不给出结果）。  
  - 支持参数化表个数提取（正则解析 "Parameters=xx"）。  

- 调用链（伪代码） 
sqltest_v2.cc::main()
└─ CaseReader::InitTestSequenceAndTestResultSetList()
├─ for each test_case in do_test_list.txt
│  └─ CaseReader::TestSequenceAndTestResultSetFromFile()
│     ├─ TxnIdAndSql()
│     ├─ SqlIdAndResult()
│     └─ Isolation()
└─ ResultHandler::IsTestExpectedResult()
├─ IsSqlExpectedResult()
└─ Outputter::WriteResultTotal()
