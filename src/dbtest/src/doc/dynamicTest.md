## 简介
在这个模块里，我们提供了动态测试不同数据库的框架。你可以用python 脚本文件(random_do_list.py、 mda_detect.py、mda_generate.py)自行生成输入测试文件。动态测试具体实现在（3TS/src/​dbtest/​src）下的case_cntl_v2.cc、sqltest_v2.cc、sql_cntl_v2.cc。

## 测试文件生成
运行random_do_list.py
```shell
python random_do_list.py data_num target_num
```
脚本会生成do_test_list.txt文件，里面内容形如
```
PW0-PW1
```
指示了操作类型

接下来运行mda_generate.py
```shell
python mda_generate.py db_type test_type
```
db_type有tdsql,crdb等，test_type有single,dist

运行mad_detect.py进行错误检测
```shell
python mad_detect.py
```
## 测试文件格式
静态测试首先声明ParamNum，即结果集有多少列
下面输入testSequence，格式为：执行顺序ID-事务ID-SQL语句
接下来输入隔离级别，例如serializable{...}
括号里面是像"9-0,1 1,1"的对，其中 9 是 SQL ID，"(0,1) (1,1)" 是结果。
文件可以在#后添加注释。
动态测试声明Parameters和testSequence，但是没有期望结果序列。
Parameters格式类似如下：

```
#Parameters: #column=2 #txn=2 #operations=2 #variable=2
```

## case_cntl_v2.cc 
实现了测试用例控制功能，包含从测试文件中读取数据库配置、sql 语句和预期结果，验证处理实际结果和预期结果的区别，输出比对结果到指定文件夹等功能。
代码首先通过TestSequenceAndTestResultSetFromFile函数读取指定的测试文件。这个函数解析测试文件的内容，提取测试序列和结果集信息。在读取测试文件的过程中，对每一行进行解析。根据行的内容，执行不同的操作：
如果行包含"Parameters"，则提取参数数量，并设置到测试序列对象中。<br> 
如果行以"#"开头，表示这是一条注释，跳过这行。<br> 
其他情况下，假设行包含事务ID和SQL语句，使用TxnIdAndSql函数解析这行，提取事务ID、SQL ID和SQL语句，然后创建一个TxnSql对象，并添加到测试序列中。<br> 
初始化测试序列和结果集列表：InitTestSequenceAndTestResultSetList函数读取"do_test_list.txt"文件，该文件包含要执行的测试案例列表。对于列表中的每个测试案例，函数会读取相应的测试文件，并将解析得到的测试序列和结果集添加到内部存储结构中。<br> 
最后调用IsSqlExpectedResult比较SQL查询的当前结果和预期结果。这个比较是逐项进行的。如果当前结果和预期结果的大小不同，或者任何一项不匹配，函数返回false，表示结果不符合预期。否则，返回true，表示测试通过。

数据结构：
1.Outputter：提供将测试用例和结果数据写入文件的函数
2.ResultHandler：处理和验证期望的测试结果

函数：<br> 
和v1静态测试相同的有：<br> 
CaseReader::TxnIdAndSql:解析给定的行以提取执行顺序ID、事务ID和SQL语句。 <br> 
CaseReader::SqlIdAndResult：解析给定的行以提取SQL ID及其预期结果。<br>
CaseReader::Isolation：解析隔离级别。<br>
CaseReader::InitTestSequenceAndTestResultSetList：同v1,基于提供的测试路径和数据库类型初始化 TestSequence 和 TestResultSet 列表。<br>
ResultHandler::IsSqlExpectedResult，ResultHandler::IsTestExpectedResult：比较测试结果和期望结果<br>
Outputter::WriteResultTotal，Outputter::WriteTestCaseTypeToFile，Outputter::WriteResultType：将测试结果写入文件<br>
不同：<br>
CaseReader::TestSequenceAndTestResultSetFromFile:读取文件并输出testSequence，和v1不同的是没有解析隔离级别和预期结果。<br>
Outputter::PrintAndWriteTxnSqlResult:输出事物结果，和v1不同的是只输出当前结果，没有和预期结果集的比对<br>

## sql_cntl_v2.cc
SQL控制或操作的代码。实现了与 ODBC 数据库的接口功能。包含设置数据库隔离级别、开始事务、执行增删改查等功能、处理 SQL 返回值并获取错 误信息、结束事务或回滚事务等功能。

这段代码首先使用DBConnector从连接池中获取对应会话ID的数据库连接，然后分配一个新的语句句柄。如果句柄分配失败，函数会调用DBConnector::SqlExecuteErr来获取错误信息，并输出错误提示。sql_id为1024会跳过打印输出功能然后，它会释放语句句柄并返回false，表示SQL执行失败。最终返回一个布尔值，表示SQL语句的执行是否成功。

数据结构：<br> 
DBConnector:管理所有和数据库连接相关功能，持有ODBC中的数据库连接句柄(Database Connection Handle)

和v1静态测试相同的有：<br> 
DBConnector::ErrInfoWithStmt：从ODBC句柄提取错误信息<br>
DBConnector::ExecReadSql2Int：执行SQL读语句并处理错误<br>
DBConnector::SQLStartTxn：启动事务并处理错误<br>
DBConnector::SetAutoCommit：数据库连接设为自动提交模式<br>
DBConnector::SetTimeout：设置连接timeout<br>
DBConnector::SetIsolationLevel:设置隔离级别<br>
不同：<br>
DBConnector::SqlExecuteErr：处理和记录执行错误，和v1不同的是输出了session id,如果因为莫名原因失败就返回空而没有信息<br>
DBConnector::ExecWriteSql：执行SQL写语句并处理错误，和v1不同的是使用SQLRowCount函数获取受影响的行数，如果没有受影响的行就输出错误<br>
DBConnector::SQLEndTnx：通过提交或回滚结束事务，和v1不同的是在SQL语句执行成功后会输出执行时间信息

## sqltest_v2.cc 
SQL测试的代码，包含项目的主函数。
首先利用gflags库来实现命令行参数解析。
初始化测试环境：
设置数据库连接池大小、事务超时时间等配置参数。
初始化数据库连接器（DBConnector）以连接到指定的数据库。
准备测试用的SQL语句和事务序列，这些可能包括读（SELECT）、写（INSERT, UPDATE, DELETE）、事务开始（BEGIN）和结束（COMMIT, ROLLBACK）等操作。采用多线程编程，每个线程负责一组数据库语句的执行，并且通过子线程不同的休眠时间实现程序代码按指定顺序运行。<br>
在执行过程中，将测试过程和结果记录到指定的日志文件中，以便后续分析和审计。完成所有SQL语句的执行后，释放相关资源，如关闭数据库连接、释放互斥锁等。汇总测试结果，包括成功执行的语句数量、失败的语句及其原因等。<br>
和v1不同的是不支持cassandra和yugabyte，也不支持它们的隔离级别定义
函数：
MultiThreadExecution：多线程执行SQL查询并返回结果，和v1不同的是没有对yugabyte和myrocks支持