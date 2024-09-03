## random_do_list.py

### 文件概述

`random_do_list.py` 基于预定义的操作集随机生成一系列数据库操作，此外还要确保 生成的操作列表不含任何非法模式。此脚本文件会生成 `do_test_list.txt` 文件，该文件包含不同的测试模式，即具体测试文件列表。

op_set = ["P", "R", "W", "I"] # operations involved in the case

op_set 定义了四种数据库事务操作符。分别是：

* **P (Predicate)**: 谓词操作，用于描述查询中的条件。
* **R (Read)**: 读取操作，用于从数据库中读取数据。
* **W (Write)**: 写入操作，用于向数据库中写入或更新数据。
* **I (Insert)**:  插入操作，用于将新数据插入到数据库中。

在 mda_generate.py 中定义了一个 execute_sql 函数，用来定义执行的语句，它支持的 7 种 SQL 操作，具体可见函数注释。

```python
"""
The supported SQL operations include:
- Write (W)
- Read (R)
- Predicate (P)
- Insert (I)
- Delete (D)
- Abort (A)
- Commit (C)
"""
```

### 变量定义

- `op_set`：定义了脚本中使用的操作类型集合，包含读（"R"）、写（"W"）、插入（"I"）、谓词（"P"）等操作。
- `illegal_op_set`：定义了不允许出现的操作模式集合，因为这些模式在实际数据库事务中不可能出现或没有意义。

### 函数定义

- **dfs(data_count, total_num, target_num)**：使用深度优先搜索算法来决定哪些变量需要被操作两次，以满足测试序列中的操作总数要求。
- **dfs1(data_count, res, total_num)**：递归函数，根据当前的 `data_count`生成测试用例。它尝试不同的操作组合来创建测试序列。

## mda_generate.py

### 文件概述

`mda_generate.py` 从 `do_test_list.txt` 文件中读取所需的操作模式（例如 `RW-RR` 等）， 针对每种操作模式生成一个测试用例文件。

每个测试用例的格式为  `sql_id-txn_id-sql`，`sql_id` 是语句的递增表示，`txn_id` 标识执行语句的事务。 `sql` 为具体的语句。

`mda_generate.py` 生成测试用例文件的过程大致可以分为三个阶段：

* 初始化阶段： 生成测试用例文件的表头描述、初始化表和插入测试所需数据。
* 执行阶段：执行事务和验证的 `SQL` 语句。主要目的是为不同的操作模式自动生成数据库测试用例。
* 提交阶段：提交所有事务。

`mda_generate.py` 针对上述的三个阶段分别生成测试用例写入到对应的测试用例文件里面。

#### 初始化阶段

首先会调用 write_description 编写测试用例的描述。之后会调用 init_table 初始化表结构。这里如果是分布式测试并且数据库是 tdsql 或者是 crdb。则会针对每个事务都创建一个表。

之后调用 insert_data 插入之后执行事务所使用的数据。insert_data 会根据提供的参数生成SQL插入语句，并将该语句写入指定的文件。该函数会使用名为 exist 的数组来管理数据元素的存在，以防止重复插入，同时他会更新 SQL 操作计数。

#### 执行阶段

执行阶段主要调用的方法是 execute_txn。这个函数会根据 `do_test_list.txt`中的操作模式生成执行操作事务的SQL语句，当遇到特定的操作模式时，该函数会重新排序操作语句以尽量解决冲突。

在 execute_txn 中有两个很关键的结构体。一个是 `data_op_list` 用来存储数据操作的列表。一个是 `wait_op_list` 用来存储需要等待执行的操作列表。当有操作需要延时执行的时候，这个操作就会被放入到  `wait_op_list `里面。

#### 提交阶段

提交阶段会针对每个事务生成提交语句，并将他们写入到指定的测试用例文件里。

### 类定义

- **OptionException**: 自定义异常类。
- **Txn**: 事务类，包含开始和结束时间戳。
- **Operation**: 操作类，表示一个数据库操作，包含操作类型和事务编号。
- **Wait_Operation**: 等待操作类，扩展了Operation类，添加了操作编号。

### 函数定义

- **init_table(file_name, sql_count, txn_count, table_num, db_type, test_type)**: 初始化数据库表，根据数据库类型和测试类型创建表结构，并写入SQL语句到文件。
- **check_concurrency(txn_num1, txn_num2, txn)**: 检查两个事务是否并发。
- **check_exist_op(txn, data_op_list, op, op_num, txn_count)**: 检查特定类型的操作是否存在于事务中，并且是并发的。
- **execute_sql(IsPredicate, file_name, sql_count, txn_count, op_num, op, data_num, txn, data_value, data_op_list)**: 执行SQL操作，如写入（W）、读取（R）、插入（I）、删除（D）、中止（A）和提交（C）操作。
- **insert_data(...)**: 插入数据到表中，并处理可能的异常情况，例如数据已存在。
- **delete_data(...)**: 从表中删除数据，并记录删除操作。
- **write_data(...)**: 写入数据到表中，通常将数据值增加1，并记录写入操作。
- **read_data(...)**: 从表中读取数据，并记录读取操作。
- **read_data_predicate(...)**: 使用范围条件从表中读取数据。
- **abort_txn(...)**: 中止事务，并写入回滚SQL语句。
- **commit_txn(...)**: 提交事务，并写入提交SQL语句。
- **execute_check(file_name, sql_count, txn_count, data_num, table_num)**: 执行检查事务，用于验证数据的一致性。
- **get_last_op(ops, pos)**: 获取操作列表中给定位置前的最后一个操作。
- **execute_first(...)** 和 **execute_second(...)**: 分别处理Partial Order Pair (POP)的第一次和第二次操作。
- **execute_txn(...)**: 执行事务，考虑操作顺序以解决冲突。
- **write_description(file_name, txn_num, op_num, data_num)**: 向文件写入测试用例的描述。

## mda_detect.py

### 文件概述

mda_detect.py 主要是对测试框架(sqltest_v2) 跑出来的结果进行分析，检测数据库事务并发控制中的循环依赖。进而本次并发事务是否满足可串行化。理论原理就是一个调度序列，是冲突可序列化的，当且仅当其依赖图是无环的。

> *关于这一点的数学证明，这里不展开，感兴趣的可以自行查看：* [Proof of correctness of the precedence graph test](http://www.cs.emory.edu/~cheung/Courses/554/Syllabus/7-serializability/graph-conflict2.html)

#### 主要逻辑

sqltest_v2 针对每个测试用例文件都会生成一个并发调度的结果文件。mda_detect.py 负责对于每个文件读取，并记录事务操作、边、入度、访问状态等。

* 如果读取中间检查到了错误信息。则输出错误结果并跳过后续步骤。
* 如果没有错误消息，则删除未完成的操作，构建依赖图并计算入度。

  * 如果未标记为 `go_end`，则检查图中是否存在循环。
  * 如果存在循环，输出循环结果并调用 `dfs` 方法进行深度优先搜索，记录路径。
  * 如果不存在循环，输出结果并打印路径

### 类定义

- **Edge**: 表示一个边，包含类型和输出。
- **Operation**: 表示一个操作，包含操作类型、事务编号、操作时间和值。
- **Txn**: 表示一个事务，包含开始时间和结束时间。

### 函数定义

- **get_total(lines)**: 计算查询中的最大变量数。
- **find_data(query, target)**: 从查询中提取数据。
- **set_finish_time(op_time, data_op_list, query, txn, version_list)**: 设置操作的结束时间，并更新事务列表和版本列表。
- **check_concurrency(data1, data2, txn)**: 检查两个事务是否并发。
- **get_edge_type(data1, data2, txn)**: 确定两个操作之间的边类型。
- **build_graph(data_op_list, indegree, edge, txn)**: 构建表示操作并发关系的有向图。
- **insert_edge(data1, data2, indegree, edge, txn)**: 向有向图中插入边。
- **init_record(query, version_list)**: 根据查询初始化版本列表中的记录。
- **readVersion_record(query, op_time, data_op_list, version_list)**: 读取版本化记录并更新操作。
- **read_record(op_time, txn_num, total_num, txn, data_op_list)**: 读取记录并更新数据操作。
- **write_record(op_time, txn_num, txn, data_op_list)**: 写入记录并更新数据操作。
- **delete_record(op_time, txn_num, txn, data_op_list)**: 删除记录并更新数据操作。
- **insert_record(op_time, txn_num, txn, data_op_list)**: 插入记录并更新数据操作。
- **end_record(op_time, txn_num, txn)**: 设置事务的结束时间戳。
- **operation_record(total_num, query, txn, data_op_list, version_list)**: 记录和处理数据库操作。
- **remove_unfinished_operation(data_op_list)**: 移除未完成的操作。
- **check_cycle(edge, indegree, total)**: 使用拓扑排序检查有向图中的循环。
- **dfs(result_folder, ts_now, now, type)**: 执行深度优先搜索以查找并打印有向图中的循环。
- **print_path(result_folder, ts_now, edge)**: 打印有向图中的路径。
- **output_result(file, result_folder, ts_now, IsCyclic)**: 将循环检测结果输出到结果文件。
- **print_error(result_folder, ts_now, error_message)**: 将错误消息打印到结果文件。
