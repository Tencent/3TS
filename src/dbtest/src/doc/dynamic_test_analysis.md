# 3TS-COO 动态测试代码阅读与分析文档
1
1.文件列表与目录结构
|文件名|路径|说明|
case_cntl_v2.cc|src/dbtest/src/case_cntl_v2.cc|测试用例控制逻辑|
sqltest_v2.cc  |src/dbtest/src/sqltest_v2.cc  |动态SQL测试主流程|
sql_cntl_v2.cc |src/dbtest/src/sql_cntl_v2.cc |数据库链接与执行控制|
其他辅助文件：
- case_cntl.cc / sql_cntl.cc /sqltest.cc:旧版实现（兼容保留）
- common.*:公共工具与数据结构
- *.py:MongoDB与MDA相关脚本
