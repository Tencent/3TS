test_cases: it inclucdes 33 test cases that show in the paper, and some other cases from known anomalies, and predicate anomalies.
centrailized_result: the result are from single machine node
distributed_result: the result are from the distributed environment

Each database result includes 
(1) a summary for all tested result in <result_summary> folder
(2) test results at different isolation levels

Test case structure:
Preparation: delete and construct table and rows
Execution: expected execution sequence
Validation: all serializable result

Test result structure:
Execution: request and real exeuction with timestamps
Result: consistent (rollback or serializable execution) and inconsistent (non-serializable execution) test result

Test databases:
MySQL, MyRocks, TDSQL, SQL Server, TiDB, Oracle, Oceanbase, Greenplum, PostgreSQL, CockroachDB, MongoDB