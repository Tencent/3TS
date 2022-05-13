![](assets/logo.png)

## Introduction

![](https://img.shields.io/badge/license-GPLv3-brightgreen)

**Tencent Transaction Processing Testbed System (3TS)** that is jointly developed by Tencent's CynosDB (TDSQL) team and the Key Laboratory of Data Engineering and Knowledge Engineering of the Ministry of Education of Renmin University of China. The system aims to design and construct a unified framework for transaction processing (including distributed transactions). It enables users to quickly build new concurrency control approaches via the access interface provided by the framework. Based on an comprehensive experiment study over the benchmarks, and the applications abstracted, users can select an optimal concurrency control approach. At present, 3TS has been integrated 13 mainstream concurrency control approaches, and provides common benchmarks such as TPC-C、PPS and YCSB. 3TS further provides a consistency level test benchmark, to address the issue of system selection difficulty caused by the blowout development of distributed database systems, and provides consistency level discrimination and performance test comparison.

If you want to better understand the aims of our project, please view [3TS opensource announcement](doc/en/announcement.md).

## Coo: Consistency Check

To test the consistency of real databases.
We generate the anomaly history, and simulate multi-users transcation reqeusting to databases.

Check out some test cases (e.g., [Dirty Write](test_result/test_cases/wat_sda_dirty_write_2commit.txt)) and result (e.g., [passed by MySQL](test_result/centralizend_result/mysql/serializable/wat_sda_dirty_write_2commit.txt) at Serializable Level).


## Usage
To generate Makefile (all commands are executed under '3TS/src/dbtest'):
```
cmake -S ./
```

To complie the code:
```
make
```

## Example

For test cases, it specify in "do_test_list.txt". Use "#" to exclude (comment) the test case.
We provide three levels of test cases, i.e., the basic cases (33 anomalies in the paper), the predicate cases, and the MDA cases (with multiple transactions and objects). For specific test cases to evaluate, we specify it in do_test_list.txt. 

```
// to test all test cases
cp t/bk_do_test_list_all.txt do_test_list.txt 

// to test only basic cases
cp t/bk_do_test_list_basic.txt do_test_list.txt 

// to test only predicate cases
cp t/bk_do_test_list_predicate.txt do_test_list.txt 
```

Edit "auto_test.sh" for database configurations (e.g., username, password). Edit "auto_test_all.sh" for databse (e.g., PostgreSQL, and MySQL) and isolation (e.g., SERIALIZABLE, REPEATABLE READ, READ COMMITTED, and READ UNCOMMITTED) configuration.


To run the test (under '3TS/src/dbtest'):
```
./auto_test_all.sh
```

## Analyze an anomaly case run by PostgreSQL

### Setup PostgreSQL
Please check out the general environment [setup](doc/en/setup_db_environment) and PostgreSQL [setup](doc/en/setup_db_postgresql).

Check if it successfully connects to PostgreSQL server by isql, 
```
isql pg -v
```
Once the connected information showed, we are able to run our code to test designed anomaly schedules.

The anomaly test cases are Write-read Skew and Write-read Skew Committed, the schedules are as follows:
Write-read Skew : $W_1[x_1]W_2[y_1]R_2[x_1]R_1[y_1]$
Write-read Skew Committed : $W_1[x_1]W_2[y_1]R_2[x_1]C_2R_1[y_1]$

The test result are in the following:
| Isolation level      | Write-read Skew ([SQL](test_result/test_cases/rat_dda_write_read_skew.txt)) | Write-read Skew Committed ([SQL](test_result/test_cases/rat_dda_write_read_skew_committed.txt)) |
| ----------- | ----------- | ----------- |
| Serializable      | Rollback ([result](test_result/centralizend_result/pg/serializable/rat_dda_write_read_skew.txt)) | Rollback ([result](test_result/centralizend_result/pg/serializable/rat_dda_write_read_skew_committed.txt)) |
| Repeatable Read   | Anomaly reported ([result](test_result/centralizend_result/pg/repeatable-read/rat_dda_write_read_skew.txt)) | Anomaly reported ([result](test_result/centralizend_result/pg/repeatable-read/rat_dda_write_read_skew_committed.txt)) |
| Read (Un)Committed   | Anomaly reported ([result](test_result/centralizend_result/pg/read-committed/rat_dda_write_read_skew.txt)) | Pass the test ([result](test_result/centralizend_result/pg/read-committed/rat_dda_write_read_skew_committed.txt)) |

At serializbale level, both tested cases detected consecutive RW conflicts.
At Repeatable Read level, both generated two consecutive RW confilcts and allowed.
At Read (Un)Committed level, Write-read Skew test case generated two consecutive RW ($R_2W_1,R_1W_2$) conflicts while Write-read Skew Committed test case read the newest committed version and executed into non-anomaly schedule ($R_2W_1,W_2C_2R_1$).


## License

GPLv3 @ Tencent
