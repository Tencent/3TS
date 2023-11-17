# /*
#  * Tencent is pleased to support the open source community by making 3TS available.
#  *
#  * Copyright (C) 2022 THL A29 Limited, a Tencent company.  All rights reserved. The below software
#  * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
#  * Tencent Modifications are Copyright (C) THL A29 Limited.
#  *
#  * Author:  xenitchen axingguchen tsunaouyang (xenitchen,axingguchen,tsunaouyang@tencent.com)
#  *
#  */

from operator import truediv
import os
import sys


class OptionException(Exception):
    pass


class Txn:
    def __init__(self):
        self.begin_ts = -1
        self.end_ts = max_time


class Operation:
    def __init__(self, op_type, txn_num):
        self.op_type = op_type
        self.txn_num = txn_num


class Wait_Operation:
    def __init__(self, op_type, txn_num, op_num):
        self.op_type = op_type
        self.txn_num = txn_num
        self.op_num = op_num


"""
Initialize tables for database testing.

Args:
- file_name (str): The name of the file where the SQL statements will be written.
- sql_count (int): The count of SQL statements to generate.
- txn_count (int): The count of transactions.
- table_num (int): The number of tables to create.
- db_type (str): The type of database being used.
- test_type (str): The type of test being conducted.

Returns:
int: The data_num value, which depends on the database type and determines the number of data columns in each table.

This function initializes tables for database testing by generating SQL statements and writing them to the specified 
file (`file_name`). The SQL statements include DROP TABLE IF EXISTS and CREATE TABLE statements for the specified 
number of tables (`table_num`).

The function takes into account the `db_type` and `test_type` to determine the structure of the created tables and 
the number of data columns. It returns the `data_num` value, which is an integer that depends on the database type 
and determines the number of data columns in each table.

"""
def init_table(file_name, sql_count, txn_count, table_num, db_type, test_type):
    data_num = 2
    with open(file_name, "a+") as file_test:
        for i in range(1, table_num + 1):
            drop_sql = str(sql_count) + "-" + str(txn_count) + "-" + "DROP TABLE IF EXISTS t" + str(i) + ";\n"
            file_test.write(drop_sql)
        if test_type == "single":
            for i in range(1, table_num + 1):
                # MySQL 5.1 add InnoDB for table
                # create_sql = str(sql_count) + "-" + str(txn_count) + "-" + "CREATE TABLE t" + str(i) + \
                            #  " (k INT PRIMARY KEY, v INT) ENGINE=InnoDB;\n"
                create_sql = str(sql_count) + "-" + str(txn_count) + "-" + "CREATE TABLE t" + str(i) + \
                             " (k INT PRIMARY KEY, v INT);\n"
                file_test.write(create_sql)
        elif db_type == "tdsql" or db_type == "ob_oracle":
            data_num = 4
            for i in range(1, table_num + 1):
                create_sql = str(sql_count) + "-" + str(txn_count) + "-" + "CREATE TABLE t" + str(i) + \
                             " (k INT, v INT, value1 INT, value2 INT, PRIMARY KEY (v,k)) PARTITION BY RANGE(v) " \
                             "(PARTITION p0 VALUES LESS THAN(2), PARTITION p1 VALUES LESS THAN(4));\n"
                file_test.write(create_sql)
        elif db_type == "crdb":
            data_num = 4
            for i in range(1, table_num + 1):
                create_sql = str(sql_count) + "-" + str(txn_count) + "-" + "CREATE TABLE t" + str(i) + \
                             " (k INT, v INT, value1 INT, value2 INT, PRIMARY KEY (v,k)) PARTITION BY RANGE(v) " \
                             "(PARTITION p0 VALUES FROM (MINVALUE) TO (2), " \
                             "PARTITION p1 VALUES FROM (2) TO (MAXVALUE));\n"
                file_test.write(create_sql)
        else:
            for i in range(1, table_num + 1):
                create_sql = str(sql_count) + "-" + str(txn_count) + "-" + "CREATE TABLE t" + str(i) + \
                             " (k INT, v INT) PARTITION BY RANGE(v) (PARTITION p0 VALUES LESS THAN(2), " \
                             "PARTITION p1 VALUES LESS THAN(4));\n"
                file_test.write(create_sql)
    return data_num


"""
Check if two transactions are concurrent based on their start and end times.

Args:
- data1: Information about the first transaction.
- data2: Information about the second transaction.
- txn: A list of transaction objects.

Returns:
bool: True if the transactions are concurrent, False otherwise.
"""
# if both transactions are running
# or the start time of the second transaction is less than the end time of the first transaction
# we think they are concurrent
def check_concurrency(txn_num1, txn_num2, txn):
    if txn[txn_num2].begin_ts < txn[txn_num1].end_ts:
        return True
    elif txn[txn_num1].begin_ts < txn[txn_num2].end_ts:
        return True
    else:
        return False


"""
Check if a specific operation type exists in a transaction.

Args:
- txn (list): The list of transactions.
- data_op_list (list): The list of data operations.
- op (str): The operation type to check for.
- op_num (int): The operation number to check.
- txn_count (int): The total number of transactions.

Returns:
bool: True if the specified operation type exists in the transaction and is concurrent; False otherwise.

This function checks if a specific operation type (`op`) exists in a transaction (`txn`) by examining 
the list of data operations (`data_op_list`) associated with that operation number (`op_num`). If the 
specified operation type exists in the transaction and is concurrent with other transactions, the 
function returns True; otherwise, it returns False.

The function is designed to help identify and handle concurrent operations in a transactional context.

"""
def check_exist_op(txn, data_op_list, op, op_num, txn_count):
    flag, txn_num = False, 0
    for data in data_op_list[op_num]:
        if data.op_type == op:
            flag = True
            txn_num = data.txn_num
    if flag:
        if check_concurrency(txn_num, txn_count, txn):
            return True
        return False
    return False


"""
Execute an SQL operation within a transaction.

Args:
- IsPredicate (bool): A flag indicating whether the operation is a predicate operation.
- file_name (str): The name of the file to write the SQL operation to.
- sql_count (int): The current count of SQL operations.
- txn_count (int): The total number of transactions.
- op_num (int): The operation number.
- op (str): The type of SQL operation to execute.
- data_num (int): The number of data elements involved in the operation.
- txn (list): The list of transactions.
- data_value (list): The values associated with the data elements.
- data_op_list (list): The list of data operations.

Returns:
int: The updated count of SQL operations after execution.

This function executes an SQL operation within a transaction context. It takes into account whether the 
operation is a predicate operation (IsPredicate flag) and writes the SQL operation to the specified file. 
The function also updates the SQL operation count and performs the necessary actions based on the type of
SQL operation.

The supported SQL operations include:
- Write (W)
- Read (R)
- Predicate (P)
- Insert (I)
- Delete (D)
- Abort (A)
- Commit (C)

The function returns the updated count of SQL operations after execution.

"""
def execute_sql(IsPredicate, file_name, sql_count, txn_count, op_num, op, data_num, txn, data_value, data_op_list):
    # if check_exist_op(txn, data_op_list, op, op_num, txn_count):
    #     return sql_count
    if op == "W":
        if IsPredicate:
            sql_count = write_data(file_name, sql_count, txn_count, op_num*2+1, data_num, txn, data_value, data_op_list)
        else:
            sql_count = write_data(file_name, sql_count, txn_count, op_num, data_num, txn, data_value, data_op_list)
    elif op == "R":
        if IsPredicate:
            sql_count = read_data(file_name, sql_count, txn_count, op_num*2+1, data_num, txn, data_op_list)
        else:
            sql_count = read_data(file_name, sql_count, txn_count, op_num, data_num, txn, data_op_list)
    elif op == "P":
        sql_count = read_data_predicate(file_name, sql_count, txn_count, op_num, data_num, txn, data_op_list)
    # I and D only works for single test_type now
    elif op == "I":
        sql_count = insert_data(file_name, sql_count, txn_count, op_num*2+1, 1, 1, data_num, exist, data_value)
    elif op == "D":
        sql_count = delete_data(file_name, sql_count, txn_count, op_num*2+1, 1, data_num, exist, txn, data_op_list)
    elif op == "A":
        sql_count = abort_txn(file_name, sql_count, txn_count, txn)
    elif op == "C":
        sql_count = commit_txn(file_name, sql_count, txn_count, txn)
    return sql_count


"""
Insert data into a table within a transaction.

Args:
- file_name (str): The name of the file to write the SQL insert statement to.
- sql_count (int): The current count of SQL operations.
- txn_count (int): The total number of transactions.
- cur_count (int): The current count for data insertion.
- partition_num (int): The partition number for the insert operation.
- insert_table (int): The table number to insert data into.
- data_num (int): The number of data elements to insert.
- exist (list): A list of flags indicating the existence of data elements.
- data_value (list): The values associated with the data elements.

Returns:
int: The updated count of SQL operations after the insert.

This function inserts data into a table within a transaction context. It generates an SQL insert statement
based on the provided parameters and writes the statement to the specified file. The function also updates
the SQL operation count and manages the existence of data elements to prevent duplicate inserts.

The function returns the updated count of SQL operations after the insert.

"""
def insert_data(file_name, sql_count, txn_count, cur_count, partition_num, insert_table, data_num, exist,
                data_value):
    with open(file_name, "a+") as file_test:
        try:
            #if exist[cur_count]:
            if False:
                raise OptionException
            else:
                # if it is not initialization, we need to pay attention to whether the transaction should be started
                if sql_count != 0 and txn[txn_count].begin_ts == -1:
                    txn[txn_count].begin_ts = sql_count
                    begin_sql = str(sql_count) + "-" + str(txn_count) + "-" + "BEGIN;\n"
                    file_test.write(begin_sql)
                    sql_count += 1
                exist[cur_count] = True
                if data_num == 2:
                    insert_sql = str(sql_count) + "-" + str(txn_count) + "-" + "INSERT INTO t" + \
                                 str(insert_table) + " VALUES (" + str(cur_count) + "," + str(cur_count) + ");\n"
                else:
                    insert_sql = str(sql_count) + "-" + str(txn_count) + "-" + "INSERT INTO t" + \
                                 str(insert_table) + " VALUES (" + str(cur_count) + "," + str(partition_num) + \
                                 "," + str(cur_count) + "," + str(cur_count) + ");\n"
                file_test.write(insert_sql)
                data_value[cur_count] = cur_count
        except OptionException:
            if data_num == 2:
                file_test.write("data" + " k=" + str(cur_count) + " already exists, can't insert")
                print("data" + " k=" + str(cur_count) + " already exists, can't insert")
            else:
                file_test.write("data" + " value1=" + str(cur_count) + " already exists, can't insert")
                print("data" + " value1=" + str(cur_count) + " already exists, can't insert")
    return sql_count


"""
Delete data from a table within a transaction.

Args:
- file_name (str): The name of the file to write the SQL delete statement to.
- sql_count (int): The current count of SQL operations.
- txn_count (int): The total number of transactions.
- cur_count (int): The current count for data deletion.
- delete_table (int): The table number to delete data from.
- data_num (int): The number of data elements to delete.
- exist (list): A list of flags indicating the existence of data elements.
- txn (list): A list of transactions.
- data_op_list (list): A list of data operation records.

Returns:
int: The updated count of SQL operations after the delete.

This function deletes data from a table within a transaction context. It generates an SQL delete statement 
based on the provided parameters and writes the statement to the specified file. The function also updates 
the SQL operation count, manages the existence of data elements, and records the delete operation in the 
data operation list.

The function returns the updated count of SQL operations after the delete.

"""
def delete_data(file_name, sql_count, txn_count, cur_count, delete_table, data_num, exist, txn, data_op_list):
    with open(file_name, "a+") as file_test:
        try:
            if txn[txn_count].end_ts != max_time:
                raise OptionException
            else:
                if txn[txn_count].begin_ts == -1:
                    txn[txn_count].begin_ts = sql_count
                    begin_sql = str(sql_count) + "-" + str(txn_count) + "-" + "BEGIN;\n"
                    file_test.write(begin_sql)
                    sql_count += 1
                exist[cur_count] = False
                if data_num == 2:
                    delete_sql = str(sql_count) + "-" + str(txn_count) + "-" + "DELETE FROM t" + \
                                 str(delete_table) + " WHERE k=" + str(cur_count) + ";\n"
                else:
                    delete_sql = str(sql_count) + "-" + str(txn_count) + "-" + "DELETE FROM t" + \
                                 str(delete_table) + " WHERE value1=" + str(cur_count) + ";\n"
                file_test.write(delete_sql)
                data_op_list[cur_count].append(Operation("D", txn_count))
        except OptionException:
            file_test.write("the transaction has ended and cannot be read")
            print("the transaction has ended and cannot be read")
    return sql_count


"""
Write data to a table within a transaction, incrementing its value by 1.

Args:
- file_name (str): The name of the file to write the SQL update statement to.
- sql_count (int): The current count of SQL operations.
- txn_count (int): The total number of transactions.
- op_num (int): The current operation number.
- data_num (int): The number of data elements to write.
- txn (list): A list of transactions.
- data_value (list): A list of data values.
- data_op_list (list): A list of data operation records.

Returns:
int: The updated count of SQL operations after the write.

This function writes data to a table within a transaction context, incrementing its value by 1. It generates
an SQL update statement based on the provided parameters and writes the statement to the specified file. 
The function also updates the SQL operation count, increments the data value, and records the write operation
in the data operation list.

The function returns the updated count of SQL operations after the write.

"""
# when updating data, increment its value by 1
def write_data(file_name, sql_count, txn_count, op_num, data_num, txn, data_value, data_op_list):
    with open(file_name, "a+") as file_test:
        try:
            #if not exist[op_num] or txn[txn_count].end_ts != max_time:
            if txn[txn_count].end_ts != max_time:
                raise OptionException
            else:
                if txn[txn_count].begin_ts == -1:
                    txn[txn_count].begin_ts = sql_count
                    begin_sql = str(sql_count) + "-" + str(txn_count) + "-" + "BEGIN;\n"
                    file_test.write(begin_sql)
                    sql_count += 1
                if data_num == 2:
                    write_sql = str(sql_count) + "-" + str(txn_count) + "-" + "UPDATE t1 SET v=" + \
                                str(data_value[op_num] + 1) + " WHERE k=" + str(op_num) + ";\n"
                else:
                    write_sql = str(sql_count) + "-" + str(txn_count) + "-" + "UPDATE t" + str(txn_count) + \
                                " SET value2=" + str(data_value[op_num] + 1) + " WHERE value1=" + \
                                str(op_num) + ";\n"
                file_test.write(write_sql)
                data_op_list[op_num].append(Operation("W", txn_count))
                data_value[op_num] += 1
        except OptionException:
            file_test.write("data doesn't exist or the transaction has ended and cannot be updated")
            print("data doesn't exist or the transaction has ended and cannot be updated")
    return sql_count


"""
Read data from a table within a transaction.

Args:
- file_name (str): The name of the file to write the SQL read statement to.
- sql_count (int): The current count of SQL operations.
- txn_count (int): The total number of transactions.
- op_num (int): The current operation number.
- data_num (int): The number of data elements to read.
- txn (list): A list of transactions.
- data_op_list (list): A list of data operation records.

Returns:
int: The updated count of SQL operations after the read.

This function reads data from a table within a transaction context. It generates an SQL select statement 
based on the provided parameters and writes the statement to the specified file. The function also updates
the SQL operation count and records the read operation in the data operation list.

The function returns the updated count of SQL operations after the read.

"""
def read_data(file_name, sql_count, txn_count, op_num, data_num, txn, data_op_list):
    with open(file_name, "a+") as file_test:
        try:
            if txn[txn_count].end_ts != max_time:
                raise OptionException
            else:
                if txn[txn_count].begin_ts == -1:
                    txn[txn_count].begin_ts = sql_count
                    begin_sql = str(sql_count) + "-" + str(txn_count) + "-" + "BEGIN;\n"
                    file_test.write(begin_sql)
                    sql_count += 1
                if data_num == 2:
                    read_sql = str(sql_count) + "-" + str(txn_count) + "-" + "SELECT * FROM t1 WHERE k=" + \
                               str(op_num) + ";\n"
                else:
                    read_sql = str(sql_count) + "-" + str(txn_count) + "-" + "SELECT * FROM t" + str(txn_count) + \
                               " WHERE value1=" + str(op_num) + ";\n"
                file_test.write(read_sql)
                data_op_list[op_num].append(Operation("R", txn_count))
        except OptionException:
            file_test.write("the transaction has ended and cannot be read")
            print("the transaction has ended and cannot be read")
    return sql_count


"""
Read data from a table with a predicate (range condition) within a transaction.

Args:
- file_name (str): The name of the file to write the SQL read statement to.
- sql_count (int): The current count of SQL operations.
- txn_count (int): The total number of transactions.
- op_num (int): The current operation number.
- data_num (int): The number of data elements to read.
- txn (list): A list of transactions.
- data_op_list (list): A list of data operation records.

Returns:
int: The updated count of SQL operations after the read.

This function reads data from a table within a transaction context with a predicate (range condition). It generates
an SQL select statement based on the provided parameters and writes the statement to the specified file. 
The function also updates the SQL operation count and records the read operation in the data operation list.

The function returns the updated count of SQL operations after the read.

"""
def read_data_predicate(file_name, sql_count, txn_count, op_num, data_num, txn, data_op_list):
    with open(file_name, "a+") as file_test:
        try:
            if txn[txn_count].end_ts != max_time:
                raise OptionException
            else:
                if txn[txn_count].begin_ts == -1:
                    txn[txn_count].begin_ts = sql_count
                    begin_sql = str(sql_count) + "-" + str(txn_count) + "-" + "BEGIN;\n"
                    file_test.write(begin_sql)
                    sql_count += 1
                if data_num == 2:
                    read_sql = str(sql_count) + "-" + str(txn_count) + "-" + "SELECT * FROM t1 WHERE k>" + \
                               str(op_num*2) + " and k<" + str(op_num*2+2) + ";\n"
                else:
                    read_sql = str(sql_count) + "-" + str(txn_count) + "-" + "SELECT * FROM t" + str(txn_count) + \
                               " WHERE value1>" + str(op_num*2) + " and value1<" + str(op_num*2+2) + ";\n"
                file_test.write(read_sql)
                data_op_list[op_num].append(Operation("P", txn_count))
        except OptionException:
            file_test.write("the transaction has ended and cannot be read")
            print("the transaction has ended and cannot be read")
    return sql_count


"""
Abort (rollback) a transaction.

Args:
- file_name (str): The name of the file to write the SQL rollback statement to.
- sql_count (int): The current count of SQL operations.
- txn_count (int): The total number of transactions.
- txn (list): A list of transactions.

Returns:
int: The updated count of SQL operations after the rollback.

This function aborts (rolls back) a transaction by generating an SQL rollback statement based on the provided 
parameters and writes the statement to the specified file. It updates the SQL operation count and marks the 
transaction as ended. If the transaction has already ended, it logs an error message.

The function returns the updated count of SQL operations after the rollback.

"""
def abort_txn(file_name, sql_count, txn_count, txn):
    with open(file_name, "a+") as file_test:
        try:
            if txn[txn_count].end_ts != max_time:
                raise OptionException
            else:
                txn[txn_count].end_ts = sql_count
                abort_sql = str(sql_count) + "-" + str(txn_count) + "-" + "ROLLBACK;\n"
                file_test.write(abort_sql)
        except OptionException:
            file_test.write("transaction" + str(txn_count) + " ended and can't be rolled back again")
            print("transaction" + str(txn_count) + " ended and can't be rolled back again")
    return sql_count


"""
Commit a transaction.

Args:
- file_name (str): The name of the file to write the SQL commit statement to.
- sql_count (int): The current count of SQL operations.
- txn_count (int): The total number of transactions.
- txn (list): A list of transactions.

Returns:
int: The updated count of SQL operations after the commit.

This function commits a transaction by generating an SQL commit statement based on the provided parameters and
writes the statement to the specified file. It updates the SQL operation count and marks the transaction as ended. 
If the transaction has already ended, it logs an error message.

The function returns the updated count of SQL operations after the commit.

"""
def commit_txn(file_name, sql_count, txn_count, txn):
    with open(file_name, "a+") as file_test:
        try:
            if txn[txn_count].end_ts != max_time:
                raise OptionException
            else:
                txn[txn_count].end_ts = sql_count
                commit_sql = str(sql_count) + "-" + str(txn_count) + "-" + "COMMIT;\n"
                file_test.write(commit_sql)
        except OptionException:
            file_test.write("transaction" + str(txn_count) + " ended and can't be committed again")
            print("transaction" + str(txn_count) + " ended and can't be committed again")
    return sql_count


"""
Execute a check transaction to read data from tables and order the results.

Args:
- file_name (str): The name of the file to write SQL statements to.
- sql_count (int): The current count of SQL operations.
- txn_count (int): The total number of transactions.
- data_num (int): The number of data columns in each table.
- table_num (int): The total number of tables.

This function generates and executes a check transaction. It begins the transaction, performs ordered SELECT 
queries on all tables to read data, and then commits the transaction. The generated SQL statements are written
to the specified file.

"""
def execute_check(file_name, sql_count, txn_count, data_num, table_num):
    with open(file_name, "a+") as file_test:
        begin_sql = str(sql_count) + "-" + str(txn_count) + "-" + "BEGIN;\n"
        file_test.write(begin_sql)
        sql_count += 1
        for i in range(1, table_num + 1):
            if data_num == 2:
                read_sql = str(sql_count) + "-" + str(txn_count) + "-" + "SELECT * FROM t1 ORDER BY k;\n"
            else:
                read_sql = str(sql_count) + "-" + str(txn_count) + "-" + "SELECT * FROM t" + str(i) + \
                           " ORDER BY k;\n"
            file_test.write(read_sql)
            sql_count += 1
        commit_sql = str(sql_count) + "-" + str(txn_count) + "-" + "COMMIT;\n"
        file_test.write(commit_sql)
        sql_count += 1

"""
Check the last operation before the current position in a list of operations.

Args:
- ops (list): A list of operations.
- pos (int): The current position in the list.

Returns:
- last_op_type (str): The type of the last operation before the current position.
- last_txn_num (int): The transaction number of the last operation before the current position.

This function examines the list of operations and checks the last operation that occurred before the current position. 
It returns the type of that operation (e.g., "C" for commit, "A" for abort, or a specific operation type) and the 
corresponding transaction number.

"""
# check if the txn commit/abort before or not
def get_last_op(ops, pos):
    if pos == 0:
        return -1, -1
    if ops[pos-1][1] == "C" or ops[pos-1][1] == "A":
        return ops[pos-1][2], int(ops[pos-1][3])
    else:
        return ops[pos-1][1], int(ops[pos-1][2])

"""
Process the first operation of a Partial Order Pair (POP) for a given transaction.

Args:
- IsPredicate (bool): Indicates whether the operation is a predicate operation.
- num (int): The number of operations in the POP.
- ops (list): A list of operations in the POP.
- file_name (str): The name of the file where SQL queries are logged.
- sql_count (int): The current count of SQL queries.
- txn_count (int): The current transaction count.
- data_num (int): The number of data parameters.
- txn (list): A list of transactions.
- data_value (list): A list of data values.
- data_op_list (list): A list of data operations.

Returns:
- sql_count (int): The updated count of SQL queries after processing the first operation of the POP.

This function processes the first operation of a Partial Order Pair (POP) for a given transaction. It checks whether
the last operation before the current operation has the same type and number. If not, it executes the SQL query for 
the current operation, updates the SQL query count, and advances the transaction count. The function returns the 
updated SQL query count.

"""
# process first operation of a POP
def execute_first(IsPredicate, num, ops, file_name, sql_count, txn_count, data_num, txn, data_value, data_op_list):
    for i in range(num):
        op1 = ops[i][0]
        if ops[i][1] == "C" or ops[i][1] == "A":
            need_ac[txn_count] = ops[i][1]
            op_num = int(ops[i][3:])
        else:
            op_num = int(ops[i][2:])
        last_op_type, last_op_num = get_last_op(ops, i)
        if last_op_num == op_num and last_op_type == op1:
            return sql_count
        sql_count = execute_sql(IsPredicate, file_name, sql_count, txn_count, op_num, op1, data_num, txn, data_value, data_op_list)
        sql_count += 1
        txn_count += 1
        if txn_count == num + 1: txn_count = 1
    return sql_count

"""
Process the second operation of a Partial Order Pair (POP) for a given transaction.

Args:
- IsPredicate (bool): Indicates whether the operation is a predicate operation.
- num (int): The number of operations in the POP.
- ops (list): A list of operations in the POP.
- need_ac (list): A list indicating whether each operation requires an "AC" (Abort or Commit) operation.
- file_name (str): The name of the file where SQL queries are logged.
- sql_count (int): The current count of SQL queries.
- txn_count (int): The current transaction count.
- data_num (int): The number of data parameters.
- txn (list): A list of transactions.
- data_value (list): A list of data values.
- data_op_list (list): A list of data operations.

Returns:
- sql_count (int): The updated count of SQL queries after processing the second operation of the POP.

This function processes the second operation of a Partial Order Pair (POP) for a given transaction. It iterates 
through the list of operations, executes the SQL queries for each operation, updates the SQL query count, and 
advances the transaction count. If an operation requires an "AC" (Abort or Commit) operation, it is also executed. 
The function returns the updated SQL query count.

"""
# process second operation of a POP
def execute_second(IsPredicate, num, ops, need_ac, file_name, sql_count, txn_count, data_num, txn, data_value, data_op_list):
    for i in range(num):
        if ops[i][1] == "C" or ops[i][1] == "A":
            op2 = ops[i][2]
            op_num = int(ops[i][3:])
        else:
            op2 = ops[i][1]
            op_num = int(ops[i][2:])
        sql_count = execute_sql(IsPredicate, file_name, sql_count, txn_count, op_num, op2, data_num, txn, data_value, data_op_list)
        sql_count += 1
        if need_ac[txn_count] != "NO":
            sql_count = execute_sql(IsPredicate, file_name, sql_count, txn_count, i, need_ac[txn_count], data_num,
                                    txn, data_value, data_op_list)
            sql_count += 1
        txn_count += 1
        if txn_count == num + 1: txn_count = 1
    return sql_count 


"""
Execute a transaction considering the order of operations for conflict resolution.

Args:
- IsPredicate (bool): Indicates whether the operation is a predicate operation.
- num (int): The number of operations in the transaction.
- ops (list): A list of operations in the transaction.
- need_ac (list): A list indicating whether each operation requires an "AC" (Abort or Commit) operation.
- file_name (str): The name of the file where SQL queries are logged.
- sql_count (int): The current count of SQL queries.
- data_num (int): The number of data parameters.
- txn (list): A list of transactions.
- data_value (list): A list of data values.
- wait_op_list (list): A list of wait operations.
- data_op_list (list): A list of data operations.

Returns:
- sql_count (int): The updated count of SQL queries after executing the transaction.

This function executes a transaction while considering the order of operations for conflict resolution. 
It iterates through the list of operations, executes SQL queries, and manages the execution order to 
resolve conflicts. The function returns the updated SQL query count.

"""
# if the data of adjacent pattern operations are not the same,
# reorder statements so that conflict-free statements execute first
# otherwise, execute in the original order
# such as RW0-RW1, we will execute as order R1[x]-R2[y]-W2[x]-W1[y]
# IR0-ICW1-RW1, we will execute as order I1[x]-I2[y]-R2[x]-C2-W3[y]-R3[y]-W1[y]
def execute_txn(IsPredicate, num, ops, need_ac, file_name, sql_count, data_num, txn, data_value, wait_op_list,data_op_list):
    for i in range(num):
        op1 = ops[i][0]
        if ops[i][1] == "C" or ops[i][1] == "A":
            op2 = ops[i][2]
            need_ac[i+1] = ops[i][1]
            op_num = int(ops[i][3:])
        else:
            op2 = ops[i][1]
            op_num = int(ops[i][2:])
        next_txn_count = i + 2
        if next_txn_count == num+1: next_txn_count = 1

        if i == 0:
            sql_count = execute_sql(IsPredicate, file_name, sql_count, i+1, op_num, op1, data_num, txn, data_value, data_op_list)
            sql_count += 1
            wait_op_list.append(Wait_Operation(op2, next_txn_count, op_num))
        else:
            last_op_type, last_op_num = get_last_op(ops, i)
            if last_op_num == op_num:
                #if last_op_num and last_op_type both are same as now operation, we will only execute later operation
                if op1 == last_op_type:
                    wait_op_list.pop()
                for data in wait_op_list:
                    sql_count = execute_sql(IsPredicate, file_name, sql_count, data.txn_num, data.op_num, data.op_type, data_num, txn, data_value, data_op_list)
                    sql_count += 1
                    if need_ac[data.txn_num] != "NO" and data.op_num != op_num:
                        sql_count = execute_sql(IsPredicate, file_name, sql_count, data.txn_num, data.op_num, need_ac[data.txn_num], data_num, txn, data_value, data_op_list)
                        sql_count += 1
                wait_op_list = []

                sql_count = execute_sql(IsPredicate, file_name, sql_count, i+1, op_num, op1, data_num, txn, data_value, data_op_list)
                sql_count += 1

                if need_ac[i+1] != "NO":
                    sql_count = execute_sql(IsPredicate, file_name, sql_count, i+1, op_num, need_ac[i+1], data_num, txn, data_value, data_op_list)
                    sql_count += 1
                
                sql_count = execute_sql(IsPredicate, file_name, sql_count, next_txn_count, op_num, op2, data_num, txn, data_value, data_op_list)
                sql_count += 1
            else:
                sql_count = execute_sql(IsPredicate, file_name, sql_count, i+1, op_num, op1, data_num, txn, data_value, data_op_list)
                sql_count += 1

                if len(wait_op_list) == 0:
                    if need_ac[i+1] != "NO":
                        sql_count = execute_sql(IsPredicate, file_name, sql_count, i+1, op_num, need_ac[i+1], data_num, txn, data_value, data_op_list)
                        sql_count += 1
                wait_op_list.append(Wait_Operation(op2, next_txn_count, op_num))

    for data in wait_op_list:
        sql_count = execute_sql(IsPredicate, file_name, sql_count, data.txn_num, data.op_num, data.op_type, data_num, txn, data_value, data_op_list)
        sql_count += 1

        if need_ac[data.txn_num] != "NO":
            sql_count = execute_sql(IsPredicate, file_name, sql_count, data.txn_num, data.op_num, need_ac[data.txn_num], data_num, txn, data_value, data_op_list)
            sql_count += 1
    wait_op_list = []

    return sql_count


"""
Write a description for the test case to the specified file.

Args:
- file_name (str): The name of the file where the description will be written.
- txn_num (int): The number of transactions.
- op_num (int): The number of operations.
- data_num (int): The number of data parameters.

This function writes a description for the test case to the specified file. The description includes 
information about  the test case pattern, parameters, and structure.

"""
def write_description(file_name, txn_num, op_num, data_num):
    with open(file_name, "w+") as file_test:
        description = "#\n"
        description += "# Test case description\n"
        description += "# POPG Pattern: "
        patterns = file_name.split("/")[-1].split(".")[0].split("-")
        for i in range(len(patterns)):
            description += "T" + str(i+1) + " ==" + patterns[i] + "==> "
        description += "T1\n"
        description += "# Parameters: #column=2 #txn=" + str(txn_num) + " #operations=" + str(op_num) + " #variable=" + str(data_num) +  "\n"
        description += "# Structure: Sequence-Session-Query" +  "\n"
        description += "# When sequence=0, it is a preparation phase, otherwise an execution phase" +  "\n"
        description += "#\n"
        file_test.write(description)

# target folder
case_folder = "t/test_case_v2"
# pattern files
do_test_list = "do_test_list.txt"
# [single,distributed] => for local test or distributed test
db_type = sys.argv[1]
# [tdsql] => for pg/sql standard queries
test_type = sys.argv[2]
max_time = 99999999999999999999
with open(do_test_list, "r") as f:
    lines = f.readlines()
if not os.path.exists(case_folder):
    os.mkdir(case_folder)


# for each popg, generate popg test case and write into file.
for popg in lines:
    popg = popg.replace("\n", "")
    popg = popg.replace(" ", "")
    if popg == "":
        continue
    # check whether popg is predicate operation
    if popg.find("I") != -1 or popg.find("D") != -1 or popg.find("P") != -1:
        IsPredicate = True
    else:
        IsPredicate = False
    if popg[0] == "#":
        continue
    ops = popg.split('-')

    path_store = case_folder
    if not os.path.exists(path_store):
        os.mkdir(path_store)
    file_name = path_store + "/" + popg + ".txt"

    num = len(ops)
    if (db_type == "tdsql" or db_type == "crdb") and test_type == "dist":
        table_num = num
    else:
        table_num = 1
    sql_count, txn_count = 0, 1

    # description
    write_description(file_name, num, num, num)

    # preparation
    data_num = init_table(file_name, sql_count, txn_count, table_num, db_type, test_type)

    # exist means whether data can be inserted, only data that doesn't exist can be inserted
    # when the first write of one object is insert, then we will not insert the data in firsthand
    exist = [False] * (2*num+2)
    visit = [False] * (2*num+2)
    data_value = {}
    for i in range(2*num+2):
        data_value[i] = 0
    cur_count, partition_num, insert_table = 0, 1, 1
    if IsPredicate:
        insert_num = num + 1
    else:
        insert_num = num
    
    # barrier data insertion
    for i in range(1, insert_num + 1):
        insert_data(file_name, sql_count, txn_count, cur_count, partition_num, insert_table, data_num, exist,
                    data_value)
        if IsPredicate:
            cur_count += 2
        else:
            cur_count += 1
        partition_num ^= 2
        insert_table += 1
        if table_num == 1: insert_table = 1
    
    # for a data, if W and D operation at an earlier time, we will insert this data when initialized
    for i in range(len(ops)):
        if ops[i][1] == "C" or ops[i][1] == "A":
            op_num = int(ops[i][3:])
        else:
            op_num = int(ops[i][2:])
        if IsPredicate and not visit[op_num]:
            visit[op_num] = True
            if ops[i][0] == "R" or ops[i][0] == "P":
                if ops[i].find("I") == -1:
                    insert_data(file_name, sql_count, txn_count, 2*op_num+1, partition_num, insert_table, data_num, exist, data_value)
            elif ops[i][0] == "D" or ops[i][0] == "W":
                insert_data(file_name, sql_count, txn_count, 2*op_num+1, partition_num, insert_table, data_num, exist,
                            data_value)
            partition_num ^= 2
            insert_table += 1
            if table_num == 1: insert_table = 1

    # execution
    txn = [Txn() for i in range(num + 2)]
    wait_op_list = []
    data_op_list = [[] for i in range(2*num + 2)]
    
    # need_ac means whether need abort/commit immediately and if necessary which operation need to be done
    need_ac = ["NO"] * (num + 1)
    sql_count += 1
    sql_count = execute_txn(IsPredicate, num, ops, need_ac, file_name, sql_count, data_num, txn, data_value, wait_op_list,data_op_list)
    
    # reorder statements so that conflict-free statements execute first
    # sql_count = execute_first(IsPredicate, num, ops, file_name, sql_count, txn_count, data_num, txn, data_value, data_op_list)
    # txn_count = 2
    # sql_count = execute_second(IsPredicate, num, ops, need_ac, file_name, sql_count, txn_count, data_num, txn, data_value, data_op_list)
    for i in range(1, num + 1):
        if txn[i].end_ts == max_time:
            commit_txn(file_name, sql_count, i, txn)
            sql_count += 1

    # verification
    # execute_check(file_name, sql_count, num + 1, data_num, table_num)
