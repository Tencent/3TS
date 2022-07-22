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

# check if the txn commit/abort before or not
def get_last_op(ops, pos):
    if pos == 0:
        return -1, -1
    if ops[pos-1][1] == "C" or ops[pos-1][1] == "A":
        return ops[pos-1][2], int(ops[pos-1][3])
    else:
        return ops[pos-1][1], int(ops[pos-1][2])

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
