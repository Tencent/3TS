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


import queue
import os
import time
import copy


class Edge:
    def __init__(self, type, out, begin_time):
        self.type = type
        self.out = out
        self.time = begin_time

    def __repr__(self):
        return "Edge(begin_time={}, type={}, out={})".format(self.time, self.type, self.out)


class Operation:
    def __init__(self, op_type, txn_num, op_time, op_value):
        self.op_type = op_type
        self.txn_num = txn_num
        self.op_time = op_time
        self.value = op_value
        self.keyvalue = -1


class Txn:
    def __init__(self):
        self.begin_ts = -1
        self.end_ts = 99999999999999999999
        self.isolation = "serializable"


"""
Print bug detection result and edge based on build graph.

Args:
- edge (list): A list of queries.
- txn (list): A list of transaction objects.
- error_type (list): A list of bug information of edges.

Returns:
None
"""


def print_graph(edge, txn, error_type):
    for i, edges in enumerate(edge):
        if i == 0 or i == len(edge) - 1:
            continue
        print("Transaction {}:-----{}-----".format(i, txn[i].isolation))
        with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
            f.write("Transaction {}:-----{}-----".format(i, txn[i].isolation) + "\n")
        for e in edges:
            with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
                f.write("  {}".format(e) + "\n")
        error_output = []
        if error_type[i][0] == 1:
            error_output.append("Dirty-Write")
        if error_type[i][1] == 1:
            error_output.append("Dirty-Read")
        if error_type[i][2] == 1:
            error_output.append("Unrepeatable-Read")
        if error_type[i][3] == 1:
            error_output.append("Phantom-Read")
        if error_output == []:
            error_output.append("No error in this isolation level.")
        print("  Error Detection: " + '; '.join(error_output))
        with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
            f.write("  Error Detection: " + '; '.join(error_output) + "\n")


"""
Print data_op_list for program testing.

Args:
- data_op_list (list): A list of data operations.

Returns:
None
"""


def print_data_op_list(data_op_list):
    for k, list in enumerate(data_op_list):
        if k < len(data_op_list) - 1:
            print("\nk:{}---".format(k))
            for i, data in enumerate(list):
                print("op:{}--{}-".format(data.op_type, data.txn_num))


"""
Find the total variable number.

Args:
- lines (list): A list of queries.

Returns:
int: The maximum variable number found in the queries.
"""


def get_total(lines):
    num = 0
    for query in lines:
        query = query.replace("\n", "")
        query = query.replace(" ", "")
        if query.find("INSERT") != -1 or query.find("insert") != -1:  # query[0:2] == "Q0" and
            tmp = find_data(query, "(")
            num = max(num, tmp)
        # elif query[0:2] == "Q1":
        #     break
    return num


"""
Find the total transaction number.

Args:
- lines (list): A list of queries.

Returns:
int: The maximum transaction number found in the queries.
"""


def get_total_txn(lines):
    num = 0
    for query in lines:
        query = query.replace("\n", "")
        query = query.replace(" ", "")
        t1 = query.find('-')
        if t1 != -1:
            t2 = query.find('-', t1 + 1)
            if t2 != -1:
                tmp = int(query[t1 + 1:t2])
                num = max(num, tmp)
    return num


"""
Transform the validation part of test cases into data in program.

Args:
- lines (list): A list of queries.

Returns:
list: The validation data from test cases.
"""


def get_validation(lines):
    st = -1
    for i, query in enumerate(lines):
        if query.find("{") != -1:
            st = i
            break
    seg = [st]
    for i in range(st + 1, len(lines)):
        if lines[i] == "\n":
            seg.append(i)
        if lines[i] == "}\n" or lines[i] == "}":
            seg.append(i)
            break
    validation = [[] for _ in range(len(seg) - 1)]
    validate_group = [[] for _ in range(len(seg) - 1)]
    for i in range(len(seg) - 1):
        for j in range(seg[i] + 1, seg[i + 1]):
            validate_group[i].append(lines[j].replace("\n", ""))
    for cnt, j in enumerate(validate_group):
        for i in j:
            sa = i
            while True:
                st = sa.find("-")
                k1 = sa[:st]
                cc = sa.find(",")
                k2 = sa[st + 1:cc]
                sp = sa.find(" ")
                if sp == -1:
                    validation[cnt].append([k1, k2, sa[cc + 1:]])
                    break
                else:
                    validation[cnt].append([k1, k2, sa[cc + 1:sp]])
                    sa = sa[:st + 1] + sa[sp + 1:]
    return validation


"""
Extract the data we need from a query.

Args:
- query (str): The input query string.
- target (str): The target substring to search for.

Returns:
int: The extracted data value, or -1 if not found.
"""


def find_data(query, target):
    pos = query.find(target)
    if pos == -1:
        return pos
    pos += len(target)
    data_value = ""
    for i in range(pos, len(query)):
        if query[i].isdigit():
            data_value += query[i]
        else:
            break
    if data_value == "":
        return -1
    data_value = int(data_value)
    return data_value


"""
Extract the isolation level setting of query.

Args:
- query (str): The input query string.

Returns:
str: Corresponding isolation level setting.
"""


def find_isolation(query):  # TODO: In this program, set all test cases "read-repeatable".
    #    if query.find("read-uncommitted") != -1:
    #        return "read-uncommitted"
    #    if query.find("read-committed") != -1:
    #        return "read-committed"
    #    if query.find("repeatable-read") != -1:
    #        return "repeatable-read"
    #    if query.find("serializable") != -1:
    #        return "serializable"
    return "read-repeatable"


"""
When a statement is executed, this function sets the end time, modifies the transaction list,
and updates the version list as needed.

Args:
- op_time (int): The operation time of the statement.
- data_op_list (list): A list of data operations.
- txn (list): A list of transaction objects.
- version_list (list): A list of version lists for data operations.

Returns:
None
"""


def set_finish_time(op_time, data_op_list, txn, version_list):
    data_value = int(op_time)
    for t in txn:
        if t.begin_ts == op_time:
            t.begin_ts = data_value
        if t.end_ts == op_time:
            t.end_ts = data_value
    for i, list1 in enumerate(data_op_list):
        for op in list1:
            if op.op_time == op_time:
                op.op_time = data_value
                if op.op_type == "W":
                    version_list[i].append(op.value)
                    op.value = len(version_list[i]) - 1
                elif op.op_type == "D":
                    version_list[i].append(-1)
                    op.value = len(version_list[i]) - 1
                elif op.op_type == "I":
                    version_list[i].append(op.value)
                    op.value = len(version_list[i]) - 1


"""
Check if two transactions are concurrent based on their start and end times.

Args:
- data1: Information about the first transaction.
- data2: Information about the second transaction.
- txn: A list of transaction objects.

We think they are concurrent if both transactions are running
or the start time of the second transaction is less than the end time of the first transaction

Returns:
bool: True if the transactions are concurrent, False otherwise.
"""


def check_concurrency(data1, data2, txn):
    if txn[data2.txn_num].begin_ts < txn[data1.txn_num].end_ts:
        return True
    elif txn[data1.txn_num].begin_ts < txn[data2.txn_num].end_ts:  # TODO maybe a bug: don't need
        return True
    else:
        return False


"""
Determine the type of edge between two operations based on their read or write versions.

Args:
- data1: Information about the first operation.
- data2: Information about the second operation.
- txn: A list of transaction objects.

Decide which operation comes first depending on the read or write version
if later operation happened after the first txn commit time, edge type will add "C"

Returns:
tuple: A tuple containing three values:
    - A string indicating the edge type ('R', 'W', 'CR', 'CW').
    - Information about the operation that comes first.
    - Information about the operation that comes second.
"""


def get_edge_type(data1, data2, txn):
    if data1.op_time <= data2.op_time:
        before, after = data1, data2
    else:
        before, after = data2, data1
    if after.op_time > txn[before.txn_num].end_ts:
        state = "C"
    else:
        state = ""
    return before.op_type + state + after.op_type, before, after


"""
Build a directed graph representing the concurrency relationships between operations.

Args:
- data_op_list: A list of lists, where each inner list contains information about operations for a specific transaction.
- indegree: A list representing the in-degrees of each operation node in the graph.
- edge: A list representing the edges (concurrency relationships) between operations.
- txn: A list of transaction objects.
- error_type (list): A list of bug information of edges.

This function constructs a directed graph where nodes represent operations, and edges represent concurrency relationships
between operations. It iterates through the list of operations for each transaction and calls the 'insert_edge' function 
to create edges in the graph based on concurrency relationships.

Returns:
None
"""


def build_graph(data_op_list, indegree, edge, txn, error_type):
    for list1 in data_op_list:
        for i, data in enumerate(list1):
            for j in range(0, i):
                insert_edge(list1[j], data, indegree, edge, txn, error_type)


"""
Insert an edge into the directed graph representing concurrency relationships between operations and record its bug detection result.

Args:
- data1: An operation object representing the first operation.
- data2: An operation object representing the second operation.
- indegree: A list representing the in-degrees of each transaction in the graph.
- edge: A list representing the edges (concurrency relationships) between operations for each transaction.
- txn: A list of transaction objects.
- error_type (list): A list of bug information of edges.

This function inserts an edge into the directed graph to represent the concurrency relationship between 'data1' and 'data2'. 
It first checks if the two operations are concurrent by calling the 'check_concurrency' function. If they are concurrent, it 
determines the edge type using the 'get_edge_type' function and adds the edge to the 'edge' list.
Then, it conducts bug detection with edge type and isolation level information based on concurrency relationship.

The 'indegree' list is updated to reflect the in-degree of the target transaction node when an edge is inserted.
The 'error_type' list is updated to record the bug detection result for edges in 'edge' list.

Dirty-Write detection happens in isolation level: "read-uncommited", "read-committed", "repeatable-read", "serializable"
For these edge type conditions: "WCW", "WW", "WCD", "WD", "ICW","IW", "ICD", "ID", "DCW", "DW", "DCI", "DI"
Also deduce that:
WI does not exist. If it does, there must be an equivalent edge of WD + DI
II does not exist. If it does, there must be an equivalent edge of ID + DI
DW is allowed to exist. When UPDATE, use the condition to query the data containing D
DD does not exist. If it does, there must be an equivalent edge of DI + ID

Dirty-Read detection happens in isolation level: "read-committed", "repeatable-read", "serializable"
For these edge type conditions: "WCR", "WR"

Unrepeatable-Read detection happens in isolation level: "repeatable-read", "serializable"
For these edge type conditions: "RCW", "RW"

Phantom-Read detection happens in isolation level: "serializable"
For these edge type conditions: "RCI", "RI", "RCD", "RD", "ICR", "IR", "DCR", "DR"

Returns:
None
"""


def insert_edge(data1, data2, indegree, edge, txn, error_type):
    if check_concurrency(data1, data2, txn):
        edge_type, data1, data2 = get_edge_type(data1, data2, txn)
        if data1.txn_num == data2.txn_num or edge_type in ["RCR", "RR"]:
            return
        if edge_type in ["WCW", "WW", "WCD", "WD", "ICW", "IW", "ICD", "ID", "DCW", "DW", "DCI", "DI"]:
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num, data1.op_time))
            error_type[data1.txn_num][0] = 1
        elif edge_type in ["WCR", "WR"] and (
                txn[data2.txn_num].isolation == "read-committed" or txn[data2.txn_num].isolation == "repeatable-read" or
                txn[data2.txn_num].isolation == "serializable"):
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num, data1.op_time))
            error_type[data1.txn_num][1] = 1
        elif edge_type in ["RCW", "RW"] and (
                txn[data1.txn_num].isolation == "repeatable-read" or txn[data1.txn_num].isolation == "serializable"):
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num, data1.op_time))
            error_type[data1.txn_num][2] = 1
        elif edge_type in ["RCI", "RI", "RCD", "RD"] and txn[data1.txn_num].isolation == "serializable":
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num, data1.op_time))
            error_type[data1.txn_num][3] = 1
        elif edge_type in ["ICR", "IR", "DCR", "DR"] and txn[data2.txn_num].isolation == "serializable":
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num, data1.op_time))
            error_type[data1.txn_num][3] = 1


"""
Initialize a record in the version list based on the information in the query.

Args:
- query: A query string that contains information about a record.
- version_list: A list of lists representing versioned records.
- data_record (list of int list): List of data in int form.

This function initializes a record in the 'version_list' based on the information provided in the 'query'. It extracts the 'key'
and 'value' of the record from the query using the 'find_data' function and appends the 'value' to the corresponding version list.

Returns:
None
"""


def init_record(query, version_list, data_record):
    key = find_data(query, "(")
    value = find_data(query, ",")
    version_list[key].append(value)
    data_record.append([key, value, 0, -1])


"""
Read the versioned record based on the information in the query.

Args:
- query (str): A query string that contains information about reading a versioned record.
- op_time (int): The operation time of the read operation.
- data_op_list (list): A list of lists representing data operations.
- version_list (list): A list of lists representing versioned records.

This function reads the versioned record specified in the 'query'. It extracts the 'key' and 'value' from the query, which are 
used to identify the record and version to read. The function checks if the specified version exists in the version list and 
updates the 'op.value' accordingly. If the version doesn't exist or if the read operation is not successful, an error message 
is returned.

Returns:
str: An error message indicating the result of the read operation. An empty string means the read was successful.
"""


def readVersion_record(query, op_time, data_op_list, version_list):
    error_message = ""
    data = query.split(")")
    if len(data) == 1:
        for list1 in data_op_list:
            for op in list1:
                if op.op_time == op_time:
                    value = op.value
                    if len(version_list[value]) == 0:
                        op.value = -1
                    else:
                        if -1 not in version_list[value]:
                            error_message = "Value exists, but did not successully read"
                            return error_message
                        pos = version_list[value].index(-1)
                        op.value = pos
    else:
        for s in data:
            key = find_data(s, "(")
            value = find_data(s, ",")
            for i, list1 in enumerate(data_op_list):
                for op in list1:
                    if key == i and op.op_time == op_time:
                        value1 = op.value
                        if len(version_list[value1]) == 0:
                            op.value = -1
                        else:
                            if version_list[value1].count(value) == 0:
                                error_message = "Read version that does not exist"
                                return error_message
                            pos = version_list[value1].index(value)
                            op.value = pos

    return error_message


"""
Read records based on the information in the query and update data operations.

Args:
- op_time (int): The operation time of the read operation.
- txn_num (int): The transaction number.
- total_num (int): The total number of records.
- txn (list): A list of transactions.
- data_op_list (list): A list of lists representing data operations.
- operation_by_time (list): A list for records of all operations after Q0.

This function reads records specified in the query and updates the 'data_op_list' accordingly. It extracts information from 
the 'query' to determine which records to read and what type of operation to perform (read or predicate). The function also 
sets the 'begin_ts' of the transaction if it's not already set.

The 'query' is analyzed to identify specific record keys or predicates and create corresponding 'Operation' objects in the 
'data_op_list'. Depending on the structure of the query, this function handles various cases, such as reading single records,
handling predicates, and selecting all rows in a table.

Returns:
None
"""


# TODO: need to judge the name of attributes, this program only considers the name for key "k" and value "v".
def read_record(op_time, txn_num, total_num, txn, data_op_list, operation_by_time):
    if txn[txn_num].begin_ts == -1:
        txn[txn_num].begin_ts = op_time
    # for some distributed cases which have 4 param, write part is same
    if query.find("v=") != -1:
        op_data = find_data(query, "v=")
        data_op_list[op_data].append(Operation("R", txn_num, op_time, op_data))
    # for normal cases
    elif query.find("k=") != -1:
        op_data = find_data(query, "k=")
        data_op_list[op_data].append(Operation("R", txn_num, op_time, op_data))
        operation_by_time.append(Operation("R", txn_num, op_time, op_data))
    # for predicate cases
    elif query.find("k>") != -1:
        left = find_data(query, "k>") + 1
        right = find_data(query, "k<")
        for i in range(left, right):
            data_op_list[i].append(Operation("R", txn_num, op_time, i))  # P
    elif query.find("v>") != -1:
        left = find_data(query, "v>") + 1
        right = find_data(query, "v<")
        for i in range(left, right):
            data_op_list[i].append(Operation("R", txn_num, op_time, i))  # p
    else:
        # it means select all rows in table
        for i in range(total_num + 1):
            data_op_list[i].append(Operation("R", txn_num, op_time, i))
        operation_by_time.append(Operation("R", txn_num, op_time, -1))


"""
Write records based on the information in the query and update data operations.

Args:
- op_time (int): The operation time of the write operation.
- txn_num (int): The transaction number.
- txn (list): A list of transactions.
- data_op_list (list): A list of lists representing data operations.
- operation_by_time (list): A list for records of all operations after Q0.

This function writes records specified in the query and updates the 'data_op_list' accordingly. It extracts information from the 
'query' to determine which records to write and what type of operation to perform (write). The function also sets the 'begin_ts' 
of the transaction if it's not already set.

For predicate cases or no discriminator cases, it searches queries in range and record their operations.

The 'query' is analyzed to identify specific record keys and values, and it creates corresponding 'Operation' objects in the 'data_op_list'.

Returns:
None
"""


def write_record(op_time, txn_num, txn, data_op_list, operation_by_time):
    if txn[txn_num].begin_ts == -1:
        txn[txn_num].begin_ts = op_time
    if query.find("k=") != -1:
        op_data = find_data(query, "k=")
        op_value = find_data(query, "v=")
        data_op_list[op_data].append(Operation("W", txn_num, op_time, op_value))
        data = Operation("W", txn_num, op_time, op_data)
        data.keyvalue = op_value
        operation_by_time.append(data)
    elif query.find("k>") != -1:
        left = find_data(query, "k>") + 1
        right = find_data(query, "k<")
        for i in range(left, right):
            data_op_list[i].append(Operation("W", txn_num, op_time, i))
    elif query.find("v>") != -1:
        left = find_data(query, "v>") + 1
        right = find_data(query, "v<")
        for i in range(left, right):
            data_op_list[i].append(Operation("W", txn_num, op_time, i))
    else:
        for i in range(total_num + 1):
            data_op_list[i].append(Operation("W", txn_num, op_time, i))


"""
Delete records based on the information in the query and update data operations.

Args:
- op_time (int): The operation time of the delete operation.
- txn_num (int): The transaction number.
- txn (list): A list of transactions.
- data_op_list (list): A list of lists representing data operations.
- operation_by_time (list): A list for records of all operations after Q0.

This function deletes records specified in the query and updates the 'data_op_list' accordingly. It extracts information from the 
'query' to determine which records to delete and what type of operation to perform (delete). The function also sets the 'begin_ts' 
of the transaction if it's not already set.

For predicate cases or no discriminator cases, it searches queries in range and record their operations.

The 'query' is analyzed to identify specific record keys, and it creates corresponding 'Operation' objects in the 'data_op_list'.

Returns:
None
"""


def delete_record(op_time, txn_num, txn, data_op_list, operation_by_time):
    if txn[txn_num].begin_ts == -1:
        txn[txn_num].begin_ts = op_time
    if query.find("v=") != -1:
        op_data = find_data(query, "v=")
        data_op_list[op_data].append(Operation("D", txn_num, op_time, op_data))
    elif query.find("k=") != -1:
        op_data = find_data(query, "k=")
        data_op_list[op_data].append(Operation("D", txn_num, op_time, op_data))
        operation_by_time.append(Operation("D", txn_num, op_time, op_data))
    # for predicate cases
    elif query.find("k>") != -1:
        left = find_data(query, "k>") + 1
        right = find_data(query, "k<")
        for i in range(left, right):
            data_op_list[i].append(Operation("D", txn_num, op_time, i))  # P
    elif query.find("v>") != -1:
        left = find_data(query, "v>") + 1
        right = find_data(query, "v<")
        for i in range(left, right):
            data_op_list[i].append(Operation("D", txn_num, op_time, i))  # p
    else:
        # it means select all rows in table
        for i in range(total_num + 1):
            data_op_list[i].append(Operation("D", txn_num, op_time, i))


"""
Insert records based on the information in the query and update data operations.

Args:
- op_time (int): The operation time of the insert operation.
- txn_num (int): The transaction number.
- txn (list): A list of transactions.
- data_op_list (list): A list of lists representing data operations.
- operation_by_time (list): A list for records of all operations after Q0.

This function inserts records specified in the query and updates the 'data_op_list' accordingly. It extracts information from the 
'query' to determine which records to insert and what type of operation to perform (insert). The function also sets the 'begin_ts' 
of the transaction if it's not already set.

The 'query' is analyzed to identify specific record keys and their corresponding values, and it creates corresponding 'Operation' 
objects in the 'data_op_list'.

Returns:
None
"""


def insert_record(op_time, txn_num, txn, data_op_list, operation_by_time):
    if txn[txn_num].begin_ts == -1 and op_time != 0:
        txn[txn_num].begin_ts = op_time
    key = find_data(query, "(")
    value = find_data(query, ",")
    data = Operation("I", txn_num, op_time, key)
    data.keyvalue = value
    data_op_list[key].append(Operation("I", txn_num, op_time, value))
    operation_by_time.append(data)


"""
Set the end timestamp for a transaction.

Args:
- op_time (int): The operation time when the transaction ends.
- txn_num (int): The transaction number.
- txn (list): A list of transactions.
- operation_by_time (list): A list for records of all operations after Q0.

This function sets the 'end_ts' attribute of a transaction specified by 'txn_num' to the given 'op_time'. It marks the end of the 
transaction's execution.

Returns:
None
"""


def end_record(op_time, txn_num, txn, operation_by_time):
    txn[txn_num].end_ts = op_time
    operation_by_time.append(Operation("C", txn_num, op_time, -1))


"""
Record and process database operations.

Args:
- total_num (int): The total number of database operations.
- query (str): The SQL query representing a database operation.
- txn (list): A list of transactions.
- data_op_list (list): A list of data operations.
- version_list (list): A list of version information for data operations.
- operation_by_time (list): A list for records of all operations after Q0.
- data_record (list of int list): List of data in int form.

This function records and processes database operations based on the provided SQL query. It updates the transaction list, data 
operation list, and version list accordingly. The 'total_num' parameter specifies the total number of database operations.

"returnresult", "finished"  may not exist in input file.
Assume "Rollback" will not be inserted to existing data.

Returns:
str: An error message (if any), or an empty string if the operation is successful.
"""


def operation_record(total_num, query, txn, data_op_list, version_list, operation_by_time, data_record):
    error_message = ""
    t1 = query.find('-')
    if t1 == -1:
        return error_message
    t2 = query.find('-', t1 + 1)
    if t2 == -1:
        return error_message
    op_time = int(query[0:t1])
    txn_num = int(query[t1 + 1:t2])
    if op_time == 0 and (query.find("INSERT") != -1 or query.find("insert") != -1):
        init_record(query, version_list, data_record)
        return error_message
    if query.find("returnresult") != -1:
        error_message = readVersion_record(query, op_time, data_op_list, version_list)
        return error_message
    if query.find("finished") != -1:
        set_finish_time(op_time, data_op_list, query, txn, version_list)
        return error_message
    if op_time == -1 or txn_num == -1:
        return error_message
    if query.find("BEGIN") != -1 or query.find("begin") != -1:
        txn[txn_num].isolation = find_isolation(query)
    elif query.find("SELECT") != -1 or query.find("select") != -1:
        read_record(op_time, txn_num, total_num, txn, data_op_list, operation_by_time)
    elif query.find("UPDATE") != -1 or query.find("update") != -1:
        write_record(op_time, txn_num, txn, data_op_list, operation_by_time)
    elif query.find("DELETE") != -1 or query.find("delete") != -1:
        delete_record(op_time, txn_num, txn, data_op_list, operation_by_time)
    elif query.find("INSERT") != -1 or query.find("insert") != -1:
        insert_record(op_time, txn_num, txn, data_op_list, operation_by_time)
    elif query.find("ROLLBACK") != -1 or query.find("rollback") != -1:
        operation_by_time.append(Operation("RB", txn_num, op_time, -1))
    elif query.find("COMMIT") != -1 or query.find("commit") != -1:
        if op_time != 0:
            end_record(op_time, txn_num, txn, operation_by_time)
    set_finish_time(op_time, data_op_list, txn, version_list)
    return error_message


"""
Remove unfinished operations from the data operation list to prevent redundant edges.

Args:
- data_op_list (list): A list of data operations.

This function iterates through the data operation list and removes any unfinished operations based on their operation time. 
Unfinished operations are those with an operation time less than 10,000,000.

Returns:
None
"""


def remove_unfinished_operation(data_op_list):
    for list1 in data_op_list:
        for i, op in enumerate(list1):
            if op.op_time < 10000000:
                list1.pop(i)


"""
Check for cycles in a directed graph using topological sorting.

Args:
- edge (List[List[Edge]]): A list representing the directed edges in the graph.
- indegree (List[int]): A list representing the in-degrees of nodes in the graph.
- total (int): The total number of nodes in the graph.

This function checks for cycles in a directed graph by performing topological sorting. It takes as input the directed edges (`edge`), 
in-degrees of nodes (`indegree`), and the total number of nodes in the graph (`total`).

Returns:
bool: True if a cycle is detected, False otherwise.
"""


def check_cycle(edge, indegree, total):
    q = queue.Queue()
    for i, degree in enumerate(indegree):
        if degree == 0: q.put(i)
    ans = []
    while not q.empty():
        now = q.get()
        ans.append(now)
        for val in edge[now]:
            next_node = val.out
            indegree[next_node] -= 1
            if indegree[next_node] == 0:
                q.put(next_node)
    if len(ans) == total:
        return False
    return True


"""
Perform depth-first search (DFS) to find and print possible loops in a directed graph.

Args:
- result_folder (str): The path to the folder where the results will be saved.
- ts_now (str): The current timestamp or identifier for result file naming.
- e (Edge): The current edge being visited, used for iteration search.

This function performs depth-first search (DFS) to find and print loops in a directed graph. It takes as input the result folder 
path (`result_folder`), the current timestamp or identifier for result file naming (`ts_now`), the current node being visited (`now`),
and the type of edge leading to the current node (`type`).

The function recursively explores the graph, tracking the visited nodes and edges to detect loops. When a loop is found, it is printed
to a result file in the specified result folder.

Note: This function assumes that global variables like 'visit', 'visit1', 'path', 'edge_type', and 'edge' are defined elsewhere.

"""


def dfs(result_folder, ts_now, e):
    visit1[e.out] = 1
    if visit[e.out] == 1: return
    visit[e.out] = 1
    path.append(e)
    for v in edge[e.out]:
        if visit[v.out] == 0:
            dfs(result_folder, ts_now, v)
        else:
            path.append(v)
            content = ""
            list_loop = []
            for i in range(len(path) - 1, -1, -1):
                if i != len(path) - 1 and path[i].out == path[len(path) - 1].out:
                    break
                index = 0
                while (index < len(list_loop) and path[list_loop[index]].time < path[i].time):
                    index += 1
                list_loop.insert(index, i)
            for idx in list_loop:
                content = content + "->" + path[idx].type + "->" + str(path[idx].out)
            content = str(path[list_loop[-1]].out) + content
            print(content)
            path.pop()
    path.pop()
    visit[e.out] = 0


"""
Print the paths in a directed graph to console.

Args:
- edge (list of lists): A list of lists representing the directed edges in the graph.

This function prints the paths in a directed graph to a result file. It takes as input the result folder path (`result_folder`), 
the current timestamp or identifier for result file naming (`ts_now`), and a list of lists (`edge`) representing the directed edges 
in the graph.

The function iterates through the edges and writes the paths to the result file in the specified result folder.

"""


def print_path(edge):
    flag = 0
    s = ""
    for i in range(len(edge)):
        for v in edge[i]:
            if flag == 0:
                flag = 1
            else:
                s += ", "
            s += str(i) + "->" + v.type + "->" + str(v.out)
    print(s)


"""
Output the result of cycle detection to console.

Args:
- file (str): The name of the file or input source being analyzed.
- result_folder (str): The path to the folder where the results will be saved.
- ts_now (str): The current timestamp or identifier for result file naming.
- IsCyclic (str): A string indicating whether a cycle was detected.

This function outputs the result of cycle detection to a result file. It takes as input the name of the file or input source being 
analyzed (`file`), the result folder path (`result_folder`), the current timestamp or identifier for result file naming (`ts_now`),
and a string (`IsCyclic`) indicating whether a cycle was detected.

The function writes the result, including the file name and the cyclic status, to the specified result file in the result folder.

"""


def output_result(file, IsCyclic):
    print(file + ": " + IsCyclic + "\n")


"""
Print an error message to console.

Args:
- result_folder (str): The path to the folder where the results will be saved.
- ts_now (str): The current timestamp or identifier for result file naming.
- error_message (str): The error message to be printed.

This function prints an error message to a result file. It takes as input the result folder path (`result_folder`), the current 
timestamp or identifier for result file naming (`ts_now`), and the error message (`error_message`) to be printed.

The function appends the error message to the specified result file in the result folder and adds a newline for separation.

"""


def print_error(error_message):
    print(error_message)
    print("\n")


"""
Detect anomaly in MySQL environment at "SERIALIZABLE" isolation level.

Args:
- total_num (int): Total number of variables.
- total_num_txn (int): Total number of transactions.
- operation_by_time (list of Operation): List of operations ordered by time sequence.

Returns:
Str: A bug detection result in MySQL environment at "SERIALIZABLE" isolation level, including "Avoid" and "Rollback".
"""


def mysql_serializable_check(total_num, total_num_txn, operation_by_time):
    lock_status = [["N" for _ in range(total_num_txn + 1)] for _ in range(total_num + 1)]
    finished = [0 for _ in range(len(operation_by_time))]
    pending = [-1 for _ in range(total_num_txn + 1)]
    iteration_count = 0
    while finished.count(1) < len(finished):
        for i, op in enumerate(operation_by_time):
            if finished[i] != 0 or pending[op.txn_num] != -1: continue
            if op.op_type == "R":
                if op.value != -1:
                    no_lock = 1
                    for j in range(1, total_num_txn + 1):
                        if (lock_status[op.value][j][0] == "W" or lock_status[op.value][j][0] == "D" or
                            lock_status[op.value][j][0] == "I") and op.op_time > int(lock_status[op.value][j][1:]) and \
                                lock_status[op.value][0][1:] != str(op.txn_num):
                            no_lock = 0
                            break
                    if no_lock == 1:
                        finished[i] = 1
                        if lock_status[op.value][0] == "N":
                            lock_status[op.value][0] = "R" + str(op.txn_num)
                    else:
                        pending[op.txn_num] = 1
                    lock_status[op.value][op.txn_num] = "R" + str(op.op_time)
                else:
                    no_lock = 1
                    for j in range(total_num + 1):
                        if no_lock == 0: break
                        for k in range(1, total_num_txn + 1):
                            if (lock_status[j][k][0] == "W" or lock_status[j][k][0] == "D" or lock_status[j][k][
                                0] == "I") and op.op_time > int(lock_status[j][k][1:]) and lock_status[j][0][1:] != str(
                                op.txn_num):
                                no_lock = 0
                                break
                    if no_lock == 1:
                        finished[i] = 1
                    else:
                        pending[op.txn_num] = 1
                    for j in range(total_num + 1):
                        lock_status[j][op.txn_num] = "R" + str(op.op_time)
                        if finished[i] == 1 and lock_status[j][0] == "N":
                            lock_status[j][0] = "R" + str(op.txn_num)
            elif op.op_type == "W" or op.op_type == "D" or op.op_type == "I":
                no_lock = 1
                for j in range(1, total_num_txn + 1):
                    if lock_status[op.value][j] != "N" and j != op.txn_num and op.op_time > int(
                            lock_status[op.value][j][1:]):
                        no_lock = 0
                        break
                if lock_status[op.value][0] == "W" + str(op.txn_num):
                    no_lock = 1
                if no_lock == 1:
                    finished[i] = 1
                    if lock_status[op.value][0] == "N":
                        lock_status[op.value][0] = "W" + str(op.txn_num)
                else:
                    pending[op.txn_num] = 1
                lock_status[op.value][op.txn_num] = op.op_type + str(op.op_time)
            elif op.op_type == "C" or op.op_type == "RB":
                for j in range(total_num + 1):
                    lock_status[j][op.txn_num] = "N"
                    if lock_status[j][0][1:] == str(op.txn_num):
                        lock_status[j][0] = "N"
                finished[i] = 1
                for j in range(1, total_num_txn + 1):
                    pending[j] = -1
            break
        iteration_count += 1
        if iteration_count > 2 * len(finished) + 1:
            break
    if iteration_count > 2 * len(finished) + 1:
        return "Rollback"
    else:
        return "Avoid"


"""
Detect anomaly in MySQL environment at "REPEATABLE-READ" isolation level.

Args:
- total_num (int): Total number of variables.
- total_num_txn (int): Total number of transactions.
- operation_by_time (list of Operation): List of operations ordered by time sequence.
- validate_group (list of str list): List of validation data from test cases.
- data_record (list of int list): List of data in int form.

Returns:
Str: A bug detection result in MySQL environment at "READ-REPEATABLE" isolation level, including "Avoid", "Anomaly" and "Rollback".
The matched test case number is attached to the right side of "Avoid" message.
"""


def mysql_readrepeatable_check(total_num, total_num_txn, operation_by_time, validate_group, data_record):
    lock_status = [["N" for _ in range(total_num_txn + 1)] for _ in range(total_num + 1)]
    finished = [0 for _ in range(len(operation_by_time))]
    pending = [-1 for _ in range(total_num_txn + 1)]
    txn_version = [1 for _ in range(total_num_txn + 1)]
    begin = [-1 for _ in range(total_num_txn + 1)]
    iteration_count = 0
    test_group = []
    pre_version = [copy.deepcopy(data_record) for _ in range(total_num_txn + 1)]
    for op in operation_by_time:
        if begin[op.txn_num] == -1:
            begin[op.txn_num] = op.op_time
    while finished.count(1) < len(finished):
        for i, op in enumerate(operation_by_time):
            if finished[i] != 0 or pending[op.txn_num] != -1:
                continue
            if op.op_type == "R":
                finished[i] = 1
                is_write = 0
                con = 0
                for j in operation_by_time:
                    if j.txn_num == op.txn_num and j.op_time < op.op_time:
                        if j.op_type != "R" and j.op_type != "C" and j.value == op.value: con = op.txn_num
                        is_write = 1
                        if j.op_type == "C":
                            is_write = 0
                            con = 0
                    if j.op_time > op.op_time: break
                if is_write == 0:
                    pre_version[op.txn_num] = copy.deepcopy(pre_version[0])
                for data in pre_version[con]:
                    if (data[0] == op.value or op.value == -1) and (
                            data[3] == -1 or txn_version[op.txn_num] < data[3]) and txn_version[op.txn_num] >= data[2]:
                        test_group.append([str(op.op_time), str(data[0]), str(data[1])])
            elif op.op_type == "W" or op.op_type == "D" or op.op_type == "I":
                no_lock = 1
                for j in range(1, total_num_txn + 1):
                    if lock_status[op.value][j][0] == "W" and j != op.txn_num and op.op_time > int(
                            lock_status[op.value][j][1:]):
                        no_lock = 0
                        break
                if lock_status[op.value][0] == "W" + str(op.txn_num):
                    no_lock = 1
                if no_lock == 1:
                    finished[i] = 1
                    if lock_status[op.value][0] == "N":
                        lock_status[op.value][0] = "W" + str(op.txn_num)
                    txn_version[op.txn_num] += 1
                    if op.op_type == "W":
                        for data in pre_version[op.txn_num]:
                            if data[0] == op.value and data[3] == -1:
                                data[3] = txn_version[op.txn_num]
                        pre_version[op.txn_num].append([op.value, op.keyvalue, txn_version[op.txn_num], -1])
                    elif op.op_type == "D":
                        for data in pre_version[op.txn_num]:
                            if data[0] == op.value and data[3] == -1:
                                data[3] = txn_version[op.txn_num]
                    elif op.op_type == "I":
                        pre_version[op.txn_num].append([op.value, op.keyvalue, txn_version[op.txn_num], -1])
                else:
                    pending[op.txn_num] = 1
                lock_status[op.value][op.txn_num] = op.op_type + str(op.op_time)
            elif op.op_type == "C" or op.op_type == "RB":
                for j in range(total_num + 1):
                    lock_status[j][op.txn_num] = "N"
                    if lock_status[j][0][1:] == str(op.txn_num):
                        lock_status[j][0] = "N"
                finished[i] = 1
                for j in range(1, total_num_txn + 1):
                    pending[j] = -1
                is_write = 0
                for j in operation_by_time:
                    if j.op_type != "R" and j.txn_num == op.txn_num and j.op_time < op.op_time:
                        is_write = 1
                        break
                if is_write == 1 and op.op_type == "C":
                    new_version = txn_version[op.txn_num]
                    for j in range(len(txn_version)):
                        if begin[j] > begin[op.txn_num]:
                            txn_version[j] = new_version
                    for j in pre_version[op.txn_num]:
                        if j in pre_version[0]: continue
                        ill = 0
                        for cnt, k in enumerate(pre_version[0]):
                            if j[:3] == k[:3]:
                                if k[3] == -1:
                                    del pre_version[0][cnt]
                                else:
                                    ill = 1
                            elif j[0] == k[0]:
                                if j[2] >= k[2]:
                                    del pre_version[0][cnt]
                                else:
                                    ill = 1
                        if ill == 0: pre_version[0].append(j)
            break
        iteration_count += 1
        if iteration_count > 2 * len(finished) + 1:
            break
    if iteration_count > 2 * len(finished) + 1:
        return "Rollback"
    else:
        for cnt, i in enumerate(validate_group):
            can_pass = 1
            for j in test_group:
                if j not in i:
                    can_pass = 0
                    break
            if can_pass == 1:
                return "Avoid [" + (str)(cnt + 1) + "]"
        return "Anomaly"


"""
Assumption:
The modifications of transactions at any isolation level are mutually visible, which is equivalent to a single storage, without read-write buffer.
This program sets isolation level to "serializable" for all test cases.
Assume that the inserted data key is in ascending order from 0.
Assume all execution time Qi have no "finished" statement in dynamic test.
"""

run_result_folder = "pg/serializable"
result_folder = "check_result/" + run_result_folder
do_test_list = "do_test_list.txt"
ts_now = time.strftime("%Y%m%d_%H%M%S", time.localtime())
if not os.path.exists(result_folder):
    os.makedirs(result_folder)

with open(do_test_list, "r") as f:
    files = f.readlines()
for file in files:
    file = file.replace("\n", "")
    file = file.replace(" ", "")
    if file == "":
        continue
    if file[0] == "#":
        continue
    with open(run_result_folder + "/" + file + ".txt", "r") as f:
        lines = f.readlines()

    total_num = get_total(lines)  # total number of variables
    total_num_txn = get_total_txn(lines)  # total number of txn
    txn = [Txn() for i in range(total_num_txn + 2)]  # total num of transaction
    data_op_list = [[] for i in range(total_num + 2)]  # record every operation that occurs on the variable
    edge = [[] for i in range(total_num_txn + 2)]  # all edges from the current point
    error_type = [[0 for _ in range(4)] for i in range(total_num_txn + 2)]
    indegree = [0] * (total_num_txn + 2)  # in-degree of each point
    visit = [0] * (total_num_txn + 2)  # in dfs, whether the current point has been visited
    visit1 = [0] * (total_num_txn + 2)  # we will only use unvisited points as the starting point of the dfs
    path = []  # points in cycle
    edge_type = []  # edge type of the cycle
    version_list = [[] for i in range(total_num + 2)]
    go_end = False  # if test result is "Rollback" or "Timeout", we will don't check
    data_record = []  # record data of operations in int form
    validate_group = get_validation(lines)  # The validation data from test cases.
    operation_by_time = []  # record all operations after Q0

    error_message = ""
    for query in lines:
        query = query.replace("\n", "")
        query = query.replace(" ", "")
        if query.find("Rollback") != -1 or query.find("Timeout") != -1:
            go_end = True
        error_message = operation_record(total_num, query, txn, data_op_list, version_list, operation_by_time,
                                         data_record)
        if error_message != "":
            break

    if error_message != "":
        output_result(file, "Error")
        print_error(result_folder)
        continue

    cycle = False

    build_graph(data_op_list, indegree, edge, txn, error_type)
    if find_isolation(lines) == "serializable":
        check_result = mysql_serializable_check(total_num, total_num_txn, operation_by_time)
    elif find_isolation(lines) == "read-repeatable":
        check_result = mysql_readrepeatable_check(total_num, total_num_txn, operation_by_time, validate_group,
                                                  data_record)

    print("{}".format(file) + ": " + check_result + "\n")

    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
        f.write("{}".format(file) + ": " + check_result + "\n\n")
