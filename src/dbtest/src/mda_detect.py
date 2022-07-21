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


import Queue
import os
import time


class Edge:
    def __init__(self, type, out):
        self.type = type
        self.out = out


class Operation:
    def __init__(self, op_type, txn_num, op_time, value):
        self.op_type = op_type
        self.txn_num = txn_num
        self.op_time = op_time
        self.value = value


class Txn:
    def __init__(self):
        self.begin_ts = -1
        self.end_ts = 99999999999999999999


# find total variable number
def get_total(lines):
    num = 0
    for query in lines:
        query = query.replace("\n", "")
        query = query.replace(" ", "")
        if query[0:2] == "Q0" and query.find("INSERT") != -1:
            tmp = find_data(query, "(")
            num = max(num, tmp)
        elif query[0:2] == "Q1":
            break
    return num


# extract the data we need in query
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


# when a statement is executed, set the end time and modify the version list
def set_finish_time(op_time, data_op_list, query, txn, version_list):
    pos = query.find("finishedat:")
    pos += len("finishedat:")
    data_value = ""
    tmp, tmp1 = "", ""
    for i in range(pos, len(query)):
        if query[i].isdigit():
            tmp += query[i]
        else:
            for j in range(3 - len(tmp)):
                tmp1 += "0"
            tmp = tmp1 + tmp
            data_value += tmp
            tmp, tmp1 = "", ""
    data_value = int(data_value)
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


# if both transactions are running
# or the start time of the second transaction is less than the end time of the first transaction
# we think they are concurrent
def check_concurrency(data1, data2, txn):
    if txn[data2.txn_num].begin_ts < txn[data1.txn_num].end_ts:
        return True
    elif txn[data1.txn_num].begin_ts < txn[data2.txn_num].end_ts:
        return True
    else:
        return False


# decide which operation comes first depending on the read or write version
# if later operation happened after the first txn commit time, edge type will add "C"
def get_edge_type(data1, data2, txn):
    if data1.value <= data2.value:
        before, after = data1, data2
    else:
        before, after = data2, data1
    # if data1.op_type == "D" or data2.op_type == "D":
    #     if data1.value < data2.value:
    #         before, after = data2, data1
    #     else:
    #         before, after = data1, data2
    if data2.op_time > txn[data1.txn_num].end_ts:
        state = "C"
    else:
        state = ""
    return before.op_type + state + after.op_type, before, after


def build_graph(data_op_list, indegree, edge, txn):
    for list1 in data_op_list:
        for i, data in enumerate(list1):
            for j in range(0, i):
                insert_edge(list1[j], data, indegree, edge, txn)


def insert_edge(data1, data2, indegree, edge, txn):
    if check_concurrency(data1, data2, txn):
        edge_type, data1, data2 = get_edge_type(data1, data2, txn)
        if edge_type != "RR" and edge_type != "RCR" and data1.txn_num != data2.txn_num:
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num))


def init_record(query, version_list):
    key = find_data(query, "(")
    value = find_data(query, ",")
    version_list[key].append(value)


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
    # for i, list1 in enumerate(data_op_list):
    #     print(i)
    #     if list1:
    #         print("")
    #         print(list1[0].txn_num)
    #         print(list1[0].op_type)
    #         print(list1[0].op_time)
    #         print(list1[0].op_value)
            



def read_record(op_time, txn_num, total_num, txn, data_op_list):
    if txn[txn_num].begin_ts == -1:
        txn[txn_num].begin_ts = op_time
    # for some distributed cases which have 4 param, write part is same
    if query.find("value1=") != -1:
        op_data = find_data(query, "value1=")
        data_op_list[op_data].append(Operation("R", txn_num, op_time, op_data))
    # for normal cases
    elif query.find("k=") != -1:
        op_data = find_data(query, "k=")
        data_op_list[op_data].append(Operation("R", txn_num, op_time, op_data))
    # for predicate cases
    elif query.find("k>") != -1:
        left = find_data(query, "k>") + 1
        right = find_data(query, "k<")
        for i in range(left, right):
            data_op_list[i].append(Operation("P", txn_num, op_time, i))
    elif query.find("value1>") != -1:
        left = find_data(query, "value1>") + 1
        right = find_data(query, "value1<")
        for i in range(left, right):
            data_op_list[i].append(Operation("P", txn_num, op_time, i))
    else:
        # it means select all rows in table
        for i in range(total_num):
            data_op_list[i].append(Operation("R", txn_num, op_time, i))


def write_record(op_time, txn_num, txn, data_op_list):
    if txn[txn_num].begin_ts == -1:
        txn[txn_num].begin_ts = op_time
    if query.find("value1=") != -1:
        op_data = find_data(query, "value1=")
        op_value = find_data(query, "value2=")
        data_op_list[op_data].append(Operation("W", txn_num, op_time, op_value))
    elif query.find("k=") != -1:
        op_data = find_data(query, "k=")
        op_value = find_data(query, "v=")
        data_op_list[op_data].append(Operation("W", txn_num, op_time, op_value))


def delete_record(op_time, txn_num, txn, data_op_list):
    if txn[txn_num].begin_ts == -1:
        txn[txn_num].begin_ts = op_time
    if query.find("value1=") != -1:
        op_data = find_data(query, "value1=")
        data_op_list[op_data].append(Operation("D", txn_num, op_time, op_data))
    elif query.find("k=") != -1:
        op_data = find_data(query, "k=")
        data_op_list[op_data].append(Operation("D", txn_num, op_time, op_data))


def insert_record(op_time, txn_num, txn, data_op_list):
    if txn[txn_num].begin_ts == -1 and op_time != 0:
        txn[txn_num].begin_ts = op_time
    key = find_data(query, "(")
    value = find_data(query, ",")
    data_op_list[key].append(Operation("I", txn_num, op_time, value))


def end_record(op_time, txn_num, txn):
    txn[txn_num].end_ts = op_time


def operation_record(total_num, query, txn, data_op_list, version_list):
    error_message = ""
    op_time = find_data(query, "Q")
    txn_num = find_data(query, "T")
    if op_time == 0 and query.find("INSERT") != -1:
        init_record(query, version_list)
        return error_message
    if query.find("returnresult") != -1:
        error_message = readVersion_record(query, op_time, data_op_list, version_list)
        return error_message
    if query.find("finished") != -1:
        set_finish_time(op_time, data_op_list, query, txn, version_list)
        return error_message
    if op_time == -1 or txn_num == -1:
        return error_message
    if query.find("SELECT") != -1:
        read_record(op_time, txn_num, total_num, txn, data_op_list)
        return error_message
    elif query.find("UPDATE") != -1:
        write_record(op_time, txn_num, txn, data_op_list)
        return error_message
    elif query.find("DELETE") != -1:
        delete_record(op_time, txn_num, txn, data_op_list)
        return error_message
    elif query.find("INSERT") != -1:
        insert_record(op_time, txn_num, txn, data_op_list)
        return error_message
    elif query.find("COMMIT") != -1:
        if op_time != 0:
            end_record(op_time, txn_num, txn)
        return error_message
    return error_message
    


# remove failed statements to prevent redundant edges from being built
def remove_unfinished_operation(data_op_list):
    for list1 in data_op_list:
        for i, op in enumerate(list1):
            if op.op_time < 10000000:
                list1.pop(i)

# toposort to determine whether there is a cycle
def check_cycle(edge, indegree, total):
    q = Queue.Queue()
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


# for loop graphs, print the loop
def dfs(result_folder, ts_now, now, type):
    visit1[now] = 1
    if visit[now] == 1: return
    visit[now] = 1
    path.append(now)
    edge_type.append(type)
    for v in edge[now]:
        if visit[v.out] == 0:
            dfs(result_folder, ts_now, v.out, v.type)
        else:
            path.append(v.out)
            edge_type.append(v.type)
            with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
                for i in range(0, len(path)):
                    f.write(str(path[i]))
                    if i != len(path) - 1: f.write("->" + edge_type[i+1] + "->")
                f.write("\n\n")
            path.pop()
            edge_type.pop()
    path.pop()
    edge_type.pop()
    visit[now] = 0


def print_path(result_folder, ts_now, edge):
    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
        flag = 0
        for i in range(len(edge)):
            for v in edge[i]:
                if flag == 0:
                    flag = 1
                else:
                    f.write(", ")
                f.write(str(i) + "->" + v.type + "->" + str(v.out))
        f.write("\n\n")


def output_result(file, result_folder, ts_now, IsCyclic):
    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
        f.write(file + ": " + IsCyclic + "\n")


def print_error(result_folder, ts_now, error_message):
    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
        f.write(error_message + "\n")
        f.write("\n\n")


run_result_folder = "pg/serializable"
result_folder = "check_result/" + run_result_folder
do_test_list = "do_test_list.txt"
#ts_now = "_2param_3txn_insert"
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
    txn = [Txn() for i in range(total_num + 2)]  # total num of transaction
    data_op_list = [[] for i in range(total_num + 2)]  # record every operation that occurs on the variable
    edge = [[] for i in range(total_num + 2)]  # all edges from the current point
    indegree = [0] * (total_num + 2)  # in-degree of each point
    visit = [0] * (total_num + 2)  # in dfs, whether the current point has been visited
    visit1 = [0] * (total_num + 2)  # we will only use unvisited points as the starting point of the dfs
    path = []  # points in cycle
    edge_type = []  # edge type of the cycle
    version_list = [[] for i in range(total_num + 2)]
    go_end = False  # if test result is "Rollback" or "Timeout", we will don't check

    error_message = ""
    for query in lines:
        query = query.replace("\n", "")
        query = query.replace(" ", "")
        if query.find("Rollback") != -1 or query.find("Timeout") != -1:
            go_end = True
        error_message = operation_record(total_num, query, txn, data_op_list, version_list)
        if error_message != "":
            break
    
    if error_message != "":
        output_result(file, result_folder, ts_now, "Error")
        print_error(result_folder, ts_now, error_message)
        continue
    
    cycle = False
    remove_unfinished_operation(data_op_list)
    build_graph(data_op_list, indegree, edge, txn)
    if not go_end:
        cycle = check_cycle(edge, indegree, total_num + 2)
    if cycle:
        output_result(file, result_folder, ts_now, "Cyclic")
        for i in range(total_num + 2):
            if visit1[i] == 0:
                dfs(result_folder, ts_now, i, "null")
    else:
        output_result(file, result_folder, ts_now, "Avoid")
        print_path(result_folder, ts_now, edge)
