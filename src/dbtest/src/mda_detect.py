# -*- coding: utf-8 -*-

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


class Edge:
    def __init__(self, type, out, begin_time):
        self.type = type
        self.out = out
        self.time = begin_time
    def __repr__(self):
        return "Edge(begin_time={}, type={}, out={})".format(self.time, self.type, self.out)

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
        self.isolation = "serializable"

# print edge after build graph
def print_graph(edge,txn,error_type):
    for i, edges in enumerate(edge):
        if i == 0 or i == len(edge)-1:
            continue
        print("Transaction {}:-----{}-----".format(i,txn[i].isolation))
        with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
            f.write("Transaction {}:-----{}-----".format(i,txn[i].isolation)+"\n")
        for e in edges:
            with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
                f.write("  {}".format(e) + "\n")
        error_output=[]
        if error_type[i][0] == 1:
            error_output.append("Dirty-Write")
        if error_type[i][1] == 1:
            error_output.append("Dirty-Read")
        if error_type[i][2] == 1:
            error_output.append("Unrepeatable-Read")
        if error_type[i][3] == 1:
            error_output.append("Phantom-Read")
        if error_output==[]:
            error_output.append("No error in this isolation level.")
        print("  Error Detection: "+'; '.join(error_output))
        with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
            f.write("  Error Detection: "+'; '.join(error_output)+"\n")


# print data_op_list
def print_data_op_list(data_op_list):
    for k,list in enumerate(data_op_list):
        if k< len(data_op_list)-1:
            print("\nk:{}---".format(k))
            for i, data in enumerate(list):
                    print("op:{}--{}-".format(data.op_type,data.txn_num))

# find total variable number
def get_total(lines):
    num = 0
    for query in lines:
        query = query.replace("\n", "")
        query = query.replace(" ", "")
        if query.find("INSERT") != -1: # query[0:2] == "Q0" and 
            tmp = find_data(query, "(")
            num = max(num, tmp)
        # elif query[0:2] == "Q1":
        #     break
    return num

# find total Txn number
def get_total_txn(lines):
    num = 0
    for query in lines:
        query = query.replace("\n", "")
        query = query.replace(" ", "")
        if query[0:1] == "Q" and query.find("T") != -1:
            tmp = find_data(query, "T")
            num = max(num, tmp)
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

# extract the isolation from content 
def find_isolation(query):
    if query.find("read-uncommitted") != -1:
        return "read-uncommitted"
    if query.find("read-committed") != -1:
        return "read-committed"
    if query.find("repeatable-read") != -1:
        return "repeatable-read"
    if query.find("serializable") != -1:
        return "serializable"

# when a statement is executed, set the end time and modify the version list
def set_finish_time(op_time, data_op_list, query, txn, version_list):
    # pos = query.find("finished at:")
    # pos += len("finished at:")
    # data_value = ""
    # tmp, tmp1 = "", ""
    # for i in range(pos, len(query)):
    #     if query[i].isdigit():
    #         tmp += query[i]
    #     else:
    #         for j in range(3 - len(tmp)):
    #             tmp1 += "0"
    #         tmp = tmp1 + tmp
    #         data_value += tmp
    #         tmp, tmp1 = "", ""
    # data_value = int(data_value)
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

# if both transactions are running
# or the start time of the second transaction is less than the end time of the first transaction
# we think they are concurrent
def check_concurrency(data1, data2, txn):
    if txn[data2.txn_num].begin_ts < txn[data1.txn_num].end_ts:
        return True
    elif txn[data1.txn_num].begin_ts < txn[data2.txn_num].end_ts: # TODO maybe a bug: don't need
        return True
    else:
        return False


def check_edge_exit(edge,src_txn,src_type,tar_txn,tar_type):
    for e in edge[src_txn]:
        if e.out == tar_txn and e.type[0] == src_type and e.type[-1] == tar_type:
            return True
    return False

# decide which operation comes first depending on the read or write version
# if later operation happened after the first txn commit time, edge type will add "C"
def get_edge_type(data1, data2, txn):
    if data1.op_time <= data2.op_time:          
        before, after = data1, data2
    else:
        before, after = data2, data1
    # if data1.op_type == "D" or data2.op_type == "D":
    #     if data1.value < data2.value:
    #         before, after = data2, data1
    #     else:
    #         before, after = data1, data2
    if after.op_time > txn[before.txn_num].end_ts: 
        state = "C"
    else:
        state = ""
    return before.op_type + state + after.op_type, before, after


def build_graph(data_op_list, indegree, edge, txn, error_type):
    for list1 in data_op_list:
        for i, data in enumerate(list1):
            for j in range(0, i):
                insert_edge(list1[j], data, indegree, edge, txn, error_type)



def insert_edge(data1, data2, indegree, edge, txn, error_type):
    if check_concurrency(data1, data2, txn):
        edge_type, data1, data2 = get_edge_type(data1, data2, txn)
        if data1.txn_num == data2.txn_num or edge_type in ["RCR", "RR"]:
            return 
        #* read-uncommitted： Dirty Write
        # WI does not exist. If it does, there must be an equivalent edge of WD + DI
        # II does not exist. If it does, there must be an equivalent edge of ID + DI
        # DW is allowed to exist. When UPDATE, use the condition to query the data containing D
        # DD does not exist. If it does, there must be an equivalent edge of DI + ID
        if edge_type in ["WCW", "WW", "WCD", "WD", "ICW","IW", "ICD", "ID", "DCW", "DW", "DCI", "DI"]:   
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num, data1.op_time))
            error_type[data1.txn_num][0]=1
        #* read-committed： Dirty Read
        elif edge_type in ["WCR","WR"] and (txn[data2.txn_num].isolation == "read-committed" or txn[data2.txn_num].isolation == "repeatable-read" or txn[data2.txn_num].isolation == "serializable"):   
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num, data1.op_time))
            error_type[data1.txn_num][1] = 1
        #* repeatable-read： Unrepeatable Read
        elif edge_type in ["RCW","RW"] and (txn[data1.txn_num].isolation == "repeatable-read" or txn[data1.txn_num].isolation == "serializable"):   
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num, data1.op_time))
            error_type[data1.txn_num][2] = 1
        #* serializable： Phantom Read
        elif edge_type in ["RCI","RI","RCD","RD"] and txn[data1.txn_num].isolation == "serializable":   
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num, data1.op_time))
            error_type[data1.txn_num][3] = 1
        #* serializable： Phantom Read
        elif edge_type in ["ICR","IR","DCR","DR"] and txn[data2.txn_num].isolation == "serializable":   
            indegree[data2.txn_num] += 1
            edge[data1.txn_num].append(Edge(edge_type, data2.txn_num,data1.op_time)) 
            error_type[data1.txn_num][3] = 1

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
            data_op_list[i].append(Operation("R", txn_num, op_time, i)) # P
    elif query.find("value1>") != -1:
        left = find_data(query, "value1>") + 1
        right = find_data(query, "value1<")
        for i in range(left, right):
            data_op_list[i].append(Operation("R", txn_num, op_time, i)) # p
    else:
        # it means select all rows in table
        for i in range(total_num+1):
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
    # for predicate cases
    elif query.find("k>") != -1:
        left = find_data(query, "k>") + 1
        right = find_data(query, "k<")
        for i in range(left, right):
            data_op_list[i].append(Operation("W", txn_num, op_time, i)) # P
    elif query.find("value1>") != -1:
        left = find_data(query, "value1>") + 1
        right = find_data(query, "value1<")
        for i in range(left, right):
            data_op_list[i].append(Operation("W", txn_num, op_time, i)) # p
    else:
        # it means select all rows in table
        for i in range(total_num+1):
            data_op_list[i].append(Operation("W", txn_num, op_time, i))

def delete_record(op_time, txn_num, txn, data_op_list):
    if txn[txn_num].begin_ts == -1:
        txn[txn_num].begin_ts = op_time
    if query.find("value1=") != -1:
        op_data = find_data(query, "value1=")
        data_op_list[op_data].append(Operation("D", txn_num, op_time, op_data))
    elif query.find("k=") != -1:
        op_data = find_data(query, "k=")
        data_op_list[op_data].append(Operation("D", txn_num, op_time, op_data))
    # for predicate cases
    elif query.find("k>") != -1:
        left = find_data(query, "k>") + 1
        right = find_data(query, "k<")
        for i in range(left, right):
            data_op_list[i].append(Operation("D", txn_num, op_time, i)) # P
    elif query.find("value1>") != -1:
        left = find_data(query, "value1>") + 1
        right = find_data(query, "value1<")
        for i in range(left, right):
            data_op_list[i].append(Operation("D", txn_num, op_time, i)) # p
    else:
        # it means select all rows in table
        for i in range(total_num+1):
            data_op_list[i].append(Operation("D", txn_num, op_time, i))

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
    #  print("total_num:{}, query:{},optime: {}, txn_num: {}\n".format(total_num,query, op_time, txn_num))
    if op_time == 0 and query.find("INSERT") != -1:
        init_record(query, version_list)
        return error_message
    if query.find("returnresult") != -1: #! 1"returnresult"  maybe don't exist
        error_message = readVersion_record(query, op_time, data_op_list, version_list)
        return error_message
    if query.find("finished") != -1: #! "finished"  maybe don't exist
        set_finish_time(op_time, data_op_list, query, txn, version_list)
        return error_message
    if op_time == -1 or txn_num == -1:
        return error_message
    if query.find("BEGIN") != -1: # TODO: Need a related interface, I assume that it is read from the do_test_list file.:
        txn[txn_num].isolation = find_isolation(query)
    elif query.find("SELECT") != -1:
        read_record(op_time, txn_num, total_num, txn, data_op_list)
    elif query.find("UPDATE") != -1:
        write_record(op_time, txn_num, txn, data_op_list)
    elif query.find("DELETE") != -1:    
        delete_record(op_time, txn_num, txn, data_op_list)
    elif query.find("INSERT") != -1:    #! assume existing data will not be inserted ("Rollback")
        insert_record(op_time, txn_num, txn, data_op_list)
    elif query.find("COMMIT") != -1:
        if op_time != 0:
            end_record(op_time, txn_num, txn)
    set_finish_time(op_time, data_op_list, query, txn, version_list)
    return error_message
    


# remove failed statements to prevent redundant edges from being built
def remove_unfinished_operation(data_op_list):
    for list1 in data_op_list:
        for i, op in enumerate(list1):
            if op.op_time < 10000000:
                list1.pop(i)

# toposort to determine whether there is a cycle
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


# for loop graphs, print the loop
def dfs(result_folder, ts_now , e):
    visit1[e.out] = 1
    if visit[e.out] == 1: return
    visit[e.out] = 1
    path.append(e)
    for v in edge[e.out]:
        if visit[v.out] == 0:
            dfs(result_folder, ts_now, v)
        else:
            path.append(v)
#            with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
            content = ""
            list_loop = []
            for i in range(len(path) - 1, -1, -1):
                if i != len(path) - 1 and path[i].out == path[len(path) - 1].out:
                    break
                index = 0
                while(index < len(list_loop) and path[list_loop[index]].time < path[i].time):
                    index += 1
                list_loop.insert(index,i)
            for idx in list_loop:
                content = content + "->" + path[idx].type + "->" + str(path[idx].out)
            content = str(path[list_loop[-1]].out) + content
            print(content)
            path.pop()
    path.pop()
    visit[e.out] = 0


# # for loop graphs, print the loop
# # Contains redundant edge information and the starting point of the ring is unreasonable
# def dfs(result_folder, ts_now, now, type):
#     visit1[now] = 1
#     if visit[now] == 1: return
#     visit[now] = 1
#     path.append(now)
#     edge_type.append(type)
#     for v in edge[now]:
#         if visit[v.out] == 0:
#             dfs(result_folder, ts_now, v.out, v.type)
#         else:
#             path.append(v.out)
#             edge_type.append(v.type)
#             with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
#                 for i in range(0, len(path)):
#                     f.write(str(path[i]))
#                     if i != len(path) - 1: f.write("->" + edge_type[i+1] + "->")
#                 f.write("\n\n")
#             path.pop()
#             edge_type.pop()
#     path.pop()
#     edge_type.pop()
#     visit[now] = 0

def print_path(result_folder, ts_now, edge):
#    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
    flag = 0
    s=""
    for i in range(len(edge)):
        for v in edge[i]:
            if flag == 0:
                flag = 1
            else:
                s+=", "
            s+=str(i) + "->" + v.type + "->" + str(v.out)
    print(s)

def output_result(file, result_folder, ts_now, IsCyclic):
    print(file + ": " + IsCyclic + "\n")


def print_error(result_folder, ts_now, error_message):
    print(error_message)
    print("\n")





#! ------Some assumption------
# The modifications of transactions at any isolation level are mutually visible, which is equivalent to a single storage, without read-write buffer
# There are statements to set the isolation level of each transaction in the input file, after "BEGIN"
    # BEGIN T1 set_isolation=repeatable-read
    # BEGIN T2 set_isolation=serializable
    # BEGIN T3 set_isolation=read-uncommitted
    # BEGIN T4 set_isolation=read-committed
# Assume that the inserted data key is in ascending order from 0

run_result_folder = "pg/mda_detect_test"
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
    total_num_txn = get_total_txn(lines)  # total number of txn
    txn = [Txn() for i in range(total_num_txn + 2)]  # total num of transaction
    data_op_list = [[] for i in range(total_num + 2)]  # record every operation that occurs on the variable
    edge = [[] for i in range(total_num_txn + 2)]  # all edges from the current point
    error_type=[[0 for _ in range(4)] for i in range(total_num_txn+2)]
    indegree = [0] * (total_num_txn + 2)  # in-degree of each point
    visit = [0] * (total_num_txn + 2)  # in dfs, whether the current point has been visited
    visit1 = [0] * (total_num_txn + 2)  # we will only use unvisited points as the starting point of the dfs
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
        # print("total_num:{}, total_num_txn:{}, query:{},ts_now: {}, file: {}\n".format(total_num,total_num_txn,query,ts_now,run_result_folder + "/" + file + ".txt"))
        error_message = operation_record(total_num, query, txn, data_op_list, version_list)
        if error_message != "":
            break
    
    if error_message != "":
        output_result(file, result_folder, ts_now, "Error")
        print_error(result_folder, ts_now, error_message)
        continue
    
    cycle = False
    # remove_unfinished_operation(data_op_list) 动态测试中默认所有的执行时间 Qi 都没有 finish 字段
    build_graph(data_op_list, indegree, edge, txn, error_type)
#    for i in data_op_list:
#        for j in i:
#            print(j.op_type,j.op_time,j.txn_num,j.value)
    print("--------file:{}--------".format(file))
    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
        f.write("--------file:{}--------".format(file) + "\n")
    print_graph(edge,txn,error_type)
    # print_data_op_list(data_op_list)
    if not go_end:
        cycle = check_cycle(edge, indegree, total_num_txn+2)
    if cycle:
        output_result(file, result_folder, ts_now, "Cyclic")
        for i in range(total_num_txn + 2):
            if visit1[i] == 0:
                # dfs(result_folder, ts_now, i, "null")
                dfs(result_folder, ts_now, Edge("null",i,-1))
    else:
        output_result(file, result_folder, ts_now, "Avoid")
        print_path(result_folder, ts_now, edge)
    print("---------------------------------\n")
    with open(result_folder + "/check_result" + ts_now + ".txt", "a+") as f:
        f.write("---------------------------------\n\n")
