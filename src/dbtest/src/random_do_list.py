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

from operator import index
from pprint import pprint
import random
import sys
import os

#op_set = ["RW", "WR", "WW", "IP", "PI", "DP", "PD", "IR", "RI", "DR", "RD", "RCW", "WCR", "WCW", "ICP", "PCI", "DCP", "PCD",  "ICR", "RCI", "DCR", "RCD"]
#op_set = ["RW", "WR", "IP", "PI", "DP", "PD"]
op_set = ["P", "R", "W", "I"] # operations involved in the case
illegal_op_set = ["RR", "RP", "PR", "PP"] # patterns can't occur in the case
do_test_list = "do_test_list.txt"

total_test_num = 1
min_txn = 2
max_txn = 20

"""
Decide which variable to operate twice.

Args:
- data_count (list): A list of integers representing the current count of operations for each variable.
- total_num (int): The total number of operations counted so far.
- target_num (int): The target number of operations where some variables should be operated twice.

This function uses depth-first search (DFS) to explore different combinations of variables to operate twice.
If data_num = 3, target_num = 5, and data_count = [2, 1, 2], it means the first and third variables are operated twice.

"""
def dfs(data_count, total_num, target_num):
    s = ""
    for v in data_count:
        s += str(v)
        if v > 2:
            return
    if total_num == target_num and visit.count(s) == 0:
        visit.append(s)
        res = []
        data_count1 = []
        for v in data_count:
            data_count1.append(v)
        dfs1(data_count1, res, total_num)
    for i in range(data_num):
        data_count[i] += 1
        if total_num <= target_num:
            dfs(data_count, total_num+1, target_num)
        data_count[i] -= 1
    

"""
Generate test cases based on the current data_count.

Args:
- data_count (list): A list of integers representing the current count of operations for each variable.
- res (list): A list to store the generated test case.
- total_num (int): The total number of operations to be generated.

This function recursively generates test cases based on the given data_count. It explores different 
combinations of operations for each variable to create a test case.

"""
# According to now data_count to generate test case
def dfs1(data_count, res, total_num):
    if len(res) == total_num:
        popg = ""
        for i in range(total_num):
            popg += res[i]
            if i != total_num-1:
                popg += "-"
            else:
                popg += "\n"
        with open(do_test_list, "a+") as f:
            f.write(popg)
        return
    for i in range(len(data_count)):
        if data_count[i] != 0:
            data_count[i] -= 1
            for j in range(len(op_set)):
                for k in range(len(op_set)):
                    op = op_set[j] + op_set[k]
                    if illegal_op_set.count(op) == 0:
                        op += str(i)
                        res.append(op)
                        data_count1, data_count2 = [], []
                        for v in data_count:
                            data_count1.append(v)
                            data_count2.append(v)
                        dfs1(data_count1, res, total_num)
                        res.pop()
                        if len(res) != 0:
                            res1 = []
                            for v in res:
                                res1.append(v)
                            op1 = op_set[j] + "C" + op_set[k] + str(i)
                            res1.append(op1)
                            dfs1(data_count2, res1, total_num)
                            res1.pop()
            


data_num = int(sys.argv[1])
target_num = int(sys.argv[2])
data_count = [1] * data_num
visit = []
if os.path.exists(do_test_list):
    os.remove(do_test_list)
dfs(data_count, data_num, target_num)
# for i in range(total_test_num):
#     txn_num = random.randint(min_txn, max_txn)
#     popg = ""
#     commit_num = txn_num / 2
#     for j in range(txn_num):
#         while True:
#             op_num = random.randint(0, len(op_set)-1)
#             if op_num == 2 or op_num == 9:
#                 continue
#             if op_num <= 10:
#                 break
#             else:
#                 if commit_num != 0:
#                     commit_num -= 1
#                     break
#         popg += op_set[op_num]
#         popg += str(j)
#         if j != txn_num-1:
#             popg += "-"
#         else:
#             popg += "\n"
#     if i == 0:
#         with open(do_test_list, "w+") as f:
#             f.write(popg)
#     else:
#         with open(do_test_list, "a+") as f:
#             f.write(popg)
