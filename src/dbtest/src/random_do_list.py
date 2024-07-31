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

# op_set = ["RW", "WR", "WW", "IP", "PI", "DP", "PD", "IR", "RI", "DR", "RD", "RCW", "WCR", "WCW", "ICP", "PCI", "DCP", "PCD",  "ICR", "RCI", "DCR", "RCD"]
# op_set = ["RW", "WR", "IP", "PI", "DP", "PD"]
op_set = ["P", "R", "W", "I"]  # operations involved in the case
illegal_op_set = ["RR", "RP", "PR", "PP"]  # patterns can't occur in the case
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
        print(data_count1)
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


# data_num = int(sys.argv[1])
# target_num = int(sys.argv[2])

# database = sys.argv[3]
# isolation_level = sys.argv[4]


# Define sets for read and write operations
read_op_set = ["P", "R"]
write_op_set = ["I", "W"]


"""
To avoid dirty writes, a long write lock is acquired before the write operation.
In all cases of W1[x]W2[x], W2[x] is blocked until the transaction containing W1[x] is committed.
This breaks some cycles in the POP graph mainly in two ways:

1. Adjusting the order of transactions to make them serializable:
   Example: W1[X], W2[X], W1[X], C1, C2 (POP: W1[x]W2[x], W2[X]W1[x])
   can be serialized as W1[X] -> W1[X] -> C1 -> W2[X] -> C2 (POP: W1[X]C W2[x])
   Another example: W1[X], W2[X], R1[X], C1, C2 (POP: W1[x]W2[x], W2[X]R1[x])
   can be serialized as W1[X] -> R1[X] -> C1 -> W2[X] -> C2 (POP: W1[X]C W2[x], R1[X]C W2[x])

2. Causing deadlocks that prevent transaction sequences from being executed:
   Example: W1[X], W2[Y], W2[X], W1[Y], C1, C2 (POP: W1[x]W2[x], W2[Y]W1[Y]) causes a deadlock.
   Deadlocks in the POP occur only with cycles formed by write dependencies.
   If there are other dependencies in the cycle, deadlocks do not occur in this case, because there only is long write lock.

Note: long write locks do not eliminate RW, WR, and WW dependencies on non-X tuples.
They eliminate WW, WR, and RW dependencies on X tuples.
Consider:
1. T1 == RW0 ==> T2 == PW0 ==> T1
2. T1 == RW0 ==> T2 == PW1 ==> T1
The first case does not result in a cycle after eliminating dirty writes.
The second case is not detected by the database but can lead to data inconsistency.
"""


def eliminate_dirty_write_lock(conflicts):
    num_txns = len(conflicts)
    broken_edges = set()  # To store broken edges caused by write dependencies

    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        value = conflict[-1]
        if len(conflict) == 3:  # Only consider WW conflicts, not WCW
            # WW conflict, the second write op needs to be blocked until the txn of the first op commits
            if first_op in write_op_set and second_op in write_op_set:
                if i < num_txns - 1:
                    i += 1  # Skip the next conflict
                    next_value = conflicts[i][-1]
                    # If the next transaction has a dependency with the current transaction on the same object x, break this edge
                    if value == next_value:
                        broken_edges.add((i - 1, i))

    # If we have broken some edges, then the cycle in POP is broken
    if not broken_edges:
        return True

    # Check write cycles
    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        value = conflict[-1]

        if first_op in read_op_set:  # other dependency
            return False
        else:
            if len(conflict) == 3:
                if second_op in read_op_set:  # WR conflict: check if W1[X]R2[X] -> ?2[X]W2[X] then W1[X] -> W2[X]
                    if i < num_txns - 1:
                        i += 1  # Skip the next conflict
                        next_value = conflicts[i][-1]
                        next_second_op = conflicts[i][-2]
                        if value != next_value or next_second_op in read_op_set:
                            return False
            elif second_op not in write_op_set:  # not WCW
                return False
    # contains a write cycles
    return True


"""
To avoid dirty reads, a short read lock is acquired before the read operation.
In all cases of W1[x]R2[x], R2[x] is blocked until the transaction containing W1[x] is committed.
This breaks some cycles in the POP graph mainly in two ways:
1. Adjusting the order of transactions to make them serializable
2. Causing deadlocks that prevent transaction sequences from being executed:
Deadlocks in the POP occur only with cycles formed by write and read dependencies.

Note: Short read locks do not eliminate RW, WR, and WW dependencies on non-X tuples.
They eliminate WW, WR, and RW dependencies on X tuples.
"""


def eliminate_dirty_read_lock(conflicts):
    num_txns = len(conflicts)
    broken_edges = set()  # To store broken edges caused by write dependencies

    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        value = conflict[-1]
        if len(conflict) == 3:  # Only consider WR conflicts, not WCR
            # WR conflict, the second write op needs to be blocked until the txn of the first op commits
            if first_op in write_op_set and second_op in read_op_set:
                if i < num_txns - 1:
                    i += 1  # Skip the next conflict
                    next_value = conflicts[i][-1]
                    # If the next transaction has a dependency with the current transaction on the same object x, break this edge
                    if value == next_value:
                        broken_edges.add((i - 1, i))

    # If we have broken some edges, then the cycle in POP is broken
    if not broken_edges:
        return True

    # Check write and read cycles
    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        value = conflict[-1]

        if first_op in read_op_set:  # other dependency
            return False
        else:
            if len(conflict) == 3:
                if second_op in read_op_set:  # WR conflict: check if W1[X]R2[X] -> ?2[X]W2[X] then W1[X] -> W2[X]
                    if i < num_txns - 1:
                        next_value = conflicts[i+1][-1]
                        next_second_op = conflicts[i+1][-2]
                        if value == next_value and next_second_op in write_op_set:
                            i += 1  # Skip the next conflict
    # contains a write and read cycle
    return True


"""
With MVCC, a snapshot is taken before each read operation, converting W_1[X]R_2[X] to R_2[X]CW_1[X]. 
This can eliminate some cycles in POP.
"""


def eliminate_dirty_read_mvcc(conflicts):
    num_txns = len(conflicts)
    broken_edges = set()  # To store broken edges caused by write dependencies

    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        if len(conflict) == 3:  # Only consider WR conflicts, not WCR
            if first_op in write_op_set and second_op in read_op_set:
                if (i, i+1 % (num_txns)) in broken_edges:  # if break twice, then it is same as the origin POP(differ edge type)
                    broken_edges.remove((i, i+1 % (num_txns)))
                else:
                    broken_edges.add(i+1 % (num_txns), i)
    # If we have broken some edges, then the cycle in POP is broken
    if not broken_edges:
        return True


"""
To avoid dirty reads, a long read lock is acquired before the read operation.
In all cases of R1[x]W2[x], W2[x] is blocked until the transaction containing R1[x] is committed.
This breaks some cycles in the POP graph mainly in two ways:
1. Adjusting the order of transactions to make them serializable
2. Causing deadlocks that prevent transaction sequences from being executed:
Deadlocks in the POP occur only with cycles formed by write, read and anti-dependencies. 

Note: 
1.long read locks do not eliminate RW, WR, and WW dependencies on non-X tuples.
They eliminate WW, WR, and RW dependencies on X tuples.
2.not include P-I edge, because roww-level locks do not constraint the predicate's condition range
"""


def eliminate_non_repatable_read_lock(conflicts):
    num_txns = len(conflicts)
    broken_edges = set()  # To store broken edges caused by write dependencies

    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        value = conflict[-1]
        if len(conflict) == 3:  # Only consider RW conflicts, not RCW
            # RW conflict, the second write op needs to be blocked until the txn of the first op commits
            if first_op is "R" and second_op in write_op_set:  # note not include "P", because we use row-level lock
                if i < num_txns - 1:
                    i += 1  # Skip the next conflict
                    next_value = conflicts[i][-1]
                    # If the next transaction has a dependency with the current transaction on the same object x, break this edge
                    if value == next_value:
                        broken_edges.add((i - 1, i))

    # If we have broken some edges, then the cycle in POP is broken
    if not broken_edges:
        return True

    # Check cycles
    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        value = conflict[-1]
        if first_op is "P" and second_op is "I":
            return False

    # contains a write and read cycle
    return True


"""
mysql-style
With MVCC, a snapshot is taken before the first non-control statement of each transaction is executed, 
and W_1[X]R_2[X] is converted to R_2[X]CW_1[X]. 
This can eliminate some cycles in POP.
Unlike read-committed, it will also convert some W[X]CR[X] to R[X]W[X].
"""


def eliminate_non_repeatable_read_mvcc1(conflicts):
    num_txns = len(conflicts)
    broken_edges = set()  # To store broken edges caused by write dependencies

    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        if first_op in write_op_set and second_op in read_op_set:

            if len(conflict) == 3 or i == num_txns-1:  # consider WCR
                # WR(WCR) conflict, the second write op needs to be blocked until the txn of the first op commits
                if (i, i+1 % (num_txns)) in broken_edges:
                    broken_edges.remove((i, i+1 % (num_txns)))
                else:
                    broken_edges.add(i+1 % (num_txns), i)
    # If we have broken some edges, then the cycle in POP is broken
    if not broken_edges:
        return True
    return False


"""
postgresql-style
With MVCC, a snapshot is taken before the first non-control statement of each transaction is executed, and W_1[X]R_2[X] is converted to R_2[X]CW_1[X]. This can eliminate some cycles in POP.
Unlike read-committed, it will also convert some W[X]CR[X] to R[X]W[X].

Unlike mysql-style, pg does not only use long write locks to avoid dirty writes.
If concurrent writes occur, the later updated write operation needs to wait for the previous write operation.
If the previous write operation is committed, the transaction in which the later updated write operation is located must be rolled back.

"""
def eliminate_non_repeatable_read_mvcc2(conflicts):
    num_txns = len(conflicts)
    broken_edges = set()  # To store broken edges caused by write dependencies

    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        if len(conflict) == 3:  # Only consider WW conflicts, not WCW
            # WW conflict, the second write op needs to be blocked until the txn of the first op commits
            if first_op in write_op_set and second_op in write_op_set:
                if i < num_txns - 2:
                    broken_edges.add((i, i+1))
                    broken_edges.add((i+1, i+2))
                    i += 2  # Skip the next two conflicts

    # If we have broken some edges, then the cycle in POP is broken
    if not broken_edges:
        return True

    for i in range(num_txns):
        conflict = conflicts[i]
        first_op = conflict[0]
        second_op = conflict[-2]
        if first_op in write_op_set and second_op in read_op_set:

            if len(conflict) == 3 or i == num_txns-1:
                # WR(WCR) conflict, the second write op needs to be blocked until the txn of the first op commits
                if (i, i+1 % (num_txns)) in broken_edges:
                    broken_edges.remove((i, i+1 % (num_txns)))
                else:
                    broken_edges.add(i+1 % (num_txns), i)
    # If we have broken some edges, then the cycle in POP is broken
    if not broken_edges:
        return True


def eliminate_phantom_read_pred_lock(conflicts):
    # num_txns = len(conflicts)
    # broken_edges = set()  # To store broken edges caused by write dependencies

    # for i in range(num_txns):
    #     conflict = conflicts[i]
    #     first_op = conflict[0]
    #     second_op = conflict[-2]
    #     value = conflict[-1]
    #     if len(conflict) == 3:  # Only consider RW conflicts, not RCW
    #         # RW conflict, the second write op needs to be blocked until the txn of the first op commits
    #         if first_op in read_op_set and second_op in write_op_set:  # note not include "P", because we use row-level lock
    #             if i < num_txns - 1:
    #                 i += 1  # Skip the next conflict
    #                 next_value = conflicts[i][-1]
    #                 # If the next transaction has a dependency with the current transaction on the same object x, break this edge
    #                 if value == next_value:
    #                     broken_edges.add((i - 1, i))

    # # If we have broken some edges, then the cycle in POP is broken
    # if not broken_edges:
    #     return True

    # # Check cycles
    
    return True


data_num = 2
target_num = 2
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
