[text](mda_detect.py) 修改日志
# 思考
1. 用于加边建立图的节点对应一个操作还是一个事务。

# 原来代码中问题
### 数组访问越界问题
现象
```python
total_num = get_total(lines) # 统计的个数是插入数据的个数，不是事务的个数。
txn = [Txn() for i in range(total_num + 2)] # 导致构造的 txn 数组较小
.... # 还有 indegree edge 数组的大小应该是事务的个数。
``` 
解决：构造一个新函数获取事务个数。
```python
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
total_num_txn = get_total_txn(lines)  # total number of txn
```
效果：不同数据使用不同的初始长度
```python
# total_num:     data_op_list, version_list
# total_num_txn: txn, edge, total_num_txn, visit, visit1
```
