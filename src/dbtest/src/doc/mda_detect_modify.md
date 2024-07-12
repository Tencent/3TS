[text](mda_detect.py) 修改日志
# 思考&分析
1. 用于加边建立图的节点对应一个操作还是一个事务。答：一个事务。
2. 目标：一个对数据库的操作文件（运行效果文件）中有多个事务，每一个事务有不同的隔离级别，通过执行结果和隔离级别判断是否满足一致性
3. 原来输出未所有事务是否满足一致性，现在输出为每个事务是否满足一致性？ 单个整体报错 or 多个报错？ 多个，每个错误都识别，兼容单个整体报错（实现较难）
4. 有两个检测思路：
    1. 修改建立图的过程中加边策略，保留循环检测流程。（当前实现方式）
    2. 保留加边策略，修改冲突检测流程。
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


### 默认字符串少了空格
```python
    pos = query.find("finished at:")
    pos += len("finished at:")
```

### "R" 类型的操作并没有修改 value 值为下标：
```python
    if data1.value <= data2.value:          
        before, after = data1, data2
    else:
        before, after = data2, data1
```

