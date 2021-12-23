# 3TS-DA 测试

这个文档是一致性测试说明文档，用户可以产生事务执行序列，通过环检测我们可以得到一致的和有异常的数据。我们可以通过一致的数据测试算法的假回滚率，通过有异常的数据测试算法的正确性。

##  产生数据

#### a. 在3TS目录下，从模板配置拷贝配置文件
 
    cp config.cfg.template config.cfg
 
下面是一个配置文件的例子，使用DLI_IDENTIFY产生3-2-6（事务大小-操作数据项-操作数目）的配置：
Check it out an example configuration in config.cfg
 
    FilterRun = {
      thread_num = 10L; // number of threads
      generator = "TraversalGenerator"; // history generator
      outputters = ("CompareOutputter", "RollbackRateOutputter"); // result outputters
      algorithms = ( // concurrency control algorithms and filters
        { name = "DLI_IDENTIFY"; } 
      );
    };

    /* ========== history generators ========= */
    // Generate all histories meeting such conditions.
    TraversalGenerator = {
      trans_num = 3L; // number of transactions
            item_num = 2L; // number of variable items
            max_dml = 6L; // max number of DML operations
            subtask_num = 10L; // number of subtasks
            subtask_id = 0L; // the id of subtask to run
            with_abort = true; // generate history with Abort operation
      with_scan = "NONE_HAVE"; // generate history with ScanOdd operation ("NONE_HAVE", "ALL_HAVE", "NO_LIMIT")
      with_write = "NO_LIMIT"; // generate history with Write operation ("NONE_HAVE", "ALL_HAVE", "NO_LIMIT")
            tcl_position = "ANYWHERE"; // generate TCL operation in history position ("TAIL", "ANYWHERE", "NOWHERE")
      allow_empty_trans = false; // transactions generated can be without DML operations
      dynamic_history_len = false; // number of DML operation can be less than <max_dml>
      anomaly_rank = "RAT_WAT_IAT"; //IAT_RAT_WAT //WAT_RAT_IAT//RAT_WAT_IAT 
    };
 

#### b. 编译代码
 ```
    ./make.sh
 ```
 
编译成功产生 '3TS' 文件。

#### c. 运行产生数据
```
    ./3TS --conf_path=config.cfg
 ```


## 测试假回滚率
从上面的运行中，我们产生数据 'compare.txt'，里面包括一致和有异常的数据，分别用 'DLI_IDENTIFY OK' 和 'NOT DLI_IDENTIFY OK' 标识。测试假回滚，我们需要一致的数据，在'DLI_IDENTIFY OK'下可以截取出来。

#### a. 通过以下脚本获取一致的数据，并拷贝到 'input.txt'
 ```
    sed -n '1,/] DLI_IDENTIFY OK/!p' compare.txt > input.txt
 ```
 
#### b. 我们也可以通过以下脚本获取有异常的数据：
```
    sed -n '/NOT DLI_IDENTIFY OK/,/] DLI_IDENTIFY OK/ { /NOT DLI_IDENTIFY OK/! { /] DLI_IDENTIFY OK/! p } }' compare.txt > input.txt
```

#### c. 然后我们可以用一致的数据来测试假回滚，用有异常的数据来测试算法的正确性。我们需要把 'input.txt' 放在测试目录下（比如 '3TS/contrib/deneva '）。
我们首先需要在配置文件 'config.h' 声明工作负载为DA，指定需要测试的算法，比如：
```
    #define WORKLOAD DA
    #define CC_ALG SSI
```

在一致性测试中，我们需要使用单线程操作，修改配置文件 'config.h' 中的配置：
```
    #define NODE_CNT 1
    #define THREAD_CNT 1
    #define REM_THREAD_CNT 1
    #define SEND_THREAD_CNT 1

    #define CLIENT_NODE_CNT 1
    #define CLIENT_THREAD_CNT 1
    #define CLIENT_REM_THREAD_CNT 1
    #define CLIENT_SEND_THREAD_CNT 1
```

另外，服务端和客户端需要在一台机器上运行，需要修改 'ifconfig.txt' 配置文件，给定需要运行的IP：
```
    166.111.69.50
    166.111.69.50
```

修改完参数之后，下一步就是运行程序，程序将会从 'input.txt' 读取数据，并且让算法逐条按序列执行，最后返回提交或者回滚信息。数据的例子如下：
```
    W0a R1b W1a R1c C1 W0b C0
    R2a R3b W2b W3a C2 C3
```

每一行代表一个执行序列数据。

然后我们可以通过如下运行程序：
```
    ./rundb -nid0    # server side
    ./runcl -nid1    # client side
```

程序结束后，我们可以在 'commit_histroy.txt' 查看结果。通过对比正式的执行情况，我们可以分析算法是否按设定逻辑执行。如果执行有异常的数据都回滚了，则表示算法运行正确。


