# 3TS 性能测试步骤

本文档是测试说明文档，用户可以通过测试3TS里算法，得到一些性能数据如TPS。

## 配置可用IP
手动测试需要配置虚拟或者服务器IP，需要修改在'3TS/contrib/deneva'文件下的'ifconfig.txt'文件：


#### a. 配置虚拟IP：
```
ifconfig bond1:0 166.111.69.50 netmask 255.255.255.192 up
ifconfig bond1:1 166.111.69.51 netmask 255.255.255.192 up
```
上面的命令会绑定166.111.69.50和166.111.69.51到bind1接口。

#### b. 在'ifconfig.txt'下配置IPs：
```
166.111.69.50
166.111.69.51
```
后面我们起server和client时会配备一些参数(e.g., -nid0, nid1)，就会指定到对应行的IP。

##  免密登录

需要把生成的公钥 (e.g., id_rsa.pub)放到需要免密登录的机器的'~/. ssh/authorized_keys'文件下。

##  配置参数
在'3TS/contrib/deneva/config.h'下配置参数，下面是一个常用的一个参数：
``` 
#define THREAD_CNT 4
#define WORKLOAD YCSB
#define CC_ALG  NO_WAIT
```
其中他们分别带别线程数，YCSB的工作负载，和NO_WAIT的并发控制算法。

##  编译代码
```
make clean
make deps
make -j 10
```
##  运行代码
首先需要把编译好的 'rundb' 和 'runcl' 文件放在相应的服务器上 (比如在 '3TS/output/' 目录下)。

#### a. 启动服务端程序 (在 '3TS/output/' 目录下)：
```
./rundb -nid0
```
#### b. 启动客户端程序 (在 '3TS/output/' 目录下)：
```
./runcl -nid1
```
#### c. 在相应的机器上查看logs数据：

server log: 3TS/output/dbresult0.out
client log: 3TS/output/clresult1.out



# 详细的workload配置文件

想要跑不一样的工作附在，可以修改 'config.h' 文件去配置参数。

### 常规的参数

```
#define NODE_CNT 1     // node count
#define THREAD_CNT 64  // thread count
#define CC_ALG NO_WAIT // CC protocol
#define WORKLOAD YCSB  // benchmark, e.g., YCSB or TPCC
#define DISTRIBUTE_PERCENT 0.15    // distributed query rate for YCSB, distributed transction rate for TPCC
```

### YCSB负载相关的参数


```
#define SYNTH_TABLE_SIZE 16777216 // table size, i.e., number of tuple rows
#define TXN_WRITE_PERC 0.1 // update rate, 0.1 means 10% txns has writes and 90% txns are read-only
#define TUP_WRITE_PERC 0.1 // write/read ratio, 0.1 means 10% of operations are writes and 90% are reads in a txn
#define ZIPF_THETA 0.9     // skew factor, 0.9 means extreme skew data, 0.0 means the distribution is uniform
```


### TPCC负载相关的参数

```
#define NUM_WH 128       // table size, i.e., number of ware house
#define PERC_PAYMENT 0.0 // Payment percent, 0 means 100% payment txns, 1 means 100% neworder txns, 0.5 means payment and neworder txns are both 50%
```


# 使用脚本测试3TS性能

##  相关文件
以下相关文件在 '3TS/contrib/deneva/scripts' 目录下

```
experiment.py        # workload specification
run_experiments.py   # workload runs
run_config.py        # configuration of folder, username, IPs
vcloud_deploy.sh     # deploy the server/client machines by the IPs
helper.py            # helper functions
kill.sh              # kill process before start in case occupaied IP or ports
testall.sh           # start the scripts by specifying the workload names
```

## 配置负载参数

#### a. 编辑用户名，运行server和client程序的目录，执行程序的IP地址：
```
    username = "centos"
    vcloud_uname = '/testfolder/output'
    vcloud_machines = ["166.111.69.50", "166.111.69.51"] # configure the real IPs here
```
#### b. 在 'experiment&#46;py' 文件里编辑负载的参数，下面是一个标准的编辑模板，其中包括YCSB和TPCC工作负载的模板：

YCSB模板:
```
def ycsb_scaling_server_distribute(given_node, thread, given_skew, given_update_rate, given_rw_ratio, dist_perc,qs=10,ds=128):
    wl = 'YCSB'
    nnodes = [given_node]
    algos=['NO_WAIT','WAIT_DIE', 'MVCC','OPT_SSI','TIMESTAMP','OCC','SILO','WSI','MAAT']
    base_table_size=2097152*8*ds/128
    txn_write_perc = [given_update_rate]  
    tup_write_perc = [given_rw_ratio]
    load = [10000]
    tcnt = [thread]
    ctcnt = [thread/2]
    scnt = [thread/4]
    rcnt = [thread/4]
    skew = [given_skew]
    distribute_rate = [dist_perc]
    query_size = [qs]
    # the following are corresponding parameters in deneva
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT",'DISTRIBUTE_PERCENT',"REQ_PER_QUERY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr,dr,qs] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo,dr,qs in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos,distribute_rate,query_size)]
    return fmt,exp
```
TPCC模板:
```
def tpcc_scaling_server_distribute(given_nodes, thread, given_perment_perc, dist_perc):
    wl = 'TPCC'
    nnodes = [given_nodes]
    nalgos=['NO_WAIT','WAIT_DIE', 'MVCC','OPT_SSI','TIMESTAMP','OCC','SILO','WSI','MAAT']
    npercpay=[given_perment_perc]
    wh=128
    load = [10000]
    tcnt = [thread]
    ctcnt = [thread/2]
    scnt = [thread/4]
    rcnt = [thread/4]
    distribute_rate = [dist_perc]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT",'DISTRIBUTE_PERCENT']
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr,sthr,rthr,sthr,rthr,dr] for thr,cthr,sthr,rthr,tif,pp,n,cc,dr in itertools.product(tcnt,ctcnt,scnt,rcnt,load,npercpay,nnodes,nalgos,distribute_rate)]
    return fmt,exp
```
#### c. 配置一个负载：
```
# set up a workload by giving parameter, the query size and data size can be left empty.
# run on one server and one client with 64 thread
# skew is set to 0.5. update rate is 0.9, RW ratio is 0.9
# distributed rate is 0% (it does not matter for single machine), txn query size is 2.
def ycsb_scaling_query_size_02():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.9, 0.9, 0.0, 2) # node, thread, skew, update_rate, rw, dist rate, and query size

# configure the map function for the script run
experiment_map = {
    'ycsb_scaling_query_size_02':ycsb_scaling_query_size_02
}    
```


##  跑一个测试：

在 'testall&#46;sh' 文件里，需要给定一个需要运行的负载的名字：
```
python run_experiments.py -e -c vcloud ycsb_scaling_query_size_02
```

执行脚本文件即可运行负载测试：
```
./testall.sh
```
当执行此脚本时，配置文件会自动更新到 '3TS/contrib/deneva/config.h' 下，当程序运行结束之后，可以在运行脚本的机器上找到相对应的logs文件，格式为 '3TS/contrib/deneva/results/0_<CC_ALG>.out'。


下面我们来看批次跑工作负载，下面的一些例子提供更详细的信息。


## 例子：工作负载测试
通过下面的步骤，我们可以测不一样的YCSB(Main read, Main write, Low contention, and High contention workloads) 和PCC(Payment, Neworder, and Mix workloads)工作负载。

我们需要修改脚本文件 'experiments&#46;py':
```
# prepare the functions
def ycsb_scaling_single_write():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.9, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
def ycsb_scaling_single_read():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.1, 0.1, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
def ycsb_scaling_single_low():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.5, 0.5, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
def ycsb_scaling_single_high():
    return ycsb_scaling_server_distribute(1, 64, 0.9, 0.5, 0.5, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size

def tpcc_scaling_single_payment():
    return tpcc_scaling_server_distribute(1, 64, 1.0, 0.0) # node, thread, payment_perc, and dist rate
def tpcc_scaling_single_neworder():
    return tpcc_scaling_server_distribute(1, 64, 0.0, 0.0) # node, thread, payment_perc, and dist rate
def tpcc_scaling_single_mix():
    return tpcc_scaling_server_distribute(1, 64, 0.5, 0.0) # node, thread, payment_perc, and dist rate
    

# configure the map function for the script run
experiment_map = {
    'ycsb_scaling_single_write':ycsb_scaling_single_write,
    'ycsb_scaling_single_read':ycsb_scaling_single_read,
    'ycsb_scaling_single_low':ycsb_scaling_single_low,
    'ycsb_scaling_single_high':ycsb_scaling_single_high,
    'tpcc_scaling_single_payment':tpcc_scaling_single_payment,
    'tpcc_scaling_single_neworder':tpcc_scaling_single_neworder,
    'tpcc_scaling_single_mix':tpcc_scaling_single_mix
}    
```
将要运行的工作负载函数名更新在运行脚本 'testall&#46;sh':
```
python run_experiments.py -e -c vcloud ycsb_scaling_single_write
python run_experiments.py -e -c vcloud ycsb_scaling_single_read
python run_experiments.py -e -c vcloud ycsb_scaling_single_low
python run_experiments.py -e -c vcloud ycsb_scaling_single_high
python run_experiments.py -e -c vcloud tpcc_scaling_single_payment
python run_experiments.py -e -c vcloud tpcc_scaling_single_neworder
python run_experiments.py -e -c vcloud tpcc_scaling_single_mix
```


## 例子：可扩展性测试
通过下面的步骤，我们可以测试并发控制算法的可扩展性，我们以YCSB的线程可扩展为例写例子，可扩展性也可以扩展节点数，TPCC下的工作负载也同理。
我们需要修改脚本文件 'experiments&#46;py':
```
# prepare the functions
def ycsb_scaling_single_scalibility_04():
    return ycsb_scaling_server_distribute(1, 4, 0.5, 0.9, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
...
def ycsb_scaling_single_scalibility_64():
    return ycsb_scaling_server_distribute(1, 64, 0.9, 0.5, 0.5, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
    

# configure the map function for the script run
experiment_map = {
    'ycsb_scaling_single_scalibility_04':ycsb_scaling_single_scalibility_04,
    ...
    'ycsb_scaling_single_scalibility_64':ycsb_scaling_single_scalibility_64,
}    
```
最后更新运行脚本 'testall&#46;sh'来进行测试。

## 例子：分布式事务比例和分布式方式测试
通过下面的步骤，我们可以测试分布式事务的比例影响。其中YCSB是分布式查询语句的比例，而TPCC是分布式事务的比例。给定20%的分布式查询比例，YCSB每个事务有20%的查询是分布式查询。而给定20%的分布式事务比例，TPCC有20%是分布式事务，而80%是本地事务。
我们需要修改脚本文件 'experiments&#46;py':
```
# prepare the functions
def ycsb_scaling_dist_rate_00():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.9, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
...
def ycsb_scaling_dist_rate_10():
    return ycsb_scaling_server_distribute(1, 64, 0.9, 0.5, 0.5, 1.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
    
# configure the map function for the script run
experiment_map = {
    'ycsb_scaling_dist_rate_00':ycsb_scaling_dist_rate_00,
    ...
    'ycsb_scaling_dist_rate_10':ycsb_scaling_dist_rate_10,
}    
```
最后更新运行脚本 'testall&#46;sh'来进行测试。

## 例子：偏斜率测试
通过下面的步骤，我们可以测试偏斜率（skew factor）对性能的影响。给定0偏斜率，则数据访问是一个随机（uniform）访问，而给定0.9的偏斜率，则数据访问会集中在一小部分热点数据上做查询和更新。
我们需要修改脚本文件 'experiments&#46;py':
```
# prepare the functions
def ycsb_scaling_skew_factor_00():
    return ycsb_scaling_server_distribute(1, 64, 0.0, 0.9, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
def ycsb_scaling_skew_factor_01():
    return ycsb_scaling_server_distribute(1, 64, 0.1, 0.9, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
...
def ycsb_scaling_skew_factor_08():
    return ycsb_scaling_server_distribute(1, 64, 0.8, 0.9, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
def ycsb_scaling_skew_factor_09():
    return ycsb_scaling_server_distribute(1, 64, 0.9, 0.9, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size

# configure the map function for the script run
experiment_map = {
    'ycsb_scaling_skew_factor_00':ycsb_scaling_skew_factor_00,
    'ycsb_scaling_skew_factor_00':ycsb_scaling_skew_factor_01,
    ...
    'ycsb_scaling_skew_factor_00':ycsb_scaling_skew_factor_08,
    'ycsb_scaling_skew_factor_00':ycsb_scaling_skew_factor_09
}    
```
最后更新运行脚本 'testall&#46;sh'来进行测试。


## 例子：更新率测试
通过下面的步骤，我们可以测试更新率（update rate）对性能的影响。给定0更新率，则所有的事务都是只读（read-only），而给定1.0，则所有的事务都不是只读，读含有更新操作。
我们需要修改脚本文件 'experiments&#46;py':
```
# prepare the functions
def ycsb_scaling_update_rate_00():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.0, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
def ycsb_scaling_update_rate_01():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.1, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
...
def ycsb_scaling_update_rate_09():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.9, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size
def ycsb_scaling_update_rate_10():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 1.0, 0.9, 0.0, 10) # node, thread, skew, update_rate, rw, dist rate, and query size

# configure the map function for the script run
experiment_map = {
    'ycsb_scaling_update_rate_00':ycsb_scaling_update_rate_00,
    'ycsb_scaling_update_rate_01':ycsb_scaling_update_rate_01,
    ...
    'ycsb_scaling_update_rate_09':ycsb_scaling_update_rate_09,
    'ycsb_scaling_update_rate_10':ycsb_scaling_update_rate_10
}    
```
最后更新运行脚本 'testall&#46;sh'来进行测试。


## 例子：事务大小和数据大小测试
通过下面的步骤，我们可以测试事务大小和数据大小对性能的影响。
我们需要修改脚本文件 'experiments&#46;py':
```
# prepare the functions
# txn sizes
def ycsb_scaling_txn_size_02():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.0, 0.9, 0.0, 2) # node, thread, skew, update_rate, rw, dist rate, and query size
...
def ycsb_scaling_txn_size_64():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.1, 0.9, 0.0, 64) # node, thread, skew, update_rate, rw, dist rate, and query size

# data sizes
def ycsb_scaling_data_size_01():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 0.9, 0.9, 0.0, 10, 1) # node, thread, skew, update_rate, rw, dist rate, query size, data size
...
def ycsb_scaling_data_size_128():
    return ycsb_scaling_server_distribute(1, 64, 0.5, 1.0, 0.9, 0.0, 10,128) # node, thread, skew, update_rate, rw, dist rate, query size, and data size

# configure the map function for the script run
experiment_map = {
    'ycsb_scaling_txn_size_02':ycsb_scaling_txn_size_02,
    ...
    'ycsb_scaling_txn_size_64':ycsb_scaling_txn_size_64,
    
    'ycsb_scaling_data_size_01':ycsb_scaling_data_size_01,
    ...
    'ycsb_scaling_data_size_128':ycsb_scaling_data_size_128
}    
```
最后更新运行脚本 'testall&#46;sh'来进行测试。


## 例子：延迟和内存分配测试
我们用 [tc](https://wiki.debian.org/TrafficControl) 手工插入延迟去测试性能，需要在测试之前完成。 内存分配我们使用 [jemlloc](http://jemalloc.net/) 去优化内存，需要使用其编译代码. 



## Example: isolation level test
通过下面的步骤，我们可以测试不同隔离级别都性能的影响。
我们需要修改脚本文件 'experiments&#46;py'的YCSB工作负载模板:
```
def ycsb_scaling_server_distribute(given_node, thread, given_skew, given_update_rate, given_rw_ratio, dist_perc,qs=10,ds=128):
    wl = 'YCSB'
    nnodes = [given_node]
    algos=['NO_WAIT','WAIT_DIE', 'MVCC','OPT_SSI','TIMESTAMP','OCC','SILO','WSI','MAAT']
    base_table_size=2097152*8*ds/128
    txn_write_perc = [given_update_rate]  
    tup_write_perc = [given_rw_ratio]
    load = [10000]
    tcnt = [thread]
    ctcnt = [thread/2]
    scnt = [thread/4]
    rcnt = [thread/4]
    skew = [given_skew]
    distribute_rate = [dist_perc]
    query_size = [qs]
    isolation_level = ["SERIALIZABLE"]  # You can test for "SERIALIZABLE", "READ_COMMITTED", "READ_UNCOMMITTED" levels and "NO_LOCK" for allow WW conflicts.
    # the following are corresponding parameters in deneva
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT",'DISTRIBUTE_PERCENT',"REQ_PER_QUERY","ISOLATION_LEVEL"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr,dr,qs,isl] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo,dr,qs,isl in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos,distribute_rate,query_size,isolation_level)]
    return fmt,exp
```
最后更新运行脚本 'testall&#46;sh'来进行测试。

