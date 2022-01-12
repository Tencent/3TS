# 3TS performance test 

This manual helps to run 3TS performance test properly. Users are expected to obtain the TPS of some CC protocols.

## Configure IP addresses
Single run needs to configure virtual/real machines by 'ifconfig.txt' under folder '3TS/contrib/deneva', e.g.:

#### a. setup virtual IPs:
```
ifconfig bond1:0 166.111.69.50 netmask 255.255.255.192 up
ifconfig bond1:1 166.111.69.51 netmask 255.255.255.192 up
```
These bind 166.111.69.50 and 166.111.69.51 to interface bond1.
#### b. edit virtual machines or real IPs in ifconfig.txt
```
166.111.69.50
166.111.69.51
```
Later, the argument (e.g., -nid0, nid1) specified to the server or client will read the corresponding IP in the specific row.

##  Passwordless login 

Put the public key (e.g., id_rsa.pub) to the remote hosts ~/. ssh/authorized_keys.

##  Configure parameters
Configure paramters in file '3TS/contrib/deneva/config.h', The following are typical configurable parameters.
``` 
#define THREAD_CNT 4
#define WORKLOAD YCSB
#define CC_ALG  NO_WAIT
```
where these parameters represent for the number of threads, data set, and CC protocol.

##  Compile the code
```
make clean
make deps
make -j 10
```
##  Start to run
Copy complied 'rundb' and 'runcl' files to corresponding machines. (e.g., 3TS/output/)
#### a. Start a server (under 3TS/output/):
```
./rundb -nid0
```
#### b. Start a client (under 3TS/output/):
```
./runcl -nid1
```
#### c. Check result logs on started machines

server log: 3TS/output/dbresult0.out
client log: 3TS/output/clresult1.out



# Detailed workload configuration

You can modify the 'config.h' file to run benchmarks with different parameters.

### General parameters

```
#define NODE_CNT 1     // node count
#define THREAD_CNT 64  // thread count
#define CC_ALG NO_WAIT // CC protocol
#define WORKLOAD YCSB  // benchmark, e.g., YCSB or TPCC
#define DISTRIBUTE_PERCENT 0.15    // distributed query rate for YCSB, distributed transction rate for TPCC
```

### YCSB workloads


```
#define SYNTH_TABLE_SIZE 16777216 // table size, i.e., number of tuple rows
#define TXN_WRITE_PERC 0.1 // update rate, 0.1 means 10% txns has writes and 90% txns are read-only
#define TUP_WRITE_PERC 0.1 // write/read ratio, 0.1 means 10% of operations are writes and 90% are reads in a txn
#define ZIPF_THETA 0.9     // skew factor, 0.9 means extreme skew data, 0.0 means the distribution is uniform
```


### TPCC workloads

```
#define NUM_WH 128       // table size, i.e., number of ware house
#define PERC_PAYMENT 0.0 // Payment percent, 0 means 100% payment txns, 1 means 100% neworder txns, 0.5 means payment and neworder txns are both 50%
```


# 3TS Performance test by scripting

##  Related files
Related configurable scripts under folder "3TS/contrib/deneva/scripts_tmp"

```
experiment.py        # workload specification
run_experiments.py   # workload runs
run_config.py        # configuration of folder, username, IPs
vcloud_deploy.sh     # deploy the server/client machines by the IPs
helper.py            # helper functions
kill.sh              # kill process before start in case occupied IP or ports
testall.sh           # start the scripts by specifying the workload names
```

## Configure parameters

#### a. edit username, folder to run, IP of machines in run_config.py, e.g,:
```
    username = "centos"
    vcloud_uname = '/testfolder/output'
    vcloud_machines = ["166.111.69.50", "166.111.69.51"] # configure the real IPs here
```
#### b. edit workload parameters in experiment&#46;py. The following are templates for some parameters configuration for YCSB and TPCC workloads:
YCSB template:
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
TPCC template:
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
#### c. Set up a workload:
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

##  Run a worklaod:

In the testall&#46;sh, you will deploy the environment with workload name:
```
python run_experiments.py -e -c vcloud ycsb_scaling_query_size_02
```

Simply run the workload by:
```
./testall.sh
```
When runing the test, the new configuration will be automatically replaced in "3TS/contrib/deneva/config.h".

Check server result logs on started machines, the server log is in 3TS/contrib/deneva/results/0_<CC_ALG>.out

To configure more workloads to run in a batch, please see the following examples for more details.


## Example: workloads test
By the following steps, you can explore the performance of CC protocols by varying workloads from YCSB (Main read, Main write, Low contention, and High contention workloads) and TPCC (Payment, Neworder, and Mix workloads).
Prepare workloads in 'experiments&#46;py':
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
Prepare scripts in 'testall&#46;sh':
```
python run_experiments.py -e -c vcloud ycsb_scaling_single_write
python run_experiments.py -e -c vcloud ycsb_scaling_single_read
python run_experiments.py -e -c vcloud ycsb_scaling_single_low
python run_experiments.py -e -c vcloud ycsb_scaling_single_high
python run_experiments.py -e -c vcloud tpcc_scaling_single_payment
python run_experiments.py -e -c vcloud tpcc_scaling_single_neworder
python run_experiments.py -e -c vcloud tpcc_scaling_single_mix
```


## Example: scalability test
By the following steps, you can explore the performance of CC protocols by varying threads by YCSB workloads. You can also configure to scale for nodes and for TPCC workloads. 
Prepare workloads in 'experiments&#46;py':
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
Update 'testall&#46;sh' to run workloads



## Example: distributed rates and means test
YCSB and TPCC configure distributed query rates and distributed transaction rates, respectively. 20% distributed query rate means each query in a transaction has a 20% possibility to access a remote partition. 20 distributed transaction rate means 20% of transactions have remote access while the other 80% are local run transactions. 

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
Update 'testall&#46;sh' to run workloads


## Example: skew factor test
By the following steps, you can explore the performance of CC protocols by varying skew factor (from 0.0 (uniform) to 0.9 (extremely skew)).
Prepare workloads in 'experiments&#46;py':
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
Update 'testall&#46;sh' to run workloads


## Example: update rate test
By the following steps, you can explore the performance of CC protocols by varying update rate (from 0.0 (read-only) to 1.0 (100% update txns)).
Prepare workloads in 'experiments&#46;py':
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
Update 'testall&#46;sh' to run workloads


## Example: transaction size and data size test
By the following steps, you can explore the performance of CC protocols by varying transaction sizes and query sizes.
Prepare workloads in 'experiments&#46;py':
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
Update 'testall&#46;sh' to run workloads


## Example: delay and memory allocation test
We use [tc](https://wiki.debian.org/TrafficControl) to manually insert artificial delays for the network, and [jemlloc](http://jemalloc.net/) for the memory optimization when complied. 


## Example: different lock granularities test
We implemented two granularities for SILO and WSI, i.e., lock row and lock critical section. Lock row has two mechanisms, wait die and no wait.
We can modify "config.h" to enable different implementations:
```
// [WSI]
#define WSI_LOCK_CS true   // CS: critical section, NW: no wait, WD: wait_die  // default
#define WSI_LOCK_NW false   // CS: critical section, NW: no wait, WD: wait_die
#define WSI_LOCK_WD false   // CS: critical section, NW: no wait, WD: wait_die
// [SILO]
#define SILO_LOCK_CS false   // CS: critical section, NW: no wait, WD: wait_die
#define SILO_LOCK_NW true   // CS: critical section, NW: no wait, WD: wait_die  // default
#define SILO_LOCK_WD false   // CS: critical section, NW: no wait, WD: wait_die
```
Then we can run the workload as previously.

## Example: isolation level test
By the following steps, you can explore the performance of CC protocols by varying isolation levels.

We can modify the YCSB template to add isolation level parameters:
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

