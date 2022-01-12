import itertools
#from paper_plots import *
# Experiments to run and analyze
# Go to end of file to fill in experiments
SHORTNAMES = {
    "CLIENT_NODE_CNT" : "CN",
    "CLIENT_THREAD_CNT" : "CT",
    "CLIENT_REM_THREAD_CNT" : "CRT",
    "CLIENT_SEND_THREAD_CNT" : "CST",
    "NODE_CNT" : "N",
    "THREAD_CNT" : "T",
    "REM_THREAD_CNT" : "RT",
    "SEND_THREAD_CNT" : "ST",
    "CC_ALG" : "",
    "WORKLOAD" : "",
    "MAX_TXN_PER_PART" : "TXNS",
    "MAX_TXN_IN_FLIGHT" : "TIF",
    "PART_PER_TXN" : "PPT",
    "TUP_READ_PERC" : "TRD",
    "TUP_WRITE_PERC" : "TWR",
    "TXN_READ_PERC" : "RD",
    "TXN_WRITE_PERC" : "WR",
    "ZIPF_THETA" : "SKEW",
    "MSG_TIME_LIMIT" : "BT",
    "MSG_SIZE_MAX" : "BS",
    "DATA_PERC":"D",
    "ACCESS_PERC":"A",
    "PERC_PAYMENT":"PP",
    "MPR":"MPR",
    "REQ_PER_QUERY": "RPQ",
    "MODE":"",
    "PRIORITY":"",
    "ABORT_PENALTY":"PENALTY",
    "STRICT_PPT":"SPPT",
    "NETWORK_DELAY":"NDLY",
    "NETWORK_DELAY_TEST":"NDT",
    "REPLICA_CNT":"RN",
    "SYNTH_TABLE_SIZE":"TBL",
    "ISOLATION_LEVEL":"LVL",
    "YCSB_ABORT_MODE":"ABRTMODE",
    "NUM_WH":"WH",
}

fmt_title=["NODE_CNT","CC_ALG","ACCESS_PERC","TXN_WRITE_PERC","PERC_PAYMENT","MPR","MODE","MAX_TXN_IN_FLIGHT","SEND_THREAD_CNT","REM_THREAD_CNT","THREAD_CNT","TXN_WRITE_PERC","TUP_WRITE_PERC","ZIPF_THETA","NUM_WH"]

##############################
# PLOTS
##############################
#dta_target_algos=['DLI_BASE','DLI_MVCC_OCC','DLI_MVCC_BASE','DLI_OCC','MAAT']#['DLI_MVCC_OCC','DLI_DTA','TIMESTAMP','WAIT_DIE']#['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','OCC','DLI_MVCC_OCC','DLI_OCC','DLI_BASE','DLI_MVCC_BASE','DLI_DTA']
#dta_target_algos=['NO_WAIT', 'MVCC', 'CALVIN', 'MAAT']
#dta_target_algos=['DLI_DTA3']
#dta_target_algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','OCC','DLI_MVCC_OCC','DLI_BASE','DLI_MVCC_BASE']
dta_target_algos=['TIMESTAMP']
# tpcc load
#tpcc_loads = ['50', '100', '200', '500', '1000', '2000', '5000']
tpcc_loads = ['50', '100', '200', '500', '1000', '2000', '5000']
# ycsb load
ycsb_loads = ['50', '100', '200', '500', '1000', '2000', '5000']

def pps_scaling():
    wl = 'PPS'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','WOOKONG']
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,tif] for tif,n,cc in itertools.product(load,nnodes,nalgos)]
    return fmt,exp


def ycsb_scaling():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    nnodes = [1]
    # nnodes = [2,4]
    algos=["MVCC"]
    # algos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP']
	  # algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    # algos=['MVCC','MAAT','TIMESTAMP','WOOKONG','OCC']
    # algos=['MAAT','MVCC','TIMESTAMP','OCC','DLI_DTA3','DLI_OCC']
    # algos=['CNULL']
    # base_table_size=1048576*8
    base_table_size=2097152*8
    base_table_size=2097152
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [2]
    ctcnt = [1]
    scnt = [1]
    rcnt = [1]
    skew = [0.5]
    #skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

# YCSB workload with read/write ratio 1:1
def ycsb_scaling_rw_55():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    nnodes = [1]
    # nnodes = [2,4]
    algos=['MVCC','DLI_IDENTIFY_2',"MAAT","OCC","TIMESTAMP","DLI_IDENTIFY"]
    # algos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP']
	  # algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    # algos=['MVCC','MAAT','TIMESTAMP','WOOKONG','OCC']
    # algos=['MAAT','MVCC','TIMESTAMP','OCC','DLI_DTA3','DLI_OCC']
    # algos=['CNULL']
    # base_table_size=1048576*8
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    ctcnt = [4]
    scnt = [2]
    rcnt = [2]
    skew = [0.5]
    #skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

# YCSB workload with read/write ratio 1:9
def ycsb_scaling_rw_19():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    nnodes = [1]
    # nnodes = [2,4]
    algos=['MVCC','DLI_IDENTIFY_2',"MAAT","OCC","TIMESTAMP","DLI_IDENTIFY"]
    # algos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP']
	  # algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    # algos=['MVCC','MAAT','TIMESTAMP','WOOKONG','OCC']
    # algos=['MAAT','MVCC','TIMESTAMP','OCC','DLI_DTA3','DLI_OCC']
    # algos=['CNULL']
    # base_table_size=1048576*8
    base_table_size=2097152*8
    txn_write_perc = [0.9]
    tup_write_perc = [0.9]
    load = [10000]
    tcnt = [4]
    ctcnt = [4]
    scnt = [2]
    rcnt = [2]
    skew = [0.5]
    #skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

# YCSB workload with read/write ratio 9:1
def ycsb_scaling_rw_91():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    nnodes = [1]
    # nnodes = [2,4]
    algos=['MVCC','DLI_IDENTIFY_2',"MAAT","OCC","TIMESTAMP","DLI_IDENTIFY"]
    # algos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP']
	  # algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    # algos=['MVCC','MAAT','TIMESTAMP','WOOKONG','OCC']
    # algos=['MAAT','MVCC','TIMESTAMP','OCC','DLI_DTA3','DLI_OCC']
    # algos=['CNULL']
    # base_table_size=1048576*8
    base_table_size=2097152*8
    txn_write_perc = [0.1]
    tup_write_perc = [0.1]
    load = [10000]
    tcnt = [4]
    ctcnt = [4]
    scnt = [2]
    rcnt = [2]
    skew = [0.5]
    #skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ycsb_scaling1():
    wl = 'YCSB'
    nnodes = [2,4,8,16,32]
    # nnodes = [1,2]
    algos=['WOOKONG']
    # algos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    ctcnt = [4]
    skew = [0.5]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr] for thr,cthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ycsb_scaling2():
    wl = 'YCSB'
    nnodes = [2,4,8,16,32]
    # nnodes = [1,2]
    algos=['WOOKONG']
    # algos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    ctcnt = [4]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr] for thr,cthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ecwc():
    wl = 'YCSB'
    nnodes = [2]
    algos=['NO_WAIT','WAIT_DIE','MVCC','CALVIN','TIMESTAMP','MAAT','WOOKONG']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp


def ycsb_scaling_abort():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','WOOKONG']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000,12000]
    load = [10000]
    tcnt = [4]
    skew = [0.6,0.7]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'true'] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp


def ycsb_skew():
    wl = 'YCSB'
    nnodes = [16]
    algos=['WOOKONG']
    # algos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP']
    # algos=['MAAT']
    # algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    # skew = [0.65,0.7,0.75,0.8,0.9]
    # skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_skew1():
    wl = 'YCSB'
    nnodes = [2]
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
   # skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.0,0.6,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress1():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.0]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress2():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.25]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress3():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.55]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress4():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.65]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress5():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.75]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress6():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress7():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.0]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress8():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.2]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress9():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.4]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress10():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.6]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress11():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.8]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_stress12():
    wl = 'YCSB'
    nnodes = [1]
    #algos=['WOOKONG','WAIT_DIE','MVCC','MAAT','TIMESTAMP','OCC']
    algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [1.0]
    tup_write_perc = [0.5]
    load = ycsb_loads
    #load = ycsb_loads
    tcnt = [4]
    #skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_writes():
    wl = 'YCSB'
    nnodes = [16]
    algos=['WOOKONG']
    # algos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP']
    # algos=dta_target_algos
    base_table_size=2097152*8
    txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    tup_write_perc = [0.5]
    load = [20000]
    tcnt = [4]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,txn_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp



def isolation_levels():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
    algos=['NO_WAIT']
    levels=["READ_UNCOMMITTED","READ_COMMITTED","SERIALIZABLE","NOLOCK"]
    base_table_size=2097152*8
    load = [10000]
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    skew = [0.6,0.7]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","ISOLATION_LEVEL","MAX_TXN_IN_FLIGHT","ZIPF_THETA"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,level,ld,sk] for txn_wr_perc,tup_wr_perc,algo,sk,ld,n,level in itertools.product(txn_write_perc,tup_write_perc,algos,skew,load,nnodes,levels)]
    return fmt,exp

def ycsb_partitions():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','WOOKONG']
    load = [10000,12000]
    load = [10000]
    nparts = [1,2,4,6,8,10,12,14,16]
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    tcnt = [4]
    skew = [0.6]
    rpq =  16
    fmt = ["WORKLOAD","REQ_PER_QUERY","PART_PER_TXN","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,rpq,p,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp

def ycsb_partitions_distr():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','WOOKONG']
    load = [10000]
    nparts = [2,4,6,8,10,12,14,16]
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    tcnt = [4]
    skew = [0.6]
    rpq =  16
    fmt = ["WORKLOAD","REQ_PER_QUERY","PART_PER_TXN","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,rpq,p,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp

def tpcc_scaling():
    wl = 'TPCC'
    # nnodes = [2]
    nnodes = [2,4,8,16,32]
    nalgos=['WOOKONG']
    # nalgos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP']
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC','CALVIN','WOOKONG','TICTOC','DLI_DTA','DLI_DTA1','DLI_DTA2','DLI_DTA3','DLI_MVCC_OCC','DLI_MVCC']
    # nalgos=['WOOKONG']
    # nalgos=['NO_WAIT']
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=32
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [10000]
    tcnt = [4]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_neworder():
    wl = 'TPCC'
    # nnodes = [2]
    nnodes = [1]
    # make it line 5
    algos=['MVCC',"DLI_IDENTIFY_SSI"]
    # nalgos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP'] ['MVCC',"TIMESTAMP","OCC","DLI_IDENTIFY_SSI","SSI"]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC','CALVIN','WOOKONG','TICTOC','DLI_DTA','DLI_DTA1','DLI_DTA2','DLI_DTA3','DLI_MVCC_OCC','DLI_MVCC']
    # nalgos=['WOOKONG']
    # nalgos=['NO_WAIT']
    # 0 for new order 1 for paymment
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=32
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [10000]
    tcnt = [4]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,algos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_payment():
    wl = 'TPCC'
    # nnodes = [2]
    nnodes = [1]
    # make it line 5
    algos=['MVCC',"DLI_IDENTIFY_SSI"]
    # nalgos=['WOOKONG','TICTOC','SILO','MVCC','MAAT','TIMESTAMP'] ['MVCC',"TIMESTAMP","OCC","DLI_IDENTIFY_SSI","SSI"]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC','CALVIN','WOOKONG','TICTOC','DLI_DTA','DLI_DTA1','DLI_DTA2','DLI_DTA3','DLI_MVCC_OCC','DLI_MVCC']
    # nalgos=['WOOKONG']
    # nalgos=['NO_WAIT']
    # 0 for new order 1 for paymment
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=32
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [10000]
    tcnt = [4]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,algos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp


def tpcc_cstress():
    wl = 'TPCC'
    nnodes = [1]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN','WOOKONG']
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC','CALVIN','WOOKONG','TICTOC','DLI_DTA','DLI_DTA1','DLI_DTA2','DLI_DTA3','DLI_MVCC_OCC','DLI_MVCC']
    # nalgos=['MAAT','MVCC','TIMESTAMP','OCC','DLI_DTA3','DLI_OCC']
    nalgos=['MVCC']
    #nalgos=['NO_WAIT']
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=128
    # wh=64
    # load = [1000,2000,3000,4000,5000]
    load = [5000]
    tcnt = [16]
    ctcnt = [16]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]
#
    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_cstress1():
    wl = 'TPCC'
    nnodes = [1]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN','WOOKONG']
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC','CALVIN','WOOKONG','TICTOC','DLI_DTA','DLI_DTA1','DLI_DTA2','DLI_DTA3','DLI_MVCC_OCC','DLI_MVCC']
    # nalgos=['WOOKONG']
    nalgos=['MAAT','MVCC','TIMESTAMP','OCC','DLI_DTA3','DLI_OCC']
    #nalgos=['NO_WAIT']
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=128
    # wh=64
    # load = [1000,2000,3000,4000,5000]
    load = [5000]
    tcnt = [16]
    ctcnt = [16]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_cstress2():
    wl = 'TPCC'
    nnodes = [1]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN','WOOKONG']
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC','CALVIN','WOOKONG','TICTOC','DLI_DTA','DLI_DTA1','DLI_DTA2','DLI_DTA3','DLI_MVCC_OCC','DLI_MVCC']
    # nalgos=['MAAT','MVCC','TIMESTAMP','OCC','DLI_DTA3','DLI_OCC']
    # nalgos=['WOOKONG']
    nalgos=['DLI_DTA3','DLI_OCC']
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=128
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [5000]
    #load = [1000]
    tcnt = [16]
    ctcnt = [16]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp
# def tpcc_cstress2():
#     wl = 'YCSB'
#     nnodes = [2]
#     # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN','WOOKONG']
#     # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC','CALVIN','WOOKONG','TICTOC','DLI_DTA','DLI_DTA1','DLI_DTA2','DLI_DTA3','DLI_MVCC_OCC','DLI_MVCC']
#     nalgos=['MAAT','MVCC','TIMESTAMP','OCC','WOOKONG']
#     # npercpay=[0.0]
#     base_table_size=2097152*8
#     txn_write_perc = [0.5]
#     tup_write_perc = [0.5]
#     skew = [0.6]
#     # wh=64
#     # load = [10000,20000,30000,40000,50000]
#     load = [10000,20000]
#     tcnt = [16]
#     ctcnt = [16]
#     # fmt = ["WORKLOAD","NODE_CNT","CC_ALG","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
#     # exp = [[wl,n,cc,wh*n,tif,thr,cthr] for thr,cthr,tif,n,cc in itertools.product(tcnt,ctcnt,load,nnodes,nalgos)]

#     fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT"]
#     exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr] for thr,cthr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,ctcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,nalgos)]
#     # wh=4
#     # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
#     return fmt,exp
def tpcc_cstress3():
    wl = 'TPCC'
    nnodes = [1]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN','WOOKONG']
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC','CALVIN','WOOKONG','TICTOC','DLI_DTA','DLI_DTA1','DLI_DTA2','DLI_DTA3','DLI_MVCC_OCC','DLI_MVCC']
    # nalgos=['WOOKONG']
    #nalgos=['NO_WAIT']
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=128
    # wh=64
    load = [1000,2000,3000,4000,5000]
    #load = [1000]
    tcnt = [60]
    ctcnt = [60]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling1():
    wl = 'TPCC'
    nnodes = [1,2]
    nalgos=dta_target_algos
    npercpay=[0.0]
    wh=128
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_stress1():
    wl = 'TPCC'
    nnodes = [1]
    nalgos=dta_target_algos
    npercpay=[0.0]
    wh=128
    load = tpcc_loads
    #load = tpcc_loads
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_debug():
    wl = 'TPCC'
    nnodes = [1,2]
    nalgos=['WOOKONG']
    npercpay=[1.0]
    wh=32
    load = [20000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

#def tpcc_scaling1():
#    wl = 'TPCC'
    # nnodes = [1,2,4,8,16,32]
#    nnodes = [32]
    # nalgos=['WOOKONG','MAAT','MVCC','TIMESTAMP']
#    nalgos=['WOOKONG']
#    npercpay=[0.0]
#    wh=32
#    load = [10000]
#    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
#    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
#    return fmt,exp

def tpcc_scaling2():
    wl = 'TPCC'
    nnodes = [1,2]
    # nnodes = [4]
    #nalgos=['WOOKONG','MAAT','MVCC','TIMESTAMP']
    nalgos=dta_target_algos
    npercpay=[1.0]
    wh=128
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_stress2():
    wl = 'TPCC'
    nnodes = [1]
    # nnodes = [4]
    #nalgos=['WOOKONG','MAAT','MVCC','TIMESTAMP']
    #nalgos=['TIMESTAMP']
    nalgos=dta_target_algos
    npercpay=[1.0]
    wh=128
    load = tpcc_loads
    #load = tpcc_loads
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling3():
    wl = 'TPCC'
    nnodes = [1,2]
    # nnodes = [4]
    #nalgos=['WOOKONG','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC']
    nalgos=dta_target_algos
    npercpay=[0.5]
    wh=128
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_dist_ratio():
    wl = 'TPCC'
    nnodes = [16]
    nalgos=['WOOKONG','WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC']
    npercpay=[1.0]
    wh=32
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp


def tpcc_scaling_whset():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN','WOOKONG']
    npercpay=[0.0,0.5,1.0]
    wh=128
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH"]
    exp = [[wl,n,cc,pp,wh] for pp,n,cc in itertools.product(npercpay,nnodes,nalgos)]
    wh=256
    exp = exp + [[wl,n,cc,pp,wh] for pp,n,cc in itertools.product(npercpay,nnodes,nalgos)]
    return fmt,exp

def ycsb_skew_abort_writes():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','WOOKONG']
    base_table_size=2097152*8
    txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'true'] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_skew_abort():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','WOOKONG']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.825,0.85,0.875,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'true'] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp


def ycsb_partitions_abort():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','WOOKONG']
    load = [10000]
    nparts = [1,2,4,6,8,10,12,14,16]
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    tcnt = [4]
    skew = [0.6]
    rpq =  16
    fmt = ["WORKLOAD","REQ_PER_QUERY","PART_PER_TXN","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT","YCSB_ABORT_MODE"]
    exp = [[wl,rpq,p,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,1,'true'] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp

def network_sweep():
    wl = 'YCSB'
    nalgos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','TIMESTAMP','CALVIN','WOOKONG']
    algos=['CALVIN']
# Network delay in ms
    ndelay=[0,0.05,0.1,0.25,0.5,0.75,1,1.75,2.5,5,7.5,10,17.5,25,50]
    ndelay = [int(n*1000000) for n in ndelay]
    nnodes = [2]
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    base_table_size=2097152*8
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","NETWORK_DELAY_TEST","NETWORK_DELAY","SET_AFFINITY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,"true",d,"false"] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,d,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,ndelay,nalgos)]
    return fmt,exp

##############################
# END PLOTS
##############################

experiment_map = {
    'tpcc_scaling_neworder': tpcc_scaling_neworder,
    'tpcc_scaling_payment': tpcc_scaling_payment,
    'pps_scaling': pps_scaling,
    'ycsb_scaling_rw_55': ycsb_scaling_rw_55,
    'ycsb_scaling_rw_19': ycsb_scaling_rw_19,
    'ycsb_scaling_rw_91': ycsb_scaling_rw_91,
    'ycsb_scaling': ycsb_scaling,
    'ycsb_scaling1': ycsb_scaling1,
    'ycsb_scaling2': ycsb_scaling2,
    'ycsb_scaling_abort': ycsb_scaling_abort,
    'ycsb_stress1': ycsb_stress1,
    'ycsb_stress2': ycsb_stress2,
    'ycsb_stress3': ycsb_stress3,
    'ycsb_stress4': ycsb_stress4,
    'ycsb_stress5': ycsb_stress5,
    'ycsb_stress6': ycsb_stress6,
    'ycsb_stress7': ycsb_stress7,
    'ycsb_stress8': ycsb_stress8,
    'ycsb_stress9': ycsb_stress9,
    'ycsb_stress10': ycsb_stress10,
    'ycsb_stress11': ycsb_stress11,
    'ycsb_stress12': ycsb_stress12,
    'ycsb_writes': ycsb_writes,
    'ycsb_skew': ycsb_skew,
    'ycsb_skew1': ycsb_skew1,
    'isolation_levels': isolation_levels,
    'ycsb_partitions': ycsb_partitions,
    'ycsb_partitions_abort': ycsb_partitions_abort,
    'tpcc_scaling': tpcc_scaling,
    'tpcc_cstress':tpcc_cstress,
    'tpcc_cstress1':tpcc_cstress1,
    'tpcc_cstress2':tpcc_cstress2,
    'tpcc_cstress3':tpcc_cstress3,
    'tpcc_scaling_debug': tpcc_scaling_debug,
    'tpcc_scaling1': tpcc_scaling1,
    'tpcc_scaling2': tpcc_scaling2,
    'tpcc_scaling3': tpcc_scaling3,
    'tpcc_stress1': tpcc_stress1,
    'tpcc_stress2': tpcc_stress2,
    'tpcc_dist_ratio': tpcc_dist_ratio,
    'tpcc_scaling_whset': tpcc_scaling_whset,
    'ycsb_skew_abort': ycsb_skew_abort,
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 16,
    "THREAD_CNT": 4,
    "REPLICA_CNT": 0,
    "REPLICA_TYPE": "AP",
    "REM_THREAD_CNT": 1,
    "SEND_THREAD_CNT": 1,
    "CLIENT_NODE_CNT" : "NODE_CNT",
    "CLIENT_THREAD_CNT" : 4,
    "CLIENT_REM_THREAD_CNT" : 1,
    "CLIENT_SEND_THREAD_CNT" : 1,
    "MAX_TXN_PER_PART" : 500000,
    "WORKLOAD" : "YCSB",
    "CC_ALG" : "WAIT_DIE",
    "MPR" : 1.0,
    "TPORT_TYPE":"IPC",
    "TPORT_PORT":"18000",
    "PART_CNT": "NODE_CNT",
    "PART_PER_TXN": 2,
    "MAX_TXN_IN_FLIGHT": 10000,
    "NETWORK_DELAY": '0UL',
    "NETWORK_DELAY_TEST": 'false',
    "DONE_TIMER": "1 * 60 * BILLION // ~1 minutes",
    "WARMUP_TIMER": "1 * 10 * BILLION // ~1 minutes",
    "SEQ_BATCH_TIMER": "5 * 1 * MILLION // ~5ms -- same as CALVIN paper",
    "BATCH_TIMER" : "0",
    "PROG_TIMER" : "10 * BILLION // in s",
    "NETWORK_TEST" : "false",
    "ABORT_PENALTY": "10 * 1000000UL   // in ns.",
    "ABORT_PENALTY_MAX": "5 * 100 * 1000000UL   // in ns.",
    "MSG_TIME_LIMIT": "0",
    "MSG_SIZE_MAX": 4096,
    "TXN_WRITE_PERC":0.0,
    "PRIORITY":"PRIORITY_ACTIVE",
    "TWOPL_LITE":"false",
#YCSB
    "INIT_PARALLELISM" : 8,
    "TUP_WRITE_PERC":0.0,
    "ZIPF_THETA":0.3,
    "ACCESS_PERC":0.03,
    "DATA_PERC": 100,
    "REQ_PER_QUERY": 10,
    "SYNTH_TABLE_SIZE":"65536",
#TPCC
    "NUM_WH": 'PART_CNT',
    "PERC_PAYMENT":0.0,
    "DEBUG_DISTR":"false",
    "DEBUG_ALLOC":"false",
    "DEBUG_RACE":"false",
    "MODE":"NORMAL_MODE",
    "SHMEM_ENV":"false",
    "STRICT_PPT":0,
    "SET_AFFINITY":"true",
    "LOGGING":"false",
    "SERVER_GENERATE_QUERIES":"false",
    "SKEW_METHOD":"ZIPF",
    "ENVIRONMENT_EC2":"false",
    "YCSB_ABORT_MODE":"false",
    "LOAD_METHOD": "LOAD_MAX",
    "ISOLATION_LEVEL":"SERIALIZABLE"
}

