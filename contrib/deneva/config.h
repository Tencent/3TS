#ifndef _CONFIG_H_

#define _CONFIG_H_

/***sundial****/
/*
#define WRITE_PERMISSION_LOCK         false
#define MULTI_VERSION                 false
#define ENABLE_LOCAL_CACHING          false
#define OCC_LOCK_TYPE                 WAIT_DIE
#define SUNDIAL_MV                     false
#define OCC_WAW_LOCK                  true
#define RO_LEASE                      false
#define ATOMIC_WORD                   false
#define TRACK_LAST                    false
#define UPDATE_TABLE_TS               true
#define WRITE_PERMISSION_LOCK         false
#define LOCK_ALL_BEFORE_COMMIT        false
#define LOCK_ALL_DEBUG                false
#define PAUSE __asm__ ( "pause;" );
#define COMPILER_BARRIER asm volatile("" ::: "memory");
*/

/***********************************************/
// DA Trans Creator
/***********************************************/
//which creator to use
#define CREATOR_USE_T false

//TraversalActionSequenceCreator
#define TRANS_CNT 4
#define ITEM_CNT 4
#define SUBTASK_NUM 1
#define SUBTASK_ID 0
#define MAX_DML 4
#define WITH_ABORT false
#define TAIL_DTL false
#define SAVE_HISTROY_WITH_EMPTY_OPT false
#define DYNAMIC_SEQ_LEN false

//InputActionSequenceCreator
#define INPUT_FILE_PATH "./input.txt"

// ! Parameters used to locate distributed performance bottlenecks.
#define SECOND 100 // Set the queue monitoring time.
// #define LESS_DIS // Reduce the number of yCSB remote data to 1
// #define NEW_WORK_QUEUE  // The workQueue data structure has been modified to perform 10,000 better than the original implementation.
// #define NO_2PC  // Removing 2PC, of course, would be problematic in distributed transactions.
// #define FAKE_PROCESS  // Io_thread returns as soon as it gets the request from the remote. Avoid waiting in the WORK_queue.
// #define NO_REMOTE // remove all remote txn
#define TXN_QUEUE_PERCENT 0.9 // The proportion of the transaction to take from txn_queue firstly.
// ! end of these parameters
//

// print detail log for each operation execution
#define DA_PRINT_LOG false

/***********************************************/
// Simulation + Hardware
/***********************************************/
#define NODE_CNT 1
#define THREAD_CNT 1 //trans_num
#define REM_THREAD_CNT 1
#define SEND_THREAD_CNT 1
#define CORE_CNT 2
// PART_CNT should be at least NODE_CNT
#define PART_CNT NODE_CNT
#define CLIENT_NODE_CNT NODE_CNT
#define CLIENT_THREAD_CNT 1
#define CLIENT_REM_THREAD_CNT 1
#define CLIENT_SEND_THREAD_CNT 1
#define CLIENT_RUNTIME false

#define LOAD_METHOD LOAD_MAX
#define LOAD_PER_SERVER 100

// Replication
#define REPLICA_CNT 0
// AA (Active-Active), AP (Active-Passive)
#define REPL_TYPE AP

// each transaction only accesses only 1 virtual partition. But the lock/ts manager and index are
// not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only
// used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT    PART_CNT
#define PAGE_SIZE         4096
#define CL_SIZE           64
#define CPU_FREQ          2.6
// enable hardware migration.
#define HW_MIGRATE          false

// # of transactions to run for warmup
#define WARMUP            0
// YCSB or TPCC or PPS or DA
#define WORKLOAD YCSB
// print the transaction latency distribution
#define PRT_LAT_DISTR false
#define STATS_ENABLE        true
#define TIME_ENABLE         true //STATS_ENABLE

#define FIN_BY_TIME true
#define MAX_TXN_IN_FLIGHT 1800000

#define SERVER_GENERATE_QUERIES false

/***********************************************/
// Memory System
/***********************************************/
// Three different memory allocation methods are supported.
// 1. default libc malloc
// 2. per-thread malloc. each thread has a private local memory
//    pool
// 3. per-partition malloc. each partition has its own memory pool
//    which is mapped to a unique tile on the chip.
#define MEM_ALLIGN          8

// [THREAD_ALLOC]
#define THREAD_ALLOC        false
#define THREAD_ARENA_SIZE     (1UL << 22)
#define MEM_PAD           true

// [PART_ALLOC]
#define PART_ALLOC          false
#define MEM_SIZE          (1UL << 30)
#define NO_FREE           false

/***********************************************/
// Message Passing
/***********************************************/
#define TPORT_TYPE tcp
#define TPORT_PORT 7000
#define SET_AFFINITY true

#define MAX_TPORT_NAME 128
#define MSG_SIZE 128 // in bytes
#define HEADER_SIZE sizeof(uint32_t)*2 // in bits
#define MSG_TIMEOUT 5000000000UL // in ns
#define NETWORK_TEST false
#define NETWORK_DELAY_TEST false
#define NETWORK_DELAY 0UL

#define MAX_QUEUE_LEN NODE_CNT * 2

#define PRIORITY_WORK_QUEUE false
#define PRIORITY PRIORITY_ACTIVE
#define MSG_SIZE_MAX 4096
#define MSG_TIME_LIMIT 0

/***********************************************/
// Concurrency Control
/***********************************************/

// WAIT_DIE, NO_WAIT, TIMESTAMP, MVCC, CALVIN, MAAT, SUNDIAL, SILO, BOCC, FOCC, SSI, WSI
#define ISOLATION_LEVEL SERIALIZABLE
#define CC_ALG WSI
#define LOCK_CS false   // CS: critical section, NW: no wait, WD: wait_die
#define LOCK_NW true   // CS: critical section, NW: no wait, WD: wait_die
#define LOCK_WD false   // CS: critical section, NW: no wait, WD: wait_die
#define YCSB_ABORT_MODE false
#define QUEUE_CAPACITY_NEW 1000000
// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER         false
// transaction roll back changes after abort
#define ROLL_BACK         true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN         false
#define BUCKET_CNT          31
#define ABORT_PENALTY 10 * 1000000UL   // in ns.
#define ABORT_PENALTY_MAX 5 * 100 * 1000000UL   // in ns.
#define BACKOFF true
// [ INDEX ]
#define ENABLE_LATCH        false
#define CENTRAL_INDEX       false
#define CENTRAL_MANAGER       false
#define INDEX_STRUCT        IDX_HASH
#define BTREE_ORDER         16

// [TIMESTAMP]
#define TS_TWR            false
#define TS_ALLOC          TS_CLOCK
#define TS_BATCH_ALLOC        false
#define TS_BATCH_NUM        1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
#define HIS_RECYCLE_LEN       10
#define MAX_PRE_REQ         MAX_TXN_IN_FLIGHT * NODE_CNT//1024
#define MAX_READ_REQ        MAX_TXN_IN_FLIGHT * NODE_CNT//1024
#define MIN_TS_INTVL        10 * 1000000UL // 10ms
// [OCC]
#define MAX_WRITE_SET       10
#define PER_ROW_VALID       false
// [VLL]
#define TXN_QUEUE_SIZE_LIMIT    THREAD_CNT
// [CALVIN]
#define SEQ_THREAD_CNT 4
// [SUNDIAL]
#define MAX_NUM_WAITS 4
#define PRE_ABORT true
#define OCC_LOCK_TYPE WAIT_DIE
#define OCC_WAW_LOCK true
// [SILO]
#define VALIDATION_LOCK                "no-wait" // no-wait or waiting
#define PRE_ABORT2                    "true"
#define ATOMIC_WORD                    false
/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND         false
#define LOG_REDO          false
#define LOGGING false
#define LOG_BUF_MAX 10
#define LOG_BUF_TIMEOUT 10 * 1000000UL // 10ms

/***********************************************/
// Benchmark
/***********************************************/
// max number of rows touched per transaction
#define MAX_ROW_PER_TXN       64
#define QUERY_INTVL         1UL
#define MAX_TXN_PER_PART 500000
#define FIRST_PART_LOCAL      true
#define MAX_TUPLE_SIZE        1024 // in bytes
#define GEN_BY_MPR false
#define DISTRIBUTE_PERCENT 1.0 // new added for distributed rate in YCSB and TPCC
// ==== [YCSB] ====
// SKEW_METHOD:
//    ZIPF: use ZIPF_THETA distribution
//    HOT: use ACCESS_PERC of the accesses go to DATA_PERC of the data
#define SKEW_METHOD ZIPF
#define DATA_PERC 100
#define ACCESS_PERC 0.03
#define INIT_PARALLELISM 8
#define SYNTH_TABLE_SIZE 2097152
#define ZIPF_THETA 0.3
#define TXN_WRITE_PERC 0.9
#define TUP_WRITE_PERC 0.9
#define SCAN_PERC           0
#define SCAN_LEN          20
#define PART_PER_TXN 2
#define PERC_MULTI_PART     MPR
#define REQ_PER_QUERY 10
#define FIELD_PER_TUPLE       10
#define CREATE_TXN_FILE false
#define STRICT_PPT 0
// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL          false
#define MAX_ITEMS_SMALL 10000
#define CUST_PER_DIST_SMALL 2000
#define MAX_ITEMS_NORM 100000
#define CUST_PER_DIST_NORM 3000
#define MAX_ITEMS_PER_TXN 15
// Some of the transactions read the data but never use them.
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions
// are not modeled.
#define TPCC_ACCESS_ALL       false
#define WH_UPDATE         true
#define NUM_WH PART_CNT
// % of transactions that access multiple partitions
#define MPR 1.0
#define MPIR 0.01
#define MPR_NEWORDER      20 // In %
enum TPCCTable {
    TPCC_WAREHOUSE,
    TPCC_DISTRICT,
    TPCC_CUSTOMER,
    TPCC_HISTORY,
    TPCC_NEWORDER,
    TPCC_ORDER,
    TPCC_ORDERLINE,
    TPCC_ITEM,
    TPCC_STOCK
};
enum TPCCTxnType {
    TPCC_ALL,
    TPCC_PAYMENT,
    TPCC_NEW_ORDER,
    TPCC_ORDER_STATUS,
    TPCC_DELIVERY,
    TPCC_STOCK_LEVEL
};
enum DATxnType {
    DA_READ,
    DA_WRITE,
    DA_COMMIT,
    DA_ABORT,
    DA_SCAN
};
#define MAX_DA_TABLE_SIZE 10000


extern TPCCTxnType g_tpcc_txn_type;
//#define TXN_TYPE          TPCC_ALL
#define PERC_PAYMENT 0.0
#define FIRSTNAME_MINLEN      8
#define FIRSTNAME_LEN         16
#define LASTNAME_LEN        16

#define DIST_PER_WH       10

// PPS (Product-Part-Supplier)
#define MAX_PPS_PARTS_PER 10
#define MAX_PPS_PART_KEY 10000
#define MAX_PPS_PRODUCT_KEY 1000
#define MAX_PPS_SUPPLIER_KEY 1000
#define MAX_PPS_PART_PER_PRODUCT 10
#define MAX_PPS_PART_PER_SUPPLIER 10
#define MAX_PPS_PART_PER_PRODUCT_KEY 10
#define MAX_PPS_PART_PER_SUPPLIER_KEY 10

#define PERC_PPS_GETPART 0.00
#define PERC_PPS_GETSUPPLIER 0.00
#define PERC_PPS_GETPRODUCT 0.0
#define PERC_PPS_GETPARTBYSUPPLIER 0.0
#define PERC_PPS_GETPARTBYPRODUCT 0.2
#define PERC_PPS_ORDERPRODUCT 0.6
#define PERC_PPS_UPDATEPRODUCTPART 0.2
#define PERC_PPS_UPDATEPART 0.0

enum PPSTxnType {
    PPS_ALL = 0,
    PPS_GETPART,
    PPS_GETSUPPLIER,
    PPS_GETPRODUCT,
    PPS_GETPARTBYSUPPLIER,
    PPS_GETPARTBYPRODUCT,
    PPS_ORDERPRODUCT,
    PPS_UPDATEPRODUCTPART,
    PPS_UPDATEPART
};

/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB           true
#define IDX_VERB          false
#define VERB_ALLOC          true

#define DEBUG_LOCK          false
#define DEBUG_TIMESTAMP       false
#define DEBUG_SYNTH         false
#define DEBUG_ASSERT        false
#define DEBUG_DISTR false
#define DEBUG_ALLOC false
#define DEBUG_RACE false
#define DEBUG_TIMELINE        false
#define DEBUG_BREAKDOWN       false
#define DEBUG_LATENCY       false

/***********************************************/
// MODES
/***********************************************/
// QRY Only do query operations, no 2PC
// TWOPC Only do 2PC, no query work
// SIMPLE Immediately send OK back to client
// NOCC Don't do CC
// NORMAL normal operation
#define MODE NORMAL_MODE


/***********************************************/
// Constant
/***********************************************/
// INDEX_STRUCT
#define IDX_HASH          1
#define IDX_BTREE         2
// WORKLOAD
#define YCSB            1
#define TPCC            2
#define PPS             3
#define TEST            4
#define DA 5
// Concurrency Control Algorithm
#define NO_WAIT           1
#define WAIT_DIE          2
#define DL_DETECT         3
#define TIMESTAMP         4
#define MVCC            5
#define HSTORE            6
#define HSTORE_SPEC           7
#define OCC             8
#define VLL             9
#define CALVIN      10
#define MAAT      11
#define WDL           12

#define SUNDIAL     14
#define FOCC       15
#define BOCC       16
#define SSI        17
#define WSI        18

#define DTA 26

#define SILO 27
#define CNULL 28
#define DLI_IDENTIFY_CYCLE 29
#define DLI_IDENTIFY_CHAIN 30
#define DLI_IDENTIFY_SSI 31
#define OPT_SSI          32

#define IS_GENERIC_ALG (CC_ALG == DLI_IDENTIFY_CYCLE || CC_ALG == DLI_IDENTIFY_CHAIN || CC_ALG == DLI_IDENTIFY_SSI)

// TIMESTAMP allocation method.
#define TS_MUTEX          1
#define TS_CAS            2
#define TS_HW           3
#define TS_CLOCK          4
#define LTS_CURL_CLOCK          5
#define LTS_TCP_CLOCK          6

#define LTS_TCP_IP  "10.77.110.147"
#define LTS_TCP_PORT  62389
// MODES
// NORMAL < NOCC < QRY_ONLY < SETUP < SIMPLE
#define NORMAL_MODE 1
#define NOCC_MODE 2
#define QRY_ONLY_MODE 3
#define SETUP_MODE 4
#define SIMPLE_MODE 5
// SKEW METHODS
#define ZIPF 1
#define HOT 2
// PRIORITY WORK QUEUE
#define PRIORITY_FCFS 1
#define PRIORITY_ACTIVE 2
#define PRIORITY_HOME 3
// Replication
#define AA 1
#define AP 2
// Load
#define LOAD_MAX 1
#define LOAD_RATE 2
// Transport
#define TCP 1
#define IPC 2
// Isolation levels
#define SERIALIZABLE 1
#define READ_COMMITTED 2
#define READ_UNCOMMITTED 3
#define NOLOCK 4

// Stats and timeout
#define BILLION 1000000000UL // in ns => 1 second
#define MILLION 1000000UL // in ns => 1 second
#define STAT_ARR_SIZE 1024
#define PROG_TIMER 10 * BILLION // in s
#define BATCH_TIMER 0
#define SEQ_BATCH_TIMER 5 * 1 * MILLION // ~5ms -- same as CALVIN paper
#define DONE_TIMER 1 * 60 * BILLION // ~1 minutes
#define WARMUP_TIMER 1 * 60 * BILLION    // ~1 minutes

#define SEED 0
#define SHMEM_ENV false
#define ENVIRONMENT_EC2 false

#endif
