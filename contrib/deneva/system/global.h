/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _GLOBAL_H_
#define _GLOBAL_H_

#define __STDC_LIMIT_MACROS
#include <stdint.h>
#include <unistd.h>
#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <typeinfo>
#include <list>
#include <map>
#include <set>
#include <queue>
#include <string>
#include <vector>
#include <sstream>
#include <time.h>
#include <sys/time.h>
#include <math.h>

#include "pthread.h"
#include "config.h"
#include "stats.h"
//#include "work_queue.h"
#include "pool.h"
#include "txn_table.h"
#include "logger.h"
#include "sim_manager.h"

#include <boost/lockfree/queue.hpp>
#include "da_block_queue.h"
//#include "maat.h"

using namespace std;

class mem_alloc;
class Stats;
class SimManager;
class Manager;
class Query_queue;
class OptCC;
class Focc;
class Bocc;
class ssi;
class wsi;
class Maat;
class Sundial;
class Dta;
class Dli;
class Transport;
class Remote_query;
class TxnManPool;
class TxnPool;
class AccessPool;
class TxnTablePool;
class MsgPool;
class RowPool;
class QryPool;
class TxnTable;
class QWorkQueue;
class AbortQueue;
class MessageQueue;
class Client_query_queue;
class Client_txn;
class Sequencer;
class Logger;
class TimeTable;
class DtaTimeTable;
class InOutTable;

class DAQuery;
class DABlockQueue;
class KeyXidCache;
class RtsCache;
// class QTcpQueue;
class TcpTimestamp;

typedef uint32_t UInt32;
typedef int32_t SInt32;
typedef uint64_t UInt64;
typedef int64_t SInt64;

typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Data Structure
/******************************************/
extern mem_alloc mem_allocator;
extern Stats stats;
extern SimManager * simulation;
extern Manager glob_manager;
extern Query_queue query_queue;
extern Client_query_queue client_query_queue;
extern OptCC occ_man;
extern Focc focc_man;
extern Bocc bocc_man;
extern ssi ssi_man;
extern wsi wsi_man;
extern Maat maat_man;
extern Sundial sundial_man;
extern Dta dta_man;
extern Dli dli_man;
extern Transport tport_man;
extern TxnManPool txn_man_pool;
extern TxnPool txn_pool;
extern AccessPool access_pool;
extern TxnTablePool txn_table_pool;
extern MsgPool msg_pool;
extern RowPool row_pool;
extern QryPool qry_pool;
extern TxnTable txn_table;
extern QWorkQueue work_queue;
extern AbortQueue abort_queue;
extern MessageQueue msg_queue;
extern Client_txn client_man;
extern Sequencer seq_man;
extern Logger logger;
extern TimeTable time_table;
extern DtaTimeTable dta_time_table;
extern InOutTable inout_table;

// extern QTcpQueue tcp_queue;
extern TcpTimestamp tcp_ts;

extern map<string, string> g_params;

extern bool volatile warmup_done;
extern bool volatile enable_thread_mem_pool;
extern pthread_barrier_t warmup_bar;

/******************************************/
// Client Global Params
/******************************************/
extern UInt32 g_client_thread_cnt;
extern UInt32 g_client_rem_thread_cnt;
extern UInt32 g_client_send_thread_cnt;
extern UInt32 g_client_node_cnt;
extern UInt32 g_servers_per_client;
extern UInt32 g_clients_per_server;
extern UInt32 g_server_start_node;

/******************************************/
// Global Parameter
/******************************************/
extern volatile UInt64 g_row_id;
extern bool g_part_alloc;
extern bool g_mem_pad;
extern bool g_prt_lat_distr;
extern UInt32 g_node_id;
extern UInt32 g_node_cnt;
extern UInt32 g_part_cnt;
extern UInt32 g_virtual_part_cnt;
extern UInt32 g_core_cnt;
extern UInt32 g_total_node_cnt;
extern UInt32 g_total_thread_cnt;
extern UInt32 g_total_client_thread_cnt;
extern UInt32 g_this_thread_cnt;
extern UInt32 g_this_rem_thread_cnt;
extern UInt32 g_this_send_thread_cnt;
extern UInt32 g_this_total_thread_cnt;
extern UInt32 g_thread_cnt;
extern UInt32 g_abort_thread_cnt;
extern UInt32 g_logger_thread_cnt;
extern UInt32 g_tcp_thread_cnt;
extern UInt32 g_send_thread_cnt;
extern UInt32 g_rem_thread_cnt;
extern ts_t g_abort_penalty;
extern ts_t g_abort_penalty_max;
extern bool g_central_man;
extern UInt32 g_ts_alloc;
extern bool g_key_order;
extern bool g_ts_batch_alloc;
extern UInt32 g_ts_batch_num;
extern int32_t g_inflight_max;
extern uint64_t g_msg_size;
extern uint64_t g_log_buf_max;
extern uint64_t g_log_flush_timeout;

extern UInt32 g_max_txn_per_part;
extern int32_t g_load_per_server;

extern bool g_hw_migrate;
extern UInt32 g_network_delay;
extern UInt64 g_done_timer;
extern UInt64 g_batch_time_limit;
extern UInt64 g_seq_batch_time_limit;
extern UInt64 g_prog_timer;
extern UInt64 g_warmup_timer;
extern UInt64 g_msg_time_limit;

// MVCC
extern UInt64 g_max_read_req;
extern UInt64 g_max_pre_req;
extern UInt64 g_his_recycle_len;

// YCSB
extern UInt32 g_cc_alg;
extern ts_t g_query_intvl;
extern UInt32 g_part_per_txn;
extern double g_perc_multi_part;
extern double g_txn_read_perc;
extern double g_txn_write_perc;
extern double g_tup_read_perc;
extern double g_tup_write_perc;
extern double g_zipf_theta;
extern double g_data_perc;
extern double g_access_perc;
extern UInt64 g_synth_table_size;
extern UInt32 g_req_per_query;
extern bool g_strict_ppt;
extern UInt32 g_field_per_tuple;
extern UInt32 g_init_parallelism;
extern double g_mpr;
extern double g_mpitem;

// TPCC
extern UInt32 g_num_wh;
extern double g_perc_payment;
extern bool g_wh_update;
extern char * output_file;
extern char * input_file;
extern char * txn_file;
extern UInt32 g_max_items;
extern UInt32 g_dist_per_wh;
extern UInt32 g_cust_per_dist;
extern UInt32 g_max_items_per_txn;

// PPS (Product-Part-Supplier)
extern UInt32 g_max_parts_per;
extern UInt32 g_max_part_key;
extern UInt32 g_max_product_key;
extern UInt32 g_max_supplier_key;
extern double g_perc_getparts;
extern double g_perc_getproducts;
extern double g_perc_getsuppliers;
extern double g_perc_getpartbyproduct;
extern double g_perc_getpartbysupplier;
extern double g_perc_orderproduct;
extern double g_perc_updateproductpart;
extern double g_perc_updatepart;

extern boost::lockfree::queue<DAQuery*, boost::lockfree::fixed_sized<true>> da_query_queue;
extern DABlockQueue da_gen_qry_queue;
extern bool is_server;
extern map<uint64_t, ts_t> da_start_stamp_tab;
extern set<uint64_t> da_start_trans_tab;
extern map<uint64_t, ts_t> da_stamp_tab;
extern set<uint64_t> already_abort_tab;
extern string DA_history_mem;
extern bool abort_history;
extern ofstream commit_file;
extern ofstream abort_file;
// CALVIN
extern UInt32 g_seq_thread_cnt;

// SUNDIAL
extern uint32_t g_max_num_waits;

// Replication
extern UInt32 g_repl_type;
extern UInt32 g_repl_cnt;

enum RC { RCOK=0, Commit, Abort, WAIT, WAIT_REM, ERROR, FINISH, NONE};
enum RemReqType {
    INIT_DONE = 0,
    RLK,
    RULK,
    CL_QRY,
    RQRY,
    RQRY_CONT,
    RFIN,
    RLK_RSP,
    RULK_RSP,
    RQRY_RSP,
    RACK,
    RACK_PREP,
    RACK_FIN,
    RTXN,
    RTXN_CONT,
    RINIT,
    RPREPARE,
    RPASS,
    RFWD,
    RDONE,
    CL_RSP,
    LOG_MSG,
    LOG_MSG_RSP,
    LOG_FLUSHED,
    CALVIN_ACK,
    NO_MSG
};

// Calvin
enum CALVIN_PHASE {
    CALVIN_RW_ANALYSIS = 0,
    CALVIN_LOC_RD,
    CALVIN_SERVE_RD,
    CALVIN_COLLECT_RD,
    CALVIN_EXEC_WR,
    CALVIN_DONE
};

/* Thread */
typedef uint64_t txnid_t;

/* Txn */
typedef uint64_t txn_t;

/* Table and Row */
typedef uint64_t rid_t; // row id
typedef uint64_t pgid_t; // page id



/* INDEX */
enum latch_t {LATCH_EX, LATCH_SH, LATCH_NONE};
// accessing type determines the latch type on nodes
enum idx_acc_t {INDEX_INSERT, INDEX_READ, INDEX_NONE};
typedef uint64_t idx_key_t; // key id for index
typedef uint64_t (*func_ptr)(idx_key_t);    // part_id func_ptr(index_key);

/* general concurrency control */
enum access_t {RD, WR, XP, SCAN};
/* LOCK */
enum lock_t {LOCK_EX = 0, LOCK_SH, LOCK_NONE, LOCK_COM};
/* TIMESTAMP */
enum TsType {R_REQ = 0, W_REQ, P_REQ, XP_REQ, S_REQ};

/*DA query build queue*/
//queue<DAQuery> query_build_queue;



#define GET_THREAD_ID(id)    (id % g_thread_cnt)
#define GET_NODE_ID(id)    (id % g_node_cnt)
#define GET_PART_ID(t,n)    (n)
#define GET_PART_ID_FROM_IDX(idx)    (g_node_id + idx * g_node_cnt)
#define GET_PART_ID_IDX(p)    (p / g_node_cnt)
#define ISSERVER (g_node_id < g_node_cnt)
#define ISSERVERN(id) (id < g_node_cnt)
#define ISCLIENT (g_node_id >= g_node_cnt && g_node_id < g_node_cnt + g_client_node_cnt)
#define ISREPLICA                                 \
  (g_node_id >= g_node_cnt + g_client_node_cnt && \
   g_node_id < g_node_cnt + g_client_node_cnt + g_repl_cnt * g_node_cnt)
#define ISREPLICAN(id)                     \
  (id >= g_node_cnt + g_client_node_cnt && \
   id < g_node_cnt + g_client_node_cnt + g_repl_cnt * g_node_cnt)
#define ISCLIENTN(id) (id >= g_node_cnt && id < g_node_cnt + g_client_node_cnt)
#define IS_LOCAL(tid) (tid % g_node_cnt == g_node_id || CC_ALG == CALVIN)
#define IS_REMOTE(tid) (tid % g_node_cnt != g_node_id || CC_ALG == CALVIN)
#define IS_LOCAL_KEY(key) (key % g_node_cnt == g_node_id)

/*
#define GET_THREAD_ID(id)    (id % g_thread_cnt)
#define GET_NODE_ID(id)    (id / g_thread_cnt)
#define GET_PART_ID(t,n)    (n*g_thread_cnt + t)
*/

#define MSG(str, args...) \
  { printf("[%s : %d] " str, __FILE__, __LINE__, args); }  //    printf(args); }

// principal index structure. The workload may decide to use a different
// index structure for specific purposes. (e.g. non-primary key access should use hash)
#if (INDEX_STRUCT == IDX_BTREE)
#define INDEX        index_btree
#else  // IDX_HASH
#define INDEX        IndexHash
#endif

/************************************************/
// constants
/************************************************/
#ifndef UINT64_MAX
#define UINT64_MAX         18446744073709551615UL
#endif // UINT64_MAX

#endif
