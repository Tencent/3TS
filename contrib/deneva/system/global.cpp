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

#include "global.h"
#include "mem_alloc.h"
#include "stats.h"
#include "sim_manager.h"
#include "manager.h"
#include "query.h"
#include "client_query.h"
#include "occ.h"
#include "bocc.h"
#include "focc.h"
#include "ssi.h"
#include "wsi.h"
#include "dta.h"
#include "transport.h"
#include "work_queue.h"
#include "abort_queue.h"
#include "client_query.h"
#include "client_txn.h"
#include "logger.h"
#include "maat.h"
#include "manager.h"
#include "mem_alloc.h"
#include "msg_queue.h"
#include "pool.h"
#include "query.h"
#include "sequencer.h"
#include "sim_manager.h"
#include "stats.h"
#include "transport.h"
#include "txn_table.h"
#include "work_queue.h"
#include "client_txn.h"
#include "sequencer.h"
#include "logger.h"
#include "silo.h"
#include "maat.h"
#include "sundial.h"
#include "http.h"


#include <boost/lockfree/queue.hpp>
#include "da_block_queue.h"


mem_alloc mem_allocator;
Stats stats;
SimManager * simulation;
Manager glob_manager;
Query_queue query_queue;
Client_query_queue client_query_queue;
OptCC occ_man;
Focc focc_man;
Bocc bocc_man;
Maat maat_man;
Silo silo_man;
ssi ssi_man;
opt_ssi opt_ssi_man;
wsi wsi_man;
Sundial sundial_man;
Dta dta_man;
#if IS_GENERIC_ALG
UniAlgManager<CC_ALG> uni_alg_man;
#endif
Transport tport_man;
TxnManPool txn_man_pool;
TxnPool txn_pool;
AccessPool access_pool;
TxnTablePool txn_table_pool;
MsgPool msg_pool;
RowPool row_pool;
QryPool qry_pool;
TxnTable txn_table;
QWorkQueue work_queue;
AbortQueue abort_queue;
MessageQueue msg_queue;
Client_txn client_man;
Sequencer seq_man;
Logger logger;
TimeTable time_table;
DtaTimeTable dta_time_table;
SiloTimeTable silo_time_table;
InOutTable inout_table;
// QTcpQueue tcp_queue;
TcpTimestamp tcp_ts;
boost::lockfree::queue<DAQuery*, boost::lockfree::fixed_sized<true>> da_query_queue{100};
DABlockQueue da_gen_qry_queue(50);
bool is_server=false;
map<uint64_t, ts_t> da_start_stamp_tab;
set<uint64_t> da_start_trans_tab;
map<uint64_t, ts_t> da_stamp_tab;
set<uint64_t> already_abort_tab;

bool volatile warmup_done = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;

ts_t g_abort_penalty = ABORT_PENALTY;
ts_t g_abort_penalty_max = ABORT_PENALTY_MAX;
bool g_central_man = CENTRAL_MAN;
UInt32 g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
UInt32 g_ts_batch_num = TS_BATCH_NUM;
int32_t g_inflight_max = MAX_TXN_IN_FLIGHT;
//int32_t g_inflight_max = MAX_TXN_IN_FLIGHT/NODE_CNT;
uint64_t g_msg_size = MSG_SIZE_MAX;
int32_t g_load_per_server = LOAD_PER_SERVER;

bool g_hw_migrate = HW_MIGRATE;

volatile UInt64 g_row_id = 0;
bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
UInt32 g_cc_alg = CC_ALG;
ts_t g_query_intvl = QUERY_INTVL;
UInt32 g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;
double g_txn_read_perc = 1.0 - TXN_WRITE_PERC;
double g_txn_write_perc = TXN_WRITE_PERC;
double g_tup_read_perc = 1.0 - TUP_WRITE_PERC;
double g_tup_write_perc = TUP_WRITE_PERC;
double g_zipf_theta = ZIPF_THETA;
double g_data_perc = DATA_PERC;
double g_access_perc = ACCESS_PERC;
bool g_prt_lat_distr = PRT_LAT_DISTR;
UInt32 g_node_id = 0;
UInt32 g_node_cnt = NODE_CNT;
UInt32 g_part_cnt = PART_CNT;
UInt32 g_virtual_part_cnt = VIRTUAL_PART_CNT;
UInt32 g_core_cnt = CORE_CNT;

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
UInt32 g_thread_cnt = PART_CNT/NODE_CNT;
#else
UInt32 g_thread_cnt = THREAD_CNT;
#endif
UInt32 g_rem_thread_cnt = REM_THREAD_CNT;
UInt32 g_abort_thread_cnt = 1;
#if LOGGING
UInt32 g_logger_thread_cnt = 1;
#else
UInt32 g_logger_thread_cnt = 0;
#endif
UInt32 g_send_thread_cnt = SEND_THREAD_CNT;
#if CC_ALG == CALVIN
// sequencer + scheduler thread
UInt32 g_total_thread_cnt = g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt + g_abort_thread_cnt + g_logger_thread_cnt + 3;
#else
UInt32 g_total_thread_cnt = g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt + g_abort_thread_cnt + g_logger_thread_cnt + 1;
#endif

UInt32 g_total_client_thread_cnt = g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt;
UInt32 g_total_node_cnt = g_node_cnt + g_client_node_cnt + g_repl_cnt*g_node_cnt;
UInt64 g_synth_table_size = SYNTH_TABLE_SIZE;
UInt32 g_req_per_query = REQ_PER_QUERY;
bool g_strict_ppt = STRICT_PPT == 1;
UInt32 g_field_per_tuple = FIELD_PER_TUPLE;
UInt32 g_init_parallelism = INIT_PARALLELISM;
UInt32 g_client_node_cnt = CLIENT_NODE_CNT;
UInt32 g_client_thread_cnt = CLIENT_THREAD_CNT;
UInt32 g_client_rem_thread_cnt = CLIENT_REM_THREAD_CNT;
UInt32 g_client_send_thread_cnt = CLIENT_SEND_THREAD_CNT;
UInt32 g_servers_per_client = 0;
UInt32 g_clients_per_server = 0;
UInt32 g_server_start_node = 0;

UInt32 g_this_thread_cnt = ISCLIENT ? g_client_thread_cnt : g_thread_cnt;
UInt32 g_this_rem_thread_cnt = ISCLIENT ? g_client_rem_thread_cnt : g_rem_thread_cnt;
UInt32 g_this_send_thread_cnt = ISCLIENT ? g_client_send_thread_cnt : g_send_thread_cnt;
UInt32 g_this_total_thread_cnt = ISCLIENT ? g_total_client_thread_cnt : g_total_thread_cnt;

UInt32 g_max_txn_per_part = MAX_TXN_PER_PART;
UInt32 g_network_delay = NETWORK_DELAY;
UInt64 g_done_timer = DONE_TIMER;
UInt64 g_batch_time_limit = BATCH_TIMER;
UInt64 g_seq_batch_time_limit = SEQ_BATCH_TIMER;
UInt64 g_prog_timer = PROG_TIMER;
UInt64 g_warmup_timer = WARMUP_TIMER;
UInt64 g_msg_time_limit = MSG_TIME_LIMIT;

UInt64 g_log_buf_max = LOG_BUF_MAX;
UInt64 g_log_flush_timeout = LOG_BUF_TIMEOUT;

// MVCC
UInt64 g_max_read_req = MAX_READ_REQ;
UInt64 g_max_pre_req = MAX_PRE_REQ;
UInt64 g_his_recycle_len = HIS_RECYCLE_LEN;

// CALVIN
UInt32 g_seq_thread_cnt = SEQ_THREAD_CNT;

// SUNDIAL
uint32_t g_max_num_waits = MAX_NUM_WAITS;

double g_mpr = MPR;
double g_mpitem = MPIR;

// PPS (Product-Part-Supplier)
UInt32 g_max_parts_per = MAX_PPS_PARTS_PER;
UInt32 g_max_part_key = MAX_PPS_PART_KEY;
UInt32 g_max_product_key = MAX_PPS_PRODUCT_KEY;
UInt32 g_max_supplier_key = MAX_PPS_SUPPLIER_KEY;
double g_perc_getparts = PERC_PPS_GETPART;
double g_perc_getproducts = PERC_PPS_GETPRODUCT;
double g_perc_getsuppliers = PERC_PPS_GETSUPPLIER;
double g_perc_getpartbyproduct = PERC_PPS_GETPARTBYPRODUCT;
double g_perc_getpartbysupplier = PERC_PPS_GETPARTBYSUPPLIER;
double g_perc_orderproduct = PERC_PPS_ORDERPRODUCT;
double g_perc_updateproductpart = PERC_PPS_UPDATEPRODUCTPART;
double g_perc_updatepart = PERC_PPS_UPDATEPART;

// TPCC
UInt32 g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
bool g_wh_update = WH_UPDATE;
char * output_file = NULL;
char * input_file = NULL;
char * txn_file = NULL;

#if TPCC_SMALL
UInt32 g_max_items = MAX_ITEMS_SMALL;
UInt32 g_cust_per_dist = CUST_PER_DIST_SMALL;
#else
UInt32 g_max_items = MAX_ITEMS_NORM;
UInt32 g_cust_per_dist = CUST_PER_DIST_NORM;
#endif
UInt32 g_max_items_per_txn = MAX_ITEMS_PER_TXN;
UInt32 g_dist_per_wh = DIST_PER_WH;

UInt32 g_repl_type = REPL_TYPE;
UInt32 g_repl_cnt = REPLICA_CNT;

map<string, string> g_params;

