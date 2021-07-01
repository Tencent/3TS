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

#ifndef _STATS_H_
#define _STATS_H_
#include <sys/times.h>
#include <sys/vtimes.h>
#include <time.h>

#include "../system/global.h"
#include "stats_array.h"
class StatValue {
public:
    StatValue() : value(0) {}
    void operator+=(double value) { this->value += value; }
    void operator=(double value) { this->value = value; }
    double get() {return value;}

private:
    double value;
};

class Stats_thd {
public:
    void init(uint64_t thd_id);
    void combine(Stats_thd * stats);
    void print(FILE * outf, bool prog);
    void print_message(FILE * outf, bool prog);
    void print_client(FILE * outf, bool prog);
    void clear();

    char _pad2[CL_SIZE];

    uint64_t* part_cnt;
    uint64_t* part_acc;

    double total_runtime;

    uint64_t parts_touched;

    // Execution
    uint64_t txn_cnt;
    uint64_t remote_txn_cnt;
    uint64_t local_txn_cnt;
    uint64_t local_txn_start_cnt;
    uint64_t total_txn_commit_cnt;
    uint64_t local_txn_commit_cnt;
    uint64_t remote_txn_commit_cnt;
    uint64_t total_txn_abort_cnt;
    uint64_t total_rw_abort_cnt;
    uint64_t total_ww_abort_cnt;
    uint64_t total_read_abort_cnt;
    uint64_t total_write_abort_cnt;
    uint64_t total_validate_abort_cnt;
    uint64_t total_txn_prewrite_cnt;
    uint64_t positive_txn_abort_cnt;
    uint64_t unique_txn_abort_cnt;
    uint64_t local_txn_abort_cnt;
    uint64_t remote_txn_abort_cnt;
    double txn_run_time;
    uint64_t multi_part_txn_cnt;
    double multi_part_txn_run_time;
    uint64_t single_part_txn_cnt;
    double single_part_txn_run_time;
    uint64_t txn_write_cnt;
    uint64_t record_write_cnt;

    // Transaction stats
    double txn_total_process_time;
    double txn_process_time;
    double txn_total_local_wait_time;
    double txn_local_wait_time;
    double txn_total_remote_wait_time;
    double txn_remote_wait_time;
    double txn_total_twopc_time;
    double txn_twopc_time;
    double txn_init_time;
    double txn_clean_time;

    // Client
    uint64_t txn_sent_cnt;
    double cl_send_intv;

    // Breakdown
    double ts_alloc_time;
    double abort_time;
    double txn_manager_time;
    double txn_index_time;
    double txn_validate_time;
    double txn_cleanup_time;
    // trans
    double trans_total_run_time;
    double trans_process_time;
    double trans_2pc_time;
    double trans_prepare_time;
    double trans_validate_time;
    double trans_finish_time;
    double trans_commit_time;
    double trans_abort_time;

    double trans_access_lock_wait_time;
    double total_access_time;
    double trans_access_read_time;
    double trans_access_pre_time;
    double trans_access_write_time;
    double trans_access_xp_time;
    double trans_access_clear_time;
    double trans_access_write_insert_time;
    double trans_access_write_release_time;
    double trans_access_pre_check_time;
    double trans_access_pre_before_time;
    double trans_access_pre_lock_time;
    double real_abort_time;
    double trans_abort_reset_time;
    // trans mvcc
    double trans_mvcc_clear_history;
    double trans_mvcc_access;
    double trans_access_write_update_time;
    // Work queue
    double work_queue_wait_time;
    uint64_t work_queue_cnt;
    uint64_t work_queue_enq_cnt;
    double work_queue_mtx_wait_time;
    uint64_t work_queue_new_cnt;
    double work_queue_new_wait_time;
    uint64_t work_queue_old_cnt;
    double work_queue_old_wait_time;
    double work_queue_enqueue_time;
    double work_queue_dequeue_time;
    uint64_t work_queue_conflict_cnt;

    // Abort queue
    uint64_t abort_queue_enqueue_cnt;
    uint64_t abort_queue_dequeue_cnt;
    double abort_queue_enqueue_time;
    double abort_queue_dequeue_time;
    double abort_queue_penalty;
    double abort_queue_penalty_extra;

    // Worker thread
    double worker_idle_time;
    double worker_activate_txn_time;
    double worker_deactivate_txn_time;
    double worker_release_msg_time;
    double worker_process_time;
    uint64_t worker_process_cnt;
    uint64_t * worker_process_cnt_by_type;
    double * worker_process_time_by_type;
    uint64_t * work_queue_wq_cnt;
    uint64_t * work_queue_tx_cnt;

    uint64_t * work_queue_ewq_cnt;
    uint64_t * work_queue_dwq_cnt;
    uint64_t * work_queue_etx_cnt;
    uint64_t * work_queue_dtx_cnt;
    // IO
    double msg_queue_delay_time;
    uint64_t msg_queue_cnt;
    uint64_t msg_queue_enq_cnt;
    double msg_send_time;
    double msg_recv_time;
    double msg_recv_idle_time;
    uint64_t msg_batch_cnt;
    uint64_t msg_batch_size_msgs;
    uint64_t msg_batch_size_bytes;
    uint64_t msg_batch_size_bytes_to_server;
    uint64_t msg_batch_size_bytes_to_client;
    uint64_t msg_send_cnt;
    uint64_t msg_recv_cnt;
    double msg_unpack_time;
    double mbuf_send_intv_time;
    double msg_copy_output_time;

    // Concurrency control, general
    uint64_t cc_conflict_cnt;
    uint64_t txn_wait_cnt;
    uint64_t txn_conflict_cnt;

    // 2PL
    uint64_t twopl_already_owned_cnt;
    uint64_t twopl_owned_cnt;
    uint64_t twopl_sh_owned_cnt;
    uint64_t twopl_ex_owned_cnt;
    uint64_t twopl_get_cnt;
    uint64_t twopl_sh_bypass_cnt;
    double twopl_owned_time;
    double twopl_sh_owned_time;
    double twopl_ex_owned_time;
    double twopl_diff_time;
    double twopl_wait_time;
    uint64_t twopl_getlock_cnt;
    uint64_t twopl_release_cnt;
    double twopl_getlock_time;
    double twopl_release_time;

    // Calvin
    uint64_t seq_txn_cnt;
    uint64_t seq_batch_cnt;
    uint64_t seq_full_batch_cnt;
    double seq_ack_time;
    double seq_batch_time;
    uint64_t seq_process_cnt;
    uint64_t seq_complete_cnt;
    double seq_process_time;
    double seq_prep_time;
    double seq_idle_time;
    double seq_queue_wait_time;
    uint64_t seq_queue_cnt;
    uint64_t seq_queue_enq_cnt;
    double seq_queue_enqueue_time;
    double seq_queue_dequeue_time;
    double sched_queue_wait_time;
    uint64_t sched_queue_cnt;
    uint64_t sched_queue_enq_cnt;
    double sched_queue_enqueue_time;
    double sched_queue_dequeue_time;
    double calvin_sched_time;
    double sched_idle_time;
    double sched_txn_table_time;
    uint64_t sched_epoch_cnt;
    double sched_epoch_diff;

    // DLI_MVCC_OCC
    double dli_mvcc_occ_validate_time;
    uint64_t dli_mvcc_occ_check_cnt;
    uint64_t dli_mvcc_occ_abort_check_cnt;
    uint64_t dli_mvcc_occ_ts_abort_cnt;

    // OCC
    double occ_validate_time;
    double occ_cs_wait_time;
    double occ_cs_time;
    double occ_hist_validate_time;
    double occ_act_validate_time;
    double occ_hist_validate_fail_time;
    double occ_act_validate_fail_time;
    uint64_t occ_check_cnt;
    uint64_t occ_abort_check_cnt;
    uint64_t occ_ts_abort_cnt;
    double occ_finish_time;

    // WSI
    double wsi_validate_time;
    double wsi_cs_wait_time;
    uint64_t wsi_check_cnt;
    uint64_t wsi_abort_check_cnt;

    // MAAT
    uint64_t maat_validate_cnt;
    double maat_validate_time;
    double maat_cs_wait_time;
    uint64_t maat_case1_cnt;
    uint64_t maat_case2_cnt;
    uint64_t maat_case3_cnt;
    uint64_t maat_case4_cnt;
    uint64_t maat_case5_cnt;
    double maat_range;
    uint64_t maat_commit_cnt;

    // // SSI
    // uint64_t ssi_validate_cnt;
    // double ssi_validate_time;
    // uint64_t ssi_commit_cnt;

    // Logging
    uint64_t log_write_cnt;
    double log_write_time;
    uint64_t log_flush_cnt;
    double log_flush_time;
    double log_process_time;

    // Transaction Table
    uint64_t txn_table_new_cnt;
    uint64_t txn_table_get_cnt;
    uint64_t txn_table_release_cnt;
    uint64_t txn_table_cflt_cnt;
    uint64_t txn_table_cflt_size;
    double txn_table_get_time;
    double txn_table_release_time;
    double txn_table_min_ts_time;

    // Latency
    StatsArr client_client_latency;
    StatsArr first_start_commit_latency;
    StatsArr last_start_commit_latency;
    StatsArr start_abort_commit_latency;

    // stats accumulated
    double lat_work_queue_time;
    double lat_msg_queue_time;
    double lat_cc_block_time;
    double lat_cc_time;
    double lat_process_time;
    double lat_abort_time;
    double lat_network_time;
    double lat_other_time;

    // stats from committed local transactions from the first starttime of the transaction
    double lat_l_loc_work_queue_time;
    double lat_l_loc_msg_queue_time;
    double lat_l_loc_cc_block_time;
    double lat_l_loc_cc_time;
    double lat_l_loc_process_time;
    double lat_l_loc_abort_time;

    // stats from committed local transactions only from the most recent start time
    double lat_s_loc_work_queue_time;
    double lat_s_loc_msg_queue_time;
    double lat_s_loc_cc_block_time;
    double lat_s_loc_cc_time;
    double lat_s_loc_process_time;

    // stats from message-managed latency
    double lat_short_work_queue_time;
    double lat_short_msg_queue_time;
    double lat_short_cc_block_time;
    double lat_short_cc_time;
    double lat_short_process_time;
    double lat_short_network_time;
    double lat_short_batch_time;

    // stats from committed non-local transactions
    double lat_l_rem_work_queue_time;
    double lat_l_rem_msg_queue_time;
    double lat_l_rem_cc_block_time;
    double lat_l_rem_cc_time;
    double lat_l_rem_process_time;

    // stats from aborted non-local transactions
    double lat_s_rem_work_queue_time;
    double lat_s_rem_msg_queue_time;
    double lat_s_rem_cc_block_time;
    double lat_s_rem_cc_time;
    double lat_s_rem_process_time;

    // stats for anomaly
    uint64_t ano_2_trans_write_skew_1;
    uint64_t ano_2_trans_write_skew_2;
    uint64_t ano_3_trans_write_skew_1;
    uint64_t ano_3_trans_write_skew_2;
    uint64_t ano_2_trans_read_skew;
    uint64_t ano_3_trans_read_skew_1;
    uint64_t ano_3_trans_read_skew_2;
    uint64_t ano_4_trans_read_skew;
    uint64_t ano_unknown;

    double * mtx;

    char _pad[CL_SIZE];
};

class Stats {
public:
    // PER THREAD statistics
    Stats_thd ** _stats;
    Stats_thd * totals;

    void init(uint64_t thread_cnt);
    void clear(uint64_t tid);
    //void add_lat(uint64_t thd_id, uint64_t latency);
    void commit(uint64_t thd_id);
    void abort(uint64_t thd_id);
    void print_client(bool prog);
    void print(bool prog);
    void print_cnts(FILE * outf);
    void print_lat_distr();
    void print_lat_distr(uint64_t min, uint64_t max);
    void print_abort_distr();
    uint64_t get_txn_cnts();
    void util_init();
    void print_util();
    int parseLine(char* line);
    void mem_util(FILE * outf);
    void cpu_util(FILE * outf);
    void print_prof(FILE * outf);

    clock_t lastCPU, lastSysCPU, lastUserCPU;
private:
    uint64_t thd_cnt;
};

#endif
