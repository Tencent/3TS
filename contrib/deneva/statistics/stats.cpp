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

#include "stats.h"

#include <sys/times.h>
#include <sys/vtimes.h>
#include <time.h>

#include "client_txn.h"
#include "global.h"
#include "helper.h"
#include "mem_alloc.h"
#include "stats_array.h"
#include "work_queue.h"

void Stats_thd::init(uint64_t thd_id) {
    DEBUG_M("Stats_thd::init part_cnt alloc\n");
    part_cnt = (uint64_t*) mem_allocator.align_alloc(sizeof(uint64_t)*g_part_cnt);
    DEBUG_M("Stats_thd::init part_acc alloc\n");
    part_acc = (uint64_t*) mem_allocator.align_alloc(sizeof(uint64_t)*g_part_cnt);
    DEBUG_M("Stats_thd::init worker_process_cnt_by_type alloc\n");
    worker_process_cnt_by_type= (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t) * NO_MSG);
    DEBUG_M("Stats_thd::init worker_process_time_by_type alloc\n");
    worker_process_time_by_type= (double *) mem_allocator.align_alloc(sizeof(double) * NO_MSG);

    DEBUG_M("Stats_thd::init work_queue_wq_cnt alloc\n");
    work_queue_wq_cnt= (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t) * SECOND);
    DEBUG_M("Stats_thd::init work_queue_tx_cnt alloc\n");
    work_queue_tx_cnt= (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t) * SECOND);

    DEBUG_M("Stats_thd::init work_queue_ewq_cnt alloc\n");
    work_queue_ewq_cnt= (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t) * SECOND);
    DEBUG_M("Stats_thd::init work_queue_dwq_cnt alloc\n");
    work_queue_dwq_cnt= (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t) * SECOND);

    DEBUG_M("Stats_thd::init work_queue_etx_cnt alloc\n");
    work_queue_etx_cnt= (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t) * SECOND);
    DEBUG_M("Stats_thd::init work_queue_dtx_cnt alloc\n");
    work_queue_dtx_cnt= (uint64_t *) mem_allocator.align_alloc(sizeof(uint64_t) * SECOND);

    DEBUG_M("Stats_thd::init mtx alloc\n");
    mtx= (double *) mem_allocator.align_alloc(sizeof(double) * 40);

    //all_lat.init(g_max_txn_per_part,ArrIncr);

    client_client_latency.init(g_max_txn_per_part,ArrIncr);
    last_start_commit_latency.init(g_max_txn_per_part,ArrIncr);
    first_start_commit_latency.init(g_max_txn_per_part,ArrIncr);
    start_abort_commit_latency.init(g_max_txn_per_part,ArrIncr);

    clear();

}

void Stats_thd::clear() {

    total_runtime=0;

    // Execution
    txn_cnt=0;
    remote_txn_cnt=0;
    local_txn_cnt=0;
    local_txn_start_cnt=0;
    total_txn_commit_cnt=0;
    local_txn_commit_cnt=0;
    remote_txn_commit_cnt=0;
    total_txn_abort_cnt=0;
    unique_txn_abort_cnt=0;
    local_txn_abort_cnt=0;
    remote_txn_abort_cnt=0;
    txn_run_time=0;
    multi_part_txn_cnt=0;
    multi_part_txn_run_time=0;
    single_part_txn_cnt=0;
    single_part_txn_run_time=0;
    txn_write_cnt=0;
    record_write_cnt=0;
    parts_touched=0;

    // Breakdown
    ts_alloc_time=0;
    abort_time=0;
    txn_manager_time=0;
    txn_index_time=0;
    txn_validate_time=0;
    txn_cleanup_time=0;

    trans_total_run_time=0;
    trans_process_time=0;
    trans_2pc_time=0;
    trans_prepare_time=0;
    trans_validate_time=0;
    trans_finish_time=0;
    trans_commit_time=0;
    trans_abort_time=0;

    trans_access_lock_wait_time=0;
    // trans mvcc
    trans_mvcc_clear_history=0;
    trans_mvcc_access=0;

    // Transaction stats
    txn_total_process_time=0;
    txn_process_time=0;
    txn_total_local_wait_time=0;
    txn_local_wait_time=0;
    txn_total_remote_wait_time=0;
    txn_remote_wait_time=0;
    txn_total_twopc_time=0;
    txn_twopc_time=0;

    // Client
    txn_sent_cnt=0;
    cl_send_intv=0;

    // Abort queue
    abort_queue_enqueue_cnt=0;
    abort_queue_dequeue_cnt=0;
    abort_queue_enqueue_time=0;
    abort_queue_dequeue_time=0;
    abort_queue_penalty=0;
    abort_queue_penalty_extra=0;

    // Work queue
    work_queue_wait_time=0;
    work_queue_cnt=0;
    work_queue_enq_cnt=0;
    work_queue_mtx_wait_time=0;
    work_queue_new_cnt=0;
    work_queue_new_wait_time=0;
    work_queue_old_cnt=0;
    work_queue_old_wait_time=0;
    work_queue_enqueue_time=0;
    work_queue_dequeue_time=0;
    work_queue_conflict_cnt=0;

    // Worker thread
    worker_idle_time=0;
    worker_activate_txn_time=0;
    worker_deactivate_txn_time=0;
    worker_release_msg_time=0;
    worker_process_time=0;
    worker_process_cnt=0;
    for(uint64_t i = 0; i < NO_MSG; i ++) {
        worker_process_cnt_by_type[i]=0;
        worker_process_time_by_type[i]=0;
    }

    for(uint64_t i = 0; i < SECOND; i ++) {
        work_queue_wq_cnt[i]=0;
        work_queue_tx_cnt[i]=0;
        work_queue_ewq_cnt[i]=0;
        work_queue_dwq_cnt[i]=0;
        work_queue_etx_cnt[i]=0;
        work_queue_dtx_cnt[i]=0;
    }
    // IO
    msg_queue_delay_time=0;
    msg_queue_cnt=0;
    msg_queue_enq_cnt=0;
    msg_send_time=0;
    msg_recv_time=0;
    msg_recv_idle_time=0;
    msg_batch_cnt=0;
    msg_batch_size_msgs=0;
    msg_batch_size_bytes=0;
    msg_batch_size_bytes_to_server=0;
    msg_batch_size_bytes_to_client=0;
    msg_send_cnt=0;
    msg_recv_cnt=0;
    msg_unpack_time=0;
    mbuf_send_intv_time=0;
    msg_copy_output_time=0;

    // Concurrency control, general
    cc_conflict_cnt=0;
    txn_wait_cnt=0;
    txn_conflict_cnt=0;

    // 2PL
    twopl_already_owned_cnt=0;
    twopl_owned_cnt=0;
    twopl_sh_owned_cnt=0;
    twopl_ex_owned_cnt=0;
    twopl_sh_bypass_cnt=0;
    twopl_owned_time=0;
    twopl_sh_owned_time=0;
    twopl_ex_owned_time=0;
    twopl_diff_time=0;
    twopl_wait_time=0;
    twopl_getlock_cnt=0;
    twopl_getlock_time=0;
    twopl_release_cnt=0;
    twopl_release_time=0;

    // Calvin
    seq_txn_cnt=0;
    seq_batch_cnt=0;
    seq_full_batch_cnt=0;
    seq_ack_time=0;
    seq_batch_time=0;
    seq_process_cnt=0;
    seq_complete_cnt=0;
    seq_process_time=0;
    seq_prep_time=0;
    seq_idle_time=0;
    seq_queue_wait_time=0;
    seq_queue_cnt=0;
    seq_queue_enq_cnt=0;
    seq_queue_enqueue_time=0;
    seq_queue_dequeue_time=0;
    sched_queue_wait_time=0;
    sched_queue_cnt=0;
    sched_queue_enq_cnt=0;
    sched_queue_enqueue_time=0;
    sched_queue_dequeue_time=0;
    calvin_sched_time=0;
    sched_idle_time=0;
    sched_txn_table_time=0;
    sched_epoch_cnt=0;
    sched_epoch_diff=0;

    // DLI_MVCC_OCC
    dli_mvcc_occ_validate_time = 0;
    dli_mvcc_occ_check_cnt = 0;
    dli_mvcc_occ_abort_check_cnt = 0;
    dli_mvcc_occ_ts_abort_cnt = 0;

    //OCC
    occ_validate_time=0;
    occ_cs_wait_time=0;
    occ_cs_time=0;
    occ_hist_validate_time=0;
    occ_act_validate_time=0;
    occ_hist_validate_fail_time=0;
    occ_act_validate_fail_time=0;
    occ_check_cnt=0;
    occ_abort_check_cnt=0;
    occ_ts_abort_cnt=0;
    occ_finish_time=0;

    // WSI
    wsi_validate_time=0;
    wsi_cs_wait_time=0;
    wsi_check_cnt=0;
    wsi_abort_check_cnt=0;

    // MAAT
    maat_validate_cnt=0;
    maat_validate_time=0;
    maat_cs_wait_time=0;
    maat_case1_cnt=0;
    maat_case2_cnt=0;
    maat_case3_cnt=0;
    maat_case4_cnt=0;
    maat_case5_cnt=0;
    maat_range=0;
    maat_commit_cnt=0;

    // Logging
    log_write_cnt=0;
    log_write_time=0;
    log_flush_cnt=0;
    log_flush_time=0;
    log_process_time=0;

    // Transaction Table
    txn_table_new_cnt=0;
    txn_table_get_cnt=0;
    txn_table_release_cnt=0;
    txn_table_cflt_cnt=0;
    txn_table_cflt_size=0;
    txn_table_get_time=0;
    txn_table_release_time=0;
    txn_table_min_ts_time=0;

    for(uint64_t i = 0; i < 40; i ++) {
        mtx[i]=0;
    }

    lat_work_queue_time=0;
    lat_msg_queue_time=0;
    lat_cc_block_time=0;
    lat_cc_time=0;
    lat_process_time=0;
    lat_abort_time=0;
    lat_network_time=0;
    lat_other_time=0;

    lat_l_loc_work_queue_time=0;
    lat_l_loc_msg_queue_time=0;
    lat_l_loc_cc_block_time=0;
    lat_l_loc_cc_time=0;
    lat_l_loc_process_time=0;
    lat_l_loc_abort_time=0;

    lat_short_work_queue_time=0;
    lat_short_msg_queue_time=0;
    lat_short_cc_block_time=0;
    lat_short_cc_time=0;
    lat_short_process_time=0;
    lat_short_network_time=0;
    lat_short_batch_time=0;

    lat_s_loc_work_queue_time=0;
    lat_s_loc_msg_queue_time=0;
    lat_s_loc_cc_block_time=0;
    lat_s_loc_cc_time=0;
    lat_s_loc_process_time=0;

    lat_l_rem_work_queue_time=0;
    lat_l_rem_msg_queue_time=0;
    lat_l_rem_cc_block_time=0;
    lat_l_rem_cc_time=0;
    lat_l_rem_process_time=0;

    lat_s_rem_work_queue_time=0;
    lat_s_rem_msg_queue_time=0;
    lat_s_rem_cc_block_time=0;
    lat_s_rem_cc_time=0;
    lat_s_rem_process_time=0;

    ano_2_trans_write_skew_1 = 0;
    ano_2_trans_write_skew_2 = 0;
    ano_3_trans_write_skew_1 = 0;
    ano_3_trans_write_skew_2 = 0;
    ano_2_trans_read_skew = 0;
    ano_3_trans_read_skew_1 = 0;
    ano_3_trans_read_skew_2 = 0;
    ano_4_trans_read_skew = 0;
    ano_unknown = 0;

    client_client_latency.clear();
    last_start_commit_latency.clear();
    first_start_commit_latency.clear();
    start_abort_commit_latency.clear();
}

void Stats_thd::print_client(FILE * outf, bool prog) {
    double txn_run_avg_time = 0;
    double tput = 0;
    if (txn_cnt > 0) txn_run_avg_time = txn_run_time / txn_cnt;
    if (total_runtime > 0) tput = txn_cnt / (total_runtime / BILLION);
    fprintf(outf,
        "total_runtime=%f"
        ",tput=%.0f" // remove the decimal places
        ",txn_cnt=%ld"
        ",txn_sent_cnt=%ld"
        ",txn_run_time=%f"
        ",txn_run_avg_time=%f"
            ",cl_send_intv=%f",
            total_runtime / BILLION, tput, txn_cnt, txn_sent_cnt, txn_run_time / BILLION,
            txn_run_avg_time / BILLION, cl_send_intv / BILLION);
    // IO
    double mbuf_send_intv_time_avg = 0;
    double msg_unpack_time_avg = 0;
    double msg_send_time_avg = 0;
    double msg_recv_time_avg = 0;
    double msg_batch_size_msgs_avg = 0;
    double msg_batch_size_bytes_avg = 0;
    double msg_queue_delay_time_avg = 0;
    if (msg_queue_cnt > 0) msg_queue_delay_time_avg = msg_queue_delay_time / msg_queue_cnt;
    if(msg_batch_cnt > 0) {
        mbuf_send_intv_time_avg = mbuf_send_intv_time / msg_batch_cnt;
        msg_batch_size_msgs_avg = msg_batch_size_msgs / msg_batch_cnt;
        msg_batch_size_bytes_avg = msg_batch_size_bytes / msg_batch_cnt;
    }
    if(msg_recv_cnt > 0) {
        msg_recv_time_avg = msg_recv_time / msg_recv_cnt;
        msg_unpack_time_avg = msg_unpack_time / msg_recv_cnt;
    }
    if(msg_send_cnt > 0) {
        msg_send_time_avg = msg_send_time / msg_send_cnt;
    }
    fprintf(outf,
    ",msg_queue_delay_time=%f"
    ",msg_queue_cnt=%ld"
    ",msg_queue_enq_cnt=%ld"
    ",msg_queue_delay_time_avg=%f"
    ",msg_send_time=%f"
    ",msg_send_time_avg=%f"
    ",msg_recv_time=%f"
    ",msg_recv_time_avg=%f"
    ",msg_recv_idle_time=%f"
    ",msg_batch_cnt=%ld"
    ",msg_batch_size_msgs=%ld"
    ",msg_batch_size_msgs_avg=%f"
    ",msg_batch_size_bytes=%ld"
    ",msg_batch_size_bytes_avg=%f"
    ",msg_batch_size_bytes_to_server=%ld"
    ",msg_batch_size_bytes_to_client=%ld"
    ",msg_send_cnt=%ld"
    ",msg_recv_cnt=%ld"
    ",msg_unpack_time=%f"
    ",msg_unpack_time_avg=%f"
    ",mbuf_send_intv_time=%f"
    ",mbuf_send_intv_time_avg=%f"
        ",msg_copy_output_time=%f",
        msg_queue_delay_time / BILLION, msg_queue_cnt, msg_queue_enq_cnt,
        msg_queue_delay_time_avg / BILLION, msg_send_time / BILLION, msg_send_time_avg / BILLION,
        msg_recv_time / BILLION, msg_recv_time_avg / BILLION, msg_recv_idle_time / BILLION,
        msg_batch_cnt, msg_batch_size_msgs, msg_batch_size_msgs_avg, msg_batch_size_bytes,
        msg_batch_size_bytes_avg, msg_batch_size_bytes_to_server, msg_batch_size_bytes_to_client,
        msg_send_cnt, msg_recv_cnt, msg_unpack_time / BILLION, msg_unpack_time_avg / BILLION,
        mbuf_send_intv_time / BILLION, mbuf_send_intv_time_avg / BILLION,
        msg_copy_output_time / BILLION);

    if (!prog) {
    client_client_latency.quicksort(0,client_client_latency.cnt-1);
    fprintf(outf,
            ",ccl0=%f"
            ",ccl1=%f"
            ",ccl10=%f"
            ",ccl25=%f"
            ",ccl50=%f"
            ",ccl75=%f"
            ",ccl90=%f"
            ",ccl95=%f"
            ",ccl96=%f"
            ",ccl97=%f"
            ",ccl98=%f"
            ",ccl99=%f"
            ",ccl100=%f",
            (double)client_client_latency.get_idx(0) / BILLION,
            (double)client_client_latency.get_percentile(1) / BILLION,
            (double)client_client_latency.get_percentile(10) / BILLION,
            (double)client_client_latency.get_percentile(25) / BILLION,
            (double)client_client_latency.get_percentile(50) / BILLION,
            (double)client_client_latency.get_percentile(75) / BILLION,
            (double)client_client_latency.get_percentile(90) / BILLION,
            (double)client_client_latency.get_percentile(95) / BILLION,
            (double)client_client_latency.get_percentile(96) / BILLION,
            (double)client_client_latency.get_percentile(97) / BILLION,
            (double)client_client_latency.get_percentile(98) / BILLION,
            (double)client_client_latency.get_percentile(99) / BILLION,
            (double)client_client_latency.get_idx(client_client_latency.cnt - 1) / BILLION);
    }

}

void Stats_thd::print(FILE * outf, bool prog) {
    fprintf(outf, "total_runtime=%f", total_runtime / BILLION);
    // Execution
    double tput = 0;
    double txn_run_avg_time = 0;
    double multi_part_txn_avg_time = 0;
    double single_part_txn_avg_time = 0;
    double avg_parts_touched = 0;
    if (total_runtime > 0) tput = txn_cnt / (total_runtime / BILLION);
    if(txn_cnt > 0) {
        txn_run_avg_time = txn_run_time / txn_cnt;
        avg_parts_touched = ((double)parts_touched) / txn_cnt;
    }
    if(multi_part_txn_cnt > 0)
        multi_part_txn_avg_time = multi_part_txn_run_time / multi_part_txn_cnt;
    if(single_part_txn_cnt > 0)
        single_part_txn_avg_time = single_part_txn_run_time / single_part_txn_cnt;
    fprintf(outf,
    ",tput=%.0f"  // remove the decimal places
    ",txn_cnt=%ld"
    ",remote_txn_cnt=%ld"
    ",local_txn_cnt=%ld"
    ",local_txn_start_cnt=%ld"
    ",total_txn_commit_cnt=%ld"
    ",local_txn_commit_cnt=%ld"
    ",remote_txn_commit_cnt=%ld"
    ",total_txn_abort_cnt=%ld"
            ",positive_txn_abort_cnt=%ld"
    ",unique_txn_abort_cnt=%ld"
    ",local_txn_abort_cnt=%ld"
    ",remote_txn_abort_cnt=%ld"
    ",txn_run_time=%f"
    ",txn_run_avg_time=%f"
    ",multi_part_txn_cnt=%ld"
    ",multi_part_txn_run_time=%f"
    ",multi_part_txn_avg_time=%f"
    ",single_part_txn_cnt=%ld"
    ",single_part_txn_run_time=%f"
    ",single_part_txn_avg_time=%f"
    ",txn_write_cnt=%ld"
    ",record_write_cnt=%ld"
    ",parts_touched=%ld"
          ",avg_parts_touched=%f",
          tput, txn_cnt, remote_txn_cnt, local_txn_cnt, local_txn_start_cnt, total_txn_commit_cnt,
          local_txn_commit_cnt, remote_txn_commit_cnt, total_txn_abort_cnt,positive_txn_abort_cnt, unique_txn_abort_cnt,
          local_txn_abort_cnt, remote_txn_abort_cnt, txn_run_time / BILLION,
          txn_run_avg_time / BILLION, multi_part_txn_cnt, multi_part_txn_run_time / BILLION,
          multi_part_txn_avg_time / BILLION, single_part_txn_cnt,
          single_part_txn_run_time / BILLION, single_part_txn_avg_time / BILLION, txn_write_cnt,
          record_write_cnt, parts_touched, avg_parts_touched);

    // Breakdown
    fprintf(outf,
    ",ts_alloc_time=%f"
    ",abort_time=%f"
    ",txn_manager_time=%f"
    ",txn_index_time=%f"
    ",txn_validate_time=%f"
        ",txn_cleanup_time=%f",
        ts_alloc_time / BILLION, abort_time / BILLION, txn_manager_time / BILLION,
        txn_index_time / BILLION, txn_validate_time / BILLION, txn_cleanup_time / BILLION);
    // trans
    fprintf(outf,
    ",trans_total_run_time=%f"
    ",trans_process_time=%f"
    ",trans_2pc_time=%f"
    ",trans_prepare_time=%f"
    ",trans_validate_time=%f"
    ",trans_finish_time=%f"
    ",trans_commit_time=%f"
    ",trans_abort_time=%f"
    ",trans_access_lock_wait_time=%f"
    ",total_access_time=%f"
    ",trans_access_read_time=%f"
    ",trans_access_pre_time=%f"
    ",trans_access_write_time=%f"
    ",trans_access_xp_time=%f"
    ",trans_access_clear_time=%f"
    ",trans_access_write_insert_time=%f"
    ",trans_access_write_release_time=%f"
    ",trans_access_pre_check_time=%f"
    ",trans_access_pre_before_time=%f"
    ",trans_access_pre_lock_time=%f"
    ",trans_access_write_update_time=%f"
    ",trans_mvcc_clear_history=%f"
    ",trans_mvcc_access=%f",
        trans_total_run_time / BILLION, trans_process_time / BILLION, trans_2pc_time / BILLION,
        trans_prepare_time / BILLION, trans_validate_time / BILLION, trans_finish_time / BILLION,
        trans_commit_time / BILLION, trans_abort_time / BILLION, trans_access_lock_wait_time / BILLION,
        total_access_time / BILLION, trans_access_read_time / BILLION,
        trans_access_pre_time / BILLION, trans_access_write_time / BILLION,
        trans_access_xp_time / BILLION, trans_access_clear_time / BILLION,
        trans_access_write_insert_time / BILLION, trans_access_write_release_time / BILLION,
        trans_access_pre_check_time / BILLION, trans_access_pre_before_time / BILLION,
        trans_access_pre_lock_time / BILLION, trans_access_write_update_time / BILLION,
        trans_mvcc_clear_history / BILLION, trans_mvcc_access / BILLION);


  // Transaction stats
    double txn_total_process_time_avg=0;
    double txn_process_time_avg=0;
    double txn_total_local_wait_time_avg=0;
    double txn_local_wait_time_avg=0;
    double txn_total_remote_wait_time_avg=0;
    double txn_remote_wait_time_avg=0;
    double txn_total_twopc_time_avg=0;
    double txn_twopc_time_avg=0;
    if(local_txn_commit_cnt > 0) {
        txn_total_process_time_avg = txn_total_process_time / local_txn_commit_cnt;
        txn_process_time_avg = txn_process_time / local_txn_commit_cnt;
        txn_total_local_wait_time_avg = txn_total_local_wait_time / local_txn_commit_cnt;
        txn_local_wait_time_avg = txn_local_wait_time / local_txn_commit_cnt;
        txn_total_remote_wait_time_avg = txn_total_remote_wait_time / local_txn_commit_cnt;
        txn_remote_wait_time_avg = txn_remote_wait_time / local_txn_commit_cnt;
        txn_total_twopc_time_avg = txn_total_twopc_time / local_txn_commit_cnt;
        txn_twopc_time_avg = txn_twopc_time / local_txn_commit_cnt;
    }
    fprintf(outf,
        ",txn_total_process_time=%f"
        ",txn_process_time=%f"
        ",txn_total_local_wait_time=%f"
        ",txn_local_wait_time=%f"
        ",txn_total_remote_wait_time=%f"
        ",txn_remote_wait_time=%f"
        ",txn_total_twopc_time=%f"
        ",txn_twopc_time=%f"
        ",txn_total_process_time_avg=%f"
        ",txn_process_time_avg=%f"
        ",txn_total_local_wait_time_avg=%f"
        ",txn_local_wait_time_avg=%f"
        ",txn_total_remote_wait_time_avg=%f"
        ",txn_remote_wait_time_avg=%f"
        ",txn_total_twopc_time_avg=%f"
        ",txn_twopc_time_avg=%f",
        txn_total_process_time / BILLION, txn_process_time / BILLION,
        txn_total_local_wait_time / BILLION, txn_local_wait_time / BILLION,
        txn_total_remote_wait_time / BILLION, txn_remote_wait_time / BILLION,
        txn_total_twopc_time / BILLION, txn_twopc_time / BILLION,
        txn_total_process_time_avg / BILLION, txn_process_time_avg / BILLION,
        txn_total_local_wait_time_avg / BILLION, txn_local_wait_time_avg / BILLION,
        txn_total_remote_wait_time_avg / BILLION, txn_remote_wait_time_avg / BILLION,
        txn_total_twopc_time_avg / BILLION, txn_twopc_time_avg / BILLION);

    // Abort queue
    double abort_queue_penalty_avg = 0;
    double abort_queue_penalty_extra_avg = 0;
    if(abort_queue_enqueue_cnt > 0)
        abort_queue_penalty_extra_avg = abort_queue_penalty_extra / abort_queue_enqueue_cnt;
    if(abort_queue_dequeue_cnt > 0)
        abort_queue_penalty_avg = abort_queue_penalty / abort_queue_enqueue_cnt;
    fprintf(outf,
        ",abort_queue_enqueue_cnt=%ld"
        ",abort_queue_dequeue_cnt=%ld"
        ",abort_queue_enqueue_time=%f"
        ",abort_queue_dequeue_time=%f"
        ",abort_queue_penalty=%f"
        ",abort_queue_penalty_extra=%f"
        ",abort_queue_penalty_avg=%f"
        ",abort_queue_penalty_extra_avg=%f"
        // Abort queue
        ,
        abort_queue_enqueue_cnt, abort_queue_dequeue_cnt, abort_queue_enqueue_time / BILLION,
        abort_queue_dequeue_time / BILLION, abort_queue_penalty / BILLION,
        abort_queue_penalty_extra / BILLION, abort_queue_penalty_avg / BILLION,
        abort_queue_penalty_extra_avg / BILLION);

    double work_queue_wait_avg_time = 0;
    double work_queue_mtx_wait_avg = 0;
    double work_queue_new_wait_avg_time = 0;
    double work_queue_old_wait_avg_time = 0;
    if(work_queue_cnt > 0) {
        work_queue_wait_avg_time = work_queue_wait_time / work_queue_cnt;
        work_queue_mtx_wait_avg = work_queue_mtx_wait_time / work_queue_cnt;
    }
    if(work_queue_new_cnt > 0)
        work_queue_new_wait_avg_time = work_queue_new_wait_time / work_queue_new_cnt;
    if(work_queue_old_cnt > 0)
        work_queue_old_wait_avg_time = work_queue_old_wait_time / work_queue_old_cnt;
    // Work queue
    fprintf(outf,
        ",work_queue_wait_time=%f"
        ",work_queue_cnt=%ld"
        ",work_queue_enq_cnt=%ld"
        ",work_queue_wait_avg_time=%f"
        ",work_queue_mtx_wait_time=%f"
        ",work_queue_mtx_wait_avg=%f"
        ",work_queue_new_cnt=%ld"
        ",work_queue_new_wait_time=%f"
        ",work_queue_new_wait_avg_time=%f"
        ",work_queue_old_cnt=%ld"
        ",work_queue_old_wait_time=%f"
        ",work_queue_old_wait_avg_time=%f"
        ",work_queue_enqueue_time=%f"
        ",work_queue_dequeue_time=%f"
        ",work_queue_conflict_cnt=%ld",
        work_queue_wait_time / BILLION, work_queue_cnt, work_queue_enq_cnt,
        work_queue_wait_avg_time / BILLION, work_queue_mtx_wait_time / BILLION,
        work_queue_mtx_wait_avg / BILLION, work_queue_new_cnt, work_queue_new_wait_time / BILLION,
        work_queue_new_wait_avg_time / BILLION, work_queue_old_cnt,
        work_queue_old_wait_time / BILLION, work_queue_old_wait_avg_time / BILLION,
        work_queue_enqueue_time / BILLION, work_queue_dequeue_time / BILLION,
        work_queue_conflict_cnt);

    // Worker thread
    double worker_process_avg_time = 0;
    if (worker_process_cnt > 0) worker_process_avg_time = worker_process_time / worker_process_cnt;
    fprintf(outf,
        ",worker_idle_time=%f"
        ",worker_activate_txn_time=%f"
        ",worker_deactivate_txn_time=%f"
        ",worker_release_msg_time=%f"
        ",worker_process_time=%f"
        ",worker_process_cnt=%ld"
        ",worker_process_avg_time=%f",
        worker_idle_time / BILLION, worker_activate_txn_time / BILLION,
        worker_deactivate_txn_time / BILLION, worker_release_msg_time / BILLION,
        worker_process_time / BILLION, worker_process_cnt, worker_process_avg_time / BILLION);
    for(uint64_t i = 0; i < NO_MSG; i ++) {
        fprintf(outf,
        ",proc_cnt_type%ld=%ld"
                ",proc_time_type%ld=%f",
                i, worker_process_cnt_by_type[i], i, worker_process_time_by_type[i] / BILLION);
    }

    for(uint64_t i = 0; i < SECOND; i ++) {
        fprintf(outf,
        ",work_queue_wq_cnt%ld=%ld"
        ",work_queue_tx_cnt%ld=%ld"
        ,i
        ,work_queue_wq_cnt[i]
        ,i
        ,work_queue_tx_cnt[i]
        );
    }

    for(uint64_t i = 0; i < SECOND; i ++) {
        fprintf(outf,
        ",work_queue_ewq_cnt%ld=%ld"
        ",work_queue_dwq_cnt%ld=%ld"
        ,i
        ,work_queue_ewq_cnt[i]
        ,i
        ,work_queue_dwq_cnt[i]
        );
    }

    for(uint64_t i = 0; i < SECOND; i ++) {
        fprintf(outf,
        ",work_queue_etx_cnt%ld=%ld"
        ",work_queue_dtx_cnt%ld=%ld"
        ,i
        ,work_queue_etx_cnt[i]
        ,i
        ,work_queue_dtx_cnt[i]
        );
    }

    // IO
    double mbuf_send_intv_time_avg = 0;
    double msg_unpack_time_avg = 0;
    double msg_send_time_avg = 0;
    double msg_recv_time_avg = 0;
    double msg_batch_size_msgs_avg = 0;
    double msg_batch_size_bytes_avg = 0;
    double msg_queue_delay_time_avg = 0;
    if (msg_queue_cnt > 0) msg_queue_delay_time_avg = msg_queue_delay_time / msg_queue_cnt;
    if(msg_batch_cnt > 0) {
        mbuf_send_intv_time_avg = mbuf_send_intv_time / msg_batch_cnt;
        msg_batch_size_msgs_avg = msg_batch_size_msgs / msg_batch_cnt;
        msg_batch_size_bytes_avg = msg_batch_size_bytes / msg_batch_cnt;
    }
    if(msg_recv_cnt > 0) {
        msg_recv_time_avg = msg_recv_time / msg_recv_cnt;
        msg_unpack_time_avg = msg_unpack_time / msg_recv_cnt;
    }
    if(msg_send_cnt > 0) {
        msg_send_time_avg = msg_send_time / msg_send_cnt;
    }
    fprintf(outf,
        ",msg_queue_delay_time=%f"
        ",msg_queue_cnt=%ld"
        ",msg_queue_enq_cnt=%ld"
        ",msg_queue_delay_time_avg=%f"
        ",msg_send_time=%f"
        ",msg_send_time_avg=%f"
        ",msg_recv_time=%f"
        ",msg_recv_time_avg=%f"
        ",msg_recv_idle_time=%f"
        ",msg_batch_cnt=%ld"
        ",msg_batch_size_msgs=%ld"
        ",msg_batch_size_msgs_avg=%f"
        ",msg_batch_size_bytes=%ld"
        ",msg_batch_size_bytes_avg=%f"
        ",msg_batch_size_bytes_to_server=%ld"
        ",msg_batch_size_bytes_to_client=%ld"
        ",msg_send_cnt=%ld"
        ",msg_recv_cnt=%ld"
        ",msg_unpack_time=%f"
        ",msg_unpack_time_avg=%f"
        ",mbuf_send_intv_time=%f"
        ",mbuf_send_intv_time_avg=%f"
        ",msg_copy_output_time=%f",
        msg_queue_delay_time / BILLION, msg_queue_cnt, msg_queue_enq_cnt,
        msg_queue_delay_time_avg / BILLION, msg_send_time / BILLION, msg_send_time_avg / BILLION,
        msg_recv_time / BILLION, msg_recv_time_avg / BILLION, msg_recv_idle_time / BILLION,
        msg_batch_cnt, msg_batch_size_msgs, msg_batch_size_msgs_avg, msg_batch_size_bytes,
        msg_batch_size_bytes_avg, msg_batch_size_bytes_to_server, msg_batch_size_bytes_to_client,
        msg_send_cnt, msg_recv_cnt, msg_unpack_time / BILLION, msg_unpack_time_avg / BILLION,
        mbuf_send_intv_time / BILLION, mbuf_send_intv_time_avg / BILLION,
        msg_copy_output_time / BILLION);

  // Concurrency control, general
    fprintf(outf,
        ",cc_conflict_cnt=%ld"
        ",txn_wait_cnt=%ld"
        ",txn_conflict_cnt=%ld",
        cc_conflict_cnt, txn_wait_cnt, txn_conflict_cnt);

  // 2PL
    double twopl_sh_owned_avg_time = 0;
    if (twopl_sh_owned_cnt > 0) twopl_sh_owned_avg_time = twopl_sh_owned_time / twopl_sh_owned_cnt;
    double twopl_ex_owned_avg_time = 0;
    if (twopl_ex_owned_cnt > 0) twopl_ex_owned_avg_time = twopl_ex_owned_time / twopl_ex_owned_cnt;
    fprintf(outf,
        ",twopl_already_owned_cnt=%ld"
        ",twopl_owned_cnt=%ld"
        ",twopl_sh_owned_cnt=%ld"
        ",twopl_ex_owned_cnt=%ld"
        ",twopl_sh_bypass_cnt=%ld"
        ",twopl_owned_time=%f"
        ",twopl_sh_owned_time=%f"
        ",twopl_ex_owned_time=%f"
        ",twopl_sh_owned_avg_time=%f"
        ",twopl_ex_owned_avg_time=%f"
        ",twopl_diff_time=%f"
        ",twopl_wait_time=%f"
        ",twopl_getlock_cnt=%ld"
        ",twopl_getlock_time=%f"
        ",twopl_release_cnt=%ld"
          ",twopl_release_time=%f",
          twopl_already_owned_cnt, twopl_owned_cnt, twopl_sh_owned_cnt, twopl_ex_owned_cnt,
          twopl_sh_bypass_cnt, twopl_owned_time / BILLION, twopl_sh_owned_time / BILLION,
          twopl_ex_owned_time / BILLION, twopl_sh_owned_avg_time / BILLION,
          twopl_ex_owned_avg_time / BILLION, twopl_diff_time / BILLION, twopl_wait_time / BILLION,
          twopl_getlock_cnt, twopl_getlock_time / BILLION, twopl_release_cnt,
          twopl_release_time / BILLION);

    // Calvin
    double seq_queue_wait_avg_time = 0;
    if (seq_queue_cnt > 0) seq_queue_wait_avg_time = seq_queue_wait_time / seq_queue_cnt;
    double sched_queue_wait_avg_time = 0;
    if (sched_queue_cnt > 0) sched_queue_wait_avg_time = sched_queue_wait_time / sched_queue_cnt;
    fprintf(outf,
        ",seq_txn_cnt=%ld"
        ",seq_batch_cnt=%ld"
        ",seq_full_batch_cnt=%ld"
        ",seq_ack_time=%f"
        ",seq_batch_time=%f"
        ",seq_process_cnt=%ld"
        ",seq_complete_cnt=%ld"
        ",seq_process_time=%f"
        ",seq_prep_time=%f"
        ",seq_idle_time=%f"
        ",seq_queue_wait_time=%f"
        ",seq_queue_cnt=%ld"
        ",seq_queue_enq_cnt=%ld"
        ",seq_queue_wait_avg_time=%f"
        ",seq_queue_enqueue_time=%f"
        ",seq_queue_dequeue_time=%f"
        ",sched_queue_wait_time=%f"
        ",sched_queue_cnt=%ld"
        ",sched_queue_enq_cnt=%ld"
        ",sched_queue_wait_avg_time=%f"
        ",sched_queue_enqueue_time=%f"
        ",sched_queue_dequeue_time=%f"
        ",calvin_sched_time=%f"
        ",sched_idle_time=%f"
        ",sched_txn_table_time=%f"
        ",sched_epoch_cnt=%ld"
        ",sched_epoch_diff=%f",
        seq_txn_cnt, seq_batch_cnt, seq_full_batch_cnt, seq_ack_time / BILLION,
        seq_batch_time / BILLION, seq_process_cnt, seq_complete_cnt, seq_process_time / BILLION,
        seq_prep_time / BILLION, seq_idle_time / BILLION, seq_queue_wait_time / BILLION,
        seq_queue_cnt, seq_queue_enq_cnt, seq_queue_wait_avg_time / BILLION,
        seq_queue_enqueue_time / BILLION, seq_queue_dequeue_time / BILLION,
        sched_queue_wait_time / BILLION, sched_queue_cnt, sched_queue_enq_cnt,
        sched_queue_wait_avg_time / BILLION, sched_queue_enqueue_time / BILLION,
        sched_queue_dequeue_time / BILLION, calvin_sched_time / BILLION,
        sched_idle_time / BILLION, sched_txn_table_time / BILLION, sched_epoch_cnt,
        sched_epoch_diff / BILLION);

    //OCC
    fprintf(outf,
        ",occ_validate_time=%f"
        ",occ_cs_wait_time=%f"
        ",occ_cs_time=%f"
        ",occ_hist_validate_time=%f"
        ",occ_act_validate_time=%f"
        ",occ_hist_validate_fail_time=%f"
        ",occ_act_validate_fail_time=%f"
        ",occ_check_cnt=%ld"
        ",occ_abort_check_cnt=%ld"
        ",occ_ts_abort_cnt=%ld"
        ",occ_finish_time=%f",
        occ_validate_time / BILLION, occ_cs_wait_time / BILLION, occ_cs_time / BILLION,
        occ_hist_validate_time / BILLION, occ_act_validate_time / BILLION,
        occ_hist_validate_fail_time / BILLION, occ_act_validate_fail_time / BILLION,
        occ_check_cnt, occ_abort_check_cnt, occ_ts_abort_cnt, occ_finish_time / BILLION);

  //MAAT
    double maat_range_avg = 0;
    double maat_validate_avg = 0;
    double maat_cs_wait_avg = 0;
    uint64_t maat_commit_avg = 0;
    if (maat_commit_cnt > 0) maat_range_avg = maat_range / maat_commit_cnt;
    if(maat_validate_cnt > 0) {
        maat_validate_avg = maat_validate_time / maat_validate_cnt;
        maat_cs_wait_avg = maat_cs_wait_time / maat_validate_cnt;
        maat_commit_avg = maat_commit_cnt / maat_validate_cnt;
    }
    fprintf(outf,
        ",maat_validate_cnt=%ld"
        ",maat_validate_time=%f"
        ",maat_validate_avg=%f"
        ",maat_cs_wait_time=%f"
        ",maat_cs_wait_avg=%f"
        ",maat_case1_cnt=%ld"
        ",maat_case2_cnt=%ld"
        ",maat_case3_cnt=%ld"
        ",maat_case4_cnt=%ld"
        ",maat_case5_cnt=%ld"
        ",maat_range=%f"
        ",maat_commit_cnt=%ld"
        ",maat_commit_avg=%ld"
        ",maat_range_avg=%f",
        maat_validate_cnt, maat_validate_time / BILLION, maat_validate_avg / BILLION,
        maat_cs_wait_time / BILLION, maat_cs_wait_avg / BILLION, maat_case1_cnt, maat_case2_cnt,
        maat_case3_cnt, maat_case4_cnt, maat_case5_cnt, maat_range / BILLION, maat_commit_cnt,
        maat_commit_avg, maat_range_avg);


  // Logging
    double log_write_avg_time = 0;
    if (log_write_cnt > 0) log_write_avg_time = log_write_time / log_write_cnt;
    double log_flush_avg_time = 0;
    if (log_flush_cnt > 0) log_flush_avg_time = log_flush_time / log_flush_cnt;
    fprintf(outf,
        ",log_write_cnt=%ld"
        ",log_write_time=%f"
        ",log_write_avg_time=%f"
        ",log_flush_cnt=%ld"
        ",log_flush_time=%f"
        ",log_flush_avg_time=%f"
        ",log_process_time=%f",
        log_write_cnt, log_write_time / BILLION, log_write_avg_time / BILLION, log_flush_cnt,
        log_flush_time / BILLION, log_flush_avg_time / BILLION, log_process_time / BILLION);

  // Transaction Table
    double txn_table_get_avg_time = 0;
    if (txn_table_get_cnt > 0) txn_table_get_avg_time = txn_table_get_time / txn_table_get_cnt;
    double txn_table_release_avg_time = 0;
    if(txn_table_release_cnt > 0)
        txn_table_release_avg_time = txn_table_release_time / txn_table_release_cnt;
    fprintf(outf,
        ",txn_table_new_cnt=%ld"
        ",txn_table_get_cnt=%ld"
        ",txn_table_release_cnt=%ld"
        ",txn_table_cflt_cnt=%ld"
        ",txn_table_cflt_size=%ld"
        ",txn_table_get_time=%f"
        ",txn_table_release_time=%f"
        ",txn_table_min_ts_time=%f"
        ",txn_table_get_avg_time=%f"
        ",txn_table_release_avg_time=%f"
        // Transaction Table
        ,
        txn_table_new_cnt, txn_table_get_cnt, txn_table_release_cnt, txn_table_cflt_cnt,
        txn_table_cflt_size, txn_table_get_time / BILLION, txn_table_release_time / BILLION,
        txn_table_min_ts_time / BILLION, txn_table_get_avg_time / BILLION,
        txn_table_release_avg_time / BILLION);

    for(uint64_t i = 0; i < 40; i ++) {
        fprintf(outf, ",mtx%ld=%f", i, mtx[i] / BILLION);
    }
    fprintf(outf,
        ",lat_work_queue_time=%f"
        ",lat_msg_queue_time=%f"
        ",lat_cc_block_time=%f"
        ",lat_cc_time=%f"
        ",lat_process_time=%f"
        ",lat_abort_time=%f"
        ",lat_network_time=%f"
        ",lat_other_time=%f"

        ",lat_l_loc_work_queue_time=%f"
        ",lat_l_loc_msg_queue_time=%f"
        ",lat_l_loc_cc_block_time=%f"
        ",lat_l_loc_cc_time=%f"
        ",lat_l_loc_process_time=%f"
        ",lat_l_loc_abort_time=%f"

        ",lat_short_work_queue_time=%f"
        ",lat_short_msg_queue_time=%f"
        ",lat_short_cc_block_time=%f"
        ",lat_short_cc_time=%f"
        ",lat_short_process_time=%f"
        ",lat_short_network_time=%f"
        ",lat_short_batch_time=%f"

        ",lat_s_loc_work_queue_time=%f"
        ",lat_s_loc_msg_queue_time=%f"
        ",lat_s_loc_cc_block_time=%f"
        ",lat_s_loc_cc_time=%f"
        ",lat_s_loc_process_time=%f"

        ",lat_l_rem_work_queue_time=%f"
        ",lat_l_rem_msg_queue_time=%f"
        ",lat_l_rem_cc_block_time=%f"
        ",lat_l_rem_cc_time=%f"
        ",lat_l_rem_process_time=%f"
        ",lat_s_rem_work_queue_time=%f"
        ",lat_s_rem_msg_queue_time=%f"
        ",lat_s_rem_cc_block_time=%f"
        ",lat_s_rem_cc_time=%f"
        ",lat_s_rem_process_time=%f",
        lat_work_queue_time / BILLION, lat_msg_queue_time / BILLION, lat_cc_block_time / BILLION,
        lat_cc_time / BILLION, lat_process_time / BILLION, lat_abort_time / BILLION,
        lat_network_time / BILLION, lat_other_time / BILLION, lat_l_loc_work_queue_time / BILLION,
        lat_l_loc_msg_queue_time / BILLION, lat_l_loc_cc_block_time / BILLION,
        lat_l_loc_cc_time / BILLION, lat_l_loc_process_time / BILLION,
        lat_l_loc_abort_time / BILLION, lat_short_work_queue_time / BILLION,
        lat_short_msg_queue_time / BILLION, lat_short_cc_block_time / BILLION,
        lat_short_cc_time / BILLION, lat_short_process_time / BILLION,
        lat_short_network_time / BILLION, lat_short_batch_time / BILLION,
        lat_s_loc_work_queue_time / BILLION, lat_s_loc_msg_queue_time / BILLION,
        lat_s_loc_cc_block_time / BILLION, lat_s_loc_cc_time / BILLION,
        lat_s_loc_process_time / BILLION, lat_l_rem_work_queue_time / BILLION,
        lat_l_rem_msg_queue_time / BILLION, lat_l_rem_cc_block_time / BILLION,
        lat_l_rem_cc_time / BILLION, lat_l_rem_process_time / BILLION,
        lat_s_rem_work_queue_time / BILLION, lat_s_rem_msg_queue_time / BILLION,
        lat_s_rem_cc_block_time / BILLION, lat_s_rem_cc_time / BILLION,
        lat_s_rem_process_time / BILLION);
    fprintf(outf,
        ",ano_2_trans_write_skew_1=%ld"
        ",ano_2_trans_write_skew_2=%ld"
        ",ano_3_trans_write_skew_1=%ld"
        ",ano_3_trans_write_skew_2=%ld"
        ",ano_2_trans_read_skew=%ld"
        ",ano_3_trans_read_skew_1=%ld"
        ",ano_3_trans_read_skew_2=%ld"
        ",ano_4_trans_read_skew=%ld"
        ",ano_unknown=%ld",
        ano_2_trans_write_skew_1, ano_2_trans_write_skew_2, ano_3_trans_write_skew_1,
        ano_3_trans_write_skew_2, ano_2_trans_read_skew, ano_3_trans_read_skew_1,
        ano_3_trans_read_skew_2, ano_4_trans_read_skew, ano_unknown);

    if (!prog) {
        last_start_commit_latency.quicksort(0,last_start_commit_latency.cnt-1);
        first_start_commit_latency.quicksort(0,first_start_commit_latency.cnt-1);
        start_abort_commit_latency.quicksort(0,start_abort_commit_latency.cnt-1);

        fprintf(
            outf,
            ",fscl0=%f"
            ",fscl1=%f"
            ",fscl10=%f"
            ",fscl25=%f"
            ",fscl50=%f"
            ",fscl75=%f"
            ",fscl90=%f"
            ",fscl95=%f"
            ",fscl96=%f"
            ",fscl97=%f"
            ",fscl98=%f"
            ",fscl99=%f"
            ",fscl100=%f"
            ",fscl_avg=%f"
            ",fscl_cnt=%ld",
            (double)first_start_commit_latency.get_idx(0) / BILLION,
            (double)first_start_commit_latency.get_percentile(1) / BILLION,
            (double)first_start_commit_latency.get_percentile(10) / BILLION,
            (double)first_start_commit_latency.get_percentile(25) / BILLION,
            (double)first_start_commit_latency.get_percentile(50) / BILLION,
            (double)first_start_commit_latency.get_percentile(75) / BILLION,
            (double)first_start_commit_latency.get_percentile(90) / BILLION,
            (double)first_start_commit_latency.get_percentile(95) / BILLION,
            (double)first_start_commit_latency.get_percentile(96) / BILLION,
            (double)first_start_commit_latency.get_percentile(97) / BILLION,
            (double)first_start_commit_latency.get_percentile(98) / BILLION,
            (double)first_start_commit_latency.get_percentile(99) / BILLION,
            (double)first_start_commit_latency.get_idx(first_start_commit_latency.cnt - 1) / BILLION,
            (double)first_start_commit_latency.get_avg() / BILLION, first_start_commit_latency.cnt);

    fprintf(outf,
            ",lscl0=%f"
            ",lscl1=%f"
            ",lscl10=%f"
            ",lscl25=%f"
            ",lscl50=%f"
            ",lscl75=%f"
            ",lscl90=%f"
            ",lscl95=%f"
            ",lscl96=%f"
            ",lscl97=%f"
            ",lscl98=%f"
            ",lscl99=%f"
            ",lscl100=%f"
            ",lscl_avg=%f"
                ",lscl_cnt=%ld",
                (double)last_start_commit_latency.get_idx(0) / BILLION,
                (double)last_start_commit_latency.get_percentile(1) / BILLION,
                (double)last_start_commit_latency.get_percentile(10) / BILLION,
                (double)last_start_commit_latency.get_percentile(25) / BILLION,
                (double)last_start_commit_latency.get_percentile(50) / BILLION,
                (double)last_start_commit_latency.get_percentile(75) / BILLION,
                (double)last_start_commit_latency.get_percentile(90) / BILLION,
                (double)last_start_commit_latency.get_percentile(95) / BILLION,
                (double)last_start_commit_latency.get_percentile(96) / BILLION,
                (double)last_start_commit_latency.get_percentile(97) / BILLION,
                (double)last_start_commit_latency.get_percentile(98) / BILLION,
                (double)last_start_commit_latency.get_percentile(99) / BILLION,
                (double)last_start_commit_latency.get_idx(last_start_commit_latency.cnt - 1) / BILLION,
                (double)last_start_commit_latency.get_avg() / BILLION, last_start_commit_latency.cnt);

        fprintf(
            outf,
            ",sacl0=%f"
            ",sacl1=%f"
            ",sacl10=%f"
            ",sacl25=%f"
            ",sacl50=%f"
            ",sacl75=%f"
            ",sacl90=%f"
            ",sacl95=%f"
            ",sacl96=%f"
            ",sacl97=%f"
            ",sacl98=%f"
            ",sacl99=%f"
            ",sacl100=%f"
            ",sacl_avg=%f"
            ",sacl_cnt=%ld",
            (double)start_abort_commit_latency.get_idx(0) / BILLION,
            (double)start_abort_commit_latency.get_percentile(1) / BILLION,
            (double)start_abort_commit_latency.get_percentile(10) / BILLION,
            (double)start_abort_commit_latency.get_percentile(25) / BILLION,
            (double)start_abort_commit_latency.get_percentile(50) / BILLION,
            (double)start_abort_commit_latency.get_percentile(75) / BILLION,
            (double)start_abort_commit_latency.get_percentile(90) / BILLION,
            (double)start_abort_commit_latency.get_percentile(95) / BILLION,
            (double)start_abort_commit_latency.get_percentile(96) / BILLION,
            (double)start_abort_commit_latency.get_percentile(97) / BILLION,
            (double)start_abort_commit_latency.get_percentile(98) / BILLION,
            (double)start_abort_commit_latency.get_percentile(99) / BILLION,
            (double)start_abort_commit_latency.get_idx(start_abort_commit_latency.cnt - 1) / BILLION,
            (double)start_abort_commit_latency.get_avg() / BILLION, start_abort_commit_latency.cnt);
    }

}

void Stats_thd::combine(Stats_thd * stats) {
    if (stats->total_runtime > total_runtime) total_runtime = stats->total_runtime;

    last_start_commit_latency.append(stats->first_start_commit_latency);
    first_start_commit_latency.append(stats->first_start_commit_latency);
    start_abort_commit_latency.append(stats->start_abort_commit_latency);
    client_client_latency.append(stats->client_client_latency);
    // Execution
    txn_cnt+=stats->txn_cnt;
    remote_txn_cnt+=stats->remote_txn_cnt;
    local_txn_cnt+=stats->local_txn_cnt;
    local_txn_start_cnt+=stats->local_txn_start_cnt;
    total_txn_commit_cnt+=stats->total_txn_commit_cnt;
    local_txn_commit_cnt+=stats->local_txn_commit_cnt;
    remote_txn_commit_cnt+=stats->remote_txn_commit_cnt;
    total_txn_abort_cnt+=stats->total_txn_abort_cnt;
    positive_txn_abort_cnt += stats->positive_txn_abort_cnt;
    unique_txn_abort_cnt+=stats->unique_txn_abort_cnt;
    local_txn_abort_cnt+=stats->local_txn_abort_cnt;
    remote_txn_abort_cnt+=stats->remote_txn_abort_cnt;
    txn_run_time+=stats->txn_run_time;
    multi_part_txn_cnt+=stats->multi_part_txn_cnt;
    multi_part_txn_run_time+=stats->multi_part_txn_run_time;
    single_part_txn_cnt+=stats->single_part_txn_cnt;
    single_part_txn_run_time+=stats->single_part_txn_run_time;
    txn_write_cnt+=stats->txn_write_cnt;
    record_write_cnt+=stats->record_write_cnt;
    parts_touched+=stats->parts_touched;

    // Breakdown
    ts_alloc_time+=stats->ts_alloc_time;
    abort_time+=stats->abort_time;
    txn_manager_time+=stats->txn_manager_time;
    txn_index_time+=stats->txn_index_time;
    txn_validate_time+=stats->txn_validate_time;
    txn_cleanup_time+=stats->txn_cleanup_time;
    // trans
    trans_total_run_time+=stats->trans_total_run_time;
    trans_process_time+=stats->trans_process_time;
    trans_2pc_time+=stats->trans_2pc_time;
    trans_prepare_time+=stats->trans_prepare_time;
    trans_validate_time+=stats->trans_validate_time;
    trans_finish_time+=stats->trans_finish_time;
    trans_commit_time+=stats->trans_commit_time;
    trans_abort_time+=stats->trans_abort_time;
    trans_access_lock_wait_time+=stats->trans_access_lock_wait_time;
    total_access_time+=stats->total_access_time;
    trans_access_read_time+=stats->trans_access_read_time;
    trans_access_pre_time+=stats->trans_access_pre_time;
    trans_access_write_time+=stats->trans_access_write_time;
    trans_access_xp_time+=stats->trans_access_xp_time;
    trans_access_clear_time+=stats->trans_access_clear_time;
    trans_access_write_insert_time+=stats->trans_access_write_insert_time;
    trans_access_write_release_time+=stats->trans_access_write_release_time;
    trans_access_pre_check_time+=stats->trans_access_pre_check_time;
    trans_access_pre_before_time+=stats->trans_access_pre_before_time;
    trans_access_pre_lock_time+=stats->trans_access_pre_lock_time;
    // trans mvcc
    trans_mvcc_clear_history+=stats->trans_mvcc_clear_history;
    trans_mvcc_access+=stats->trans_mvcc_access;
    trans_access_write_update_time+=stats->trans_access_write_update_time;
    // Transaction stats
    txn_total_process_time+=stats->txn_total_process_time;
    txn_process_time+=stats->txn_process_time;
    txn_total_local_wait_time+=stats->txn_total_local_wait_time;
    txn_local_wait_time+=stats->txn_local_wait_time;
    txn_total_remote_wait_time+=stats->txn_total_remote_wait_time;
    txn_remote_wait_time+=stats->txn_remote_wait_time;
    txn_total_twopc_time+=stats->txn_total_twopc_time;
    txn_twopc_time+=stats->txn_twopc_time;

    // Client
    txn_sent_cnt+=stats->txn_sent_cnt;
    cl_send_intv+=stats->cl_send_intv;

    // Abort queue
    abort_queue_enqueue_cnt+=stats->abort_queue_enqueue_cnt;
    abort_queue_dequeue_cnt+=stats->abort_queue_dequeue_cnt;
    abort_queue_enqueue_time+=stats->abort_queue_enqueue_time;
    abort_queue_dequeue_time+=stats->abort_queue_dequeue_time;
    abort_queue_penalty+=stats->abort_queue_penalty;
    abort_queue_penalty_extra+=stats->abort_queue_penalty_extra;

    // Work queue
    work_queue_wait_time+=stats->work_queue_wait_time;
    work_queue_cnt+=stats->work_queue_cnt;
    work_queue_enq_cnt+=stats->work_queue_enq_cnt;
    work_queue_mtx_wait_time+=stats->work_queue_mtx_wait_time;
    work_queue_new_cnt+=stats->work_queue_new_cnt;
    work_queue_new_wait_time+=stats->work_queue_new_wait_time;
    work_queue_old_cnt+=stats->work_queue_old_cnt;
    work_queue_old_wait_time+=stats->work_queue_old_wait_time;
    work_queue_enqueue_time+=stats->work_queue_enqueue_time;
    work_queue_dequeue_time+=stats->work_queue_dequeue_time;
    work_queue_conflict_cnt+=stats->work_queue_conflict_cnt;

    // Worker thread
    worker_idle_time+=stats->worker_idle_time;
    worker_activate_txn_time+=stats->worker_activate_txn_time;
    worker_deactivate_txn_time+=stats->worker_deactivate_txn_time;
    worker_release_msg_time+=stats->worker_release_msg_time;
    worker_process_time+=stats->worker_process_time;
    worker_process_cnt+=stats->worker_process_cnt;
    for(uint64_t i = 0; i < NO_MSG; i ++) {
        worker_process_cnt_by_type[i]+=stats->worker_process_cnt_by_type[i];
        worker_process_time_by_type[i]+=stats->worker_process_time_by_type[i];
    }
    for(uint64_t i = 0; i < SECOND; i ++) {
        work_queue_wq_cnt[i]+=stats->work_queue_wq_cnt[i];
        work_queue_tx_cnt[i]+=stats->work_queue_tx_cnt[i];

        work_queue_ewq_cnt[i]+=stats->work_queue_ewq_cnt[i];
        work_queue_dwq_cnt[i]+=stats->work_queue_dwq_cnt[i];
        work_queue_etx_cnt[i]+=stats->work_queue_etx_cnt[i];
        work_queue_dtx_cnt[i]+=stats->work_queue_dtx_cnt[i];
    }
    // IO
    msg_queue_delay_time+=stats->msg_queue_delay_time;
    msg_queue_cnt+=stats->msg_queue_cnt;
    msg_queue_enq_cnt+=stats->msg_queue_enq_cnt;
    msg_send_time+=stats->msg_send_time;
    msg_recv_time+=stats->msg_recv_time;
    msg_recv_idle_time+=stats->msg_recv_idle_time;
    msg_batch_cnt+=stats->msg_batch_cnt;
    msg_batch_size_msgs+=stats->msg_batch_size_msgs;
    msg_batch_size_bytes+=stats->msg_batch_size_bytes;
    msg_batch_size_bytes_to_server+=stats->msg_batch_size_bytes_to_server;
    msg_batch_size_bytes_to_client+=stats->msg_batch_size_bytes_to_client;
    msg_send_cnt+=stats->msg_send_cnt;
    msg_recv_cnt+=stats->msg_recv_cnt;
    msg_unpack_time+=stats->msg_unpack_time;
    mbuf_send_intv_time+=stats->mbuf_send_intv_time;
    msg_copy_output_time+=stats->msg_copy_output_time;

    // Concurrency control, general
    cc_conflict_cnt+=stats->cc_conflict_cnt;
    txn_wait_cnt+=stats->txn_wait_cnt;
    txn_conflict_cnt+=stats->txn_conflict_cnt;

    // 2PL
    twopl_already_owned_cnt+=stats->twopl_already_owned_cnt;
    twopl_owned_cnt+=stats->twopl_owned_cnt;
    twopl_sh_owned_cnt+=stats->twopl_sh_owned_cnt;
    twopl_ex_owned_cnt+=stats->twopl_ex_owned_cnt;
    twopl_sh_bypass_cnt+=stats->twopl_sh_bypass_cnt;
    twopl_owned_time+=stats->twopl_owned_time;
    twopl_sh_owned_time+=stats->twopl_sh_owned_time;
    twopl_ex_owned_time+=stats->twopl_ex_owned_time;
    twopl_diff_time+=stats->twopl_diff_time;
    twopl_wait_time+=stats->twopl_wait_time;
    twopl_getlock_cnt+=stats->twopl_getlock_cnt;
    twopl_release_cnt+=stats->twopl_release_cnt;
    twopl_release_time+=stats->twopl_release_time;
    twopl_getlock_time+=stats->twopl_getlock_time;

    // Calvin
    seq_txn_cnt+=stats->seq_txn_cnt;
    seq_batch_cnt+=stats->seq_batch_cnt;
    seq_full_batch_cnt+=stats->seq_full_batch_cnt;
    seq_ack_time+=stats->seq_ack_time;
    seq_batch_time+=stats->seq_batch_time;
    seq_process_cnt+=stats->seq_process_cnt;
    seq_complete_cnt+=stats->seq_complete_cnt;
    seq_process_time+=stats->seq_process_time;
    seq_prep_time+=stats->seq_prep_time;
    seq_idle_time+=stats->seq_idle_time;
    seq_queue_wait_time+=stats->seq_queue_wait_time;
    seq_queue_cnt+=stats->seq_queue_cnt;
    seq_queue_enq_cnt+=stats->seq_queue_enq_cnt;
    seq_queue_enqueue_time+=stats->seq_queue_enqueue_time;
    seq_queue_dequeue_time+=stats->seq_queue_dequeue_time;
    sched_queue_wait_time+=stats->sched_queue_wait_time;
    sched_queue_cnt+=stats->sched_queue_cnt;
    sched_queue_enq_cnt+=stats->sched_queue_enq_cnt;
    sched_queue_enqueue_time+=stats->sched_queue_enqueue_time;
    sched_queue_dequeue_time+=stats->sched_queue_dequeue_time;
    calvin_sched_time+=stats->calvin_sched_time;
    sched_idle_time+=stats->sched_idle_time;
    sched_txn_table_time+=stats->sched_txn_table_time;
    sched_epoch_cnt+=stats->sched_epoch_cnt;
    sched_epoch_diff+=stats->sched_epoch_diff;

    //OCC
    occ_validate_time+=stats->occ_validate_time;
    occ_cs_wait_time+=stats->occ_cs_wait_time;
    occ_cs_time+=stats->occ_cs_time;
    occ_hist_validate_time+=stats->occ_hist_validate_time;
    occ_act_validate_time+=stats->occ_act_validate_time;
    occ_hist_validate_fail_time+=stats->occ_hist_validate_fail_time;
    occ_act_validate_fail_time+=stats->occ_act_validate_fail_time;
    occ_check_cnt+=stats->occ_check_cnt;
    occ_abort_check_cnt+=stats->occ_abort_check_cnt;
    occ_ts_abort_cnt+=stats->occ_ts_abort_cnt;
    occ_finish_time+=stats->occ_finish_time;

    // MAAT
    maat_validate_cnt+=stats->maat_validate_cnt;
    maat_validate_time+=stats->maat_validate_time;
    maat_cs_wait_time+=stats->maat_cs_wait_time;
    maat_case1_cnt+=stats->maat_case1_cnt;
    maat_case2_cnt+=stats->maat_case2_cnt;
    maat_case3_cnt+=stats->maat_case3_cnt;
    maat_case4_cnt+=stats->maat_case4_cnt;
    maat_case5_cnt+=stats->maat_case5_cnt;
    maat_range+=stats->maat_range;
    maat_commit_cnt+=stats->maat_commit_cnt;

    // Logging
    log_write_cnt+=stats->log_write_cnt;
    log_write_time+=stats->log_write_time;
    log_flush_cnt+=stats->log_flush_cnt;
    log_flush_time+=stats->log_flush_time;
    log_process_time+=stats->log_process_time;

    // Transaction Table
    txn_table_new_cnt+=stats->txn_table_new_cnt;
    txn_table_get_cnt+=stats->txn_table_get_cnt;
    txn_table_release_cnt+=stats->txn_table_release_cnt;
    txn_table_cflt_cnt+=stats->txn_table_cflt_cnt;
    txn_table_cflt_size+=stats->txn_table_cflt_size;
    txn_table_get_time+=stats->txn_table_get_time;
    txn_table_release_time+=stats->txn_table_release_time;
    txn_table_min_ts_time+=stats->txn_table_min_ts_time;

    for(uint64_t i = 0; i < 40; i ++) {
        mtx[i]+=stats->mtx[i];
    }

    // Latency

    lat_work_queue_time+=stats->lat_work_queue_time;
    lat_msg_queue_time+=stats->lat_msg_queue_time;
    lat_cc_block_time+=stats->lat_cc_block_time;
    lat_cc_time+=stats->lat_cc_time;
    lat_process_time+=stats->lat_process_time;
    lat_abort_time+=stats->lat_abort_time;
    lat_network_time+=stats->lat_network_time;
    lat_other_time+=stats->lat_other_time;

    lat_l_loc_work_queue_time+=stats->lat_l_loc_work_queue_time;
    lat_l_loc_msg_queue_time+=stats->lat_l_loc_msg_queue_time;
    lat_l_loc_cc_block_time+=stats->lat_l_loc_cc_block_time;
    lat_l_loc_cc_time+=stats->lat_l_loc_cc_time;
    lat_l_loc_process_time+=stats->lat_l_loc_process_time;
    lat_l_loc_abort_time+=stats->lat_l_loc_abort_time;

    lat_short_work_queue_time+=stats->lat_short_work_queue_time;
    lat_short_msg_queue_time+=stats->lat_short_msg_queue_time;
    lat_short_cc_block_time+=stats->lat_short_cc_block_time;
    lat_short_cc_time+=stats->lat_short_cc_time;
    lat_short_process_time+=stats->lat_short_process_time;
    lat_short_network_time+=stats->lat_short_network_time;
    lat_short_batch_time+=stats->lat_short_batch_time;

    lat_s_loc_work_queue_time+=stats->lat_s_loc_work_queue_time;
    lat_s_loc_msg_queue_time+=stats->lat_s_loc_msg_queue_time;
    lat_s_loc_cc_block_time+=stats->lat_s_loc_cc_block_time;
    lat_s_loc_cc_time+=stats->lat_s_loc_cc_time;
    lat_s_loc_process_time+=stats->lat_s_loc_process_time;

    lat_l_rem_work_queue_time+=stats->lat_l_rem_work_queue_time;
    lat_l_rem_msg_queue_time+=stats->lat_l_rem_msg_queue_time;
    lat_l_rem_cc_block_time+=stats->lat_l_rem_cc_block_time;
    lat_l_rem_cc_time+=stats->lat_l_rem_cc_time;
    lat_l_rem_process_time+=stats->lat_l_rem_process_time;

    lat_s_rem_work_queue_time+=stats->lat_s_rem_work_queue_time;
    lat_s_rem_msg_queue_time+=stats->lat_s_rem_msg_queue_time;
    lat_s_rem_cc_block_time+=stats->lat_s_rem_cc_block_time;
    lat_s_rem_cc_time+=stats->lat_s_rem_cc_time;
    lat_s_rem_process_time+=stats->lat_s_rem_process_time;

    ano_2_trans_write_skew_1 += stats->ano_2_trans_write_skew_1;
    ano_2_trans_write_skew_2 += stats->ano_2_trans_write_skew_2;
    ano_3_trans_write_skew_1 += stats->ano_3_trans_write_skew_1;
    ano_3_trans_write_skew_2 += stats->ano_3_trans_write_skew_2;
    ano_2_trans_read_skew += stats->ano_2_trans_read_skew;
    ano_3_trans_read_skew_1 += stats->ano_3_trans_read_skew_1;
    ano_3_trans_read_skew_2 += stats->ano_3_trans_read_skew_2;
    ano_4_trans_read_skew += stats->ano_4_trans_read_skew;
    ano_unknown += stats->ano_unknown;
}

void Stats::init(uint64_t thread_cnt) {
    if (!STATS_ENABLE) return;

    thd_cnt = thread_cnt;
        _stats = new Stats_thd * [thread_cnt];
        totals = new Stats_thd;

    for(uint64_t i = 0; i < thread_cnt; i++) {
        _stats[i] = (Stats_thd *)mem_allocator.align_alloc(sizeof(Stats_thd));
        _stats[i]->init(i);
        _stats[i]->clear();
    }

    totals->init(0);
    totals->clear();
}

void Stats::clear(uint64_t tid) {}

void Stats::print_client(bool prog) {
    fflush(stdout);
    if (!STATS_ENABLE) return;

    totals->clear();
    for (uint64_t i = 0; i < thd_cnt; i++) totals->combine(_stats[i]);

        FILE * outf;
        if (output_file != NULL)
            outf = fopen(output_file, "w");
    else
        outf = stdout;
    if(prog)
        fprintf(outf, "[prog] ");
    else
        fprintf(outf, "[summary] ");
    totals->print_client(outf,prog);
    mem_util(outf);
    cpu_util(outf);

    if(prog) {
        fprintf(outf,"\n");
            //for (uint32_t k = 0; k < g_node_id; ++k) {
            for (uint32_t k = 0; k < g_servers_per_client; ++k) {
        printf("tif_node%u=%d, ", k, client_man.get_inflight(k));
        }
        printf("\n");
    } else {
    }

    if (output_file != NULL) {
        fflush(outf);
        fclose(outf);
    }
    fflush(stdout);
}

void Stats::print(bool prog) {
    fflush(stdout);
    if (!STATS_ENABLE) return;

    totals->clear();
    for (uint64_t i = 0; i < thd_cnt; i++) totals->combine(_stats[i]);
        FILE * outf;
        if (output_file != NULL)
            outf = fopen(output_file, "w");
    else
        outf = stdout;
    if(prog)
        fprintf(outf, "[prog] ");
    else
        fprintf(outf, "[summary] ");
    totals->print(outf,prog);
    mem_util(outf);
    cpu_util(outf);

    fprintf(outf,"\n");
    fflush(outf);
    if(!prog) {
    }
    fprintf(outf,"\n");
    fflush(outf);
    if (output_file != NULL) {
        fclose(outf);
    }
}

uint64_t Stats::get_txn_cnts() {
    if (!STATS_ENABLE || g_node_id >= g_node_cnt) return 0;
    uint64_t limit =  g_thread_cnt + g_rem_thread_cnt;
    uint64_t total_txn_cnt = 0;
    for (uint64_t tid = 0; tid < limit; tid ++) {
        total_txn_cnt += _stats[tid]->txn_cnt;
    }

    return total_txn_cnt;
}

void Stats::print_lat_distr() {
#if PRT_LAT_DISTR
    printf("\n[all_lat] ");
    uint64_t limit = 0;
    if(g_node_id < g_node_cnt)
        limit = g_thread_cnt;
    else
        limit = g_client_thread_cnt;
    for (UInt32 tid = 0; tid < limit; tid++) _stats[tid]->all_lat.print(stdout);
#endif
}

void Stats::print_lat_distr(uint64_t min, uint64_t max) {
#if PRT_LAT_DISTR
    printf("\n[all_lat] ");
    _stats[0]->all_lat.print(stdout,min,max);
#endif
}

void Stats::util_init() {
    struct tms timeSample;
    lastCPU = times(&timeSample);
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;
}

void Stats::print_util() {}

int Stats::parseLine(char* line){
    int i = strlen(line);
    while (*line < '0' || *line > '9') line++;
    line[i-3] = '\0';
    i = atoi(line);
    return i;
}

void Stats::mem_util(FILE * outf) {
    FILE* file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

// Physical memory used by current process, in KB
    while (fgets(line, 128, file) != NULL){
        if (strncmp(line, "VmRSS:", 6) == 0){
            result = parseLine(line);
        fprintf(outf, ",phys_mem_usage=%d", result);
        }
        if (strncmp(line, "VmSize:", 7) == 0){
            result = parseLine(line);
        fprintf(outf, ",virt_mem_usage=%d", result);
        }
    }
    fclose(file);

}

void Stats::cpu_util(FILE * outf) {
    clock_t now;
    struct tms timeSample;
    double percent;

    now = times(&timeSample);
    if (now <= lastCPU || timeSample.tms_stime < lastSysCPU || timeSample.tms_utime < lastUserCPU) {
        //Overflow detection. Just skip this value.
        percent = -1.0;
    } else {
        percent = (timeSample.tms_stime - lastSysCPU) + (timeSample.tms_utime - lastUserCPU);
        percent /= (now - lastCPU);
        if(ISSERVER) {
            percent /= (g_total_thread_cnt);//numProcessors;
        } else if(ISCLIENT){
            percent /= (g_total_client_thread_cnt);//numProcessors;
        }
        percent *= 100;
    }
    fprintf(outf, ",cpu_ttl=%f", percent);
    lastCPU = now;
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;
}


