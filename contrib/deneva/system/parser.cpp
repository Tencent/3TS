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
#include "helper.h"

void print_usage() {
    printf("[usage]:\n");
    printf("\t-nidINT       ; NODE_ID\n");
    printf("\t-pINT       ; PART_CNT\n");
    printf("\t-nINT       ; NODE_CNT\n");
    printf("\t-cnINT       ; CLIENT_NODE_CNT\n");

    printf("\t-vINT       ; VIRTUAL_PART_CNT\n");

    printf("\t-tINT       ; THREAD_CNT\n");
    printf("\t-trINT       ; REM_THREAD_CNT\n");
    printf("\t-tsINT       ; SEND_THREAD_CNT\n");
    printf("\t-ctINT       ; CLIENT_THREAD_CNT\n");
    printf("\t-ctrINT       ; CLIENT_REM_THREAD_CNT\n");
    printf("\t-ctsINT       ; CLIENT_SEND_THREAD_CNT\n");

    printf("\t-tppINT       ; MAX_TXN_PER_PART\n");
    printf("\t-tifINT       ; MAX_TXN_IN_FLIGHT\n");
    printf("\t-mprINT       ; MPR\n");
    printf("\t-mpiINT       ; MPIR\n");
    printf("\t-doneINT       ; DONE_TIMER\n");
    printf("\t-btmrINT       ; BATCH_TIMER\n");
    printf("\t-stmrINT       ; SEQ_BATCH_TIMER\n");
    printf("\t-progINT       ; PROG_TIMER\n");
    printf("\t-abrtINT       ; ABORT_PENALTY (ms)\n");

    printf("\t-qINT       ; QUERY_INTVL\n");
    printf("\t-dINT       ; PRT_LAT_DISTR\n");
    printf("\t-aINT       ; PART_ALLOC (0 or 1)\n");
    printf("\t-mINT       ; MEM_PAD (0 or 1)\n");

    printf("\t-rnINT       ; REPLICA_CNT (0+)\n");
    printf("\t-rtINT       ; REPL_TYPE (AA: 1, AP: 2)\n");

    printf("\t-o STRING   ; output file\n");
    printf("\t-i STRING   ; input file\n");
    printf("\t-cf STRING   ; txn file\n");
    printf("\t-ndly   ; NETWORK_DELAY\n");
    printf("  [YCSB]:\n");
    printf("\t-dpFLOAT       ; DATA_PERC\n");
    printf("\t-apFLOAT       ; ACCESS_PERC\n");
    printf("\t-pptINT       ; PART_PER_TXN\n");
    printf("\t-spptINT       ; STRICT_PPT\n");
    printf("\t-eINT       ; PERC_MULTI_PART\n");
    printf("\t-wFLOAT     ; WRITE_PERC\n");
    printf("\t-zipfFLOAT     ; ZIPF_THETA\n");
    printf("\t-sINT       ; SYNTH_TABLE_SIZE\n");
    printf("\t-rpqINT       ; REQ_PER_QUERY\n");
    printf("\t-fINT       ; FIELD_PER_TUPLE\n");
    printf("  [TPCC]:\n");
    printf("\t-whINT       ; NUM_WH\n");
    printf("\t-ppFLOAT    ; PERC_PAYMENT\n");
    printf("\t-upINT      ; WH_UPDATE\n");

}

void parser(int argc, char * argv[]) {

    g_params["validation_lock"] = VALIDATION_LOCK;
    g_params["pre_abort"] = PRE_ABORT2;

    for (int i = 1; i < argc; i++) {
        assert(argv[i][0] == '-');
        if (argv[i][1] == 'n' && argv[i][2] == 'd' && argv[i][3] == 'l' && argv[i][4] == 'y')
            g_network_delay = atoi( &argv[i][5] );
        else if (argv[i][1] == 'd' && argv[i][2] == 'o' && argv[i][3] == 'n' && argv[i][4] == 'e')
            g_done_timer = atoi( &argv[i][5] );
        else if (argv[i][1] == 'b' && argv[i][2] == 't' && argv[i][3] == 'm' && argv[i][4] == 'r')
            g_batch_time_limit = atoi( &argv[i][5] );
        else if (argv[i][1] == 's' && argv[i][2] == 't' && argv[i][3] == 'm' && argv[i][4] == 'r')
            g_seq_batch_time_limit = atoi( &argv[i][5] );
        else if (argv[i][1] == 's' && argv[i][2] == 'p' && argv[i][3] == 'p' && argv[i][4] == 't')
            g_strict_ppt = atoi( &argv[i][5] ) == 1;
        else if (argv[i][1] == 'p' && argv[i][2] == 'r' && argv[i][3] == 'o' && argv[i][4] == 'g')
            g_thread_cnt = atoi( &argv[i][5] );
        else if (argv[i][1] == 'a' && argv[i][2] == 'b' && argv[i][3] == 'r' && argv[i][4] == 't')
            g_abort_penalty = atoi( &argv[i][5] );
        else if (argv[i][1] == 'z' && argv[i][2] == 'i' && argv[i][3] == 'p' && argv[i][4] == 'f')
            g_zipf_theta = atof( &argv[i][5] );
        else if (argv[i][1] == 'n' && argv[i][2] == 'i' && argv[i][3] == 'd')
            g_node_id = atoi( &argv[i][4] );
        else if (argv[i][1] == 'c' && argv[i][2] == 't' && argv[i][3] == 'r')
            g_client_rem_thread_cnt = atoi( &argv[i][4] );
        else if (argv[i][1] == 'c' && argv[i][2] == 't' && argv[i][3] == 's')
            g_client_send_thread_cnt = atoi( &argv[i][4] );
        else if (argv[i][1] == 'l' && argv[i][2] == 'p' && argv[i][3] == 's')
            g_load_per_server = atoi( &argv[i][4] );
        else if (argv[i][1] == 't' && argv[i][2] == 'p' && argv[i][3] == 'p')
            g_max_txn_per_part = atoi( &argv[i][4] );
        else if (argv[i][1] == 't' && argv[i][2] == 'i' && argv[i][3] == 'f')
            g_inflight_max = atoi( &argv[i][4] );
        else if (argv[i][1] == 'm' && argv[i][2] == 'p' && argv[i][3] == 'r')
            g_mpr = atof( &argv[i][4] );
        else if (argv[i][1] == 'm' && argv[i][2] == 'p' && argv[i][3] == 'i')
            g_mpitem = atof( &argv[i][4] );
        else if (argv[i][1] == 'p' && argv[i][2] == 'p' && argv[i][3] == 't')
            g_part_per_txn = atoi( &argv[i][4] );
        else if (argv[i][1] == 'r' && argv[i][2] == 'p' && argv[i][3] == 'q')
            g_req_per_query = atoi( &argv[i][4] );
        else if (argv[i][1] == 'c' && argv[i][2] == 'n')
            g_client_node_cnt = atoi( &argv[i][3] );
        else if (argv[i][1] == 't' && argv[i][2] == 'r')
            g_rem_thread_cnt = atoi( &argv[i][3] );
        else if (argv[i][1] == 't' && argv[i][2] == 's')
            g_send_thread_cnt = atoi( &argv[i][3] );
        else if (argv[i][1] == 'c' && argv[i][2] == 't')
            g_client_thread_cnt = atoi( &argv[i][3] );
        else if (argv[i][1] == 'w' && argv[i][2] == 'h')
            g_num_wh = atoi( &argv[i][3] );
        else if (argv[i][1] == 'c' && argv[i][2] == 'f')
            txn_file = argv[++i];
        else if (argv[i][1] == 'p' && argv[i][2] == 'p')
            g_perc_payment = atof( &argv[i][3] );
        else if (argv[i][1] == 'u' && argv[i][2] == 'p')
            g_wh_update = atoi( &argv[i][3] );
        else if (argv[i][1] == 'd' && argv[i][2] == 'p')
            g_data_perc = atof( &argv[i][3] );
        else if (argv[i][1] == 'a' && argv[i][2] == 'p')
            g_access_perc = atof( &argv[i][3] );
        else if (argv[i][1] == 'r' && argv[i][2] == 'n')
            g_repl_cnt = atof( &argv[i][3] );
        else if (argv[i][1] == 'r' && argv[i][2] == 't')
            g_repl_type = atof( &argv[i][3] );
        else if (argv[i][1] == 't'&& argv[i][2] == 'w') {
            g_txn_write_perc = atof( &argv[i][3] );
            g_txn_read_perc = 1.0 - atof( &argv[i][3] );
        } else if (argv[i][1] == 'p')
            g_part_cnt = atoi( &argv[i][2] );
        else if (argv[i][1] == 'n')
            g_node_cnt = atoi( &argv[i][2] );
        else if (argv[i][1] == 't')
            g_thread_cnt = atoi( &argv[i][2] );
        else if (argv[i][1] == 'q')
            g_query_intvl = atoi( &argv[i][2] );
        else if (argv[i][1] == 'd')
            g_prt_lat_distr = atoi( &argv[i][2] );
        else if (argv[i][1] == 'a')
            g_part_alloc = atoi( &argv[i][2] );
        else if (argv[i][1] == 'm')
            g_mem_pad = atoi( &argv[i][2] );
        else if (argv[i][1] == 'o')
            output_file = argv[++i];
        else if (argv[i][1] == 'i')
            input_file = argv[++i];
        else if (argv[i][1] == 'e')
            g_perc_multi_part = atof( &argv[i][2] );
        else if (argv[i][1] == 'w') {
            g_tup_write_perc = atof( &argv[i][2] );
            g_tup_read_perc = 1.0 - atof( &argv[i][2] );
        } else if (argv[i][1] == 's')
            g_synth_table_size = atoi( &argv[i][2] );
        else if (argv[i][1] == 'f')
            g_field_per_tuple = atoi( &argv[i][2] );
        else if (argv[i][1] == 'h') {
            print_usage();
            exit(0);
        } else {
            printf("%s\n",argv[i]);
            fflush(stdout);
            assert(false);
        }
    }
    g_total_thread_cnt = g_thread_cnt + g_rem_thread_cnt + g_send_thread_cnt + g_abort_thread_cnt + 1;
#if LOGGING
    g_total_thread_cnt += g_logger_thread_cnt; // logger thread
#endif
#if CC_ALG == CALVIN
    g_total_thread_cnt += 2; // sequencer + scheduler thread
    // Remove abort thread
    g_abort_thread_cnt = 0;
    g_total_thread_cnt -= 1;
#endif
#if WORKLOAD == DA
    g_total_thread_cnt -= 1;
    g_abort_thread_cnt = 0;
#endif
    g_total_client_thread_cnt =
        g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt;
    g_total_node_cnt = g_node_cnt + g_client_node_cnt + g_repl_cnt*g_node_cnt;
    if(ISCLIENT) {
        g_this_thread_cnt = g_client_thread_cnt;
        g_this_rem_thread_cnt = g_client_rem_thread_cnt;
        g_this_send_thread_cnt = g_client_send_thread_cnt;
        g_this_total_thread_cnt = g_total_client_thread_cnt;
    } else {
        g_this_thread_cnt = g_thread_cnt;
        g_this_rem_thread_cnt = g_rem_thread_cnt;
        g_this_send_thread_cnt = g_send_thread_cnt;
        g_this_total_thread_cnt = g_total_thread_cnt;
    }
    // Scale # of keys with cluster size
    g_max_part_key *= g_node_cnt;
    g_max_product_key *= g_node_cnt;
    g_max_supplier_key *= g_node_cnt;

    //g_inflight_max = g_inflight_max / g_node_cnt;
    printf("CC Alg %d\n",CC_ALG);
    printf("g_done_timer %ld\n",g_done_timer);
    printf("g_thread_cnt %d\n",g_thread_cnt );
    printf("g_abort_penalty %ld\n",g_abort_penalty);
    printf("g_zipf_theta %f\n",g_zipf_theta );
    printf("g_node_id %d\n",g_node_id );
    printf("g_client_rem_thread_cnt %d\n",g_client_rem_thread_cnt );
    printf("g_client_send_thread_cnt %d\n",g_client_send_thread_cnt );
    printf("g_max_txn_per_part %d\n",g_max_txn_per_part );
    printf("g_load_per_server %d\n",g_load_per_server );
    printf("g_inflight_max %d\n",g_inflight_max );
    printf("g_mpr %f\n",g_mpr );
    printf("g_mpitem %f\n",g_mpitem );
    printf("g_part_per_txn %d\n",g_part_per_txn );
    printf("g_req_per_query %d\n",g_req_per_query );
    printf("g_client_node_cnt %d\n",g_client_node_cnt );
    printf("g_rem_thread_cnt %d\n",g_rem_thread_cnt );
    printf("g_send_thread_cnt %d\n",g_send_thread_cnt );
    printf("g_client_thread_cnt %d\n",g_client_thread_cnt );
    printf("g_num_wh %d\n",g_num_wh );
    printf("g_perc_payment %f\n",g_perc_payment );
    printf("g_wh_update %d\n",g_wh_update );
    printf("g_part_cnt %d\n",g_part_cnt );
    printf("g_node_cnt %d\n",g_node_cnt );
    printf("g_thread_cnt %d\n",g_thread_cnt );
    printf("g_query_intvl %ld\n",g_query_intvl );
    printf("g_prt_lat_distr %d\n",g_prt_lat_distr );
    printf("g_part_alloc %d\n",g_part_alloc );
    printf("g_mem_pad %d\n",g_mem_pad );
    printf("g_perc_multi_part %f\n",g_perc_multi_part );
    printf("g_tup_read_perc %f\n",g_tup_read_perc );
    printf("g_tup_write_perc %f\n",g_tup_write_perc );
    printf("g_txn_read_perc %f\n",g_txn_read_perc );
    printf("g_txn_write_perc %f\n",g_txn_write_perc );
    printf("g_synth_table_size %ld\n",g_synth_table_size );
    printf("g_max_part_key %d\n",g_max_part_key);
    printf("g_max_product_key %d\n",g_max_product_key);
    printf("g_max_supplier_key %d\n",g_max_supplier_key);
    printf("g_field_per_tuple %d\n",g_field_per_tuple );
    printf("g_data_perc %f\n",g_data_perc);
    printf("g_access_perc %f\n",g_access_perc);
    printf("g_strict_ppt %d\n",g_strict_ppt);
    printf("g_network_delay %d\n",g_network_delay);
    printf("g_total_thread_cnt %d\n",g_total_thread_cnt);
    printf("g_total_client_thread_cnt %d\n",g_total_client_thread_cnt);
    printf("g_total_node_cnt %d\n",g_total_node_cnt);
    printf("g_seq_batch_time_limit %ld\n",g_seq_batch_time_limit);

    // Initialize client-specific globals
    if (g_node_id >= g_node_cnt) init_client_globals();
        init_globals();
}
