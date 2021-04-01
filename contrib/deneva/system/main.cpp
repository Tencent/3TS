/*
    Tencent is pleased to support the open source community by making 3TS available.

    Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
    in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
    Tencent Modifications are Copyright (C) THL A29 Limited.

    Author: hongyaozhao@ruc.edu.cn

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

#include "abort_thread.h"
#include "calvin_thread.h"
#include "client_query.h"
#include "global.h"
#include "io_thread.h"
#include "log_thread.h"
#include "logger.h"
#include "maat.h"
#include "manager.h"
#include "math.h"
#include "msg_queue.h"
#include "occ.h"
#include "pps.h"
#include "query.h"
#include "sequencer.h"
#include "sim_manager.h"
#include "abort_queue.h"
#include "thread.h"
#include "tpcc.h"
#include "transport.h"
#include "work_queue.h"
#include "worker_thread.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "da.h"
#include "maat.h"
#include "ssi.h"
#include "wsi.h"
#include "focc.h"
#include "bocc.h"
#include "dta.h"
#include "client_query.h"
#include "sundial.h"
#include "http.h"

void network_test();
void network_test_recv();
void * run_thread(void *);

WorkerThread * worker_thds;
WorkerNumThread * worker_num_thds;
InputThread * input_thds;
OutputThread * output_thds;
AbortThread * abort_thds;
LogThread * log_thds;
#if CC_ALG == CALVIN
CalvinLockThread * calvin_lock_thds;
CalvinSequencerThread * calvin_seq_thds;
#endif

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char *argv[]) {
    // 0. initialize global data structure
    parser(argc, argv);
#if SEED != 0
    uint64_t seed = SEED + g_node_id;
#else
    uint64_t seed = get_sys_clock();
#endif
    srand(seed);
    printf("Random seed: %ld\n",seed);

    int64_t starttime;
    int64_t endtime;
    starttime = get_server_clock();
    printf("Initializing stats... ");
    fflush(stdout);
    stats.init(g_total_thread_cnt);
    printf("Done\n");

    printf("Initializing global manager... ");
    fflush(stdout);
    glob_manager.init();
    printf("Done\n");
    printf("Initializing transport manager... ");
    fflush(stdout);
    tport_man.init();
    printf("Done\n");
    fflush(stdout);
    printf("Initializing simulation... ");
    fflush(stdout);
    simulation = new SimManager;
    simulation->init();
    printf("Done\n");
    fflush(stdout);
    Workload * m_wl;
    switch (WORKLOAD) {
        case YCSB :
            m_wl = new YCSBWorkload;
            break;
        case TPCC :
            m_wl = new TPCCWorkload;
            break;
        case PPS :
            m_wl = new PPSWorkload;
            break;
        case DA:
            m_wl = new DAWorkload;
            is_server=true;
            break;
        default:
            assert(false);
    }
    m_wl->init();
    printf("Workload initialized!\n");
    fflush(stdout);
#if NETWORK_TEST
    tport_man.init(g_node_id,m_wl);
    sleep(3);
    if(g_node_id == 0)
        network_test();
    else if(g_node_id == 1)
        network_test_recv();

    return 0;
#endif


    printf("Initializing work queue... ");
    fflush(stdout);
    work_queue.init();
    printf("Done\n");
    printf("Initializing abort queue... ");
    fflush(stdout);
    abort_queue.init();
    printf("Done\n");
    printf("Initializing message queue... ");
    fflush(stdout);
    msg_queue.init();
    printf("Done\n");
    printf("Initializing transaction manager pool... ");
    fflush(stdout);
    txn_man_pool.init(m_wl,0);
    printf("Done\n");
    printf("Initializing transaction pool... ");
    fflush(stdout);
    txn_pool.init(m_wl,0);
    printf("Done\n");
    printf("Initializing row pool... ");
    fflush(stdout);
    row_pool.init(m_wl,0);
    printf("Done\n");
    printf("Initializing access pool... ");
    fflush(stdout);
    access_pool.init(m_wl,0);
    printf("Done\n");
    printf("Initializing txn node table pool... ");
    fflush(stdout);
    txn_table_pool.init(m_wl,0);
    printf("Done\n");
    printf("Initializing query pool... ");
    fflush(stdout);
    qry_pool.init(m_wl,0);
    printf("Done\n");
    printf("Initializing msg pool... ");
    fflush(stdout);
    msg_pool.init(m_wl,0);
    printf("Done\n");
    printf("Initializing transaction table... ");
    fflush(stdout);
    txn_table.init();
    printf("Done\n");
#if CC_ALG == CALVIN
    printf("Initializing sequencer... ");
    fflush(stdout);
    seq_man.init(m_wl);
    printf("Done\n");
#endif
#if CC_ALG == MAAT
    printf("Initializing Time Table... ");
    fflush(stdout);
    time_table.init();
    printf("Done\n");
    printf("Initializing MaaT manager... ");
    fflush(stdout);
    maat_man.init();
    printf("Done\n");
#endif
#if CC_ALG == SSI
    printf("Initializing In Out Table... ");
    fflush(stdout);
    inout_table.init();
    printf("Done\n");
    printf("Initializing SSI manager... ");
    fflush(stdout);
    ssi_man.init();
    printf("Done\n");
#endif
#if CC_ALG == WSI
    printf("Initializing WSI manager... ");
    fflush(stdout);
    wsi_man.init();
    printf("Done\n");
#endif
#if CC_ALG == SUNDIAL
    printf("Initializing MaaT manager... ");
    fflush(stdout);
    sundial_man.init();
    printf("Done\n");
#endif
#if LOGGING
    printf("Initializing logger... ");
    fflush(stdout);
    logger.init("logfile.log");
    printf("Done\n");
#endif

#if SERVER_GENERATE_QUERIES
    printf("Initializing client query queue... ");
    fflush(stdout);
    client_query_queue.init(m_wl);
    printf("Done\n");
    fflush(stdout);
#endif

#if WORKLOAD==DA
    commit_file.open("commit_histroy.txt",ios::app);
    abort_file.open("abort_histroy.txt",ios::app);
#endif

    // 2. spawn multiple threads
    uint64_t thd_cnt = g_thread_cnt;
    uint64_t wthd_cnt = thd_cnt;
    uint64_t rthd_cnt = g_rem_thread_cnt;
    uint64_t sthd_cnt = g_send_thread_cnt;
    uint64_t all_thd_cnt = thd_cnt + rthd_cnt + sthd_cnt + g_abort_thread_cnt + 1;
#if LOGGING
        all_thd_cnt += 1; // logger thread
#endif
#if CC_ALG == CALVIN
        all_thd_cnt += 2; // sequencer + scheduler thread
#endif

    if (g_ts_alloc == LTS_TCP_CLOCK) {
        printf("Initializing tcp queue... ");
        fflush(stdout);
        tcp_ts.init(all_thd_cnt);
        printf("Done\n");
    }

    printf("%ld, %ld, %ld, %d \n", thd_cnt, rthd_cnt, sthd_cnt, g_abort_thread_cnt);
    printf("all_thd_cnt: %ld, g_this_total_thread_cnt: %d \n", all_thd_cnt, g_this_total_thread_cnt);
    fflush(stdout);
    assert(all_thd_cnt == g_this_total_thread_cnt);

    pthread_t *p_thds = (pthread_t *)malloc(sizeof(pthread_t) * (all_thd_cnt));
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    worker_thds = new WorkerThread[wthd_cnt];
    worker_num_thds = new WorkerNumThread[1];
    input_thds = new InputThread[rthd_cnt];
    output_thds = new OutputThread[sthd_cnt];
    abort_thds = new AbortThread[1];
    log_thds = new LogThread[1];
#if CC_ALG == CALVIN
    calvin_lock_thds = new CalvinLockThread[1];
    calvin_seq_thds = new CalvinSequencerThread[1];
#endif
    // query_queue should be the last one to be initialized!!!
    // because it collects txn latency
    //if (WORKLOAD != TEST) {
    //    query_queue.init(m_wl);
    //}
#if CC_ALG == OCC
    printf("Initializing occ lock manager... ");
    occ_man.init();
    printf("Done\n");
#endif

#if CC_ALG == BOCC
    printf("Initializing occ lock manager... ");
    bocc_man.init();
    printf("Done\n");
#endif

#if CC_ALG == FOCC
    printf("Initializing occ lock manager... ");
    focc_man.init();
    printf("Done\n");
#endif

#if CC_ALG == DTA
    printf("Initializing DTA Time Table... ");
    fflush(stdout);
    dta_time_table.init();
    printf("Done\n");
    printf("Initializing DTA manager... ");
    fflush(stdout);
    dta_man.init();
    printf("Done\n");
#endif

    endtime = get_server_clock();
    printf("Initialization Time = %ld\n", endtime - starttime);
    fflush(stdout);
    warmup_done = true;
    pthread_barrier_init( &warmup_bar, NULL, all_thd_cnt);

#if SET_AFFINITY
    uint64_t cpu_cnt = 0;
    cpu_set_t cpus;
#endif
    // spawn and run txns again.
    starttime = get_server_clock();
    simulation->run_starttime = starttime;
#if WORKLOAD == DA
    simulation->last_da_query_time = starttime;
    simulation->last_da_recv_query_time = starttime;
#endif

    uint64_t id = 0;
    for (uint64_t i = 0; i < wthd_cnt; i++) {
#if SET_AFFINITY
        CPU_ZERO(&cpus);
        CPU_SET(cpu_cnt, &cpus);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
        cpu_cnt++;
#endif
        assert(id >= 0 && id < wthd_cnt);
        worker_thds[i].init(id,g_node_id,m_wl);
        pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_thds[i]);
    }
    for (uint64_t j = 0; j < rthd_cnt ; j++) {
        assert(id >= wthd_cnt && id < wthd_cnt + rthd_cnt);
        input_thds[j].init(id,g_node_id,m_wl);
        pthread_create(&p_thds[id++], NULL, run_thread, (void *)&input_thds[j]);
    }

    for (uint64_t j = 0; j < sthd_cnt; j++) {
        assert(id >= wthd_cnt + rthd_cnt && id < wthd_cnt + rthd_cnt + sthd_cnt);
        output_thds[j].init(id,g_node_id,m_wl);
        pthread_create(&p_thds[id++], NULL, run_thread, (void *)&output_thds[j]);
    }
#if LOGGING
    log_thds[0].init(id,g_node_id,m_wl);
    pthread_create(&p_thds[id++], NULL, run_thread, (void *)&log_thds[0]);
#endif

#if CC_ALG != CALVIN && WORKLOAD != DA
    abort_thds[0].init(id,g_node_id,m_wl);
    pthread_create(&p_thds[id++], NULL, run_thread, (void *)&abort_thds[0]);
#endif

#if CC_ALG == CALVIN
#if SET_AFFINITY
    CPU_ZERO(&cpus);
    CPU_SET(cpu_cnt, &cpus);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
    cpu_cnt++;
#endif

    calvin_lock_thds[0].init(id,g_node_id,m_wl);
    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_lock_thds[0]);
#if SET_AFFINITY
    CPU_ZERO(&cpus);
    CPU_SET(cpu_cnt, &cpus);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
    cpu_cnt++;
#endif

    calvin_seq_thds[0].init(id,g_node_id,m_wl);
    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&calvin_seq_thds[0]);
#endif

    worker_num_thds[0].init(id,g_node_id,m_wl);
    pthread_create(&p_thds[id++], &attr, run_thread, (void *)&worker_num_thds[0]);
    for (uint64_t i = 0; i < all_thd_cnt; i++) pthread_join(p_thds[i], NULL);

    endtime = get_server_clock();

    fflush(stdout);
    printf("PASS! SimTime = %f\n", (float)(endtime - starttime) / BILLION);
    if (STATS_ENABLE) stats.print(false);

    printf("\n");
    fflush(stdout);

    m_wl->index_delete_all();

    if (g_ts_alloc == LTS_TCP_CLOCK) {
        for (uint32_t i = 0; i < all_thd_cnt; i++) {
            tcp_ts.CloseToLts(i);
        }
    }
    return 0;
}

void * run_thread(void * id) {
        Thread * thd = (Thread *) id;
    thd->run();
    return NULL;
}

void network_test() {

}

void network_test_recv() {

}
