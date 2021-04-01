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
#include "ycsb.h"
#include "tpcc.h"
#include "pps.h"
#include "da.h"
#include "thread.h"
#include "io_thread.h"
#include "client_thread.h"
#include "client_query.h"
#include "transport.h"
#include "client_txn.h"
#include "msg_queue.h"
#include "work_queue.h"
//#include <jemallloc.h>

void * f(void *);
void * g(void *);
void * worker(void *);
void * nn_worker(void *);
void * send_worker(void *);
void network_test();
void network_test_recv();
void * run_thread(void *);

ClientThread * client_thds;
InputThread * input_thds;
OutputThread * output_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char *argv[]) {
    printf("Running client...\n\n");
    // 0. initialize global data structure
    parser(argc, argv);
    assert(g_node_id >= g_node_cnt);
    //assert(g_client_node_cnt <= g_node_cnt);

    uint64_t seed = get_sys_clock();
    srand(seed);
    printf("Random seed: %ld\n",seed);


    int64_t starttime;
    int64_t endtime;
    starttime = get_server_clock();
    // per-partition malloc
    printf("Initializing stats... ");
    fflush(stdout);
    stats.init(g_total_client_thread_cnt);
    printf("Done\n");
    printf("Initializing transport manager... ");
    fflush(stdout);
    tport_man.init();
    printf("Done\n");
    printf("Initializing client manager... ");
    Workload * m_wl;
    switch (WORKLOAD) {
        case YCSB :
            m_wl = new YCSBWorkload; break;
        case TPCC :
            m_wl = new TPCCWorkload; break;
        case PPS :
            m_wl = new PPSWorkload; break;
        case DA :
            m_wl = new DAWorkload; break;
        default:
            assert(false);
    }
    m_wl->Workload::init();
    printf("workload initialized!\n");

    printf("Initializing simulation... ");
    fflush(stdout);
    simulation = new SimManager;
    simulation->init();
    printf("Done\n");
#if NETWORK_TEST
    tport_man.init(g_node_id,m_wl);
    sleep(3);
    if(g_node_id == 0)
        network_test();
    else if(g_node_id == 1)
        network_test_recv();

    return 0;
#endif


    fflush(stdout);
    client_man.init();
    printf("Done\n");
    printf("Initializing work queue... ");
    fflush(stdout);
    work_queue.init();
    printf("Done\n");
    printf("Initializing msg pool... ");
    fflush(stdout);
    msg_pool.init(m_wl,g_inflight_max);
    printf("Done\n");
    fflush(stdout);
/*
    #if WORKLOAD==DA
        g_client_rem_thread_cnt=1;
        g_client_send_thread_cnt=1;
    #endif
*/

    // 2. spawn multiple threads
    uint64_t thd_cnt = g_client_thread_cnt;
    uint64_t cthd_cnt = thd_cnt;
    uint64_t rthd_cnt = g_client_rem_thread_cnt;
    uint64_t sthd_cnt = g_client_send_thread_cnt;
    uint64_t all_thd_cnt = thd_cnt + rthd_cnt + sthd_cnt;
    assert(all_thd_cnt == g_this_total_thread_cnt);

    pthread_t *p_thds = (pthread_t *)malloc(sizeof(pthread_t) * (all_thd_cnt));
    pthread_attr_t attr;
    pthread_attr_init(&attr);

    client_thds = new ClientThread[cthd_cnt];
    input_thds = new InputThread[rthd_cnt];
    output_thds = new OutputThread[sthd_cnt];
    // query_queue should be the last one to be initialized!!!
    // because it collects txn latency
    printf("Initializing message queue... ");
    msg_queue.init();
    printf("Done\n");
    printf("Initializing client query queue... ");
    fflush(stdout);
    client_query_queue.init(m_wl);
    printf("Done\n");
    fflush(stdout);

#if CREATE_TXN_FILE
    return(0);
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
#endif
    uint64_t id = 0;
    for (uint64_t i = 0; i < thd_cnt; i++) {
#if SET_AFFINITY
        CPU_ZERO(&cpus);
#if TPORT_TYPE_IPC
        CPU_SET(g_node_id * thd_cnt + cpu_cnt, &cpus);
#elif !SET_AFFINITY
        CPU_SET(g_node_id * thd_cnt + cpu_cnt, &cpus);
#else
        CPU_SET(cpu_cnt, &cpus);
#endif
        cpu_cnt = (cpu_cnt + 1) % g_servers_per_client;
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus);
#endif
        client_thds[i].init(id,g_node_id,m_wl);
        pthread_create(&p_thds[id++], &attr, run_thread, (void *)&client_thds[i]);
    }

    for (uint64_t j = 0; j < rthd_cnt ; j++) {
        input_thds[j].init(id,g_node_id,m_wl);
        pthread_create(&p_thds[id++], NULL, run_thread, (void *)&input_thds[j]);
    }

    for (uint64_t i = 0; i < sthd_cnt; i++) {
        output_thds[i].init(id,g_node_id,m_wl);
        pthread_create(&p_thds[id++], NULL, run_thread, (void *)&output_thds[i]);
    }
    for (uint64_t i = 0; i < all_thd_cnt; i++)
        pthread_join(p_thds[i], NULL);

    endtime = get_server_clock();

    fflush(stdout);
    printf("CLIENT PASS! SimTime = %ld\n", endtime - starttime);
    if (STATS_ENABLE) stats.print_client(false);
    fflush(stdout);
    return 0;
}

void * run_thread(void * id) {
    Thread * thd = (Thread *) id;
    thd->run();
    return NULL;
}

void network_test() {
/*
    ts_t start;
    ts_t end;
    double time;
    int bytes;
    for(int i=4; i < 257; i+=4) {
        time = 0;
        for(int j=0;j < 1000; j++) {
            start = get_sys_clock();
            tport_man.simple_send_msg(i);
            while((bytes = tport_man.simple_recv_msg()) == 0) {}
            end = get_sys_clock();
            assert(bytes == i);
            time += end-start;
        }
        time = time/1000;
        printf("Network Bytes: %d, s: %f\n",i,time/BILLION);
        fflush(stdout);
    }
*/
}

void network_test_recv() {
/*
    int bytes;
    while(1) {
        if( (bytes = tport_man.simple_recv_msg()) > 0)
            tport_man.simple_send_msg(bytes);
    }
*/
}
