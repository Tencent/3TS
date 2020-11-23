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

#include "client_txn.h"
#include "mem_alloc.h"

void Inflight_entry::init() {
    num_inflight_txns = 0;
    sem_init(&mutex, 0, 1);
}

int32_t Inflight_entry::inc_inflight() {
    int32_t result;
    sem_wait(&mutex);
    if (num_inflight_txns < g_inflight_max) {
    // if (num_inflight_txns < 1) {
        result = ++num_inflight_txns;
    } else {
        result = -1;
    }
    sem_post(&mutex);
    return result;
}

int32_t Inflight_entry::dec_inflight() {
    int32_t result;
    sem_wait(&mutex);
    if(num_inflight_txns > 0) {
      result = --num_inflight_txns;
    }
    //assert(num_inflight_txns >= 0);
    sem_post(&mutex);
    return result;
}

int32_t Inflight_entry::get_inflight() {
    int32_t result;
    sem_wait(&mutex);
    result = num_inflight_txns;
    sem_post(&mutex);
    return result;
}

void Client_txn::init() {
    //inflight_txns = new Inflight_entry * [g_node_cnt];
    inflight_txns = new Inflight_entry * [g_servers_per_client];
    //for (uint32_t i = 0; i < g_node_cnt; ++i) {
    for (uint32_t i = 0; i < g_servers_per_client; ++i) {
    inflight_txns[i] = (Inflight_entry *)mem_allocator.alloc(sizeof(Inflight_entry));
        inflight_txns[i]->init();
    }
}

int32_t Client_txn::inc_inflight(uint32_t node_id) {
    assert(node_id < g_servers_per_client);
    return inflight_txns[node_id]->inc_inflight();
}

int32_t Client_txn::dec_inflight(uint32_t node_id) {
    assert(node_id < g_servers_per_client);
    return inflight_txns[node_id]->dec_inflight();
}

int32_t Client_txn::get_inflight(uint32_t node_id) {
    assert(node_id < g_servers_per_client);
    return inflight_txns[node_id]->get_inflight();
}
