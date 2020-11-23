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

#ifndef _CLIENT_TXN_H_
#define _CLIENT_TXN_H_

#include "global.h"
#include "semaphore.h"

class Inflight_entry {
public:
    void init();
    int32_t inc_inflight();
    int32_t dec_inflight();
    int32_t get_inflight();
private:
    volatile int32_t num_inflight_txns;
    sem_t mutex;
};

class Client_txn {
public:
    void init();
    int32_t inc_inflight(uint32_t node_id);
    int32_t dec_inflight(uint32_t node_id);
    int32_t get_inflight(uint32_t node_id);
private:
    Inflight_entry ** inflight_txns;
};

#endif
