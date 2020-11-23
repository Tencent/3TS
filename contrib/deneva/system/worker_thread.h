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

#ifndef _WORKERTHREAD_H_
#define _WORKERTHREAD_H_

#include "global.h"
#include "thread.h"
class Workload;
class Message;

class WorkerThread : public Thread {
public:
    RC run();
    void setup();
    void process(Message * msg);
    void fakeprocess(Message * msg);
    void check_if_done(RC rc);
    void release_txn_man();
    void commit();
    void abort();
    TxnManager * get_transaction_manager(Message * msg);
    void calvin_wrapup();
    RC process_rfin(Message * msg);
    RC process_rfwd(Message * msg);
    RC process_rack_rfin(Message * msg);
    RC process_rack_prep(Message * msg);
    RC process_rqry_rsp(Message * msg);
    RC process_rqry(Message * msg);
    RC process_rqry_cont(Message * msg);
    RC process_rinit(Message * msg);
    RC process_rprepare(Message * msg);
    RC process_rpass(Message * msg);
    RC process_rtxn(Message * msg);
    RC process_calvin_rtxn(Message * msg);
    RC process_rtxn_cont(Message * msg);
    RC process_log_msg(Message * msg);
    RC process_log_msg_rsp(Message * msg);
    RC process_log_flushed(Message * msg);
    RC init_phase();
    uint64_t get_next_txn_id();
    bool is_cc_new_timestamp();
    bool is_mine(Message* msg);
private:
    uint64_t _thd_txn_id;
    ts_t        _curr_ts;
    ts_t        get_next_ts();
    TxnManager * txn_man;
};

class WorkerNumThread : public Thread {
public:
    RC run();
    void setup();

};
#endif
