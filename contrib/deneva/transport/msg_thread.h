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

#ifndef _MSG_THREAD_H_
#define _MSG_THREAD_H_

#include "global.h"
#include "helper.h"
#include "nn.hpp"

struct mbuf {
    char * buffer;
    uint64_t starttime;
    uint64_t ptr;
    uint64_t cnt;
    bool wait;

    void init(uint64_t dest_id) { buffer = (char *)nn_allocmsg(g_msg_size, 0); }
    void reset(uint64_t dest_id) {

        starttime = 0;
        cnt = 0;
        wait = false;
        ((uint32_t*)buffer)[0] = dest_id;
        ((uint32_t*)buffer)[1] = g_node_id;
        ptr = sizeof(uint32_t) * 3;
    }
    void copy(char * p, uint64_t s) {
        assert(ptr + s <= g_msg_size);
        if (cnt == 0) starttime = get_sys_clock();
        COPY_BUF_SIZE(buffer,p,ptr,s);
    }
    bool fits(uint64_t s) { return (ptr + s) <= g_msg_size; }
    bool ready() {
        if (cnt == 0) return false;
        if ((get_sys_clock() - starttime) >= g_msg_time_limit) return true;
        return false;
    }
};

class MessageThread {
public:
    void init(uint64_t thd_id);
    void run();
    void check_and_send_batches();
    void send_batch(uint64_t dest_node_id);
    void copy_to_buffer(mbuf * sbuf, RemReqType type, BaseQuery * qry);
    uint64_t get_msg_size(RemReqType type, BaseQuery * qry);
    void rack( mbuf * sbuf,BaseQuery * qry);
    void rprepare( mbuf * sbuf,BaseQuery * qry);
    void rfin( mbuf * sbuf,BaseQuery * qry);
    void cl_rsp(mbuf * sbuf, BaseQuery *qry);
    void log_msg(mbuf * sbuf, BaseQuery *qry);
    void log_msg_rsp(mbuf * sbuf, BaseQuery *qry);
    void rinit(mbuf * sbuf,BaseQuery * qry);
    void rqry( mbuf * sbuf, BaseQuery *qry);
    void rfwd( mbuf * sbuf, BaseQuery *qry);
    void rdone( mbuf * sbuf, BaseQuery *qry);
    void rqry_rsp( mbuf * sbuf, BaseQuery *qry);
    void rtxn(mbuf * sbuf, BaseQuery *qry);
    void rtxn_seq(mbuf * sbuf, BaseQuery *qry);
    uint64_t get_thd_id() { return _thd_id;}
private:
    mbuf ** buffer;
    uint64_t buffer_cnt;
    uint64_t _thd_id;

};

#endif
